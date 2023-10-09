#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <stdexcept>
#include <thread>
#include <unordered_set>

#include "DiskManager.hpp"
#include "FileDescriptor.hpp"
#include "Iouring.hpp"
#include "PageHandler.hpp"
#include "SyncWaiter.hpp"
#include "Util.hpp"

DiskManager& test_dm     = DiskManager::get_instance();
RecordLayout test_layout = {Type::Integer, Type::Integer, {Type::String, 52}, Type::Float};

constexpr int32_t NUM_RECORDS = 63;
constexpr int32_t NUM_PAGES   = 7;

std::string file_path = "../TestFiles/testpage_";
std::vector<Record>         test_records;
std::vector<FileDescriptor> test_pages;

std::atomic<bool> read_no_comp = false;
std::atomic<bool> till_full    = false;
std::atomic<bool> del_r1       = false;
std::atomic<bool> del_r2       = false;
std::atomic<bool> clear_page   = false;

std::random_device rd;
std::mt19937 gen(rd());

/********************************************************************************/

int32_t gen_number(int32_t start, 
                   int32_t end) 
{
  if (start > end) throw std::runtime_error("Error: gen_number start > end");

  std::uniform_int_distribution<> dist(start, end);
  return dist(gen);
}

std::string gen_name() {
  static const std::string firstNames[] = {"Michael", "Omar", "Jerry", "Terrence", "Ken"};
  static const std::string lastNames[]  = {"Smith", "Doe", "Johnson", "Brown", "Davis"};

  std::uniform_int_distribution<> dist(0, 4); 
  return firstNames[dist(gen)] + " " + lastNames[dist(gen)];
}

float gen_salary() {
  std::uniform_real_distribution<> dist(30000.0, 100000.0); 
  return dist(gen);
}

int32_t gen_id() {
  std::uniform_int_distribution<> dist(10000, 99999); 
  return dist(gen);
}

Record gen_random_record(const int32_t id) {
  return Record{id, gen_id(), gen_name(), gen_salary()};
}

/********************************************************************************/

void print_record(const Record& record) {
  std::cout << "[";
  for (const auto& data : record)
  std::visit([](const auto& arg) {
               std::cout << arg << ", ";            
             }, data);
  std::cout << "]\n";
}

void print_page(PageHandler* pg_h) {
  for (int32_t i = 0; i < pg_h->get_num_records(); ++i) {
    auto [rec, stat] = pg_h->read_record(i);
    if (stat == PageResponse::Success)
      print_record(rec);
  }
}

bool records_equal(const Record& r1, 
                   const Record& r2)
{
  return (r1.size() == r2.size() && r1 == r2);
}

/********************************************************************************/

bool test_create_page_write_single_record(Record& record, 
                                          FileDescriptor& create_file) 
{
  PageHandler* pg_h = sync_wait(test_dm.create_page(create_file.fd,
                                                    test_layout)); 
  
  pg_h->add_record(record); 
  auto [rec, resp]  = pg_h->read_record(0);

  sync_wait(test_dm.return_page(pg_h->get_page_id(), pg_h->get_page_type()));
  assert(records_equal(record, rec));
  
  return true;
}

/********************************************************************************/

bool test_create_page_write_many_records(FileDescriptor& create_file,
                                         size_t num_records) 
{
  PageHandler* pg_h = sync_wait(test_dm.create_page(create_file.fd, 
                                                    test_layout)); 
  
  for (int32_t i = 0; i < num_records; ++i)
    if (pg_h->add_record(test_records[i]) != 
      PageResponse::Success)
      break;

  assert(pg_h->get_num_records() == num_records);

  for (int32_t i = 0; i < pg_h->get_num_records(); ++i) {
    auto [rec, _] = pg_h->read_record(i);
    assert(records_equal(rec, test_records[i]));
  }

  sync_wait(test_dm.return_page(pg_h->get_page_id(), pg_h->get_page_type()));
  return true;
}

/********************************************************************************/

bool test_read_existing_page(FileDescriptor& read_file) {
  PageHandler* pg_h = sync_wait(test_dm.read_page(read_file.fd, 
                                                  test_layout)); 
  
  for (int32_t i = 0; i < pg_h->get_num_records(); ++i) {
    auto [rec, _] = pg_h->read_record(i);
    assert(records_equal(rec, test_records[i]));
  }

  return true;
}

/********************************************************************************/

bool test_read_page_no_comparison(FileDescriptor& read_file) {
  PageHandler* pg_h = sync_wait(test_dm.read_page(read_file.fd, 
                                                  test_layout)); 
  read_no_comp = true;
  
  if (!clear_page)
    assert(pg_h->get_num_records() > 0);

  for (int32_t i = 0; i < pg_h->get_num_records(); ++i) {
    auto [rec, status] = pg_h->read_record(i);
    assert(status != PageResponse::PageFull);
  }

  return true;
}

/********************************************************************************/

bool test_add_til_page_full(FileDescriptor& create_file) {
  PageHandler* pg_h = sync_wait(test_dm.create_page(create_file.fd, 
                                                    test_layout));
  for (int i = 0; i < NUM_RECORDS; ++i) {
    if (pg_h->add_record(test_records[i]) != PageResponse::Success ||
        !records_equal(test_records[i], pg_h->read_record(i).record)) 
      assert(false);
  }

  PageResponse resp = pg_h->add_record(test_records[0]);
  sync_wait(test_dm.return_page(pg_h->get_page_id(), pg_h->get_page_type()));

  till_full = true;
  return resp == PageResponse::PageFull;
}

/********************************************************************************/

bool test_read_page_delete_random_records_and_compact(FileDescriptor& file, 
                                                      size_t num_del,
                                                      std::atomic<bool>* flag = nullptr) 
{
  PageHandler* pg_h = sync_wait(test_dm.read_page(file.fd,
                                                  test_layout));
   
  std::unordered_set<int32_t> removed_records;
  int32_t num_records = pg_h->get_num_records();

  assert(num_records > 0);

  for (int32_t _ = 0, rand_record; _ < num_del; ++_) {
    rand_record = gen_number(0, pg_h->get_num_records() - 1);
    
    Record  del_rec = pg_h->read_record(rand_record).record;
    int32_t rec_id  = std::get<int32_t>(del_rec[0]);

    PageResponse del_resp = pg_h->delete_record(rand_record);
    removed_records.insert(rec_id);
    
    if (del_resp != PageResponse::Success)
      break;
  
    for (int32_t i = 0 ; i < pg_h->get_num_records(); ++i) {
      Record rec = pg_h->read_record(i).record;
      int32_t rec_id  = std::get<int32_t>(rec[0]);
      assert(!removed_records.count(rec_id));
    }
  }

  if (flag) *flag = true;

  assert(pg_h->get_num_records() == num_records - num_del);
  return true;
}

/********************************************************************************/

bool test_read_page_delete_record_add_record(FileDescriptor& read_file, 
                                             int32_t record_num) 
{
  PageHandler* pg_h = sync_wait(test_dm.read_page(read_file.fd, 
                                                  test_layout)); 
  
  int32_t old_num_records = pg_h->get_num_records();
  pg_h->delete_record(record_num);

  pg_h->add_record(test_records[0]);
  int32_t last_record = pg_h->get_num_records() - 1;
  bool test_return = 
    records_equal(pg_h->read_record(last_record).record, test_records[0]);
  
  assert(test_return);

  sync_wait(test_dm.return_page(pg_h->get_page_id(), pg_h->get_page_type()));
  return test_return;
}

/********************************************************************************/

bool test_clear_page(FileDescriptor& read_file) {
  PageHandler* pg_h = sync_wait(test_dm.read_page(read_file.fd, 
                                                  test_layout)); 
  int32_t num_records = pg_h->get_num_records();
  
  for (int32_t rec = 0; pg_h->delete_record(0) != PageResponse::PageEmpty; ++rec);
  clear_page = true;

  assert(pg_h->get_num_records() == 0);
  sync_wait(test_dm.return_page(pg_h->get_page_id(), pg_h->get_page_type())); 
  return true;
}

/********************************************************************************/

enum TestPages {
  CreatePage = 0,
  CreateWriteManyPage,
  AddTillFullPage,
  FillAndDeletePage
};

enum TestPagesMT {
  CreatePageMT = 4,
  CreateWriteManyPageMT,
  AddTillFullPageMT,
  FillAndDeletePageMT
};

const int32_t num_record_to_add      = 20;
const int32_t record_num             = 10;
const int32_t first_round_deletions  = 20;
const int32_t second_round_deletions = 25; 

/********************************************************************************/

void sync_test() {
  std::cout << "\nSync Tests:\n";

  std::cout << "*******************************************\n";
  std::cout << "TEST: test_create_page_write_single_record(test_records[0], test_pages[0])\n"; 
  assert(test_create_page_write_single_record(test_records[0], 
                                              test_pages[TestPages::CreatePage]));

  std::cout << "*******************************************\n";
  std::cout << "TEST: test_create_page_write_many_records(test_pages[1], 20)\n"; 
  assert(test_create_page_write_many_records(test_pages[TestPages::CreateWriteManyPage],
                                             num_record_to_add));

  std::cout << "*******************************************\n";
  std::cout << "TEST: test_read_existing_page(test_pages[1])\n"; 
  assert(test_read_existing_page(test_pages[TestPages::CreateWriteManyPage]));

  std::cout << "*******************************************\n";
  std::cout << "TEST: test_add_til_page_full(test_pages[2])\n";
  assert(test_add_til_page_full(test_pages[TestPages::AddTillFullPage]));

  std::cout << "*******************************************\n";
  std::cout << "TEST: test_read_page_delete_random_records_and_compact(test_pages[2], first_round_deletions)\n";
  assert(test_read_page_delete_random_records_and_compact(test_pages[TestPages::AddTillFullPage], 
                                                          first_round_deletions));
  
  std::cout << "*******************************************\n";
  std::cout << "TEST: test_read_page_delete_random_records_and_compact(test_pages[2], second_round_deletions)\n";
  assert(test_read_page_delete_random_records_and_compact(test_pages[TestPages::AddTillFullPage], 
                                                          second_round_deletions));


  std::cout << "*******************************************\n";
  std::cout << "TEST: test_read_page_delete_record_add_record(test_pages[2], 10)\n";
  assert(test_read_page_delete_record_add_record(test_pages[TestPages::AddTillFullPage], 
                                                 record_num)); 

  std::cout << "*******************************************\n";
  std::cout << "TEST: test_add_til_full(test_pages[3])\n";
  assert(test_add_til_page_full(test_pages[TestPages::FillAndDeletePage]));
  std::cout << "TEST: test_clear_page(test_pages[3])\n";
  assert(test_clear_page(test_pages[TestPages::FillAndDeletePage]));
}

/********************************************************************************/

void multithreaded_test() {
  std::cout << "\nMultithreaded Test (No prints, they are confusing):\n";
 
  auto t1 = std::jthread{[]() {
    assert(test_create_page_write_single_record(test_records[0], 
                                                test_pages[TestPagesMT::CreatePageMT]));
  }};
  
  auto t2 = std::jthread{[] {
    assert(test_create_page_write_many_records(test_pages[TestPagesMT::CreateWriteManyPageMT], 
                                               num_record_to_add));
    
    assert(test_read_existing_page(test_pages[TestPagesMT::CreateWriteManyPageMT]));
  }};
  
  auto t3 = std::jthread{[]() {
    assert(test_add_til_page_full(test_pages[TestPagesMT::AddTillFullPageMT]));
    
    assert(test_read_page_delete_random_records_and_compact(test_pages[TestPagesMT::AddTillFullPageMT], 
                                                            first_round_deletions,
                                                            &del_r1)); 
    
    assert(test_read_page_delete_random_records_and_compact(test_pages[TestPagesMT::AddTillFullPageMT], 
                                                            second_round_deletions,
                                                            &del_r2));
    
    assert(test_read_page_delete_record_add_record(test_pages[TestPagesMT::AddTillFullPageMT], 
                                                   record_num)); 
     
    assert(test_clear_page(test_pages[TestPagesMT::AddTillFullPageMT]));
  }};
  
  
  auto t4 = std::jthread{[] {
    std::this_thread::sleep_for(std::chrono::milliseconds(3));
    assert(test_read_page_no_comparison(test_pages[TestPagesMT::AddTillFullPageMT]));
    assert(test_read_existing_page(test_pages[TestPagesMT::CreatePageMT]));
    assert(test_read_existing_page(test_pages[TestPagesMT::CreateWriteManyPageMT]));
  }};
}

/********************************************************************************/

int main() {
  for (int32_t i = 0; i < NUM_PAGES; ++i)
    test_pages.emplace_back(file_path + std::to_string(i), 
                            OpenMode::Create);

  for (int32_t i = 0; i < NUM_RECORDS; ++i)
    test_records.push_back(gen_random_record(i));

  sync_test();
  multithreaded_test();
  
  std::cout << "\nAll Tests Passed!\n";
}

