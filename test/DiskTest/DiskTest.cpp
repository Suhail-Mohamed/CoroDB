#include <cassert>
#include <filesystem>
#include <iostream>
#include <random>
#include <stdexcept>
#include <thread>
#include <unordered_set>

#include "DiskManager.hpp"
#include "FileDescriptor.hpp"
#include "PageHandler.hpp"
#include "SyncWaiter.hpp"
#include "Util.hpp"

DiskManager& test_dm     = DiskManager::get_instance();
RecordLayout test_layout = {Type::Integer, Type::Integer, {Type::String, 52}, Type::Float};

constexpr int32_t NUM_RECORDS = 63;
constexpr int32_t NUM_PAGES   = 5;

std::string file_path = "../TestFiles/testpage_";
std::vector<Record>         test_records;
std::vector<FileDescriptor> test_pages;

std::random_device rd;
std::mt19937 gen(rd());

/********************************************************************************/

int32_t gen_number(int32_t start, int32_t end) {
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

bool records_equal(const Record& r1, 
                   const Record& r2)
{
    return (r1.size() == r2.size() && r1 == r2);
}

/********************************************************************************/

bool test_create_page_write_single_record(Record& record, FileDescriptor& create_file) {
    PageHandler* pg_h = test_dm.create_page(create_file.fd, 
                                            test_layout); 
    pg_h->add_record(record); 
    auto [rec, resp]  = pg_h->read_record(0);
    
    sync_wait(test_dm.return_page(pg_h->page_id, pg_h->page_type));
    return records_equal(record, rec);
}

/********************************************************************************/

bool test_create_page_write_many_records(FileDescriptor& create_file, size_t num_records) {
    PageHandler* pg_h = test_dm.create_page(create_file.fd, 
                                            test_layout); 
    
    for (int32_t i = 0; i < num_records; ++i)
        if (pg_h->add_record(test_records[i]) != 
            PageResponse::Success)
            break;
    
    if (pg_h->num_records != num_records) return false;

    for (int32_t i = 0; i < pg_h->num_records; ++i) {
        auto [rec, _] = pg_h->read_record(i);
        
        if (!records_equal(rec, test_records[i]))
            return false;
    }

    sync_wait(test_dm.return_page(pg_h->page_id, pg_h->page_type));
    return true;
}

/********************************************************************************/

bool test_read_existing_page(FileDescriptor& read_file) {
    PageHandler* pg_h = sync_wait(test_dm.read_page(read_file.fd, 
                                                    test_layout)); 
    
    for (int32_t i = 0; i < pg_h->num_records; ++i) {
        auto [rec, _] = pg_h->read_record(i);
        if (!records_equal(rec, test_records[i]))
            return false;
    }

    return true;
}

/********************************************************************************/

bool test_add_til_page_full(FileDescriptor& create_file) {
    PageHandler* pg_h = test_dm.create_page(create_file.fd, 
                                            test_layout);
    
    for (int i = 0; i < NUM_RECORDS; ++i) {
        if (pg_h->add_record(test_records[i]) != PageResponse::Success ||
            !records_equal(test_records[i], pg_h->read_record(i).record)) 
            return false;
    }

    PageResponse resp = pg_h->add_record(test_records[0]);
    sync_wait(test_dm.return_page(pg_h->page_id, pg_h->page_type));
    
    return resp == PageResponse::PageFull;
}

/********************************************************************************/

bool test_read_page_delete_random_records_and_compact(FileDescriptor& file, size_t num_del) {
    PageHandler* pg_h = sync_wait(test_dm.read_page(file.fd, 
                                                    test_layout));
    std::vector<int32_t> kept_records;
    std::vector<int32_t> compact_records; 
    
    std::unordered_set<int32_t> removed_records;
    
    for (int32_t _ = 0, rand_record; _ < num_del; ++_) {
        do {
            rand_record = gen_number(0, pg_h->num_records - 1);
        } while (removed_records.count(rand_record));
        
        removed_records.insert(rand_record);
        PageResponse del_resp = pg_h->delete_record(rand_record);
        
        if (del_resp == PageResponse::PageEmpty)
            break;
        
        if (rand_record == pg_h->num_records) {
            if (auto read = pg_h->read_record(rand_record);
                read.status != PageResponse::InvalidRecord)
                std::abort();
            continue;
        } 

        if (auto read = pg_h->read_record(rand_record);
            read.status != PageResponse::DeletedRecord)
            std::abort();
    }

    for (int32_t i = 0; i < pg_h->num_records; ++i) {
        auto [rec, read_resp] = pg_h->read_record(i);
        
        if (read_resp == PageResponse::DeletedRecord)
            continue;

        if (read_resp != PageResponse::Success)
            std::abort();

        int32_t record_id = std::get<int32_t>(rec[0]); 
        kept_records.push_back(record_id);
    }

    if (auto compact_resp = pg_h->compact_page();
        compact_resp != PageResponse::Success)
        std::abort();

    for (int32_t i = 0; i < pg_h->num_records; ++i) {
        auto [rec, read_resp] = pg_h->read_record(i);
        if (read_resp != PageResponse::Success)
            std::abort();
       
        int32_t record_id = std::get<int32_t>(rec[0]); 
        compact_records.push_back(record_id);
    }
   
    std::sort(std::begin(kept_records), std::end(kept_records));
    std::sort(std::begin(compact_records), std::end(compact_records));
   
    assert(kept_records == compact_records);
    return true; 
}

/********************************************************************************/

bool test_read_page_delete_record_add_record(FileDescriptor& read_file, size_t record_num) {
    PageHandler* pg_h = sync_wait(test_dm.read_page(read_file.fd, 
                                                    test_layout)); 
    int32_t old_num_records = pg_h->num_records;
    pg_h->delete_record(record_num);

    pg_h->add_record(test_records[0]);
    bool test_return = 
        records_equal(pg_h->read_record(record_num).record, test_records[0]);

    sync_wait(test_dm.return_page(pg_h->page_id, pg_h->page_type));
    return test_return;
}

/********************************************************************************/

int main() {
    int32_t first_round_deletions  = 20;
    int32_t second_round_deletions = 40; 

    std::jthread io_thread {[](std::stop_token st) {
        while (!st.stop_requested()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            test_dm.handle_io();
        }
    }};
    
    for (int32_t i = 0; i < NUM_PAGES; ++i)
        test_pages.emplace_back(file_path + std::to_string(i), 
                                OpenMode::Create);

    for (int32_t i = 0; i < NUM_RECORDS; ++i)
        test_records.push_back(gen_random_record(i));

    std::cout << "*******************************************\n";
    std::cout << "TEST: test_create_page_write_single_record(test_records[0], test_pages[0])\n"; 
    assert(test_create_page_write_single_record(test_records[0], test_pages[0]));

    std::cout << "*******************************************\n";
    std::cout << "TEST: test_create_page_write_many_records(test_pages[1], 20)\n"; 
    assert(test_create_page_write_many_records(test_pages[1], 20));


    std::cout << "*******************************************\n";
    std::cout << "TEST: test_read_existing_page(test_pages[1])\n"; 
    assert(test_read_existing_page(test_pages[1]));

    std::cout << "*******************************************\n";
    std::cout << "TEST: test_add_til_page_full(test_pages[2])\n";
    assert(test_add_til_page_full(test_pages[2]));

    std::cout << "*******************************************\n";
    std::cout << "TEST: test_read_page_delete_random_records_and_compact(test_pages[2], 20)\n";
    assert(test_read_page_delete_random_records_and_compact(test_pages[2], first_round_deletions));

    std::cout << "*******************************************\n";
    std::cout << "TEST: test_read_page_delete_record_add_record(test_pages[2], 10)\n";
    assert(test_read_page_delete_record_add_record(test_pages[2], 10)); 
    
    std::cout << "*******************************************\n";
    std::cout << "TEST: test_read_page_delete_random_records_and_compact(test_pages[2], 40)\n";
    assert(test_read_page_delete_random_records_and_compact(test_pages[2], second_round_deletions));

    std::cout << "\nAll Tests Passed!\n";
}
