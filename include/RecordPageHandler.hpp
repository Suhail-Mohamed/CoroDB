#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

#include <mutex>
#include <set>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <memory>
#include <variant>

#include "Iouring.hpp"
#include "Util.hpp"

/*
    +---------------------------------+
    |          Page Header =          |
    |       number of records         |
    +---------------------------------+
    |           Record 0              |
    +---------------------------------+
    |           Record 1.             |
    +---------------------------------+
    |             ....                |
    +---------------------------------+
    |           Record N              |
    +---------------------------------+ 
    |                                 |
    |           FREE SPACE            |
    |              ....               | 
    +---------------------------------+
*/

static constexpr int32_t REC_HEADER_SIZE = sizeof(int32_t); 
const  RecId             PAGE_FILLED = RecId{};

/* response type when fetching records in page */
struct RecordResponse {
  Record       record;
  PageResponse status;
};

/********************************************************************************/

/* note: a RecordPagehandler is pinned for the length of its lifetime 
   this is good and bad, it is good, as, as long as the page is in scope 
   it will not get booted from the buffer pool, it is bad because we may 
   get the buffer pool filled, which could be bad. Do not a keep a reference 
   to many RecordPageHandler use it for the minimum time possible, don't 
   worry about reloading the page many times, if it is in the buffer pool 
   the fetch will be quick */
struct RecordPageHandler { 
  RecordPageHandler()
    : is_undefined_rec_pg{true},
      page_cursor{0},
      num_records{0},
      record_size{0},
      handler_ptr {nullptr}, 
      rw_mutex_ptr{nullptr}
  {};

  RecordPageHandler(Handler* handler);
  ~RecordPageHandler();

  /* these operators are only meant to be used when creating and returning a RecordPageHandler
     not safe to move a RecordPageHandler in use */
  RecordPageHandler(RecordPageHandler&& other) noexcept = default;
  RecordPageHandler& operator=(RecordPageHandler&& other) noexcept = default;

  RecId          add_record   (Record&        record); 
  RecId          delete_record(const int32_t record_num); 
  PageResponse   update_record(const int32_t record_num,
                               Record&        new_record);
  RecordResponse read_record  (const int32_t record_num,
                               const LockOpt  l_opt = LockOpt::Lock);

  const int32_t get_num_records() const 
  { return num_records; }

  const size_t get_record_size() const 
  { return record_size; }
  
  const RecordLayout get_record_layout() const
  { return handler_ptr->page_layout; }

  bool is_full() const 
  { return page_cursor + record_size > PAGE_SIZE; }

  bool is_undefined() const 
  { return is_undefined_rec_pg; }
 
private:
  int32_t read_header() const {
    return *reinterpret_cast<int32_t*>(handler_ptr->page_ptr);
  }
  
  off_t record_num_to_offset(const uint32_t record_num) const {
    return REC_HEADER_SIZE + record_num * record_size;
  }

  void update_num_records() {
    std::memcpy(handler_ptr->page_ptr, &num_records, 
                sizeof(num_records));
  }
 
  void compact_page();
  PageResponse move_record(const uint32_t from_record,
                           const uint32_t to_record);
 
  bool    is_undefined_rec_pg;
  off_t   page_cursor;
  int32_t num_records;
  int32_t record_size;
 
  Handler* handler_ptr;
  std::unique_ptr<std::shared_mutex> rw_mutex_ptr; 
  std::set<uint32_t, std::greater<uint32_t>> tombstones;
};

/*
*/
