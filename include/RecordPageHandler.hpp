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

static constexpr int32_t HEADER_SIZE = sizeof(int32_t); 

/********************************************************************************/

struct RecordPageHandler { 
  RecordPageHandler() 
    : handler_ptr {nullptr}, 
      rw_mutex_ptr{nullptr} 
  {};

  RecordPageHandler(Handler* handler);
  ~RecordPageHandler();

  /* these operators are only meant to be used when creating and returning a RecordPageHandler
     not safe to move a RecordPageHandler in use */
  RecordPageHandler(RecordPageHandler&& other) noexcept = default;
  RecordPageHandler& operator=(RecordPageHandler&& other) noexcept = default;

  PageResponse   add_record   (Record&        record); 
  PageResponse   delete_record(const uint32_t record_num); 
  PageResponse   update_record(const uint32_t record_num,
                               Record&        new_record);
  RecordResponse read_record  (const uint32_t record_num,
                               const LockOpt  l_opt = LockOpt::Lock);

  const int32_t get_num_records() const 
  { return num_records; }

  const size_t get_record_size() const 
  { return record_size; }
  
  const RecordLayout get_record_layout() const
  { return handler_ptr->page_layout; }

  bool is_full() const {
    return page_cursor + record_size > PAGE_SIZE;
  }

private:
  int32_t read_header() const {
    return *reinterpret_cast<int32_t*>(handler_ptr->page_ptr);
  }
  
  off_t record_num_to_offset(const uint32_t record_num) const {
    return HEADER_SIZE + record_num * record_size;
  }

  void update_num_records() {
    std::memcpy(handler_ptr->page_ptr, &num_records, 
                sizeof(num_records));
  }

  void compact_page();
  PageResponse move_record(const uint32_t from_record,
                           const uint32_t to_record);
 
  off_t	  page_cursor = 0;
  int32_t timestamp   = -1; 
  int32_t num_records = 0;
  int32_t record_size = 0;
 
  Handler* handler_ptr;
  std::unique_ptr<std::shared_mutex> rw_mutex_ptr; 
  std::set<uint32_t, std::greater<uint32_t>> tombstones;
};
