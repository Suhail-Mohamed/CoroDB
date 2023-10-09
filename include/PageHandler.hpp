#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <variant>

#include "Iouring.hpp"
#include "Util.hpp"

/*
    +---------------------------------+
    |            Page Header =        |
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

enum class PageResponse {
  PageFull,
  PageEmpty,
  InvalidOffset,
  InvalidRecord,
  DeletedRecord,
  Success
};

enum class LockOpt { 
  Lock, 
  DontLock
};

struct RecordResponse {
  Record       record;
  PageResponse status;
};

/********************************************************************************/

struct DiskManager;

template <size_t N>
struct PageBundle;

struct PageHandler {
  friend class DiskManager;
  
  template <size_t N>
  friend class PageBundle;

  void init_handler(Page*              page,
                    const RecordLayout layout,
                    const int32_t      p_id,
                    const int32_t      fd,
                    const PageType     p_type);

  PageResponse   add_record   (Record& record);
  PageResponse   delete_record(const uint32_t record_num);
  RecordResponse read_record  (const uint32_t record_num,
                               const LockOpt l_opt = LockOpt::Lock);

  const int32_t get_num_records() const {
    return num_records;
  }

  const int32_t get_page_id() const {
    return page_id;
  }

  const size_t get_record_size() const {
    return record_size;
  }

  const PageType get_page_type() const {
    return page_type;
  }

  RecordLayout record_layout;

private: 
  void prep_for_disk() {
    update_num_records();
  }

  int32_t read_header() {
    return *reinterpret_cast<int32_t*>(page_ptr);
  }
  
  off_t record_num_to_offset(const uint32_t record_num) {
    return HEADER_SIZE + record_num * record_size;
  }

  void update_num_records() {
    std::memcpy(page_ptr, &num_records, 
                sizeof(num_records));
  }

  PageResponse move_record(const uint32_t from_record,
                           const uint32_t to_record);

  PageResponse write_to_page (off_t&      write_offset, 
                              RecordData& record_data, 
                              const DatabaseType& db_type);
  size_t       read_from_page(const off_t read_offset, 
                              RecordData& record_data, 
                              const DatabaseType& db_type);
  
  bool is_pinned = false;
  bool is_dirty  = false;

  int32_t page_id     = 0;
  int32_t page_ref    = 0;
  int32_t page_fd     = -1;
  int32_t num_records = 0;
  
  PageType page_type;
  size_t   record_size = 0;	
  off_t	   page_cursor = 0;
  Page*	   page_ptr    = nullptr;
  
  std::shared_mutex    rw_mutex; 
  std::atomic<int32_t> page_usage  = 0;
};
