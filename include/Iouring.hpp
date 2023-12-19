#pragma once

#include "Util.hpp"

#include <cassert>
#include <cstdint>
#include <liburing.h>
#include <liburing/io_uring.h>

#include <coroutine>
#include <cstring>
#include <functional>
#include <iostream>
#include <shared_mutex>
#include <stdexcept>
#include <span>
#include <vector>

/********************************************************************************/
/* enums and types that are used to create the wrapper Handler, which other more 
   specific handlers build off of: PageHandler, IndexHandler */

enum class IOP {
  Read, 
  Write, 
  NullOp
};

enum class PageResponse {
  PageFull,
  PageEmpty,
  InvalidOffset,
  InvalidRecord,
  InvalidKey,
  InvalidRid,
  DeletedRecord,
  InvalidTimestamp,
  Failure,
  Success
};

enum class LockOpt { 
  Lock, 
  DontLock
};

enum PageType {
  IO, 
  NonPersistent, 
  NumPageTypes 
};

constexpr int32_t PAGE_SIZE         = 4096; 
constexpr int32_t DEFAULT_TIMESTAMP = -1;
using Page = std::array<uint8_t, PAGE_SIZE>;

/* RAII pin guard for pinning pages */
struct PinGuard {
  PinGuard(std::atomic<bool>& page_pin)
    : pin{page_pin} 
  { pin = true; }

  ~PinGuard() 
  { pin = false; }
  
  std::atomic<bool>& pin;
};

/********************************************************************************/
/* Handler struct: This struct is what is returned from a call to create or read page 
   in the DiskManager, it is a handler to a page meaning it provides some utility functions
   that make reading and writing records to the page simpler. Addtionally it contains 
   atomics which hold whether the page is dirty and whether the page is pinned or not */
struct Handler {
  Handler() = default;
  void init_handler(Page*              page,
                    const RecordLayout layout,
                    const int32_t      timestamp,
                    const int32_t      pg_id,
                    const int32_t      pg_num,
                    const int32_t      pg_fd,
                    const PageType     pg_type)
  {
	assert(page);
	page_ptr       = page;
	page_layout    = layout;
	page_timestamp = timestamp;
	page_id        = pg_id;
	page_num       = pg_num;
	page_fd        = pg_fd;
	page_type      = pg_type;
	page_usage     = 1;
	page_ref       = 1;
	is_dirty       = false;
	is_pinned      = false;
  }
 
  /* ensure you have dealt with conccurrent accesses before calling,
     check to make sure read_offset is valid, function does no checks */
  void get_record(off_t read_offset, 
                  const RecordLayout& layout,
                  Record& ret_record)
  {
	for (size_t i = 0; i < layout.size(); ++i)
	  read_offset += 
		read_from_page(read_offset, ret_record[i], layout[i]);
  }

  /* ensure you have dealt with conccurrent accesses before calling,
     takes offset as reference so will move it in function */
  PageResponse set_record(off_t& write_offset,
                          const RecordLayout& layout,
                          Record& write_record)
  {
	for (size_t i = 0; i < layout.size(); ++i)
      if (write_to_page(write_offset, write_record[i], layout[i]) ==
          PageResponse::InvalidRecord)
        return PageResponse::InvalidRecord;
	
	return PageResponse::Success;
  }
  
  bool is_valid_timestamp(const int32_t timestamp) {
	return timestamp == DEFAULT_TIMESTAMP || 
           timestamp == page_timestamp;
  }
  
  PageResponse write_to_page (off_t&      write_offset, 
                              RecordData& record_data, 
                              const DatabaseType& db_type);
  size_t       read_from_page(const off_t read_offset, 
                              RecordData& record_data, 
                              const DatabaseType& db_type);
  
  std::atomic<bool>    is_dirty   = false;
  std::atomic<bool>    is_pinned  = false;
  std::atomic<int32_t> page_usage = 0;

  int32_t  page_timestamp;
  int32_t  page_fd  = -1;
  int32_t  page_num = -1;
  int32_t  page_id  = -1;
  int32_t  page_ref = 0;
  PageType page_type;

  Page*        page_ptr = nullptr;
  RecordLayout page_layout;
};

/********************************************************************************/

struct RecId {
  RecId() 
    : page_num{-1}, 
      slot_num{-1} 
  {};
  
  RecId(int32_t pg_number, int32_t slot_number)
    : page_num{pg_number}, 
      slot_num{slot_number} {};

  bool operator==(const RecId& other) const {
    return other.page_num == page_num && 
           other.slot_num == slot_num;
  }

  bool operator!=(const RecId& other) const 
  { return !(*this == other); }

  int32_t page_num;
  int32_t slot_num;
};

/********************************************************************************/
/* constants used in bufferpool & io_uring */
constexpr size_t   QUEUE_SIZE     = 1024; /* size of submission and completion queues */
constexpr uint32_t TOTAL_PAGES    = 640;
constexpr uint32_t BUFF_RING_SIZE = 512;  /* size of buffer ring we register, must be power of two */ 
constexpr uint32_t PAGE_POOL_SIZE = TOTAL_PAGES - BUFF_RING_SIZE;
constexpr uint16_t BGID           = 0;    /* Buffer group id where all our buffers live */

/* used for facilitating read/write requests. The handle is used to resume a coroutine when the 
   I/O request is completed */
struct SqeData {
  int32_t status_code = -1;         
  int32_t buff_id     = -1; /* to determine what buffer read data is in */
  int32_t fd          = -1;
  off_t   offset      = 0;
  IOP	  iop	      = IOP::NullOp;
  Page*	  page_data   = nullptr;
  std::coroutine_handle<> coroutine;
};

/********************************************************************************/

struct Iouring {
  Iouring(const Iouring &)	      = delete;
  Iouring& operator=(const Iouring &) = delete;
  Iouring(Iouring &&)		      = delete;
  Iouring& operator=(Iouring &&)      = delete;
  ~Iouring() { io_uring_queue_exit(&ring); }

  static Iouring& get_instance() {
    static Iouring instance;
    return instance;
  }

  void cqe_seen(io_uring_cqe* cqe) { io_uring_cqe_seen(&ring, cqe); }	

  /* returns true if the completion queue is empty */
  bool cqe_empty() {
    io_uring_cqe* cqe;
    io_uring_peek_cqe(&ring, &cqe);
    return (cqe == nullptr);
  }

  int32_t num_submission_queue_entries() { return io_uring_sq_ready(&ring); }

  /* submits all SQE to CQ */
  int32_t submit() { return io_uring_submit(&ring); }

  uint32_t submit_and_wait(const uint32_t wait_nr);
  void	   for_each_cqe(const std::function<void(io_uring_cqe*)>& lambda);

  /* add a sqe to the submission queue, these functions are thread safe so 
     multiple threads can use this function safely */
  void read_request (SqeData& sqe_data);
  void write_request(SqeData& sqe_data);

  /* register a list of buffers (buff_lst) to a buffer ring (buff_ring) */
  void register_buffer_ring(io_uring_buf_ring*		          buff_ring, 
                            std::array<Page, BUFF_RING_SIZE>& buff_lst);

  /* adds buffer back to register buffer_ring for re-use */
  void add_buffer(io_uring_buf_ring* buff_ring, 
                  Page&              buff,
                  const uint32_t     buff_id);
  
  
  std::mutex ring_mutex;

private:
  Iouring() {
    if (auto err = io_uring_queue_init(QUEUE_SIZE, &ring, 0);
        err < 0)
      throw std::runtime_error("Error: initializing io_uring, " + 
                               std::to_string(err));
  }
  
  struct io_uring ring;
};
