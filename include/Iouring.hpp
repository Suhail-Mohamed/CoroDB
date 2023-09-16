#pragma once

#include <cstdint>
#include <liburing.h>
#include <liburing/io_uring.h>

#include <coroutine>
#include <cstring>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <span>
#include <vector>

enum class IOP {
  Read, 
  Write, 
  NullOp
};

enum PageType {
  IO, 
  NonPersistent, 
  NumPageTypes 
};

/* constants used in bufferpool & io_uring */
constexpr size_t   QUEUE_SIZE     = 1024; /* size of submission and completion queues */
constexpr int32_t  PAGE_SIZE      = 4096;
constexpr uint32_t TOTAL_PAGES    = 1280;
constexpr uint32_t BUFF_RING_SIZE = 1024; /* size of buffer ring we register, must be power of two */ 
constexpr uint32_t PAGE_POOL_SIZE = TOTAL_PAGES - BUFF_RING_SIZE;
constexpr uint16_t BGID           = 0;    /* Buffer group id where all our buffers live */

using Page = std::array<uint8_t, PAGE_SIZE>;

/********************************************************************************/

/* used for facilitating read/write requests. The handle is used to resume a coroutine when the 
   I/O request is completed */
struct SqeData {
  int32_t status_code = -1;         
  int32_t buff_id     = -1; /* to determine what buffer read data is in */
  int32_t fd          = -1;
  off_t   offset      = 0;
  IOP	  iop	      = IOP::NullOp;
  Page*   page_data   = nullptr;		   
  std::coroutine_handle<> handle;
};

/********************************************************************************/

struct Iouring {
  /* making non-copyable and non-moveable 1 instance only */
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

  bool is_cqe_empty() {
    io_uring_cqe* cqe;
    return io_uring_peek_cqe(&ring, &cqe) != 0;
  }

  int32_t num_submission_queue_entries() { return io_uring_sq_ready(&ring); }

  /* submits all SQE to CQ */
  int32_t submit() { return io_uring_submit(&ring); }

  uint32_t submit_and_wait(const uint32_t wait_nr);
  void	   for_each_cqe(const std::function<void(io_uring_cqe*)>& lambda);

  void read_request (SqeData& sqe_data);
  void write_request(SqeData& sqe_data);

  /* register a list of buffers (buff_lst) to a buffer ring (buff_ring) */
  void register_buffer_ring(io_uring_buf_ring*		      buff_ring, 
			    std::array<Page, BUFF_RING_SIZE>& buff_lst);

  /* adds buffer backed to register buffer_ring for re-use */
  void add_buffer(io_uring_buf_ring* buff_ring, 
		  Page&		     buff,
		  const uint32_t     buff_id);
private:
  struct io_uring ring;

  Iouring() {
    if (auto err = io_uring_queue_init(QUEUE_SIZE, &ring, 0);
        err < 0)
      throw std::runtime_error("Error: initializing io_uring, " + 
                               std::to_string(err));
  }
};
