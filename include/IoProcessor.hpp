#pragma once 

#include <stop_token>
#include <thread>

#include "CoroPool.hpp"
#include "Iouring.hpp"

/* All this struct does is creates a thread which constantly submits 
   IO requests from the submission queue to the completion queue of
   io_uring and for each element in the completion queue it enqueues 
   the corresponding coroutine into the coro_pool so it can resumed by 
   a thread */
struct IoProcessor {
  IoProcessor() {
    io_thread = std::jthread {
      [this]() { this->io_loop(); }
    };
  }

  ~IoProcessor() 
  { io_stop_src.request_stop(); }

private: 
  /* read elements off the io_uring completion queue and 
     add their coroutines to the coroutine pool */
  void process_cqe() {
    CoroPool& coro_pool = CoroPool::get_instance();
    Iouring&  io_uring  = Iouring::get_instance();
  
    io_uring.for_each_cqe([&io_uring, &coro_pool](io_uring_cqe* cqe) {
      SqeData* sqe_data = 
        static_cast<SqeData*>(io_uring_cqe_get_data(cqe));
       
      sqe_data->status_code = cqe->res;
      if (sqe_data->iop == IOP::Read)
        sqe_data->buff_id = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
      
      io_uring.cqe_seen(cqe);
      /* add coroutine to coro_pool to be resumed by a thread later */
      coro_pool.enqueue(sqe_data->coroutine);
    });
  }
  
  /* submits all IO requests in submission queue if there are any,
     as well as processes any completion queue entries if there are any */
  void io_loop() {
    Iouring& io_uring = Iouring::get_instance();
    
    while (!io_stop_src.stop_requested()) {
      std::lock_guard<std::mutex> lock{io_uring.ring_mutex};
      
      if (io_uring.num_submission_queue_entries() > 0)
        io_uring.submit();
      
      if (!io_uring.cqe_empty())
        process_cqe();
    }
  }

  std::stop_source io_stop_src;
  std::jthread     io_thread;
};

