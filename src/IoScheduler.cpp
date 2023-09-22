#include "IoScheduler.hpp"

void IoScheduler::start_scheduler() {
  io_thread = std::jthread {[this]() {
    this->io_loop();
  }};
}

/********************************************************************************/

void IoScheduler::process_cqe() {
  Iouring& io_uring = Iouring::get_instance();
  
  io_uring.submit_and_wait(1);
  io_uring.for_each_cqe([&io_uring](io_uring_cqe* cqe) {
    SqeData* sqe_data = 
      static_cast<SqeData*>(io_uring_cqe_get_data(cqe));
     
    sqe_data->status_code = cqe->res;
    if (sqe_data->iop == IOP::Read)
      sqe_data->buff_id = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
    
    sqe_data->handle.resume();
    io_uring.cqe_seen(cqe);
  });
}

/********************************************************************************/

void IoScheduler::io_loop() {
  Iouring& io_uring = Iouring::get_instance();
  
  while (!io_stop_src.stop_requested()) {
    if (io_uring.num_submission_queue_entries() > 0)
      process_cqe();

    std::unique_lock lock(queue_mutex);
    queue_notify.wait(lock, [&] { 
      return io_stop_src.stop_requested() || 
             !coro_queue.empty(); 
    });

    const auto coroutine = coro_queue.front();
    coro_queue.pop();
    coroutine.resume();
  }
}

