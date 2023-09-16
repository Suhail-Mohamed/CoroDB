#include "CoroPool.hpp"

CoroPool::CoroPool(size_t nr_workers) {
  for (size_t i = 0; i < nr_workers; ++i)
    workers.emplace_back([&] {
      thread_loop();
    });
}

/********************************************************************************/

CoroPool::~CoroPool() {
  stop_src.request_stop();
  queue_notify.notify_all();
}

/********************************************************************************/
