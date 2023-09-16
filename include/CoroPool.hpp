#pragma once 

#include <condition_variable>
#include <coroutine>
#include <memory>
#include <mutex>
#include <stop_token>
#include <thread>
#include <queue>
#include <vector>

struct CoroPool {
  /* getting rid of some undefined functionality */
  CoroPool(CoroPool&&)                 = delete;
  CoroPool(const CoroPool&)            = delete;
  CoroPool& operator=(CoroPool&&)      = delete;
  CoroPool& operator=(const CoroPool&) = delete;
  
  CoroPool(size_t nr_workers);
  ~CoroPool();

private:
  std::vector<std::jthread> workers;
  std::mutex                queue_mutex;
  std::condition_variable   queue_notify;
  std::stop_source          stop_src;  

  void thread_loop();
}; 
