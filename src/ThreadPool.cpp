#include "ThreadPool.hpp"

ThreadPool::ThreadPool(size_t nr_workers) {
  for (size_t i = 0; i < nr_workers; ++i)
    workers.emplace_back([this]() {
      this->thread_loop();                       
    });
}

/********************************************************************************/

ThreadPool::~ThreadPool() {
  stop_src.request_stop();
  queue_notify.notify_all();
}

/********************************************************************************/

void ThreadPool::thread_loop() {
  while (!stop_src.stop_requested()) {
    std::function<void()> task;

    /* make thread wait until something is in the queue, when there
       is assign 'task' to it and run it in the thread */
    {
      std::unique_lock<std::mutex> lock(queue_mutex);
      queue_notify.wait(lock, [this]() {
        return stop_src.stop_requested() || !task_queue.empty(); 
      });

      if (stop_src.stop_requested() && task_queue.empty())
        break;
      
      if (task_queue.empty()) continue;
      
      task = std::move(task_queue.front());
      task_queue.pop();
    }
   
    task();
  }
}
