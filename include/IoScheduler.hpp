#pragma once 

#include <condition_variable>
#include <coroutine>
#include <memory>
#include <mutex>
#include <stop_token>
#include <type_traits>
#include <queue>
#include <thread>
#include <vector>

#include "Iouring.hpp"

/* Allows multiple threads to make a IO request via 
   DiskManager in thread safe manner. This scheduler 
   is essentially a wrapper around a concurrent queue */
struct IoScheduler {
  /* Allows us to pause at some scheduled point and add
     the coroutine to our queue, then resume it when it
     has been popped off the coro_queue */
  struct SchedulerAwaitable {
    SchedulerAwaitable(IoScheduler& scheduler)
      : io_scheduler{scheduler} {}

    /* pause the coroutine we are in right away, the coroutine
       will get started when it is popped off of coro_queue */
    bool await_ready() const { return false; }
    
    void await_suspend(std::coroutine_handle<> coroutine) {
      io_scheduler.enqueue(coroutine);
    }
    
    void await_resume() const {}

    IoScheduler& io_scheduler;
  };

  [[nodiscard]] SchedulerAwaitable schedule() {
    return SchedulerAwaitable{*this};
  }

  void start_scheduler();
  ~IoScheduler() {
    io_stop_src.request_stop();
  }

private:
  std::queue<std::coroutine_handle<>> coro_queue;
  std::jthread     io_thread;
  std::stop_source io_stop_src;
  std::mutex       queue_mutex;

  void enqueue(std::coroutine_handle<> coroutine) {
    std::scoped_lock lock(queue_mutex);
    coro_queue.push(coroutine);
  }
  
  bool queue_empty() {
    std::scoped_lock lock(queue_mutex);
    return coro_queue.empty();
  }

  void process_cqe();
  void io_loop();
};

