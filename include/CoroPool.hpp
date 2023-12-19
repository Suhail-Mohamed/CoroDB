#pragma once

#include <condition_variable>
#include <coroutine>
#include <mutex>
#include <queue>
#include <stop_token>
#include <thread>

/* minus 1 because 1 thread is used to deal with IO tasks, see IoScheduler */
const int32_t NUM_THREADS = 1;

struct CoroPool { 
  CoroPool(const CoroPool&)	           = delete;
  CoroPool(CoroPool &&)		           = delete;
  CoroPool& operator=(const CoroPool&) = delete;
  CoroPool& operator=(CoroPool&&)      = delete;

  static CoroPool& get_instance() {
    static CoroPool instance;
    return instance;
  }

  /* Allows us to pause at some scheduled point and add
     the coroutine to our queue, then resume it when it
     has been popped off the coro_queue */
  struct SchedulerAwaitable {
    SchedulerAwaitable(CoroPool& pool)
      : coro_pool{pool} 
    {};

    /* pause the coroutine we are in right away, the coroutine
       will get started when it is popped off of coro_queue */
    bool await_ready() const 
    { return false; }
   
    /* add the coroutine handle to our queue as soon as the coroutine
       suspends, we resume the coroutine in our thread_loop() */
    void await_suspend(std::coroutine_handle<> coroutine)
    { coro_pool.enqueue(coroutine); }
    
    /* we don't do anything once the coroutine that was passed in 
       await_suspend is resumed */
    void await_resume() const {}
    
    CoroPool& coro_pool;
  };

  [[nodiscard]] SchedulerAwaitable schedule() 
  { return SchedulerAwaitable{*this}; };

  size_t get_size() const 
  { return coro_queue.size(); }

  /* Enqueues the given coroutine into coro_queue in a thread-safe manner.
     After inserting the coroutine, it notifies one of the threads waiting
     on cond_var in thread_loop(). This notification triggers a single thread
     to check cond_vars resume condition. Since coro_queue is now non-empty,
     the notified thread will resume the coroutine, either running it to completion
     or until it reaches another co_await point 
     You shouldn't call this method directly, unless you have a coroutine handle 
     you want to specfically schedule. */
  void enqueue(std::coroutine_handle<> coroutine) {
    std::lock_guard<std::mutex> lock{queue_mutex};
    coro_queue.push(coroutine);
    cond_var.notify_one();
  }

private:
  CoroPool() {
    for (int32_t thread; thread < NUM_THREADS; ++thread)
      threads.emplace_back([&]() { thread_loop(); });
  }

  ~CoroPool() {
    stop_source.request_stop();
    cond_var.notify_all();
  }

  /* The thread_loop function continuously waits for and 
     processes coroutines from the coro_queue
     Each thread running this function will:
     - Acquire a lock and wait on a condition variable. The condition variable is notified
       when a new coroutine is enqueued(), signaling to a thread to proceed
     - Once notified, a thread will wake up, check for a stop request, and if not stopping,
       retrieve and pop a coroutine from the coro_queue
     - The thread then releases the lock, resumes the coroutine and the process is repeated */
  void thread_loop() {
    while (!stop_source.stop_requested()) {
      std::unique_lock lock{queue_mutex};
      
      cond_var.wait(lock, [this]() {
        return !coro_queue.empty() ||
                stop_source.stop_requested();
      });

      if (stop_source.stop_requested()) break;
      const auto coroutine = coro_queue.front();
      coro_queue.pop();
      lock.unlock();

      coroutine.resume();
    }
  }

  std::stop_source        stop_source;
  std::mutex              queue_mutex;
  std::condition_variable cond_var;
  
  std::vector<std::jthread>           threads;
  std::queue<std::coroutine_handle<>> coro_queue;
};
