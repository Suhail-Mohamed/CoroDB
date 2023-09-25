#pragma once 

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <stop_token>
#include <thread>
#include <type_traits>
#include <queue>
#include <vector>

struct ThreadPool {
  /* getting rid of some undefined functionality */
  ThreadPool(ThreadPool&&)                 = delete;
  ThreadPool(const ThreadPool&)            = delete;
  ThreadPool& operator=(ThreadPool&&)      = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;
  
  ThreadPool(size_t nr_workers);
  ~ThreadPool();
  
  /* add an arbitrary function to our thread pool queue */
  template<typename Func, typename ...Args>
  std::future<typename std::result_of<Func(Args...)>::type> 
  add_task(Func&& f, Args&&... args) {
    using return_type = typename std::result_of<Func(Args...)>::type;

    /* create a shared ptr to a packaged task, which will return a std::future 
       of the function evaluated at its arguments. The reason we need
       to do this is our thread pool only works with functions that are 
       std::function<void()> ie: take no parameters */
    auto task = std::make_shared<std::packaged_task<return_type()>>(
                                 std::bind(std::forward<Func>(f), 
                                           std::forward<Args>(args)...));
    
    /* holds result of task when execueted */
    std::future<return_type> res = task->get_future();
    
    /* add the task to the queue in thread safe manner */
    {
      std::unique_lock<std::mutex> lock(queue_mutex);
      task_queue.emplace([task]() { (*task)(); });
    }
    
    /* notify condition variable so we can assign a thread to this task */
    queue_notify.notify_one();
    return res;
  }

private:
  std::queue<std::function<void()>> task_queue;
  std::vector<std::jthread>         workers;
  std::mutex              queue_mutex;
  std::condition_variable queue_notify;
  std::stop_source        stop_src;
  
  void thread_loop();
};

