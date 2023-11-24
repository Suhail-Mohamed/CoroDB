#pragma once

#include "Task.hpp" 
#include <atomic>
#include <coroutine>
#include <exception>
#include <type_traits>

template<typename T> struct SyncWaiterPromise;
template<typename T> struct SyncWaiter;

/* This structure defines what the promise type for our SyncWaiter type will be,
   crucially it ensures that when a SyncWaiter is finishing it updates the value
   of an atomic_flag (called completion_flag) it has associated with it, 
   this flag blocks the thread until the coroutine we are 'synchronously waiting' 
   for is done.
   
   we need to extract the base components of the promise type as the implementation
   of SyncWaiter<void> has to be done seperately.*/
template <typename T> struct SyncWaiterPromiseBase {
  std::suspend_never initial_suspend()        { return {}; } /* start running the SyncWaiter coroutine right away */
  auto		     final_suspend() noexcept { return FinalAwaitable{}; }
  void		     unhandled_exception()    { std::terminate(); }
  
  /* called when the SyncWaiter coroutine is completing */
  struct FinalAwaitable {
    bool await_ready() noexcept { return false; }
    
    /* when a SyncWaiter is done running it updates the value of the completion flag
       it has, this is to notify that the coroutine that we were waiting is 
       done running  */
    void await_suspend(std::coroutine_handle<SyncWaiterPromise<T>> child_coroutine) noexcept {
      std::atomic_flag& c_flag = child_coroutine.promise().completion_flag;
      c_flag.test_and_set();
      c_flag.notify_all();
    }
    
    void await_resume()	noexcept {}
  };
  
  std::atomic_flag completion_flag = ATOMIC_FLAG_INIT; 
};

/********************************************************************************/
/* non-void SyncWaiter promise type */
template <typename T> struct SyncWaiterPromise : SyncWaiterPromiseBase<T> { 
  SyncWaiter<T> get_return_object() {
    return SyncWaiter<T>{std::coroutine_handle<SyncWaiterPromise<T>>::from_promise(*this)};
  }
  
  template <typename U>
  requires std::convertible_to<U&&, T>
  void return_value(U&& value) { result = std::forward<U>(value); }
  
  /* get the result of coroutine we were waiting for from the SyncWaiter type */
  [[nodiscard]] auto get_result() &  -> T &  { return result; }
  [[nodiscard]] auto get_result() && -> T && { return std::move(result); }
  
  T result;
};

/********************************************************************************/
/* One limitation of a Task<TYPE> is it can only be co_awaited for its value in 
   a coroutine, in some cases we want the Tasks value in non-coroutine functions.
   SyncWaiter is a way to mitigate this.  By allowing non-coroutines to wait until
   a coroutine is finished processing and retrieving the value from it. */
template <typename T> struct [[nodiscard]] SyncWaiter {
  using promise_type = SyncWaiterPromise<T>;
  
  SyncWaiter(std::coroutine_handle<SyncWaiterPromise<T>> coro)
    : coroutine{coro} {}
  
  void wait()       { coroutine.promise().completion_flag.wait(false); }
  T    get_result() { return coroutine.promise().get_result(); }

  std::coroutine_handle<SyncWaiterPromise<T>> coroutine;
};

/********************************************************************************/

template <> struct SyncWaiterPromise<void> : SyncWaiterPromiseBase<void> { 
  SyncWaiter<void> get_return_object() {
    return SyncWaiter<void>{std::coroutine_handle<SyncWaiterPromise<void>>::from_promise(*this)};
  }

  void return_void() {}
};

/********************************************************************************/
/* sync_wait implementation these are the functions that you would call */

template <typename T> 
[[nodiscard]] T sync_wait(Task<T>& task) {
  /* This lambda creates a SyncWaiter that will start the Task, the Task then 
     suspends back to the make_sync_waiter function when it reaches 
     a co_await statement within itself, the make_sync_waiter is 
     the parent of this Task and is waiting on its return because
     of this it also suspends and returns back to the sync_wait function.
     in the sync_wait function we instantly wait() using the completion_flag
     in the SyncWaiter promise_type.
     Once our Task in make_sync_waiter is resumed and completed it starts 
     back-up its parent make_sync_waiter, make_sync_waiter calls co_return
     immeadiately after this which stops the wait in sync_wait allowing
     us to return the data from Task type */
  auto make_sync_waiter = [&task]() -> SyncWaiter<T> {
    co_return co_await task;
  };
  
  SyncWaiter<T> sync_waiter = make_sync_waiter();
  sync_waiter.wait();
  
  if constexpr(!std::is_same_v<T, void>)
    return sync_waiter.get_result();
}

/************************************/

/* rvalue reference version, allowing us to call sync_wait(coroutine_func)*/
template <typename T> 
[[nodiscard]] T sync_wait(Task<T>&& task) {
  auto make_sync_waiter = [&task]() -> SyncWaiter<T> {
    co_return co_await task;
  };
  
  SyncWaiter<T> sync_waiter = make_sync_waiter();
  sync_waiter.wait();
  
  if constexpr(!std::is_same_v<T, void>)
    return sync_waiter.get_result();
}

