#pragma once

#include <atomic>
#include <coroutine>
#include <exception>
#include <iostream>
#include <utility>

template<typename T> struct TaskPromise;
template<typename T> struct Task;

/* This structure defines what the promise type for our Task type will be,
   crucially it ensures that when a Task is finishing it starts up its parent
   coroutine, if the Tasks has no parent it justs start a std::noop_coroutine (ie:
   does nothing).

   we need to extract the base components of the promise type as the implementation
   of Task<void> has to be done seperately.*/
template <typename T> struct TaskPromiseBase {
  std::suspend_always initial_suspend()        { return {}; } /* only evaluate coroutine when co_await is called */
  auto                final_suspend() noexcept { return FinalAwaitable{}; }
  void                unhandled_exception()    { std::terminate(); }
  
  /* awaitable that is used to continue the parent coroutine after child is done
     running, this is called when the child is finished running */
  struct FinalAwaitable {
    /* eagerly run the final awaitable, we want to run await_suspend */
    bool await_ready() noexcept { return false; }
    
    /* continue the coroutine that started this child coroutine, this is because 
       this child coroutine is done running, as we instantiate this final awaiter
       when the child is ending (hence why it is called in final_suspend) */
    std::coroutine_handle<> 
    await_suspend(std::coroutine_handle<TaskPromise<T>> child_coroutine) noexcept {
        return child_coroutine.promise().parent_coroutine;
    }

    void await_resume() noexcept {}
    void unhandled_exception()   { std::terminate(); }
  };
  
  /* this is the coroutine which has a co_awaited Task in it, I call this the parent
     coroutine because it instantiates a call to another coroutine, called the child, when 
     this child coroutine is done it will resume this parent coroutine function */
  std::coroutine_handle<> parent_coroutine = std::noop_coroutine();
};

/********************************************************************************/

/* TaskPromise for a Task<TYPE> where TYPE != void, we seperate this logic as we have 
   to implement return_value & get_result in this TaskPromise (as this Task returns
   something) */
template <typename T> struct TaskPromise : TaskPromiseBase<T> {
  Task<T> get_return_object() {
    return Task<T>{std::coroutine_handle<TaskPromise<T>>::from_promise(*this)};
  }
  
  /* returns T type value, if the type U is convertible to T this
     function will still work */
  template <typename U>
  requires std::convertible_to<U&&, T>
  void return_value(U&& value) { result = std::forward<U>(value); }
  
  [[nodiscard]] auto get_result() &  -> T &  { return result; }
  [[nodiscard]] auto get_result() && -> T && { return std::move(result); }

  T result;
};

/********************************************************************************/

/* A Task is a generic way to represent the return value of a coroutine, think of it 
   like a std::future<TYPE>. A Task<TYPE> is going to give back a value of TYPE when 
   it is co_awaited ie: TYPE t = co_await Task<TYPE>.
   Additonally Task allows us to chain coroutines together, allowing us to start 
   a child coroutine from a parent coroutine and making the child continue the parent
   when it is completed */
template <typename T> struct [[nodiscard]] Task {
  using promise_type = TaskPromise<T>;
  
  Task(Task&& other) 
    : coroutine{std::exchange(other.coroutine, nullptr)} {}
  
  Task(std::coroutine_handle<TaskPromise<T>> coro)
    : coroutine{coro} {}
  
  ~Task() {
    if (coroutine)
      coroutine.destroy();
  }

  auto operator co_await() { 
    return TaskAwaitable{coroutine}; 
  }

  void resume() {
    if (!coroutine || coroutine.done()) 
      return;
    coroutine.resume();
  }

  struct TaskAwaitable {
    /* this task has been called with a co_await operator on it,
       this now means it is within some other coroutine, this coroutine
       it is within is the parent, and our current coroutine is now a child */
    TaskAwaitable(std::coroutine_handle<TaskPromise<T>> coroutine)
      : child_coroutine{coroutine} {}
    
    /* if there is a valid child_coroutine and it isn't done we will wait until it is done */
    bool await_ready() noexcept { return !child_coroutine || child_coroutine.done(); } 
    
    /* suspend the parent coroutine function and start running the child coroutine */
    std::coroutine_handle<>
    await_suspend(std::coroutine_handle<> parent_coroutine) noexcept {
      /* give the child its parent coroutine so it knows what to start back up 
         when it finishes running. The parent is stuck until the child is done */
      child_coroutine.promise().parent_coroutine = parent_coroutine;
      return child_coroutine;
    }
    
    /* return back the data the child Task promises to return */
    auto await_resume() noexcept -> decltype(auto) {
      if constexpr (!std::is_same_v<T, void>)
        return child_coroutine.promise().get_result();
    }

    std::coroutine_handle<TaskPromise<T>> child_coroutine = nullptr;
  };
  
  std::coroutine_handle<TaskPromise<T>> coroutine = nullptr;
};

/********************************************************************************/

/* Implementation of TaskPromise for Task<void> need this because return_void must be 
   used instead of return_value and there is no get_result */
template <> struct TaskPromise<void> : TaskPromiseBase<void> {
  Task<void> get_return_object() {
    return Task<void>{std::coroutine_handle<TaskPromise<void>>::from_promise(*this)};
  }

  void return_void() {}
};
