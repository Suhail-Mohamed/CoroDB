#pragma once

#include <cassert>
#include <cstdint>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <exception>
#include <memory>
#include <span>
#include <vector>

#include "Iouring.hpp"
#include "IoScheduler.hpp"
#include "Task.hpp"

/********************************************************************************/

struct IoAwaitable { 
  IoAwaitable(const int32_t fd,
              const off_t   offset,
	      const IOP     iop) 
  { 
    sqe_data.fd	 = fd; 
    sqe_data.iop = iop;
    sqe_data.offset = offset;
  }


  IoAwaitable(const int32_t fd,
              const off_t   offset,
	      const IOP     iop, 
	      Page*         page_data)
    : IoAwaitable{fd, offset, iop} 
  { 
    sqe_data.page_data = page_data;
  }

  /* pause the coroutine we are in right away */
  bool await_ready() const { return false; }
  
  /* give SqeData a handle to the coroutine we have paused, we will
     resume the function when we handle the I/O request */
  void await_suspend(std::coroutine_handle<> coroutine) {
    Iouring& io_uring = Iouring::get_instance();
    sqe_data.coroutine = coroutine;

    if (sqe_data.iop == IOP::Read)
      io_uring.read_request(sqe_data);
    else io_uring.write_request(sqe_data);
  }
  
  int32_t await_resume() const 
  { return sqe_data.buff_id; }

  SqeData sqe_data;
};

/********************************************************************************/

template<size_t N>
using Bitset = std::array<bool, N>;

template <size_t N>
size_t find_first_false(const Bitset<N>& b_set) {
  auto itr = std::find(std::begin(b_set), 
		       std::end(b_set), false);
  
  if (itr != std::end(b_set))
    return std::distance(std::begin(b_set), itr);

  return -1;
}

/********************************************************************************/

struct BaseBundle {
  virtual Page&	   get_page(const int32_t page_id)		    = 0;
  virtual Handler& get_page_handler(const int32_t page_id)	    = 0; 
  virtual bool     get_page_used(const int32_t page_id)             = 0;
  virtual int32_t  get_min_page_usage()                             = 0;
  virtual void	   set_page_used(const int32_t page_id, bool value) = 0;
};

template <size_t N>
struct PageBundle : BaseBundle {
  Page& get_page(const int32_t page_id) override 
  { return pages[page_id]; }

  Handler& get_page_handler(const int32_t page_id) override 
  { return page_handlers[page_id]; }
  
  bool get_page_used(const int32_t page_id) override 
  { return pages_used[page_id]; }

  int32_t find_page(const int32_t page_fd, 
                    const int32_t page_num)
  {
    auto itr = std::find_if(std::begin(page_handlers), std::end(page_handlers),
                            [page_fd, page_num] (const Handler& pg_h) {
                              return pg_h.page_num == page_num &&
                                     pg_h.page_fd  == page_fd; });
    
    if (itr != std::end(page_handlers))
      return std::distance(std::begin(page_handlers), itr);
    
    return -1;
  }
  
  int32_t get_min_page_usage() override {
    int32_t min_usage = INT32_MIN;
    int32_t page_id   = -1;

    for (size_t i = 0 ; i < N; ++i)
      if (!page_handlers[i].is_pinned && 
          page_handlers[i].page_usage < min_usage) 
      {
	min_usage = page_handlers[i].page_usage;
	page_id   = i;
      }
  
    return page_id;
  }

  void set_page_used(const int32_t page_id, 
                     bool value) override 
  { pages_used[page_id] = value; }
  
  Bitset<N>              pages_used;
  std::array<Page, N>    pages;
  std::array<Handler, N> page_handlers;
};

/********************************************************************************/

/* sometimes we don't want to add a Task to a scheduler
   as it could have been called within another Task 
   and this nesting can cause deadlocks */
enum class SchOpt {
  Schedule,
  DontSchedule
};

struct DiskManager {
  DiskManager(const DiskManager&)	     = delete;
  DiskManager(DiskManager &&)		     = delete;
  DiskManager& operator=(const DiskManager&) = delete;
  DiskManager& operator=(DiskManager&&)	     = delete;
  
  static DiskManager& get_instance() {
    static DiskManager instance;
    return instance;
  }

  [[nodiscard]] Task<Handler*> create_page(const int32_t fd,
                                           const int32_t page_num,
                                           const RecordLayout layout,
                                           const SchOpt s_opt = SchOpt::Schedule);
  [[nodiscard]] Task<Handler*> read_page  (const int32_t fd,
                                           const int32_t page_num,
                                           const RecordLayout layout,
                                           const SchOpt s_opt = SchOpt::Schedule);

private:
  [[nodiscard]] int32_t lru_replacement(const PageType page_type);
  
  Task<void> write_page (const int32_t page_id,
                         const int32_t page_num,
                         PageType      page_type,
                         const SchOpt  s_opt = SchOpt::Schedule); 
  Task<void> return_page(const int32_t  page_id,
                         const PageType page_type,
                         const SchOpt   s_opt = SchOpt::Schedule);
  
  [[nodiscard]] Task<Handler*> get_page(const int32_t  page_id,
                                        const PageType page_type,
                                        const SchOpt   s_opt = SchOpt::Schedule);

  DiskManager();
  int32_t                            timestamp_gen;
  IoScheduler			     io_scheduler;
  PageBundle<BUFF_RING_SIZE>	     io_bundles;
  PageBundle<PAGE_POOL_SIZE>	     np_bundles;
  std::unique_ptr<io_uring_buf_ring> buff_ring_ptr; 
  
  std::array<BaseBundle*, PageType::NumPageTypes> bundles;
};
