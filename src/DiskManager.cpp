#include "DiskManager.hpp"

DiskManager::DiskManager() : timestamp_gen{0} {
  void* buff_ring = nullptr;
  bundles         = {&io_bundles, &np_bundles};

  if (posix_memalign(&buff_ring, PAGE_SIZE, BUFF_RING_SIZE * sizeof(io_uring_buf)))
    throw std::runtime_error("Error: Error allocating ring buffer memory");

  buff_ring_ptr.reset(reinterpret_cast<io_uring_buf_ring*>(buff_ring));
  
  Iouring::get_instance().register_buffer_ring(buff_ring_ptr.get(), 
                                               io_bundles.pages);
  io_scheduler.start_scheduler();
}
/********************************************************************************/

Task<Handler*> DiskManager::create_page(const int32_t fd,
                                        const int32_t page_num,
                                        const RecordLayout layout,
                                        const SchOpt s_opt) 
{
  if (s_opt == SchOpt::Schedule) co_await io_scheduler.schedule();
 
  /* Incase someone tries to create the same page twice */
  if (const auto find_page = np_bundles.find_page(fd, page_num);
      find_page != -1)
  {
    auto pg_h = co_await get_page(find_page, 
                                  PageType::NonPersistent, 
                                  SchOpt::DontSchedule);
    co_return pg_h;
  }

  int32_t page_id = find_first_false(np_bundles.pages_used); 
  if (page_id == -1)
    page_id = lru_replacement(PageType::NonPersistent);

  np_bundles.pages_used[page_id] = true;
  np_bundles.page_handlers[page_id].init_handler(&np_bundles.get_page(page_id),
                                                 layout,
                                                 page_id,
                                                 timestamp_gen++,
                                                 page_num,
                                                 fd,
                                                 PageType::NonPersistent);
  
  np_bundles.get_page_handler(page_id).is_dirty = true;
  co_return &np_bundles.page_handlers[page_id];
}

/********************************************************************************/

Task<Handler*> DiskManager::read_page(const int32_t fd,
                                      const int32_t page_num,
                                      const RecordLayout layout,
                                      const SchOpt s_opt) 
{
  if (s_opt == SchOpt::Schedule) co_await io_scheduler.schedule();
  
  /* page is in our buffer pool, so we can just return it, no IO */
  if (const auto find_page = io_bundles.find_page(fd, page_num);
      find_page != -1) 
  {
    auto pg_h = co_await get_page(find_page, 
                                  PageType::IO, 
                                  SchOpt::DontSchedule); 
    co_return pg_h;
  }

  /* no free pages for IO so we have to return one */
  if (find_first_false(io_bundles.pages_used) == -1) {
    int32_t replaced_page = lru_replacement(PageType::IO); 
    co_await return_page(replaced_page, 
                         PageType::IO, 
                         SchOpt::DontSchedule);
  } 

  const int32_t page_id = co_await IoAwaitable{fd,
                                               page_num * PAGE_SIZE,
                                               IOP::Read};
 
  io_bundles.pages_used[page_id] = true;
  io_bundles.page_handlers[page_id].init_handler(&io_bundles.get_page(page_id), 
                                                 layout,
                                                 page_id, 
                                                 timestamp_gen++,
                                                 page_num,
                                                 fd,
                                                 PageType::IO);
  co_return &io_bundles.page_handlers[page_id];
}

/********************************************************************************/

int32_t DiskManager::lru_replacement(const PageType page_type) {
  BaseBundle* b_bundle = bundles[page_type];
  return b_bundle->get_min_page_usage();
}

/********************************************************************************/

Task<void> DiskManager::write_page(const int32_t  page_id,
                                   const int32_t  page_num,
                                   const PageType page_type,
                                   const SchOpt   s_opt) 
{
  if (s_opt == SchOpt::Schedule) co_await io_scheduler.schedule();
  BaseBundle* b_bundle = bundles[page_type];

  /* no free pages for IO so we have to return one */
  if (find_first_false(io_bundles.pages_used) == -1) {
    int32_t replaced_page = lru_replacement(PageType::IO); 
    co_await return_page(replaced_page, 
                         PageType::IO, 
                         SchOpt::DontSchedule);
  }

  co_await IoAwaitable{b_bundle->get_page_handler(page_id).page_fd,
                       page_num * PAGE_SIZE,
                       IOP::Write,
                       &b_bundle->get_page(page_id)};

  b_bundle->get_page_handler(page_id).is_dirty = false;
}

/********************************************************************************/

Task<void> DiskManager::return_page(const int32_t  page_id,
                                    const PageType page_type,
                                    const SchOpt   s_opt)
{
  if (s_opt == SchOpt::Schedule) co_await io_scheduler.schedule();
  BaseBundle* b_bundle = bundles[page_type];
  
  --b_bundle->get_page_handler(page_id).page_ref;
  if (b_bundle->get_page_handler(page_id).is_dirty) {
    co_await write_page(page_id,
                        b_bundle->get_page_handler(page_id).page_num,
                        page_type, 
                        SchOpt::DontSchedule);
  }

  if (b_bundle->get_page_handler(page_id).page_ref <= 0) {
    b_bundle->set_page_used(page_id, false);
    
    if (page_type == PageType::IO)
      Iouring::get_instance().add_buffer(buff_ring_ptr.get(),
                                         io_bundles.pages[page_id],
                                         page_id);
  }
}

/********************************************************************************/

Task<Handler*> DiskManager::get_page(const int32_t  page_id,
                                     const PageType page_type,
                                     const SchOpt   s_opt) 
{
  if (s_opt == SchOpt::Schedule) co_await io_scheduler.schedule();
  BaseBundle* b_bundle = bundles[page_type];
  
  if (!b_bundle->get_page_used(page_id)) co_return nullptr;

  ++b_bundle->get_page_handler(page_id).page_ref;
  co_return &b_bundle->get_page_handler(page_id);
}
