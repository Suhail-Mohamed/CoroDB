#include "DiskManager.hpp"
#include "Iouring.hpp"

DiskManager::DiskManager() {
  void* buff_ring = nullptr;
  bundles         = {&io_bundles, &np_bundles};

  if (posix_memalign(&buff_ring, PAGE_SIZE, BUFF_RING_SIZE * sizeof(io_uring_buf)))
    throw std::runtime_error("Error: Error allocating ring buffer memory");

  buff_ring_ptr.reset(reinterpret_cast<io_uring_buf_ring*>(buff_ring));
  
  Iouring::get_instance().register_buffer_ring(buff_ring_ptr.get(), 
                                               io_bundles.pages);
}

/********************************************************************************/

int32_t DiskManager::lru_replacement(const PageType page_type) {
  BaseBundle* b_bundle = bundles[page_type];
  return b_bundle->get_min_page_usage();
}

/********************************************************************************/

void DiskManager::handle_io() {
  Iouring& io_uring = Iouring::get_instance();
  
  io_uring.submit_and_wait(1); 
  
  io_uring.for_each_cqe([&io_uring](io_uring_cqe* cqe) {
    SqeData* sqe_data = 
      static_cast<SqeData*>(io_uring_cqe_get_data(cqe));
     
    sqe_data->status_code = cqe->res;
    if (sqe_data->iop == IOP::Read)
      sqe_data->buff_id = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
    
    sqe_data->handle.resume();
    io_uring.cqe_seen(cqe);
  });
}

/********************************************************************************/

PageHandler* DiskManager::create_page(const int32_t fd,
                                      const RecordLayout layout) 
{
  int32_t page_id = find_first_false(np_bundles.pages_used); 
  if (page_id == -1)
    page_id = lru_replacement(PageType::NonPersistent);

  np_bundles.pages_used[page_id] = true;
  np_bundles.page_handlers[page_id].init_handler(&np_bundles.pages[page_id],
                                                 layout,
                                                 fd,
                                                 page_id,
                                                 PageType::NonPersistent);
  return &np_bundles.page_handlers[page_id];
}

/********************************************************************************/

Task<PageHandler*> DiskManager::read_page(const int32_t fd,
                                          const RecordLayout layout) 
{
  if (auto find_page = open_files.find_page_id(fd);
      find_page != -1) 
    co_return get_page(find_page, PageType::IO);
  
  if (find_first_false(io_bundles.pages_used) == -1) {
    int32_t replace_page = lru_replacement(PageType::IO); 
    co_await return_page(replace_page, PageType::IO);
  } 

  const int32_t page_id = 
    co_await IoAwaitable{fd, IOP::Read};
 
  open_files.link_file_to_page(fd, page_id);
  io_bundles.pages_used[page_id] = true;
  io_bundles.page_handlers[page_id].init_handler(&io_bundles.pages[page_id], 
                                                 layout,
                                                 fd,
                                                 page_id, 
                                                 PageType::IO);
  co_return &io_bundles.page_handlers[page_id];
}

/********************************************************************************/

Task<void> DiskManager::write_page(const int32_t  page_id,
                                   const PageType page_type) 
{
    BaseBundle* b_bundle = bundles[page_type];
    co_await IoAwaitable{b_bundle->get_page_handler(page_id).page_fd, 
                         IOP::Write, 
                         &b_bundle->get_page(page_id)};

    b_bundle->get_page_handler(page_id).is_dirty = false;
}

/********************************************************************************/

Task<void> DiskManager::return_page(const int32_t  page_id,
                                    const PageType page_type)
{
  BaseBundle* b_bundle = bundles[page_type];
  if (b_bundle->get_page_handler(page_id).is_dirty) {
      b_bundle->get_page_handler(page_id).prep_for_disk();
      co_await write_page(page_id, page_type);
  }

  if (page_type == PageType::IO) {
    Iouring::get_instance().add_buffer(buff_ring_ptr.get(),
                                       io_bundles.pages[page_id],
                                       page_id);
    open_files.unlink_file(page_id); 
  }

  b_bundle->set_page_used(page_id, false);
}

/********************************************************************************/

PageHandler* DiskManager::get_page(const int32_t  page_id,
                                   const PageType page_type) 
{
  BaseBundle* b_bundle = bundles[page_type];
  if (!b_bundle->get_page_used(page_id)) return nullptr;

  ++b_bundle->get_page_handler(page_id).page_usage;
  return &b_bundle->get_page_handler(page_id);
}
