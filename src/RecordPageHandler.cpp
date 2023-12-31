#include "RecordPageHandler.hpp"

RecordPageHandler::RecordPageHandler(Handler* handler) 
  : rw_mutex_ptr       {std::make_unique<std::shared_mutex>()},
    is_undefined_rec_pg{false}
{
  assert(handler);
  handler_ptr = handler;
  record_size = calc_record_size(handler_ptr->page_layout); 
  handler_ptr->is_pinned = true;
  
  if (handler_ptr->page_type == PageType::NonPersistent) {
    num_records = 0;
    page_cursor = REC_HEADER_SIZE;
  } else {
    num_records = read_header();
    page_cursor = REC_HEADER_SIZE + record_size * num_records;
  }
} 

/********************************************************************************/

RecordPageHandler::~RecordPageHandler() {
  handler_ptr->is_pinned = false;
  if (handler_ptr->is_dirty) {
    compact_page();
    update_num_records();
  }
}

/********************************************************************************/

RecId RecordPageHandler::add_record(Record& record) {
  std::unique_lock lock{*rw_mutex_ptr};

  if (tombstones.empty() && is_full())
    return PAGE_FILLED;

  handler_ptr->is_dirty = true;
  if (!tombstones.empty()) {
    int32_t tomb_idx = *tombstones.rbegin(); 
    update_record(*tombstones.rbegin(), record);
    tombstones.erase(--std::end(tombstones));
    return {handler_ptr->page_num, tomb_idx};
  }

  handler_ptr->set_record(page_cursor, handler_ptr->page_layout, 
                          record);

  return {handler_ptr->page_num, num_records++};
}

/********************************************************************************/

RecId RecordPageHandler::delete_record(const int32_t record_num) {
  assert(record_num < num_records && record_num >= 0);
  std::unique_lock lock{*rw_mutex_ptr};
  
  handler_ptr->is_dirty = true;
  tombstones.insert(record_num);
  return {handler_ptr->page_num, record_num};
}

/********************************************************************************/

PageResponse RecordPageHandler::update_record(const  int32_t record_num,
                                              Record&        new_record)
{
  assert(record_num < num_records && record_num >= 0);
  std::unique_lock lock{*rw_mutex_ptr};
  
  if (tombstones.count(record_num))
    return PageResponse::DeletedRecord;

  off_t write_offset = record_num_to_offset(record_num);
  
  if (write_offset > page_cursor)
    return PageResponse::PageFull;

  handler_ptr->set_record(write_offset, handler_ptr->page_layout, 
                          new_record);
  handler_ptr->is_dirty = true;
  return PageResponse::Success;
}

/********************************************************************************/

/* zero based indexing for record_num, ie: first record is record_num = 0 */
RecordResponse RecordPageHandler::read_record(const int32_t record_num,
                                              const LockOpt l_opt) 
{
  assert(record_num < num_records && record_num >= 0);
  Record ret_record;
  
  if (l_opt == LockOpt::Lock) std::shared_lock lock{*rw_mutex_ptr};
  
  if (tombstones.count(record_num)) 
    return {std::move(ret_record), PageResponse::DeletedRecord};

  off_t read_offset = record_num_to_offset(record_num);
  
  if (read_offset > page_cursor)
    return {std::move(ret_record), PageResponse::PageFull};
 
  ret_record.resize(handler_ptr->page_layout.size());

  handler_ptr->get_record(read_offset, handler_ptr->page_layout,
                          ret_record);
  return {std::move(ret_record), PageResponse::Success};
}

/********************************************************************************/

void RecordPageHandler::compact_page() {
  if (tombstones.empty()) return;

  std::unique_lock lock{*rw_mutex_ptr};
  size_t num_tombstones = tombstones.size();

  /* ineffiecient but I don't mind */
  for (uint32_t rec_num : tombstones) {
    for (uint32_t rec = rec_num; rec < num_records - 1; ++rec)
      move_record(rec + 1, rec);
  }

  num_records -= num_tombstones;
  tombstones.clear();
}

/********************************************************************************/

PageResponse RecordPageHandler::move_record(const uint32_t from_record, 
                                            const uint32_t to_record)
{
  assert(to_record   < num_records && to_record   >= 0 && 
         from_record < num_records && from_record >= 0);

  off_t  write_offset = record_num_to_offset(to_record);
  Record old_record   = read_record(from_record, 
                                    LockOpt::DontLock).record;

  handler_ptr->set_record(write_offset, handler_ptr->page_layout, 
                          old_record);
  return PageResponse::Success;
}
