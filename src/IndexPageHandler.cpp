#include "IndexPageHandler.hpp"
#include "IndexMetaData.hpp"
#include "Iouring.hpp"

IndexPageHandler::IndexPageHandler(Handler* handler, 
                                   const IndexMetaData* index_meta_data)
{
  assert(handler);
  assert(index_meta_data);
  handler_ptr   = handler;
  meta_data_ptr = index_meta_data;
  timestamp     = handler_ptr->page_timestamp;
  
  /* we pin a index page for as long as it exists */
  handler_ptr->is_pinned = true;
  page_hdr.read_header(handler_ptr->page_ptr);
}

/********************************************************************************/

IndexPageHandler::~IndexPageHandler() {
  handler_ptr->is_pinned = false;
  if (handler_ptr->is_dirty)
    page_hdr.write_header(handler_ptr->page_ptr);
}

/********************************************************************************/

int32_t IndexPageHandler::lower_bound(const Record key_value) {
  int32_t low  = 0;
  int32_t high = page_hdr.num_keys;
  
  while (low < high) {
    int32_t mid = (low + high) / 2;
    if (key_value <= get_key(mid))
      high = mid;
    else low = mid + 1;
  }

  return low;
}

/********************************************************************************/

int32_t IndexPageHandler::upper_bound(const Record key_value) {
  int32_t low  = 0;
  int32_t high = page_hdr.num_keys;
  
  while (low < high) {
    int32_t mid = (low + high) / 2;
    if (key_value < get_key(mid))
      high = mid;
    else low = mid + 1;
  }

  return low;
}

/********************************************************************************/

int32_t IndexPageHandler::find_child(const int32_t child_page_num) {
  for (int32_t idx = 0; idx < page_hdr.num_children; ++idx) {
    if (get_rid(idx).page_num == child_page_num)
      return idx;
  }

  return -1;
}

/********************************************************************************/
/* abstractions for key and rid movement within page */

const Record IndexPageHandler::get_key(const int32_t key_idx) { 
  assert(key_idx < page_hdr.num_keys && key_idx >= 0);
  Record key;
  
  off_t key_offset = key_idx_to_offset(key_idx);

  handler_ptr->get_record(key_offset, meta_data_ptr->get_key_layout(), key);
  return key;
}

/*************************/

const std::vector<Record> IndexPageHandler::get_keys(const int32_t key_idx, 
                                                     const int32_t num_keys)
{
  std::vector<Record> keys;
  for (int32_t idx = key_idx; idx < key_idx + num_keys; ++idx)
    keys.push_back(get_key(idx));
  
  return keys;
}

/*************************/

PageResponse IndexPageHandler::set_key(const int32_t key_idx, 
                                       Record        new_key_value) 
{
  assert(key_idx < page_hdr.num_keys && key_idx >= 0);

  handler_ptr->is_dirty = true;
  off_t key_offset = key_idx_to_offset(key_idx);     
  return handler_ptr->set_record(key_offset, meta_data_ptr->get_key_layout(), 
                                 new_key_value);
}

/*************************/

PageResponse IndexPageHandler::set_keys(const int32_t key_idx,
                                        const std::vector<Record>& new_key_values)
{
  int32_t idx = key_idx;
  for (const Record& key : new_key_values)
    if (set_key(idx++, key) != PageResponse::Success)
      return PageResponse::Failure;
  
  handler_ptr->is_dirty = true;
  return PageResponse::Success;
}

/*************************/

PageResponse IndexPageHandler::insert_keys(const std::vector<Record>& key_values,
                                           const int32_t key_idx) 
{
  assert(key_idx < page_hdr.num_keys && key_idx >= 0);

  if (page_hdr.num_keys + key_values.size() > meta_data_ptr->get_max_num_keys())
    return PageResponse::PageFull;
  
  page_hdr.num_keys += key_values.size();

  PageResponse shift_resp = shift_keys(key_idx, key_values.size());
  PageResponse set_resp   = set_keys(key_idx, key_values);

  assert(shift_resp == PageResponse::Success && 
         set_resp   == PageResponse::Success);
  handler_ptr->is_dirty = true;
  return PageResponse::Success; 
}

/*************************/

PageResponse IndexPageHandler::erase_key(const int32_t key_idx) {
  assert(key_idx < page_hdr.num_keys && key_idx >= 0);

  for (int32_t idx = key_idx + 1; idx < page_hdr.num_keys; ++idx)
    if (set_key(idx - 1, get_key(idx)) != PageResponse::Success)
      return PageResponse::Failure;
  
  --page_hdr.num_keys;
  handler_ptr->is_dirty = true;
  return PageResponse::Success;
}

/*************************/

const RecId IndexPageHandler::get_rid(const int32_t rid_idx) {
  assert(rid_idx < page_hdr.num_children && rid_idx >= 0);
  RecId rec_id;
  
  off_t rid_offset = rid_idx_to_offset(rid_idx);
  read_rid(rid_offset, rec_id);
  
  return rec_id;
}

/*************************/

const std::vector<RecId> IndexPageHandler::get_rids(const int32_t rid_idx,
                                                    const int32_t num_rids)
{
  std::vector<RecId> rids;
  for (int32_t idx = rid_idx; idx < rid_idx + num_rids; ++idx)
    rids.push_back(get_rid(idx));
  
  return rids;
}

/*************************/

PageResponse IndexPageHandler::set_rid(const int32_t rid_idx,
                                       RecId new_rid_value)
{
  assert(rid_idx < page_hdr.num_children && rid_idx >= 0);

  off_t rid_offset = rid_idx_to_offset(rid_idx);     
  write_rid(new_rid_value, rid_offset);

  handler_ptr->is_dirty = true;
  return PageResponse::Success;
}

/*************************/

PageResponse IndexPageHandler::set_rids(const int32_t rid_idx,
                                        const std::vector<RecId>& new_rid_values)
{
  int32_t idx = rid_idx;
  
  for (const RecId& rid : new_rid_values)
    if (set_rid(idx++, rid) != PageResponse::Success)
      return PageResponse::Failure;
  
  handler_ptr->is_dirty = true;
  return PageResponse::Success;
}

/*************************/

PageResponse IndexPageHandler::insert_rids(const std::vector<RecId>& rid_values,
                                           const int32_t rid_idx) 
{
  assert(rid_idx < page_hdr.num_children && rid_idx >= 0);
  
  if (page_hdr.num_children + rid_values.size() > meta_data_ptr->get_max_num_keys())
    return PageResponse::PageFull;
  
  page_hdr.num_children += rid_values.size(); 
  
  shift_rids(rid_idx, rid_values.size());
  set_rids(rid_idx, rid_values);

  handler_ptr->is_dirty = true;
  return PageResponse::Success;
}

/*************************/

PageResponse IndexPageHandler::erase_rid(const int32_t rid_idx) {
  assert(rid_idx < page_hdr.num_children && rid_idx >= 0);
  
  for (int32_t idx = rid_idx + 1; idx < page_hdr.num_keys; ++idx)
    if (set_rid(idx - 1, get_rid(idx)) != PageResponse::Success)
      return PageResponse::Failure;
  
  --page_hdr.num_children;
  handler_ptr->is_dirty = true;
  return PageResponse::Success;
}

/*************************/

PageResponse IndexPageHandler::shift_keys(const int32_t key_idx,
                                          const int32_t shift_size) 
{
  for (int32_t idx = page_hdr.num_keys - 1; idx >= key_idx; --idx)
    if (set_key(idx + shift_size, get_key(idx)) != PageResponse::Success)
      return PageResponse::InvalidRecord;

  handler_ptr->is_dirty = true;
  return PageResponse::Success;
}

/*************************/

PageResponse IndexPageHandler::shift_rids(const int32_t rid_idx,
                                          const int32_t shift_size) 
{
  for (int32_t idx = page_hdr.num_children - 1; idx >= rid_idx; --idx)
    set_rid(idx + shift_size, get_rid(idx));

  handler_ptr->is_dirty = true;
  return PageResponse::Success;
}
