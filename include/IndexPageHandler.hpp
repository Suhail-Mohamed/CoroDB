#pragma once 

#include "IndexMetaData.hpp"
#include "Iouring.hpp"
#include "Util.hpp"

struct IndexPageHandler {
  IndexPageHandler() 
    : handler_ptr{nullptr}, 
      meta_data_ptr{nullptr} {};
  
  IndexPageHandler(Handler* handler, 
                   const IndexMetaData* index_meta_data);
  
  ~IndexPageHandler();

  /* returns first value that is greater than or equal to key_value */
  int32_t lower_bound(const Record key_value);
  
  /* returns first value that is greater than key_value */
  int32_t upper_bound(const Record key_value);

  /* returns which rid points to same page as the child page num */
  int32_t find_child(const int32_t child_page_num);

  /*    abstractions for CRUD on keys and record ids      */
  /******************* key abstractions ********************/
  const Record              get_key(const int32_t key_idx);
  const std::vector<Record> get_keys(const int32_t key_idx,
                                     const int32_t num_keys);
  const Record get_max_key() 
  { return get_key(page_hdr.num_keys - 1); }

  const Record get_min_key() 
  { return get_key(0); }

  /* setting keys */
  PageResponse set_key(const int32_t key_idx, 
                       Record        new_key_value);
  PageResponse set_keys(const int32_t key_idx,
                        const std::vector<Record>& new_key_values);
 
  /* key insertion functionality */
  PageResponse insert_keys(const std::vector<Record>& key_values,
                           const int32_t key_idx);
  
  PageResponse insert_key(const Record key_value,
                          const int32_t key_idx) 
  { return insert_keys({key_value}, key_idx); };
  
  PageResponse push_back_key(const Record key_value)
  { return insert_keys({key_value}, get_num_keys() - 1); }
  
  PageResponse push_back_keys(const std::vector<Record>& key_values)
  { return insert_keys(key_values, get_num_keys() - 1); }
 
  /* erasing key functionality */
  PageResponse erase_key(const int32_t key_idx);
  
  /******************* record id abstractions ********************/
  const RecId              get_rid(const int32_t rid_idx);
  const std::vector<RecId> get_rids(const int32_t rid_idx,
                                    const int32_t num_rids);
  const RecId get_max_rid() 
  { return get_rid(page_hdr.num_children - 1); }

  const RecId get_min_rid() 
  { return get_rid(0); }

  /* setting rids */
  PageResponse set_rid(const int32_t rid_idx,
                       RecId   new_rid_value);
  PageResponse set_rids(const int32_t key_idx,
                        const std::vector<RecId>& new_rid_values);
 
  /* rid insertion functionality */
  PageResponse insert_rid(const RecId rid_value,
                          const int32_t rid_idx)
  { return insert_rids({rid_value}, rid_idx); }
  PageResponse insert_rids(const std::vector<RecId>& rid_values,
                           const int32_t rid_idx);

  PageResponse push_back_rid(const RecId rid_value)
  { return insert_rids({rid_value}, get_num_children() - 1); }
  
  PageResponse push_back_rids(const std::vector<RecId>& rid_values)
  { return insert_rids(rid_values, get_num_children() - 1); }
 
  /* erasing key functionality */
  PageResponse erase_rid(const int32_t rid_idx);

  const int32_t get_page_num() const
  { return handler_ptr->page_num; }

  /* setters and getters for page header data, use setters 
     so page becomes dirty and edits are written to disk */
  const int32_t get_parent() const
  { return page_hdr.parent; }

  const int32_t get_next_free() const
  { return page_hdr.next_free; }

  const int32_t get_num_keys() const
  { return page_hdr.num_keys; }
  
  const int32_t get_num_children() const
  { return page_hdr.num_children; }

  const int32_t get_prev_leaf() const
  { return page_hdr.prev_leaf; }
  
  const int32_t get_next_leaf() const
  { return page_hdr.next_leaf; }

  const bool get_is_leaf() const
  { return page_hdr.is_leaf; }
 
  void set_page_header(const IndexPageHdr new_header) {
    handler_ptr->is_dirty = true;
    page_hdr = new_header;
  }

  void set_parent(int32_t new_parent) { 
    handler_ptr->is_dirty = true;
    page_hdr.parent = new_parent; 
  }
  
  void set_num_keys(int32_t new_num_keys) { 
    handler_ptr->is_dirty = true;
    page_hdr.num_keys = new_num_keys; 
  }
  
  void set_num_children(int32_t new_num_children) { 
    handler_ptr->is_dirty = true;
    page_hdr.num_children = new_num_children; 
  }
  
  void set_next_free(int32_t new_next_free) { 
    handler_ptr->is_dirty = true;
    page_hdr.next_free = new_next_free; 
  }
  
  void set_prev_leaf(int32_t new_prev_leaf) { 
    handler_ptr->is_dirty = true;
    page_hdr.prev_leaf = new_prev_leaf; 
  }

  void set_next_leaf(int32_t new_next_leaf) { 
    handler_ptr->is_dirty = true;
    page_hdr.next_leaf = new_next_leaf; 
  }

  void set_is_leaf(bool new_is_leaf) { 
    handler_ptr->is_dirty = true;
    page_hdr.is_leaf = new_is_leaf; 
  }

  /* directly updating the page_hdr doesn't make the 
     page dirty, so updates may not be reflected, 
     use setters */
  IndexPageHdr page_hdr;
  Handler*     handler_ptr;

private:
  PageResponse shift_keys(const int32_t key_idx,
                          const int32_t shift_size);

  PageResponse shift_rids(const int32_t rid_idx,
                          const int32_t shift_size);

  off_t key_idx_to_offset(const int32_t key_idx) const { 
    return meta_data_ptr->get_key_offset() + 
           meta_data_ptr->get_key_size() * key_idx; 
  }

  off_t rid_idx_to_offset(const int32_t rid_idx) const {
    return meta_data_ptr->get_rid_offset() + 
           sizeof(RecId) * rid_idx;
  }
  
  RecId read_rid(const off_t offset, RecId& rid) {
    assert(offset < PAGE_SIZE);
    const uint8_t* data_ptr = handler_ptr->page_ptr->data() + offset;
    
    std::memcpy(&rid.page_num, data_ptr, sizeof(rid.page_num));
    
    data_ptr += sizeof(rid.page_num);
    std::memcpy(&rid.slot_num, data_ptr, sizeof(rid.slot_num));

    return rid;
  }
  
  void write_rid(const RecId& rid, const off_t offset) {
    assert(offset < PAGE_SIZE);
    uint8_t* page_offset = handler_ptr->page_ptr->data() + offset;

    std::memcpy(page_offset, &rid.page_num, sizeof(rid.page_num));
    page_offset += sizeof(rid.page_num);

    std::memcpy(page_offset, &rid.slot_num, sizeof(rid.slot_num));
  }

  const IndexMetaData* meta_data_ptr; 
  int32_t              timestamp;
  RecordLayout         key_layout;
};

