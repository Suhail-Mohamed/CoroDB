#pragma once

#include <cstring>

#include "FileDescriptor.hpp"
#include "Iouring.hpp"
#include "Util.hpp"


/********************************************************************************/

constexpr int32_t NO_PARENT    = -1;
constexpr int32_t NO_KEYS      = 0;
constexpr int32_t NO_KIDS      = 0;
constexpr int32_t NO_PREV_LEAF = -1;
constexpr int32_t NO_NEXT_LEAF = -1;
constexpr int32_t NO_FREE_PAGE = -1;

struct IndexPageHdr {
  IndexPageHdr()
    : parent      {NO_PARENT},
      next_free   {NO_FREE_PAGE},
      num_keys    {NO_KEYS},
      num_children{NO_KIDS},
      prev_leaf   {NO_PREV_LEAF},
      next_leaf   {NO_NEXT_LEAF},
      is_leaf     {false}
  {};
  
  IndexPageHdr(Handler* init_page)
    : parent      {NO_PARENT},
      next_free   {NO_FREE_PAGE},
      num_keys    {NO_KEYS},
      num_children{NO_KIDS},
      prev_leaf   {NO_PREV_LEAF},
      next_leaf   {NO_NEXT_LEAF},
      is_leaf     {false}
  { write_header(init_page->page_ptr); };

  IndexPageHdr(int32_t parent_page,
               int32_t next_free_page,  
               int32_t number_keys,
               int32_t number_children, 
               int32_t prev_leaf_page, 
               int32_t next_leaf_page,
               bool is_leaf_page)
    : parent      {parent_page},
      next_free   {next_free_page},
      num_keys    {number_keys},
      num_children{number_children},
      prev_leaf   {prev_leaf_page},
      next_leaf   {next_leaf_page},
      is_leaf     {is_leaf_page} 
  {};

  void write_header(Page* page) {
    assert(page);
    off_t page_offset = 0;

    std::memcpy(page->data() + page_offset, &parent, sizeof(parent));
    page_offset += sizeof(parent);

    std::memcpy(page->data() + page_offset, &next_free, sizeof(next_free));
    page_offset += sizeof(next_free);

    std::memcpy(page->data() + page_offset, &num_keys, sizeof(num_keys));
    page_offset += sizeof(num_keys);

    std::memcpy(page->data() + page_offset, &num_children, sizeof(num_children));
    page_offset += sizeof(num_children);

    std::memcpy(page->data() + page_offset, &prev_leaf, sizeof(prev_leaf));
    page_offset += sizeof(prev_leaf);

    std::memcpy(page->data() + page_offset, &next_leaf, sizeof(next_leaf));
    page_offset += sizeof(next_leaf);

    std::memcpy(page->data() + page_offset, &is_leaf, sizeof(is_leaf));
  }

  void read_header(const Page* page) {
    assert(page);
    off_t page_offset = 0;

    parent = *reinterpret_cast<const int32_t*>(page_offset);
    page_offset += sizeof(parent);
    
    next_free = *reinterpret_cast<const int32_t*>(page_offset);
    page_offset += sizeof(next_free);
    
    num_keys = *reinterpret_cast<const int32_t*>(page_offset);
    page_offset += sizeof(num_keys);
  
    num_children = *reinterpret_cast<const int32_t*>(page_offset);
    page_offset += sizeof(num_children);
    
    prev_leaf = *reinterpret_cast<const int32_t*>(page_offset);
    page_offset += sizeof(prev_leaf);

    next_leaf = *reinterpret_cast<const int32_t*>(page_offset);
    page_offset += sizeof(next_leaf);

    is_leaf = *reinterpret_cast<const bool*>(page_offset);
  }

  int32_t parent;
  int32_t next_free;
  int32_t num_keys;
  int32_t num_children;
  int32_t prev_leaf;
  int32_t next_leaf;
  bool    is_leaf;
};

/********************************************************************************/

constexpr int32_t TREE_ORDER  = PAGE_SIZE; 
constexpr int32_t EMPTY_INDEX = -1;

struct IndexMetaData {
  IndexMetaData()
    : num_pages     {EMPTY_INDEX},
      root_page     {EMPTY_INDEX},
      first_free_pg {NO_FREE_PAGE},
      first_leaf    {EMPTY_INDEX},
      last_leaf     {EMPTY_INDEX},
      key_size      {calc_record_size(key_layout)}
  {};

  IndexMetaData(RecordLayout key,
                const std::string data_file) 
    : IndexMetaData{} 
  {
    key_layout     = key;
    meta_data_file = data_file;

    btree_order = (PAGE_SIZE - sizeof(IndexPageHdr)) / (key_size + sizeof(RecId));
    assert(btree_order > 2);

    num_key_attr = key_layout.size();
    key_offset   = sizeof(IndexPageHdr);
    rid_offset   = key_offset + key_size * btree_order;
    write_meta_data();
  };

  IndexMetaData(RecordLayout key,
                const std::filesystem::path path)
    : IndexMetaData{key, path.string()}
  {};

  IndexMetaData(const std::string data_file)
    : meta_data_file{data_file} 
  { read_meta_data(); };

  IndexMetaData(const std::filesystem::path data_file)
    : meta_data_file{data_file.string()} 
  { read_meta_data(); };
  
  const int32_t get_order() const 
  { return btree_order; }
  
  const int32_t get_num_pages() const 
  { return num_pages; }
  
  const int32_t get_root_page() const 
  { return root_page; }
  
  const int32_t get_first_free_page() const 
  { return first_free_pg; }
  
  const int32_t get_first_leaf() const
  { return first_leaf; }
  
  const int32_t get_last_leaf() const
  { return last_leaf; }

  const int32_t get_key_size() const
  { return key_size; }

  const int32_t get_num_key_attr() const
  { return num_key_attr; }

  const int32_t get_key_offset() const
  { return key_offset; }
  
  const int32_t get_rid_offset() const
  { return rid_offset; }

  const RecordLayout& get_key_layout() const
  { return key_layout; }
 
  void increase_num_pages() { ++num_pages; }
  void decrease_num_pages() { --num_pages; }
  
  void set_first_free_page(int32_t free_page)
  { first_free_pg = free_page; }
  
  void set_root_page(int32_t new_root_page)
  { root_page = new_root_page; }

  void set_last_leaf(int32_t new_last_leaf)
  { last_leaf = new_last_leaf; }

private:
  void write_meta_data() {
    FileDescriptor out{meta_data_file};

    out.file_write(&btree_order  , sizeof(btree_order));
    out.file_write(&num_pages    , sizeof(num_pages));
    out.file_write(&root_page    , sizeof(root_page));
    out.file_write(&first_free_pg, sizeof(first_free_pg));
    out.file_write(&first_leaf   , sizeof(first_leaf));
    out.file_write(&last_leaf    , sizeof(last_leaf));
    out.file_write(&key_size     , sizeof(key_size));
    out.file_write(&num_key_attr , sizeof(num_key_attr));
    out.file_write(&key_offset   , sizeof(key_offset));
    out.file_write(&rid_offset   , sizeof(rid_offset));

    for (int32_t i = 0; i < num_key_attr; ++i)
      out.file_write(&key_layout[i], sizeof(DatabaseType));
  }

  void read_meta_data() {
    FileDescriptor in{meta_data_file};
    
    in.file_read(&btree_order  , sizeof(btree_order));
    in.file_read(&num_pages    , sizeof(num_pages));
    in.file_read(&root_page    , sizeof(root_page));
    in.file_read(&first_free_pg, sizeof(first_free_pg));
    in.file_read(&first_leaf   , sizeof(first_leaf));
    in.file_read(&last_leaf    , sizeof(last_leaf));
    in.file_read(&key_size     , sizeof(key_size));
    in.file_read(&num_key_attr , sizeof(num_key_attr));
    in.file_read(&key_offset   , sizeof(key_offset));
    in.file_read(&rid_offset   , sizeof(rid_offset));

    for (int32_t i = 0; i < num_key_attr; ++i) {
      DatabaseType db_type;
      in.file_read(&db_type, sizeof(db_type));
      key_layout.push_back(db_type);
    }
  }

  int32_t btree_order; 
  int32_t num_pages;
  int32_t root_page;
  int32_t first_free_pg;
  int32_t first_leaf;
  int32_t last_leaf;
  int32_t key_size;
  int32_t num_key_attr;
  int32_t key_offset;
  int32_t rid_offset;

  std::string  meta_data_file;
  RecordLayout key_layout;
};
