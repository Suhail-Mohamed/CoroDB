#pragma once

#include <algorithm>
#include <filesystem>
#include <span>
#include <sstream>

#include "BTree.hpp"
#include "DiskManager.hpp"
#include "FileDescriptor.hpp"
#include "IndexMetaData.hpp"
#include "Iouring.hpp"
#include "TableRecord.hpp"
#include "Util.hpp"

static constexpr int32_t IDX_HEADER_SIZE  = sizeof(int32_t) + sizeof(int32_t); 
static constexpr bool    INSERT_INTO_TREE = true;
static constexpr bool    DELETE_FROM_TREE = false;

struct IndexManager {
  IndexManager(std::filesystem::path index_folder_path);

  /* create a new index */
  Task<PageResponse> create_index(const std::span<std::string> new_index, 
                                  const int32_t                num_attr,
                                  const RecordLayout&          index_layout); 
 
  /* gets a btree index based on the attr_list, if no index exists returns 
     an undefined BTree */
  Task<BTree> get_index(std::vector<std::string>& attr_list);
  Task<BTree> get_index(const std::span<std::string> attr_list, 
                        const int32_t                num_attr);

  /* get a BTree when given an index, not recommended unless you know 
     index exists */
  BTree get_index(const int32_t index_id) 
  { return get_btree(index_id); }
  
  Task<int32_t> find_index(const std::span<std::string> attr_list,
                           const int32_t                num_attr);
  Task<int32_t> find_index(std::vector<std::string>& attr_list)
  { co_return co_await find_index(attr_list, attr_list.size()); }

  /* when a new tuple is added we add its content to all indexes that 
     this table has */
  Task<void> insert_into_indexes(const TableRecord& table_record,
                                 const RecId rec_id)
  { co_await update_trees(table_record, rec_id, INSERT_INTO_TREE); }
  
  /* when a new tuple is removed we remove its content from all indexes that 
     this table has */
  Task<void> delete_from_indexes(const TableRecord& table_record,
                                 const RecId rec_id) 
  { co_await update_trees(table_record, rec_id, DELETE_FROM_TREE); }

private:
  Task<void>    update_trees(const TableRecord& table_record,
                             const RecId        rec_id,
                             const bool         is_insert);
  BTree         get_btree(const int32_t index_num);
  Task<void>    init_index_folder(const std::string   new_index_name,
                                  const RecordLayout& index_layout);
  
  void read_header() {
    assert(handler_ptr);
    page_cursor = *reinterpret_cast<int32_t*>(handler_ptr->page_ptr);
    num_index   = *reinterpret_cast<int32_t*>(handler_ptr->page_ptr + sizeof(page_cursor));
  }

  void update_header() {
    std::memcpy(handler_ptr->page_ptr, 
                &page_cursor, 
                sizeof(page_cursor));
    
    std::memcpy(handler_ptr->page_ptr + sizeof(page_cursor), 
                &num_index, 
                sizeof(num_index));
  }

  Task<void> load_catalog() {
    bool should_read_header = (handler_ptr == nullptr);
    handler_ptr = co_await DiskManager::get_instance().read_page(catalog_file.fd, 
                                                                 0, 
                                                                 RecordLayout{});
    page_timestamp = handler_ptr->page_timestamp;
    if (should_read_header) read_header();
  }
  
  int32_t               num_index; 
  int32_t               page_timestamp;
  off_t                 page_cursor; 
  Handler*              handler_ptr;
  FileDescriptor        catalog_file;
  std::filesystem::path parent_index_folder;
};
