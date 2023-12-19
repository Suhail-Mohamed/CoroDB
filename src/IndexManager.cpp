#include "IndexManager.hpp"

IndexManager::IndexManager(std::filesystem::path index_folder_path)
  : parent_index_folder{index_folder_path},
    handler_ptr        {nullptr}
{ 
  const auto catalog_path = index_folder_path / "CATALOG_FILE";
  if (!std::filesystem::exists(catalog_path)) {
    catalog_file = FileDescriptor{catalog_path, OpenMode::Create};
    page_cursor  = IDX_HEADER_SIZE;
    num_index    = 0;
  } else
    catalog_file = FileDescriptor{catalog_path};
};

/********************************************************************************/

Task<PageResponse> IndexManager::create_index(const std::span<std::string> new_index,
                                              const int32_t                num_attr,
                                              const RecordLayout&          index_layout)
{  
  if (co_await find_index(new_index, num_attr) != -1)
    co_return PageResponse::Success;
  
  PinGuard pin {handler_ptr->is_pinned};
  int32_t  total_size = 0;

  for (int32_t i = 0; i < num_attr; ++i)
    total_size += new_index[i].size();

  if (page_cursor + total_size + sizeof(num_index) > PAGE_SIZE)
    co_return PageResponse::PageFull;

  Page& catalog_page = *handler_ptr->page_ptr;
  for (int32_t cur_str = 0; cur_str < num_attr; ++cur_str) {
    for (char c : new_index[cur_str])
      catalog_page[page_cursor++] = c;

    if (cur_str < num_attr - 1)
      catalog_page[page_cursor++] = ',';
  }
  
  std::memcpy(&catalog_page[page_cursor], 
              &num_index, 
              sizeof(num_index));
  
  page_cursor += sizeof(int32_t);
  catalog_page[page_cursor++] = '\n';

  co_await init_index_folder("INDEX" + std::to_string(num_index),
                             index_layout);    
  ++num_index;
  co_return PageResponse::Success;
}

/********************************************************************************/

Task<BTree> IndexManager::get_index(std::vector<std::string>& attr_list) {
  const int32_t get_index_id = co_await find_index(attr_list, attr_list.size());
  if (get_index_id == -1) co_return BTree{};

  co_return get_btree(get_index_id);
}

/********************************************************************************/

Task<BTree> IndexManager::get_index(const std::span<std::string> attr_list,
                                    const int32_t                num_attr)
{
  const int32_t get_index_id = co_await find_index(attr_list, num_attr);
  if (get_index_id == -1) co_return BTree{};

  co_return get_btree(get_index_id);
}

/********************************************************************************/

Task<int32_t> IndexManager::find_index(const std::span<std::string> attr_list,
                                       const int32_t                num_attr) 
{
  if (!handler_ptr || !handler_ptr->is_valid_timestamp(page_timestamp))
    co_await load_catalog();

  PinGuard pin {handler_ptr->is_pinned};
  
  std::string current_line;
  std::string attribute;

  Page& catalog_page = *handler_ptr->page_ptr;
  for (int32_t i = 0; i < page_cursor; ++i) {
    if (catalog_page[i] == '\n') {
      const auto last_comma = current_line.find_last_of(',');
      std::stringstream ss {current_line.substr(0, last_comma)};
      
      int32_t attr_count = 0;
      int32_t is_equal   = 0;
      const int32_t cur_index_id = std::stoi(current_line.substr(last_comma + 1));

      while (std::getline(ss, attribute,',') && 
             attr_count < num_attr) 
        is_equal += (attribute == attr_list[attr_count++]); 
      
      if (is_equal == num_attr) 
        co_return cur_index_id;
      
      current_line.clear();
    } else 
      current_line += static_cast<char>(catalog_page[i]);
  }
  
  co_return -1;
}

/********************************************************************************/

Task<void> IndexManager::update_trees(const TableRecord& table_record,
                                      const RecId        rec_id,
                                      const bool         is_insert)
{
  if (!handler_ptr || !handler_ptr->is_valid_timestamp(page_timestamp))
    co_await load_catalog();

  PinGuard pin {handler_ptr->is_pinned};
  
  std::string current_line;
  std::string attribute;

  Page& catalog_page = *handler_ptr->page_ptr;
  for (int32_t i = 0; i < page_cursor; ++i) {
    if (catalog_page[i] == '\n') {
      const auto last_comma = current_line.find_last_of(',');
      std::stringstream ss {current_line.substr(0, last_comma)};
      
      std::vector<std::string> index_attr;
      const int32_t cur_index_id = std::stoi(current_line.substr(last_comma + 1));

      while (std::getline(ss, attribute,','))
        index_attr.push_back(attribute);

      BTree tree {std::move(get_btree(cur_index_id))};
      
      if (is_insert)
        co_await tree.insert_entry(table_record.get_subset(index_attr), rec_id);
      else
        co_await tree.delete_entry(table_record.get_subset(index_attr), rec_id);
      
      current_line.clear();
    } else 
      current_line += static_cast<char>(catalog_page[i]);
  }
}

/********************************************************************************/

BTree IndexManager::get_btree(const int32_t index_num) {
  const auto folder_name  = "INDEX" + std::to_string(index_num);
  const auto index_folder = parent_index_folder / folder_name;

  if (!std::filesystem::exists(index_folder))
    throw std::runtime_error("Error: Trying to access index folder that does not exists");

  const auto meta_data_file  = index_folder / "META_DATA";
  const auto index_data_file = index_folder / "INDEX_DATA"; 

  return BTree{IndexMetaData {meta_data_file},
               FileDescriptor{index_data_file}};
}

/********************************************************************************/

Task<void> IndexManager::init_index_folder(const std::string   new_index_name,
                                           const RecordLayout& index_layout) 
{
  const auto new_index_folder_path = parent_index_folder / new_index_name; 
  const auto new_meta_data_file    = new_index_folder_path / "META_DATA";
  const auto new_index_data_file   = new_index_folder_path / "INDEX_DATA";
  
  FileDescriptor{new_meta_data_file, OpenMode::Create};
  IndexMetaData {index_layout, new_meta_data_file};
  
  const auto data_file_fd     = FileDescriptor{new_index_data_file, OpenMode::Create};
  Handler* index_data_handler = co_await DiskManager::get_instance().create_page(data_file_fd.fd, 
                                                                                 0,
                                                                                 index_layout);
  IndexPageHdr{index_data_handler};
}
