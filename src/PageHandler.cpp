#include "PageHandler.hpp"

void PageHandler::init_handler(Page*              page,
                               const RecordLayout layout,
                               const int32_t      fd,
                               const int32_t      p_id,
                               const PageType     p_type) 
{
  page_id       = p_id;
  page_ref      = 1;
  page_fd       = fd;
  page_type     = p_type;
  page_ptr      = page;
  is_pinned     = true;
  record_layout = layout;
  page_usage    = 1;
  record_size   = calc_record_size(layout);
  
  if (page_type == PageType::NonPersistent) {
    num_records = 0;
    page_cursor = HEADER_SIZE;
  } else {
    num_records = get_num_records();
    page_cursor = HEADER_SIZE + record_size * num_records;
  }

  tomb_stones.clear();
} 

/********************************************************************************/

PageResponse PageHandler::write_to_page(off_t&      write_offset, 
                                        RecordData& record_data, 
                                        const DatabaseType& db_type) 
{
  void* page_offset = page_ptr->data() + write_offset;

  switch (db_type.type) {
    case Type::Integer:
      if (!std::holds_alternative<int32_t>(record_data))
        return PageResponse::InvalidRecord;
      
      std::memcpy(page_offset, 
                  &std::get<int32_t>(record_data),
                  db_type.type_size); 
      break;
    case Type::Float:
      if (!std::holds_alternative<float>(record_data))
        return PageResponse::InvalidRecord;
      
      std::memcpy(page_offset, 
                  &std::get<float>(record_data),
                  db_type.type_size); 
      break;
    case Type::String:
      if (!std::holds_alternative<std::string>(record_data))
        return PageResponse::InvalidRecord;
      
      std::get<std::string>(record_data).
        resize(db_type.type_size, '\0');
      
      std::memcpy(page_offset, 
                  std::get<std::string>(record_data).c_str(),
                  db_type.type_size); 
      break;
    default: throw std::runtime_error("Error: Invalid database type used");
  }

  write_offset += db_type.type_size;
  return PageResponse::Success;
}

/********************************************************************************/

size_t PageHandler::read_from_page(const off_t read_offset, 
                                   RecordData& record_data, 
                                   const DatabaseType& db_type) 
{
  const void* page_offset = page_ptr->data() + read_offset;

  switch (db_type.type) {
    case Type::Integer: 
      record_data = 
        *reinterpret_cast<const int32_t*>(page_offset);
      break;
    case Type::Float:
      record_data = 
        *reinterpret_cast<const float*>(page_offset);
      break;
    case Type::String :
      record_data = std::string(
          reinterpret_cast<const char*>(page_offset), 
          db_type.type_size);
      break;
    default: throw std::runtime_error("Error: Invalid database type used");
  }

  return db_type.type_size;
}

/********************************************************************************/

PageResponse PageHandler::add_record(Record& record) {
  bool  from_tomb = false;
  off_t write_offset;

  if (!tomb_stones.empty()) {
    write_offset = record_num_to_offset(tomb_stones.back()); 
    from_tomb    = true;
  } else if (page_cursor + record_size < PAGE_SIZE)
    write_offset = page_cursor;
  else return PageResponse::PageFull;

  for (size_t i = 0; i < record_layout.size(); ++i)
      if (write_to_page(write_offset, record[i], record_layout[i]) ==
          PageResponse::InvalidRecord)
        return PageResponse::InvalidRecord;
  
  if (from_tomb) 
    tomb_stones.pop_back();
  else ++num_records;

  page_cursor = std::max(page_cursor, write_offset);

  ++page_usage;
  is_dirty = true;

  return PageResponse::Success;
}

/********************************************************************************/

PageResponse PageHandler::delete_record(const uint32_t record_num) {
  if (num_records == 0) 
    return PageResponse::PageEmpty;
 
  if (record_num >= num_records || in_tomb_stones(record_num))
    return PageResponse::InvalidRecord;
  
  is_dirty = true;
  ++page_usage;
  
  if (record_num == num_records - 1) {
    page_cursor -= record_size;
    --num_records;
  } else tomb_stones.push_back(record_num);

  return PageResponse::Success;
}

/********************************************************************************/

/* zero based indexing for record_num, ie: first record is record_num = 0 */
RecordResponse PageHandler::read_record(const uint32_t record_num) {
  Record ret_record;
  
  if (record_num >= num_records)
    return {std::move(ret_record), PageResponse::InvalidRecord};

  if (in_tomb_stones(record_num))
    return {std::move(ret_record), PageResponse::DeletedRecord};
  
  off_t read_offset = record_num_to_offset(record_num);
  
  if (read_offset > page_cursor)
    return {std::move(ret_record), PageResponse::PageFull};
 
  ret_record.resize(record_layout.size());

  for (size_t i = 0; i < record_layout.size(); ++i)
    read_offset += 
      read_from_page(read_offset, ret_record[i], record_layout[i]);
  
  ++page_usage;
  return {std::move(ret_record), PageResponse::Success};
}

/********************************************************************************/

PageResponse PageHandler::move_record(const uint32_t new_record_num, 
                                      Record& record)
{
  if (new_record_num > num_records)
    return PageResponse::InvalidRecord;

  off_t write_offset = record_num_to_offset(new_record_num);

  for (size_t i = 0; i < record_layout.size(); ++i)
    if (write_to_page(write_offset, record[i], record_layout[i]) ==
        PageResponse::InvalidRecord)
      return PageResponse::InvalidRecord;

  return PageResponse::Success;
}

/********************************************************************************/

PageResponse PageHandler::compact_page() {
  if (tomb_stones.empty()) return PageResponse::Success;

  const size_t tomb_stones_size = tomb_stones.size();
  std::sort(std::begin(tomb_stones), 
            std::end(tomb_stones), std::greater<int>());

  for (uint32_t r_num = num_records - 1; 
       record_num_to_offset(r_num) >= record_num_to_offset(tomb_stones.back()); 
       --r_num) 
  {
    auto [record, read_resp] = read_record(r_num);
    if (read_resp == PageResponse::DeletedRecord) {
      page_cursor -= record_size;
      continue;
    }
    
    if (read_resp != PageResponse::Success)
      return read_resp;
    
    if (auto move_resp = move_record(tomb_stones.back(), record);
        move_resp != PageResponse::Success)
      return move_resp;
    
    page_cursor -= record_size;
    tomb_stones.pop_back();
  } 

  num_records -= tomb_stones_size;
  tomb_stones.clear();
  return PageResponse::Success;
}
