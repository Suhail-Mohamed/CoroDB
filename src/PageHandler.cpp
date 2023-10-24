#include "PageHandler.hpp"

void PageHandler::init_handler(Page*              page,
                               const RecordLayout layout,
                               const int32_t      fd,
                               const int32_t      timestamp,
                               const int32_t      p_id,
                               const PageType     p_type) 
{
  page_id        = p_id;
  page_timestamp = timestamp;
  page_ref       = 1;
  page_fd        = fd;
  page_type      = p_type;
  page_ptr       = page;
  is_pinned      = true;
  record_layout  = layout;
  page_usage     = 1;
  record_size    = calc_record_size(layout);
  
  if (page_type == PageType::NonPersistent) {
    num_records = 0;
    page_cursor = HEADER_SIZE;
  } else {
    num_records = read_header();
    page_cursor = HEADER_SIZE + record_size * num_records;
  }
} 

/********************************************************************************/

PageResponse PageHandler::add_record(Record& record, 
                                     const int32_t timestamp) 
{
  if (timestamp != DEFAULT_TIMESTAMP && timestamp != page_timestamp)
    return PageResponse::InvalidTimestamp;

  std::unique_lock lock{rw_mutex};
  PinGuard pin_guard{is_pinned};

  if (is_full())
    return PageResponse::PageFull;

  off_t write_offset = page_cursor;
  for (size_t i = 0; i < record_layout.size(); ++i)
      if (write_to_page(write_offset, record[i], record_layout[i]) ==
          PageResponse::InvalidRecord)
        return PageResponse::InvalidRecord;
  
  ++num_records;
  page_cursor = write_offset;

  ++page_usage;
  is_dirty = true;

  return PageResponse::Success;
}

/********************************************************************************/

PageResponse PageHandler::delete_record(const uint32_t record_num,
                                        const int32_t  timestamp) 
{
  if (timestamp != DEFAULT_TIMESTAMP && timestamp != page_timestamp)
    return PageResponse::InvalidTimestamp;

  std::unique_lock lock{rw_mutex};
  PinGuard pin_guard{is_pinned};

  if (num_records == 0) 
    return PageResponse::PageEmpty;
 
  if (record_num >= num_records)
    return PageResponse::InvalidRecord;
  
  /* move the record to the last record of the table and move page
     cursor back 1 record size */
  if (record_num != num_records - 1 && 
      move_record(num_records - 1, record_num) == 
      PageResponse::InvalidRecord)
    return PageResponse::InvalidRecord;

  --num_records;
  page_cursor -= record_size;

  ++page_usage;
  is_dirty = true;
  
  return PageResponse::Success;
}

/********************************************************************************/

PageResponse PageHandler::update_record(const uint32_t record_num,
                                        Record&        new_record,
                                        const int32_t  timestamp)
{
  if (timestamp != DEFAULT_TIMESTAMP && timestamp != page_timestamp)
    return PageResponse::InvalidTimestamp;

  std::unique_lock lock{rw_mutex};
  PinGuard pin_guard{is_pinned};
  
  if (num_records == 0) 
    return PageResponse::PageEmpty;
 
  if (record_num >= num_records)
    return PageResponse::InvalidRecord;
   
  off_t write_offset = record_num_to_offset(record_num);
  
  if (write_offset > page_cursor)
    return PageResponse::PageFull;

  for (size_t i = 0; i < record_layout.size(); ++i)
      if (write_to_page(write_offset, new_record[i], record_layout[i]) ==
          PageResponse::InvalidRecord)
        return PageResponse::InvalidRecord;

  return PageResponse::Success;
}

/********************************************************************************/

/* zero based indexing for record_num, ie: first record is record_num = 0 */
RecordResponse PageHandler::read_record(const uint32_t record_num,
                                        const LockOpt  l_opt,
                                        const int32_t  timestamp) 
{
  Record ret_record;
  
  if (timestamp != DEFAULT_TIMESTAMP && timestamp != page_timestamp)
    return {std::move(ret_record), PageResponse::InvalidTimestamp};
  
  if (l_opt == LockOpt::Lock) std::shared_lock lock{rw_mutex};
  PinGuard pin_guard{is_pinned};

  if (num_records == 0) 
    return {std::move(ret_record), PageResponse::PageEmpty};
 
  if (record_num >= num_records)
    return {std::move(ret_record), PageResponse::InvalidRecord};
  
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

PageResponse PageHandler::move_record(const uint32_t from_record, 
                                      const uint32_t to_record)
{
  if (to_record > num_records || from_record > num_records)
    return PageResponse::InvalidRecord;

  off_t  write_offset = record_num_to_offset(to_record);
  Record old_record   = read_record(from_record, 
                                    LockOpt::DontLock).record;

  for (size_t i = 0; i < record_layout.size(); ++i)
    if (write_to_page(write_offset, old_record[i], record_layout[i]) ==
        PageResponse::InvalidRecord)
      return PageResponse::InvalidRecord;

  return PageResponse::Success;
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

