#include "Iouring.hpp"

/********************************************************************************/
/*                              Handler functions                               */
/********************************************************************************/

PageResponse Handler::write_to_page(off_t&      write_offset, 
                                    RecordData& record_data, 
                                    const DatabaseType& db_type) 
{
  assert(page_ptr);
  uint8_t* page_offset = page_ptr->data() + write_offset;

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

size_t Handler::read_from_page(const off_t read_offset, 
                               RecordData& record_data, 
                               const DatabaseType& db_type) 
{
  assert(page_ptr);
  const uint8_t* page_offset = page_ptr->data() + read_offset;

  switch (db_type.type) {
    case Type::Integer: {
      int32_t value;
      std::memcpy(&value, page_offset, sizeof(value));
      record_data = value;
      break;
    }
    case Type::Float: {
      float value;
      std::memcpy(&value, page_offset, sizeof(value));
      record_data = value;
      break;
    }
    case Type::String:
      record_data = std::string(
          reinterpret_cast<const char*>(page_offset), db_type.type_size
        );
      break;
    default: throw std::runtime_error("Error: Invalid database type used");
  }

  return db_type.type_size;
}

/********************************************************************************/
/*                              Iouring functions                               */
/********************************************************************************/

void Iouring::for_each_cqe(const std::function<void(io_uring_cqe *)>& lambda) {
  io_uring_cqe* cqe  = nullptr;
  uint32_t      head = 0;

  io_uring_for_each_cqe(&ring, head, cqe) { lambda(cqe); }
}

/********************************************************************************/

uint32_t Iouring::submit_and_wait(const uint32_t wait_nr) {
  const uint32_t result = io_uring_submit_and_wait(&ring, wait_nr);
  
  if (result < 0) 
    throw std::runtime_error("Error: error submitting I/O requests");
  return result;
}

/********************************************************************************/

void Iouring::read_request(SqeData& sqe_data) {
  io_uring_sqe* sqe = io_uring_get_sqe(&ring);
  sqe->buf_group    = BGID;
  io_uring_prep_read(sqe, 
                     sqe_data.fd, 
                     nullptr, 
                     PAGE_SIZE, 
                     sqe_data.offset);
  io_uring_sqe_set_flags(sqe, IOSQE_BUFFER_SELECT);
  io_uring_sqe_set_data(sqe, &sqe_data);
}

/********************************************************************************/

void Iouring::write_request(SqeData& sqe_data) {
  io_uring_sqe* sqe = io_uring_get_sqe(&ring); 
  sqe->buf_group    = BGID;
  io_uring_prep_write(sqe, 
                      sqe_data.fd, 
                      sqe_data.page_data->data(), 
                      sqe_data.page_data->size(), 
                      sqe_data.offset);
  io_uring_sqe_set_data(sqe, &sqe_data);
}

/********************************************************************************/

void Iouring::register_buffer_ring(io_uring_buf_ring*                 buff_ring,
                                   std::array<Page, BUFF_RING_SIZE>&  buff_lst)
{
  io_uring_buf_reg reg_buf{.ring_addr    = reinterpret_cast<uint64_t>(buff_ring),
                           .ring_entries = BUFF_RING_SIZE,
                           .bgid         = BGID};

  if (io_uring_register_buf_ring(&ring, &reg_buf, 0) != 0)
    throw std::runtime_error("Error: failed to register buffer ring");
  
  /* buffer id is the index into the buffer_lst */
  io_uring_buf_ring_init(buff_ring);
  for (uint32_t b_id = 0; b_id < BUFF_RING_SIZE; ++b_id)
    io_uring_buf_ring_add(buff_ring,
                          buff_lst[b_id].data(),
                          buff_lst[b_id].size(), 
                          b_id,
                          io_uring_buf_ring_mask(BUFF_RING_SIZE),
                          b_id);

  io_uring_buf_ring_advance(buff_ring, BUFF_RING_SIZE);
}

/********************************************************************************/

void Iouring::add_buffer(io_uring_buf_ring* buff_ring,
                         Page&              buff, 
                         const uint32_t     buff_id)
{
  const uint32_t mask = io_uring_buf_ring_mask(BUFF_RING_SIZE);

  io_uring_buf_ring_add(buff_ring, 
                        buff.data(), 
                        buff.size(),
                        buff_id, 
                        mask,
                        buff_id);

  io_uring_buf_ring_advance(buff_ring, 1);
}
