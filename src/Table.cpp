#include "Table.hpp"

Table::Table(TableMetaData&  table_meta_data,
             FileDescriptor& table_pages_filedescriptor)
  : disk_manager   {DiskManager::get_instance()}, 
    meta_data      {table_meta_data},
    table_pages_fd {table_pages_filedescriptor} 
{}

/********************************************************************************/

Task<QueryReturn> Table::execute_command(const SQLStatement &sql_stmt){
  co_return QueryReturn{TableResponse::Success};
}

/********************************************************************************/

Task<PageResponse> Table::execute_delete(const SQLStatement& sql_stmt,
                                         const int32_t       page_num) 
{
  /*
  TablePage t_pg    = co_await read_page(page_num);
  uint32_t  rec_num = 0;
  std::vector<uint32_t> del_rec;
  
  for (const auto& [record, response] : t_pg) {
    if (response != PageResponse::Success)
      co_return response;

    if (apply_clause(record, sql_stmt.where_tree))
      del_rec.push_back(rec_num);

    ++rec_num;
  }

  for (uint32_t rec : del_rec)
    if (auto response = t_pg.pg_h->delete_record(rec);
        response != PageResponse::Success)
      co_return response;
  */
  co_return PageResponse::Success;
}

/********************************************************************************/

Task<PageResponse> Table::execute_update(const SQLStatement& sql_stmt,
                                         const int32_t       page_num) 
{
  /*
  TablePage t_pg = co_await read_page(page_num);
  
  for (auto [record, response] : t_pg) {
    if (response != PageResponse::Success)
      co_return response;

    if (apply_clause(record, sql_stmt.where_tree))
      for (int32_t n_set = 0; n_set < sql_stmt.num_set; ++n_set)
        set_attribute(record, 
                      sql_stmt.set_attr[n_set], 
                      sql_stmt.set_value[n_set]);
  }
  */
  co_return PageResponse::Success;
}

/********************************************************************************/

Task<PageResponse> Table::execute_insert(const SQLStatement& sql_stmt,
                                         const Record&       potential_insert,
                                         const int32_t       page_num)
{
  /*
  TablePage t_pg = co_await read_page(page_num);
  
  for (const auto& [record, response] : t_pg) {
    if (response != PageResponse::Success)
      co_return response;

    if(same_primary_key(record, potential_insert))
      co_return PageResponse::Failure;
  }
  */ 
  co_return PageResponse::Success;
}

/********************************************************************************/

Task<PageResponse> Table::execute_select_no_join(const SQLStatement&  sql_stmt,
                                                 std::vector<Record>& records,
                                                 const int32_t        page_num) 
{
  /*
  TablePage t_pg = co_await read_page(page_num);
  
  for (const auto& [record, response] : t_pg) {
    if (response != PageResponse::Success)
      co_return response;

    if (apply_clause(record, sql_stmt.where_tree))
      records.push_back(record);
  }
  */
  co_return PageResponse::Success;
}

/********************************************************************************/

void Table::set_attribute(Record& record, 
                          const std::string& attr,
                          const std::string& attr_value) 
{
  size_t attr_idx  = get_attr_idx(attr);
  record[attr_idx] = cast_to(attr_value, meta_data.get_record_layout()[attr_idx]); 
}

/********************************************************************************/

bool Table::same_primary_key(const Record& table_record, 
                             const Record& potential_insert)
{
  int32_t num_equal = 0;

  for (const auto& key_attr : meta_data.get_primary_key()) {
    size_t attr_idx = get_attr_idx(key_attr);
    if (table_record[attr_idx] == potential_insert[attr_idx])
      ++num_equal;
  }

  return num_equal == meta_data.get_num_primary();
}

/********************************************************************************/

RecordData Table::cast_to(const std::string&  attr_value, 
                          const DatabaseType& db_type) 
{
  switch (db_type.type) {
    case Type::String : return attr_value; break;
    case Type::Integer: return std::stoi(attr_value); break;
    case Type::Float  : return std::stof(attr_value); break;
    default: throw std::runtime_error("Error: Invalid DataType cannot cast");
  }
}

/********************************************************************************/

size_t Table::get_attr_idx(const std::string& attr) {
  auto itr = std::find(std::begin(meta_data.get_attr_lst()),
                       std::end(meta_data.get_attr_lst()), attr);
  
  if (itr == std::end(meta_data.get_attr_lst())) 
    throw std::runtime_error("Error: Attribute does not exist in record");

  return std::distance(std::begin(meta_data.get_attr_lst()), itr);
}

/********************************************************************************/

bool Table::apply_clause(const Record&  record,
                         const ASTTree& clause,
                         size_t layer)
{
  if (!clause[layer].comp && !clause[layer].conj) 
    return true;

  if (clause[layer].comp) {
    size_t     attr_idx  = get_attr_idx(clause[layer].lhs);
    RecordData comp_data = cast_to(clause[layer].rhs, 
                                   meta_data.get_record_layout()[attr_idx]);
    
    return clause[layer].comp(record[attr_idx], 
                              comp_data);
  } else
    return clause[layer].conj(apply_clause(record, clause, right(layer)),
                              apply_clause(record, clause, left(layer)));
}

/********************************************************************************/

Task<RecordPageHandler> Table::get_page(const int32_t page_num) {
  assert(page_num > meta_data.get_num_pages());

  Handler* handler = co_await disk_manager.read_page(table_pages_fd.fd,
                                                     page_num,
                                                     meta_data.get_record_layout());
  co_return RecordPageHandler{handler};
}
