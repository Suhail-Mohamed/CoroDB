#include "Table.hpp"
#include "FileDescriptor.hpp"
#include "PageHandler.hpp"
#include <filesystem>
#include <stdexcept>

Table::Table(std::filesystem::path dir, 
             const TableMetaData& m_data)
  : disk_manager{DiskManager::get_instance()}, 
    table_name  {dir.filename()},
    meta_data   {m_data},
    table_dir   {dir}
{
  if (!std::filesystem::is_directory(table_dir))
    throw std::runtime_error("Error: table constructor expects valid table directory");
}


/********************************************************************************/

Task<QueryReturn> Table::execute_command(const SQLStatement &sql_stmt){
  co_return QueryReturn{TableResponse::Success};
}

/********************************************************************************/

Task<PageResponse> Table::execute_delete(const SQLStatement& sql_stmt,
                                         const std::filesystem::directory_entry& page) 
{
  TablePage t_pg    = co_await read_page(page.path());
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
    if (auto response = t_pg.pg_h->delete_record(rec_num);
        response != PageResponse::Success)
      co_return response;

  co_return PageResponse::Success;
}

/********************************************************************************/

Task<PageResponse> Table::execute_update(const SQLStatement& sql_stmt,
                                         const std::filesystem::directory_entry& page) 
{
  TablePage t_pg = co_await read_page(page.path());
  
  for (auto [record, response] : t_pg) {
    if (response != PageResponse::Success)
      co_return response;

    if (apply_clause(record, sql_stmt.where_tree))
      for (int32_t n_set = 0; n_set < sql_stmt.num_set; ++n_set)
        set_attribute(record, 
                      sql_stmt.set_attr[n_set], 
                      sql_stmt.set_value[n_set]);
  }

  co_return PageResponse::Success;
}

/********************************************************************************/

Task<PageResponse> Table::execute_insert(const SQLStatement& sql_stmt,
                                         const Record&       potential_insert,
                                         const std::filesystem::directory_entry& page) 
{
  TablePage t_pg = co_await read_page(page.path());
  
  for (const auto& [record, response] : t_pg) {
    if (response != PageResponse::Success)
      co_return response;

    if(same_primary_key(record, potential_insert))
      co_return PageResponse::Failure;
  }
  
  co_return PageResponse::Success;
}

/********************************************************************************/

Task<PageResponse> Table::execute_select_no_join(const SQLStatement&  sql_stmt,
                                                 std::vector<Record>& records,
                                                 const std::filesystem::directory_entry& page) 
{
  TablePage t_pg = co_await read_page(page.path());
  
  for (const auto& [record, response] : t_pg) {
    if (response != PageResponse::Success)
      co_return response;

    if (apply_clause(record, sql_stmt.where_tree))
      records.push_back(record);
  }

  co_return PageResponse::Success;
}

/********************************************************************************/

void Table::set_attribute(Record& record, 
                          const std::string& attr,
                          const std::string& attr_value) 
{
  size_t attr_idx  = get_attr_idx(attr);
  record[attr_idx] = cast_to(attr_value, meta_data.layout[attr_idx]); 
}

/********************************************************************************/

bool Table::same_primary_key(const Record& table_record, 
                             const Record& potential_insert)
{
  int32_t num_equal = 0;

  for (const auto& key_attr : meta_data.primary_key) {
    size_t attr_idx = get_attr_idx(key_attr);
    if (table_record[attr_idx] == potential_insert[attr_idx])
      ++num_equal;
  }

  return num_equal == meta_data.num_primary;
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
  auto itr = std::find(std::begin(meta_data.attr_list),
                       std::end(meta_data.attr_list), attr);
  
  if (itr == std::end(meta_data.attr_list)) 
    throw std::runtime_error("Error: Attribute does not exist in record");

  return std::distance(std::begin(meta_data.attr_list), itr);
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
                                   meta_data.layout[attr_idx]);
    
    return clause[layer].comp(record[attr_idx], 
                              comp_data);
  } else
    return clause[layer].conj(apply_clause(record, clause, right(layer)),
                              apply_clause(record, clause, left(layer)));
}

/********************************************************************************/

Task<TablePage> Table::read_page(const std::string page_path) {
  FileDescriptor page_file {page_path};
  PageHandler*   pg_h = co_await disk_manager.read_page(page_file.fd, 
                                                        meta_data.layout);
  co_return TablePage{pg_h, pg_h->get_timestamp()};
}

