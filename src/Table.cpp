#include "Table.hpp"

Table::Table(std::filesystem::path dir, 
             const TableMetaData& m_data)
  : disk_manager{DiskManager::get_instance()}, 
    table_name{dir.filename()},
    meta_data{m_data},
    table_dir{dir}
{
  if (!std::filesystem::is_directory(table_dir))
    throw std::runtime_error("Error: table constructor expects table directory");
  
  auto meta_data_file = table_dir / std::string(table_name + ".data");
  if (!std::filesystem::exists(meta_data_file))
    throw std::runtime_error("Error: metadata file should have been created by DB manager");

  meta_data.data_file = meta_data_file.string();
}


/********************************************************************************/

TableResponse Table::execute_command(const SQLStatement &sql_stmt){
  return TableResponse::Success;
}

/********************************************************************************/

TableResponse Table::execute_delete(const SQLStatement& sql_stmt) {
  return TableResponse::Success;
}

/********************************************************************************/

TableResponse Table::execute_update(const SQLStatement& sql_stmt) {
  return TableResponse::Success;
}

/********************************************************************************/

TableResponse execute_select(const SQLStatement& sql_stmt,
                             std::vector<Record>& records) 
{
  return TableResponse::Success;
}

/********************************************************************************/

bool Table::apply_clause(const Record& record,
                         const ASTTree& clause)
{
  return false;
}

/********************************************************************************/

Task<void> Table::read_page(FileDescriptor page_file) {
  PageHandler* pg_h = co_await disk_manager.read_page(page_file.fd, 
                                                      meta_data.layout);
  table_pages.emplace_back(pg_h, pg_h->get_timestamp());
}

