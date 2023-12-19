#include "DatabaseManager.hpp"

Task<std::vector<TableRecord>> DatabaseManager::handle_query(const std::string query_string) {
  std::vector<TableRecord> ret_data;

  parser.parse_query(query_string);
  SQLStatement sql_stmt = parser.get_sql_stmt();

  co_await coro_pool.schedule();
  switch (sql_stmt.command) {
    case Command::Create: co_await create_table(sql_stmt); break;
    case Command::Drop  : drop_table(sql_stmt); break;
    default: ret_data = co_await table_query(sql_stmt); 
  }

  co_return ret_data;
}


/********************************************************************************/

void DatabaseManager::start_cmdline() {
  for (std::string line; std::cout << "CoroDB> " && std::getline(std::cin, line);) {
    if (!line.empty())
      auto ret_data = sync_wait(handle_query(line));
  }
}

/********************************************************************************/

Task<void> DatabaseManager::create_table(SQLStatement& sql_stmt) {
  if (loaded_tables.contains(sql_stmt.get_table_name())) co_return;

  const auto table_folder         = db_path / sql_stmt.get_table_name();
  const auto index_folder         = table_folder / "INDEX_FOLDER"; 
  const auto table_data_file      = table_folder / "TABLE_DATA_FILE";
  const auto table_meta_data_file = table_folder / "TABLE_META_DATA";

  std::filesystem::create_directories(table_folder);
  std::filesystem::create_directories(index_folder);

  IndexManager index_manager {index_folder};
  RecordLayout table_layout  {std::begin(sql_stmt.table_layout),
                              std::begin(sql_stmt.table_layout) + sql_stmt.num_attr};
  
  PageResponse response = co_await index_manager.create_index(sql_stmt.prim_key, 
                                                              sql_stmt.num_primary, 
                                                              table_layout);
  if (response != PageResponse::Success)
    throw std::runtime_error("Error: Unable to Create Table");

  const auto table_data_fd      = FileDescriptor{table_data_file, OpenMode::Create};
  const auto table_meta_data_fd = FileDescriptor{table_meta_data_file, OpenMode::Create};  
  
  auto table_ptr = std::make_unique<Table>(table_data_file, 
                                           table_meta_data_file,
                                           index_folder);

  loaded_tables[sql_stmt.get_table_name()] = std::move(table_ptr);
}

/********************************************************************************/

void DatabaseManager::drop_table(const SQLStatement& sql_stmt) {
  const auto table_folder = db_path / sql_stmt.get_table_name();
  if (!std::filesystem::is_directory(table_folder)) return;

  std::filesystem::remove_all(table_folder);
  if (loaded_tables.contains(sql_stmt.get_table_name()))
    loaded_tables.erase(sql_stmt.get_table_name());
}

/********************************************************************************/

void DatabaseManager::load_table(const std::string table_name) {
  if (loaded_tables.contains(table_name)) return; 

  const auto table_folder         = db_path / table_name;
  const auto index_folder         = table_folder / "INDEX_FOLDER";
  const auto table_data_file      = table_folder / "TABLE_DATA_FILE";
  const auto table_meta_data_file = table_folder / "TABLE_META_DATA";

  if (!std::filesystem::is_directory(table_folder) ||
      !std::filesystem::is_directory(index_folder) ||
      !std::filesystem::is_regular_file(table_data_file)||
      !std::filesystem::is_regular_file(table_meta_data_file))
    throw std::runtime_error("Error: Table does not exist cannot fetch table which does not exist");
  
  auto table_ptr = std::make_unique<Table>(table_data_file, 
                                           table_meta_data_file,
                                           index_folder);

  loaded_tables[table_name] = std::move(table_ptr);
}

/********************************************************************************/

Task<std::vector<TableRecord>> DatabaseManager::table_query(SQLStatement& sql_stmt) {
  if (loaded_tables.contains(sql_stmt.get_table_name()))
    co_return co_await loaded_tables.at(sql_stmt.get_table_name())->execute_command(sql_stmt);

  load_table(sql_stmt.get_table_name());
  if (!loaded_tables.contains(sql_stmt.get_table_name()))
    co_return std::vector<TableRecord>{};

  co_return co_await loaded_tables.at(sql_stmt.get_table_name())->execute_command(sql_stmt);
}
