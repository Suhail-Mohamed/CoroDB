#pragma once

#include <cstdlib>

#include <filesystem>
#include <memory>
#include <stdexcept>

#include "FileDescriptor.hpp"
#include "Parser.hpp"
#include "SyncWaiter.hpp"
#include "TableMetaData.hpp"
#include "Table.hpp"
#include "Util.hpp"

struct DatabaseManager {
  DatabaseManager(const DatabaseManager&)	     = delete;
  DatabaseManager(DatabaseManager &&)		     = delete;
  DatabaseManager& operator=(const DatabaseManager&) = delete;
  DatabaseManager& operator=(DatabaseManager&&)      = delete;

  static DatabaseManager& get_instance() {
    static DatabaseManager instance;
    return instance;
  }

  Task<std::vector<TableRecord>> handle_query(const std::string query_string);
  void start_cmdline();

private:
  DatabaseManager() 
    : coro_pool{CoroPool::get_instance()}
  {
    const char* home = getenv("HOME");
    if (home) {
      db_path = std::filesystem::path(home) / ".coroDB";   
      
      if (!std::filesystem::exists(db_path))
        std::filesystem::create_directories(db_path);
    } else 
      throw std::runtime_error("Error: home path '~/' cannot be found or accessed");
  };

  Task<void> create_table(SQLStatement& sql_stmt);
  void       drop_table  (const SQLStatement& sql_stmt);
  void       load_table  (const std::string table_name);
  
  Task<std::vector<TableRecord>> table_query(SQLStatement& sql_stmt);
 
  Parser                parser;
  CoroPool&             coro_pool;
  std::filesystem::path db_path;
  std::unordered_map<std::string, std::unique_ptr<Table>> loaded_tables;
};
