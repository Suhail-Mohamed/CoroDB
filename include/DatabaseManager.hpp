#pragma once

#include <cstdlib>

#include <filesystem>
#include <stdexcept>

#include "FileDescriptor.hpp"
#include "TableMetaData.hpp"
#include "Table.hpp"

struct DatabaseManager {
  DatabaseManager(const DatabaseManager&)	     = delete;
  DatabaseManager(DatabaseManager &&)		     = delete;
  DatabaseManager& operator=(const DatabaseManager&) = delete;
  DatabaseManager& operator=(DatabaseManager&&)      = delete;

  static DatabaseManager& get_instance() {
    static DatabaseManager instance;
    return instance;
  }
 
  bool create_table(const SQLStatement& sql_stmt);
  [[nodiscard]] std::shared_ptr<Table> get_table(const std::string& table_name);

private:
  DatabaseManager() {
    const char* home = getenv("HOME");
    
    if (home) {
      db_path = std::filesystem::path(home) / ".coroDB";   
      
      if (!std::filesystem::exists(db_path))
        std::filesystem::create_directories(db_path);
    } else 
      throw std::runtime_error("Error: home path '~/' cannot be found or accessed");
  };

  std::filesystem::path db_path;
  std::unordered_map<std::string, std::shared_ptr<Table>> loaded_tables;
};

