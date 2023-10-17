#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include "DiskManager.hpp"
#include "FileDescriptor.hpp"
#include "PageHandler.hpp"
#include "TableMetaData.hpp"
#include "Task.hpp"
#include "Util.hpp"

const std::string DATABASE_PATH = "/home/SuMo/.CoroDB";

/********************************************************************************/

enum class TableResponse {
  Success,
  Failure
};

struct TablePage {
  TablePage(PageHandler* handle, int32_t timestamp)
    : pg_h{handle}, pg_timestamp{timestamp} {};
  
  PageHandler* pg_h;
  int32_t      pg_timestamp;
};

struct Table {
  Table(std::filesystem::path dir, 
        const TableMetaData& m_data);

  TableResponse execute_command(const SQLStatement& sql_stmt);
  
  TableResponse execute_delete(const SQLStatement& sql_stmt);
  TableResponse execute_update(const SQLStatement& sql_stmt);
  TableResponse execute_insert(const SQLStatement& sql_stmt);
  TableResponse execute_select(const SQLStatement& sql_stmt,
                               std::vector<Record>& records);
private:
  bool       apply_clause(const Record& record, 
                          const ASTTree& clause);
  Task<void> read_page(FileDescriptor page_file);

  DiskManager&           disk_manager;
  std::string            table_name;
  TableMetaData          meta_data;
  std::filesystem::path  table_dir;
  std::vector<TablePage> table_pages;
};

/********************************************************************************/

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
  DatabaseManager()
    : db_path{DATABASE_PATH} {};

  std::filesystem::path db_path;
  std::unordered_map<std::string, std::shared_ptr<Table>> loaded_tables;
};

