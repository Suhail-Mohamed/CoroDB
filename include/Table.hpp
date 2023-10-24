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

struct QueryReturn {
  QueryReturn()
    :table_response{TableResponse::Success} {}

  QueryReturn(TableResponse tr)
    : table_response{tr} {};

  TableResponse table_response;
  std::vector<Record> records;
};

struct TablePage {
  TablePage() : pg_h{nullptr}, pg_timestamp{-1} {}
  TablePage(PageHandler* handle, int32_t timestamp)
    :  pg_h{handle}, pg_timestamp{timestamp} {}
  
  struct TablePageItr {
    TablePageItr(PageHandler* handler, int32_t timestamp, int32_t index)
      : pg_h{handler}, pg_timestamp{timestamp}, rec_num{index} {}

    RecordResponse operator*() const {
      return pg_h->read_record(rec_num  , pg_timestamp);
    }

    TablePageItr& operator++() {
      ++rec_num;
      return *this;
    }

    bool operator!=(const TablePageItr& other) {
      return rec_num != other.rec_num;
    }

    PageHandler* pg_h;
    int32_t      pg_timestamp;
    int32_t      rec_num;
  };

  TablePageItr begin() {
    if (!pg_h) 
      throw std::runtime_error("Error: Cannot call begin() on invalid TablePage");
    return TablePageItr{pg_h, pg_timestamp, 0};
  }

  TablePageItr end() {
    if (!pg_h) 
      throw std::runtime_error("Error: Cannot call end() on invalid TablePage");
    return TablePageItr{pg_h, pg_timestamp, pg_h->get_num_records()};
  }
  
  int32_t      pg_timestamp;
  PageHandler* pg_h;
};

/********************************************************************************/

struct Table {
  Table(std::filesystem::path dir, 
        const TableMetaData& m_data);

  Task<QueryReturn> execute_command(const SQLStatement& sql_stmt);
  
  Task<PageResponse> execute_delete(const SQLStatement& sql_stmt,
                                    const std::filesystem::directory_entry& page);
  Task<PageResponse> execute_update(const SQLStatement& sql_stmt,
                                    const std::filesystem::directory_entry& page);
  Task<PageResponse> execute_insert(const SQLStatement& sql_stmt,
                                    const Record&       record,
                                    const std::filesystem::directory_entry& page);
  
  Task<PageResponse> execute_select_no_join(const SQLStatement&  sql_stmt,
                                            std::vector<Record>& potential_insert,
                                            const std::filesystem::directory_entry& page);
  Task<PageResponse> execute_select_left_join(const SQLStatement&  sql_stmt,
                                              std::vector<Record>& records);
  Task<PageResponse> execute_select_right_join(const SQLStatement&  sql_stmt,
                                               std::vector<Record>& records);

private:
  void set_attribute(Record& record, 
                     const std::string& attr, 
                     const std::string& attr_value);
 
  bool same_primary_key(const Record& table_record, 
                        const Record& potential_insert);

  RecordData cast_to(const std::string&  attr_value, 
                     const DatabaseType& db_type);

  size_t get_attr_idx(const std::string& attr);
  bool   apply_clause(const Record&  record, 
                      const ASTTree& clause,
                      size_t layer = 0);
  
  [[nodiscard]] Task<TablePage> read_page(const std::string page_path);
  
  DiskManager&                 disk_manager;
  const std::string            table_name;
  const TableMetaData          meta_data;
  const std::filesystem::path  table_dir;
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

