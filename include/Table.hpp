#pragma once

#include <stdexcept>
#include <string>

#include "DiskManager.hpp"
#include "FileDescriptor.hpp"
#include "RecordPageHandler.hpp"
#include "TableMetaData.hpp"
#include "Task.hpp"
#include "Util.hpp"

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

/********************************************************************************/

struct Table {
  Table(TableMetaData&  table_meta_data,
        FileDescriptor& table_pages_filedescriptor);

  Task<QueryReturn>  execute_command(const SQLStatement& sql_stmt);
  
  Task<PageResponse> execute_delete(const SQLStatement& sql_stmt,
                                    const int32_t       page_num);
  Task<PageResponse> execute_update(const SQLStatement& sql_stmt,
                                    const int32_t       page_num);
  Task<PageResponse> execute_insert(const SQLStatement& sql_stmt,
                                    const Record&       potential_insert,
                                    const int32_t       page_num);
  
  Task<PageResponse> execute_select_no_join   (const SQLStatement&  sql_stmt,
                                               std::vector<Record>& records,
                                               const int32_t        page_num); 
  Task<PageResponse> execute_select_left_join (const SQLStatement&  sql_stmt,
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

  [[nodiscard]] Task<RecordPageHandler> get_page(const int32_t page_num);

  DiskManager& disk_manager;
  TableMetaData& meta_data;
  FileDescriptor& table_pages_fd;
};

