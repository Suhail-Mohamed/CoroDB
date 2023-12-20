#pragma once

#include <filesystem>
#include <stdexcept>
#include <string>

#include "DiskManager.hpp"
#include "FileDescriptor.hpp"
#include "IndexManager.hpp"
#include "RecordPageHandler.hpp"
#include "TableMetaData.hpp"
#include "TableRecord.hpp"
#include "Task.hpp"
#include "Util.hpp"

/********************************************************************************/

struct Table {
  Table(std::filesystem::path table_data_file,
        std::filesystem::path table_meta_data_file,
        std::filesystem::path index_folder)
    : disk_manager  {DiskManager::get_instance()},
      meta_data     {table_meta_data_file.string()},
      index_manager {index_folder},
      table_pages_fd{table_data_file.string()}
  {};

  Task<std::vector<TableRecord>> execute_command(const SQLStatement sql_stmt);
  Task<void> execute_delete(const SQLStatement& sql_stmt);
  Task<void> execute_update(const SQLStatement& sql_stmt);
  Task<void> execute_insert(const SQLStatement& sql_stmt);
  
  Task<std::vector<TableRecord>> execute_select_no_join(const SQLStatement& sql_stmt);

private:
  Task<std::vector<RecId>> search_table(const SQLStatement& sql_stmt);
  Task<std::vector<RecId>> find_matches(const SQLStatement& sql_stmt);
  Task<std::vector<RecId>> find_matches(const SQLStatement& sql_stmt, 
                                        const Record&       equality_key,
                                        const int32_t       index_id);

  Task<RecId> push_back_record(Record& record);
  bool        apply_clause(const ASTTree& clause,
                           const Record&  record,
                           size_t         layer = 0) const; 
  
  std::pair<std::vector<std::string>, Record> get_equality_attr(const SQLStatement& sql_stmt);

  [[nodiscard]] Task<RecordPageHandler> get_page(const int32_t page_num);
  [[nodiscard]] Task<RecordPageHandler> create_page();
  
  DiskManager&         disk_manager;
  TableMetaData        meta_data;
  IndexManager         index_manager;
  const FileDescriptor table_pages_fd;
};

