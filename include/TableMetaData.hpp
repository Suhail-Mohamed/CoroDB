#pragma once

#include <atomic>

#include "FileDescriptor.hpp"
#include "Util.hpp"

struct TableMetaData {
  TableMetaData(const std::string data_file)
    : meta_data_file{data_file} 
  { read_meta_data(); };

  TableMetaData(const SQLStatement& sql_stmt,
                const RecordLayout  table_record_layout, 
                const std::string   data_file)
    : num_attr      {sql_stmt.num_attr},
      num_primary   {sql_stmt.num_primary},
      num_foreign   {sql_stmt.num_foreign},
      num_pages     {-1},
      meta_data_file{data_file},
      record_layout {table_record_layout}
  {
    for(int32_t i = 0; i < sql_stmt.num_primary; ++i)
      primary_key.push_back(sql_stmt.prim_key[i]);

    for (int32_t i = 0; i < sql_stmt.num_attr; ++i)
      attr_list.push_back(sql_stmt.table_attr[i]);
    
    for (int32_t i = 0; i < sql_stmt.num_foreign; ++i)
      foreign_info.emplace_back(sql_stmt.foreign_keys[i], 
                                sql_stmt.foreign_table[i]);
  }
     
  ~TableMetaData() { write_meta_data(); }

  struct ForeignInfo {
    ForeignInfo(const std::string f_key, 
                const std::string f_table)
      : foreign_key  {f_key}, 
        foreign_table{f_table} {}
    
    std::string foreign_key;
    std::string foreign_table;
  };

  const int32_t get_num_attr() const 
  { return num_attr; }

  const int32_t get_num_foreign() const 
  { return num_foreign; }

  const int32_t get_num_primary() const 
  { return num_primary; }

  const int32_t get_num_pages() const 
  { return num_pages.load(); }

  const RecordLayout& get_record_layout() const
  { return record_layout; }

  std::vector<std::string>& get_primary_key() 
  { return primary_key; }

  const std::vector<std::string>& get_attr_lst() const 
  { return attr_list; }

  const std::vector<ForeignInfo>& get_foreign_info() const 
  { return foreign_info; }

  void increase_num_pages() { ++num_pages; }
  void decrease_num_pages() { --num_pages; }
  
  size_t get_attr_idx(const std::string& attr) const {
    auto itr = std::find(std::begin(attr_list), 
                         std::end(attr_list), attr);
  
    if (itr == std::end(attr_list)) 
      throw std::runtime_error("Error: Attribute does not exist in record");

    return std::distance(std::begin(attr_list), itr);
  }

  DatabaseType get_type_of(const std::string attr) const {
    size_t attr_idx = get_attr_idx(attr);
    return record_layout[attr_idx];
  }

private:
  void write_meta_data() {
    FileDescriptor out{meta_data_file};

    int32_t np_write = num_pages.load(); 
    out.file_write(&num_attr   , sizeof(num_attr));
    out.file_write(&np_write   , sizeof(np_write));
    out.file_write(&num_primary, sizeof(num_primary));
    out.file_write(&num_foreign, sizeof(num_foreign));

    for (int32_t i = 0; i < num_primary; ++i)
      write_string_to_file(out, primary_key[i]);

    for (int32_t i = 0; i < num_attr; ++i)
      write_string_to_file(out, attr_list[i]);

    for (int32_t i = 0; i < num_foreign; ++i) {
      write_string_to_file(out, foreign_info[i].foreign_key);
      write_string_to_file(out, foreign_info[i].foreign_table);
    }

    for (int32_t i = 0; i < num_attr; ++i)
      out.file_write(&record_layout[i], sizeof(DatabaseType));
  }

  /*************************/
  
  void read_meta_data() {
    FileDescriptor in{meta_data_file};
    
    int32_t np_read;
    in.file_read(&num_attr   , sizeof(num_attr));
    in.file_read(&np_read    , sizeof(np_read));
    in.file_read(&num_primary, sizeof(num_primary));
    in.file_read(&num_foreign, sizeof(num_foreign));

    num_pages = np_read;
    for(int32_t i = 0; i < num_primary; ++i)
      primary_key.push_back(read_string_from_file(in));

    for (int32_t i = 0; i < num_attr; ++i)
      attr_list.push_back(read_string_from_file(in));

    for (int32_t i = 0; i < num_foreign; ++i) {
      auto f_key   = read_string_from_file(in);
      auto f_table = read_string_from_file(in);
      foreign_info.emplace_back(f_key, f_table);
    }

    for (int32_t i = 0; i < num_attr; ++i) {
      DatabaseType db_type;
      in.file_read(&db_type, sizeof(db_type));
      record_layout.push_back(db_type);
    }
  }

  /*************************/
  
  void write_string_to_file(FileDescriptor& out, 
                            const std::string& str) 
  {
    int32_t str_size = str.size();
    out.file_write(&str_size, sizeof(str_size));
    out.file_write(str.c_str(), str_size);
  }

  std::string read_string_from_file(FileDescriptor& in) {
    int32_t str_size;
    in.file_read(&str_size, sizeof(str_size));
    
    std::vector<char> buffer(str_size);
    in.file_read(buffer.data(), str_size);
    
    return std::string{std::begin(buffer), std::end(buffer)};
  }
  
  int32_t num_attr;
  int32_t num_foreign;
  int32_t num_primary;
  std::atomic<int32_t> num_pages;
 
  std::string meta_data_file;
  RecordLayout record_layout;
  std::vector<std::string> primary_key;
  std::vector<std::string> attr_list;
  std::vector<ForeignInfo> foreign_info;
};
