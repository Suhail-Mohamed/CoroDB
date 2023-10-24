#include "FileDescriptor.hpp"
#include "Util.hpp"
#include <stdexcept>

struct TableMetaData {
  TableMetaData(const std::string d_file);

  TableMetaData(const RecordLayout  t_layout, 
                const std::string   d_file,
                const SQLStatement& sql_stmt)
  : layout     {t_layout}, 
    data_file  {d_file}, 
    num_attr   {sql_stmt.num_attr},
    num_foreign{sql_stmt.num_foreign},
    num_primary{sql_stmt.num_primary}
  {
    for(int32_t i = 0; i < sql_stmt.num_primary; ++i)
      primary_key.push_back(sql_stmt.prim_key[i]);

    for (int32_t i = 0; i < sql_stmt.num_attr; ++i)
      attr_list.push_back(sql_stmt.table_attr[i]);
    
    for (int32_t i = 0; i < sql_stmt.num_foreign; ++i)
      foreign_info.emplace_back(sql_stmt.foreign_keys[i], 
                                sql_stmt.foreign_table[i]);
  }

  /*********************************************/

  void write_string_to_file(FileDescriptor& out, const std::string& str) {
    int32_t str_size = str.size();
    out.file_write(&str_size, sizeof(str_size));
    out.file_write(str.c_str(), str_size);
  }

  /*********************************************/
  
  std::string read_string_from_file(FileDescriptor& in) {
    int32_t str_size;
    in.file_read(&str_size, sizeof(str_size));
    
    std::vector<char> buffer(str_size);
    in.file_read(buffer.data(), str_size);
    
    return std::string{std::begin(buffer), std::end(buffer)};
  }

  /*********************************************/
  
  void write_meta_data() {
    FileDescriptor out{data_file};

    out.file_write(&num_attr, sizeof(num_attr));
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
      out.file_write(&layout[i], sizeof(DatabaseType));
  }

  /*********************************************/
  
  void read_meta_data() {
    FileDescriptor in{data_file};
    
    in.file_read(&num_attr, sizeof(num_attr));
    in.file_read(&num_primary, sizeof(num_primary));
    in.file_read(&num_foreign, sizeof(num_foreign));
   
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
      layout.push_back(db_type);
    }
  }
  
  /*********************************************/
  
  struct ForeignInfo {
    ForeignInfo(const std::string f_key, 
                const std::string f_table)
      : foreign_key  {f_key}, 
        foreign_table{f_table} {}
    
    std::string foreign_key;
    std::string foreign_table;
  };

  /*********************************************/
  
  int32_t num_attr;
  int32_t num_foreign;
  int32_t num_primary;
  RecordLayout layout;
  std::string  data_file;
  std::vector<std::string> primary_key;
  std::vector<std::string> attr_list;
  std::vector<ForeignInfo> foreign_info;
};
