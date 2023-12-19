#pragma once

#include <cassert>
#include <stdexcept>

#include "TableMetaData.hpp"
#include "Util.hpp"

struct TableRecord {
  TableRecord()
    : meta_data{nullptr} 
  {};
  
  TableRecord(Record rec, 
              const TableMetaData* table_meta_data)
    : record   {rec},
      meta_data{table_meta_data}
  { assert(meta_data); };

  TableRecord(const SQLStatement&  sql_stmt,
              const TableMetaData* table_meta_data)
    : meta_data{table_meta_data}
  {
    assert(meta_data);
    if (sql_stmt.num_attr != meta_data->get_num_attr())
      throw std::runtime_error("Error: Invalid creation of TableRecord Type");

    for (int32_t attr = 0; attr < meta_data->get_num_attr(); ++attr) {
      auto record_data = cast_to(sql_stmt.set_attr[attr],
                                 meta_data->get_record_layout()[attr]);
      record.push_back(record_data);
    }
  };

  Record& get_record() 
  { return record; }
  
  RecordData get_attr(const std::string attr) const {
    size_t attr_idx = meta_data->get_attr_idx(attr);
    return record[attr_idx];
  }

  Record get_subset(const std::span<std::string> attr_lst,
                    const int32_t num_attr) const
  {
    Record subset_attr;
    for (int i = 0; i < num_attr; ++i)
      subset_attr.push_back(get_attr(attr_lst[i]));
  
    return subset_attr;
  }
  
  Record get_subset(const std::vector<std::string>& attr_lst) const {
    Record subset_attr;
    for (const auto& attr : attr_lst)
      subset_attr.push_back(get_attr(attr));
  
    return subset_attr;
  }

  void set_attribute(const std::string attr,
                     const std::string attr_value) 
  {
    size_t attr_idx  = meta_data->get_attr_idx(attr);
    record[attr_idx] = cast_to(attr_value, 
                               meta_data->get_record_layout()[attr_idx]); 
  }

private:
  Record record;
  const TableMetaData* meta_data;
};
