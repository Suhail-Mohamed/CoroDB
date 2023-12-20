#include "Table.hpp"

/********************************************************************************/

Task<std::vector<TableRecord>> Table::execute_command(SQLStatement sql_stmt){
  switch (sql_stmt.command) {
    case Command::Delete: 
      { co_await execute_delete(sql_stmt); co_return std::vector<TableRecord>{}; }
    case Command::Update: 
      { co_await execute_update(sql_stmt); co_return std::vector<TableRecord>{}; }
    case Command::Insert: 
      { co_await execute_insert(sql_stmt); co_return std::vector<TableRecord>{}; }
    case Command::Select: co_return co_await execute_select_no_join(sql_stmt);
    case Command::CreateIndex: { 
      co_await index_manager.create_index(sql_stmt.table_attr, 
                                          sql_stmt.num_attr, 
                                          meta_data.get_record_layout());
    }
    default: throw std::runtime_error("Error: Invalid command passed to Table"); 
  }
}

/********************************************************************************/

Task<void> Table::execute_delete(const SQLStatement& sql_stmt) { 
  std::vector<RecId> matches {co_await search_table(sql_stmt)};
  for (auto rec_id : matches) {
    RecordPageHandler rec_page {std::move(co_await get_page(rec_id.page_num))};
    rec_page.delete_record(rec_id.slot_num);
  }
}

/********************************************************************************/

Task<void> Table::execute_update(const SQLStatement& sql_stmt) {
  std::vector<RecId> matches {co_await search_table(sql_stmt)};
  
  for (auto rec_id : matches) {
    RecordPageHandler rec_page {std::move(co_await get_page(rec_id.page_num))};
    const auto [record, response] = rec_page.read_record(rec_id.slot_num);
    if (response != PageResponse::Success)
      continue;

    TableRecord table_record {record, &meta_data};
    for (int32_t attr = 0; attr < sql_stmt.num_set; ++attr)
      table_record.set_attribute(sql_stmt.set_attr[attr], 
                                 sql_stmt.set_value[attr]);
  
    rec_page.update_record(rec_id.slot_num, table_record.get_record());
  }
}

/********************************************************************************/

Task<void> Table::execute_insert(const SQLStatement& sql_stmt)
{
  if (sql_stmt.num_attr != meta_data.get_num_attr()) 
    co_return;

  TableRecord potential_insert{sql_stmt, &meta_data};

  /* we always have a index on the primary key of a table */
  BTree prim_key_index    {std::move(co_await index_manager.get_index(meta_data.get_primary_key()))};
  Record key_poten_insert {potential_insert.get_subset(meta_data.get_primary_key())}; 
 
  assert(!prim_key_index.is_undefined());
  std::vector<RecId> matches {co_await prim_key_index.get_matches(key_poten_insert)};
  if (!matches.empty()) co_return;

  RecId rec_id = co_await push_back_record(potential_insert.get_record());
  co_await prim_key_index.insert_entry(key_poten_insert, rec_id);
  co_await index_manager.insert_into_indexes(potential_insert, rec_id);
}

/********************************************************************************/

Task<std::vector<TableRecord>> Table::execute_select_no_join(const SQLStatement& sql_stmt) {
  std::vector<RecId>       matches {co_await search_table(sql_stmt)};
  std::vector<TableRecord> records;

  for (auto rec_id : matches) {
    RecordPageHandler rec_page {std::move(co_await get_page(rec_id.page_num))};
    const auto [record, response] = rec_page.read_record(rec_id.slot_num);
    if (response != PageResponse::Success)
      continue;

    records.emplace_back(record, &meta_data);
  }

  co_return records;
}

/********************************************************************************/

/* finds a potential index we can use to search the table if it is not 
   there then we can just do linear search of table, only finds indexes 
   on equality terms of the where clause */
Task<std::vector<RecId>> Table::search_table(const SQLStatement& sql_stmt) {
  auto [equality_attrs, equality_key] = get_equality_attr(sql_stmt);
  int32_t index_id = co_await index_manager.find_index(equality_attrs);

  if (index_id == -1) co_return co_await find_matches(sql_stmt);
  co_return co_await find_matches(sql_stmt, equality_key, index_id);
}

/********************************************************************************/

/* brute force search of table slow, as we have no choice */
Task<std::vector<RecId>> Table::find_matches(const SQLStatement& sql_stmt) {
  std::vector<RecId> matches;
  
  for (int32_t page = 0; page < meta_data.get_num_pages(); ++page) {
    RecordPageHandler rec_page {std::move(co_await get_page(page))};

    for (int32_t rec_num = 0; rec_num < rec_page.get_num_records(); ++rec_num) {
      const auto [record, response] = rec_page.read_record(rec_num);
      if (response != PageResponse::Success)
        continue;

      if (apply_clause(sql_stmt.where_tree, record))
        matches.push_back({page, rec_num});
    }
  }

  co_return matches; 
}

/********************************************************************************/

/* searching the table using an index that we were able to find, potentially faster 
   than linear search of table */
Task<std::vector<RecId>> Table::find_matches(const SQLStatement& sql_stmt,
                                             const Record&       equality_key,
                                             const int32_t       index_id) 
{
  std::vector<RecId> matches;
  BTree index {std::move(index_manager.get_index(index_id))};
  
  for (auto rec_id : co_await index.get_matches(equality_key)) {
    RecordPageHandler rec_page {std::move(co_await get_page(rec_id.page_num))};
    const auto [record, response] = rec_page.read_record(rec_id.slot_num);
    if (response != PageResponse::Success)
      continue;

    if (apply_clause(sql_stmt.where_tree, record))
      matches.push_back(rec_id);
  }

  co_return matches;
}

/********************************************************************************/

Task<RecId> Table::push_back_record(Record& record) {
  RecordPageHandler rec_page {std::move(co_await get_page(meta_data.get_num_pages()))};
  RecId status = rec_page.add_record(record);

  if (status == PAGE_FILLED) {
    rec_page = std::move(co_await create_page());
    status   = rec_page.add_record(record);
  }
   
  assert(status != PAGE_FILLED);
  co_return status;
}

/********************************************************************************/

bool Table::apply_clause(const ASTTree& clause,
                         const Record&  record,
                         size_t         layer) const 
{
  if (!clause[layer].comp && !clause[layer].conj) 
    return true;

  if (clause[layer].comp) {
    RecordData comp_data = cast_to(clause[layer].rhs, 
                                   meta_data.get_type_of(clause[layer].rhs));
    
    return clause[layer].comp(record[meta_data.get_attr_idx(clause[layer].lhs)], 
                              comp_data);
  } else
    return clause[layer].conj(apply_clause(clause, record, right(layer)),
                              apply_clause(clause, record, left(layer)));
}

/********************************************************************************/

std::pair<std::vector<std::string>, Record> Table::get_equality_attr(const SQLStatement& sql_stmt) {
  std::vector<std::string> equality_attrs;
  Record                   equality_val_key;

  const ASTTree& where_tree = sql_stmt.where_tree;
  for (int32_t node = 0; node < MAX_PARAMS; ++node) {
    if (where_tree[node].comp.target_type() == typeid(std::equal_to<RecordData>)) 
    {
      equality_attrs.push_back(where_tree[node].lhs);
      auto record_data = cast_to(where_tree[node].rhs, 
                                 meta_data.get_type_of(where_tree[node].lhs));
    }
  }
  
  return {equality_attrs, equality_val_key};
}

/********************************************************************************/

Task<RecordPageHandler> Table::get_page(const int32_t page_num) {
  assert(page_num < meta_data.get_num_pages());
  Handler* handler = co_await disk_manager.read_page(table_pages_fd.fd,
                                                     page_num,
                                                     meta_data.get_record_layout());
  co_return RecordPageHandler{handler};
}

/********************************************************************************/

Task<RecordPageHandler> Table::create_page() {
  meta_data.increase_num_pages();
  Handler* handler = co_await disk_manager.create_page(table_pages_fd.fd,
                                                       meta_data.get_num_pages(),
                                                       meta_data.get_record_layout());
  co_return RecordPageHandler{handler};
}
