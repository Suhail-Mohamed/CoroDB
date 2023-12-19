#pragma once

#include "DiskManager.hpp"
#include "IndexMetaData.hpp"
#include "IndexPageHandler.hpp"
#include "Iouring.hpp"
#include "Util.hpp"

/* wrapper for holding page num and index in BTree Node,
   this is not held in disk just a convience type */
struct IndexId {
  IndexId() : page_num{-1}, idx{-1} {};
  IndexId(int32_t pg_number, int32_t index)
    : page_num{pg_number}, 
      idx     {index} 
  {};

  bool operator==(const IndexId& other) const {
    return other.page_num == page_num && 
           other.idx == idx;
  }

  bool operator!=(const IndexId& other) const 
  { return !(*this == other); }

  int32_t page_num;
  int32_t idx;
};

/********************************************************************************/

struct BTree {
  BTree() 
    : disk_manager_ptr{nullptr},
      undefined_btree {true}
  {};
  
  BTree(IndexMetaData  index_meta_data,
        FileDescriptor index_pages_filedescriptor);
  
  BTree(BTree&& other) noexcept            = default;
  BTree& operator=(BTree&& other) noexcept = default;

  Task<void> insert_entry(const Record key, 
                          const RecId rec_id);
  Task<void> delete_entry(const Record key, 
                          const RecId rec_id);

  Task<std::vector<RecId>> get_matches(const Record key);

  [[nodiscard]] Task<RecId>   get_rid(const IndexId index_id);
  [[nodiscard]] Task<IndexId> lower_bound(const Record key);
  [[nodiscard]] Task<IndexId> upper_bound(const Record key);

  /* used for iterating BTree, like stl container begin and end */
  Task<IndexId> leaf_end();
  IndexId       leaf_begin();
  
  bool is_undefined() const 
  { return undefined_btree; }

private:  
  [[nodiscard]] Task<IndexPageHandler> get_node(const int32_t page_num);
  [[nodiscard]] Task<IndexPageHandler> create_node();
  
  Task<void> maintain_parent(const IndexPageHandler& node);
  Task<void> maintain_child(IndexPageHandler& new_parent, 
                            const int32_t child_idx);
  Task<void> erase_leaf(IndexPageHandler& leaf);
  void       release_node(IndexPageHandler& node);

  struct LeafItr {
    LeafItr(BTree*  btree_instance,
            IndexId start_index_id,
            IndexId end_index_id)
      : itr  {start_index_id},
        end  {end_index_id},
        btree{btree_instance}
    {};

    const bool is_end() const 
    { return itr == end; }
    
    const IndexId get_index_id() const 
    { return itr; }

    Task<RecId> get_rid() 
    { co_return co_await btree->get_rid(itr); }

    Task<void> next() {
      assert(!is_end());
      IndexPageHandler node = co_await btree->get_node(itr.page_num);
      
      assert(node.get_is_leaf());
      assert(itr.idx < node.get_num_keys());
      ++itr.idx;

      if (itr.page_num != btree->meta_data.get_last_leaf() &&
          itr.idx == node.get_num_keys()) 
      {
        itr.idx = 0;
        itr.page_num = node.get_next_leaf();
      }
    }

    IndexId itr;
    IndexId end;
    BTree*  btree;
  };
  
  bool           undefined_btree;
  DiskManager*   disk_manager_ptr;
  IndexMetaData  meta_data;
  FileDescriptor index_pages_fd;
};
