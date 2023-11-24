#include "BTree.hpp"
#include "IndexMetaData.hpp"
#include "IndexPageHandler.hpp"
#include "Iouring.hpp"
#include <stdexcept>

BTree::BTree(IndexMetaData&  index_meta_data,
             FileDescriptor& index_pages_filedescriptor)
  : disk_manager  {DiskManager::get_instance()},
    meta_data     {index_meta_data},
    index_pages_fd{index_pages_filedescriptor}
{}
  
/********************************************************************************/

Task<void> BTree::insert_entry(const Record key, 
                               const RecId rec_id)
{
  IndexId          index_id = co_await upper_bound(key);
  IndexPageHandler node     = co_await get_node(index_id.page_num);

  node.insert_key(key   , index_id.idx);
  node.insert_rid(rec_id, index_id.idx);

  if (index_id.page_num == meta_data.get_last_leaf() &&
      index_id.idx == node.get_num_keys() - 1)
    co_await maintain_parent(node);

  /* dealing with leaf node being overfilled */
  while (node.get_num_children() > meta_data.get_order()) {
    /* case when root node is overfilled */
    if (node.page_hdr.parent == NO_PARENT) {
      IndexPageHandler new_root = co_await create_node();
      new_root.set_page_header(IndexPageHdr{});
      
      new_root.insert_rid({node.get_page_num(), -1}, 0);
      new_root.insert_key(node.get_max_key(), 0);
      node.set_parent(new_root.get_page_num());
      meta_data.set_root_page(new_root.get_page_num());
    }

    /* allocate sibling node */
    IndexPageHandler sibling = co_await create_node();
    sibling.set_page_header(IndexPageHdr{node.get_parent(), /* same parent */
                                         NO_FREE_PAGE,
                                         NO_KEYS, 
                                         NO_KIDS,
                                         NO_PREV_LEAF,
                                         NO_NEXT_LEAF,
                                         node.get_is_leaf()});
    if (sibling.get_is_leaf()) {
      /* [ node ] <-> [sibling] <-> [node_nxt] */
      sibling.set_next_leaf(node.get_next_leaf());
      sibling.set_prev_leaf(node.get_page_num());
      node.set_next_leaf(sibling.get_page_num());

      IndexPageHandler node_nxt = co_await get_node(node.get_next_leaf());
      node_nxt.set_prev_leaf(sibling.get_page_num());
    }

    int32_t mid_idx      = node.get_num_children() / 2;
    int32_t num_transfer = node.get_num_keys() - mid_idx;

    /* keys from [0, mid_idx) stay in the current node, [mid_idx, num_keys) go to sibling */
    sibling.insert_keys(node.get_keys(mid_idx, num_transfer), 0);
    sibling.insert_rids(node.get_rids(mid_idx, num_transfer), 0); 
    
    node.set_num_keys(mid_idx);
    node.set_num_children(mid_idx);

    /* ensure the siblings new kids know who their parent is */
    for (int32_t child_idx = 0; child_idx < sibling.get_num_children(); ++child_idx)
      co_await maintain_child(sibling, child_idx);

    /* update parent as child has split */
    IndexPageHandler parent = co_await get_node(node.get_parent());
    int32_t child_idx = parent.find_child(node.get_page_num());
    
    parent.insert_key(node.get_max_key(), child_idx);
    parent.insert_rid({sibling.get_page_num(), -1}, child_idx + 1); 

    if (meta_data.get_last_leaf() == node.get_page_num())
      meta_data.set_last_leaf(sibling.get_page_num());

    /* check if parent has overflow */
    node = parent;
  }
}
  
/********************************************************************************/

Task<void> BTree::delete_entry(const Record key, 
                               const RecId rec_id)
{
  const int32_t min_num_children = (meta_data.get_order() + 1) / 2;

  const IndexId lb = co_await lower_bound(key);
  const IndexId ub = co_await upper_bound(key);

  for (LeafItr leaf_itr{this, lb, ub}; !leaf_itr.is_end(); co_await leaf_itr.next()) {
    const IndexId itr_index = leaf_itr.get_index_id();
    IndexPageHandler node   = co_await get_node(itr_index.page_num);
    assert(node.get_is_leaf());

    const RecId entry_rec_id = co_await leaf_itr.get_rid();
    if (entry_rec_id != rec_id) continue;
    
    node.erase_key(leaf_itr.get_index_id().idx);
    node.erase_rid(leaf_itr.get_index_id().idx);
    /* update parent incase nodes max leaf was deleted */
    co_await maintain_parent(node);

    /* solve underflow */
    while (node.get_num_children() < min_num_children) {
      /* root node underflow, only node with no sibling */
      if (node.get_parent() == NO_PARENT) {
        /* underflow permitted for the root */
        if (!node.get_is_leaf() && node.get_num_keys() <= 1) {
          const RecId only_child = node.get_rid(0);
          
          IndexPageHandler child_page = 
            co_await get_node(only_child.page_num);
          
          child_page.set_parent(NO_PARENT);
          meta_data.set_root_page(only_child.page_num);
          release_node(node);
        }
        break;
      }

      IndexPageHandler parent    = co_await get_node(node.get_parent());
      const int32_t    child_idx = parent.find_child(node.get_page_num());  

      /* start getting siblings to lend you some children */
      /* you have left sibling */
      if (child_idx > 0) {
        const RecId sibling_rid = parent.get_rid(child_idx - 1);
        
        IndexPageHandler left_sibling = 
          co_await get_node(sibling_rid.page_num);

        /* if left sibling has some children borrow one from them */
        if (left_sibling.get_num_children() > min_num_children) {
          /* all of left sibling elements are smaller than smallest 
             element in node */
          node.insert_key(left_sibling.get_max_key(), 0);
          node.insert_rid(left_sibling.get_max_rid(), 0);
          left_sibling.erase_key(left_sibling.get_num_keys() - 1);
          left_sibling.erase_rid(left_sibling.get_num_children() - 1);
        
          /* update left_siblings parent with new max_key as old one got removed */
          co_await maintain_parent(left_sibling);
          /* make sure new child knows who their parent is */
          co_await maintain_child(node, 0);
          break;
        }
      }
      /* you have a right sibling */
      if (child_idx + 1 < parent.get_num_children()) {
        const RecId sibling_rid = parent.get_rid(child_idx + 1);

        IndexPageHandler right_sibling = 
          co_await get_node(sibling_rid.page_num);
        
        /* if right sibling has some children borrow one from them */
        if (right_sibling.get_num_children() > min_num_children) {
          /* all of right sibling elements are larger than largest element in 
             node */
          node.push_back_key(right_sibling.get_min_key());
          node.push_back_rid(right_sibling.get_min_rid());
          right_sibling.erase_key(0);
          right_sibling.erase_rid(0);

          /* update nodes parent with new max_key that has been inserted */
          co_await maintain_parent(node);
          /* make sure new child knows who their parent is */
          co_await maintain_child(node, node.get_num_children() - 1);
        }
      }
      
      /* both siblings are poor, merge with one of them */
      if (child_idx > 0) {
        /* merge with left sibling, give them all your children */
        const RecId sibling_rid = parent.get_rid(child_idx - 1);
        IndexPageHandler left_sibling = 
          co_await get_node(sibling_rid.page_num);
        
        left_sibling.push_back_keys(node.get_keys(0, node.get_num_keys()));
        left_sibling.push_back_rids(node.get_rids(0, node.get_num_children()));
        int32_t new_child = 
          left_sibling.get_num_children() - node.get_num_children();

        for (; new_child < left_sibling.get_num_children(); ++new_child)
          co_await maintain_child(left_sibling, new_child);

        parent.erase_key(child_idx);
        parent.erase_rid(child_idx);
        /* left_sibling has new max_key update parent with this data */
        co_await maintain_parent(left_sibling);

        /* maintain leaf structure */
        if (node.get_is_leaf()) co_await erase_leaf(node);
        if (meta_data.get_last_leaf() == node.get_page_num())
          meta_data.set_last_leaf(left_sibling.get_page_num());
        
        release_node(node);
      } else {
        /* merge with right sibling, right sibling gives you all their children */
        assert(child_idx + 1 < parent.get_num_children());
        const RecId sibling_rid = parent.get_rid(child_idx + 1);
        IndexPageHandler right_sibling = 
          co_await get_node(sibling_rid.page_num);
        
        node.push_back_keys(
            right_sibling.get_keys(0, right_sibling.get_num_keys()));
        node.push_back_rids(
            right_sibling.get_rids(0, right_sibling.get_num_children()));

        int32_t new_child = 
          node.get_num_children() - right_sibling.get_num_children();

        for (; new_child < node.get_num_children(); ++new_child)
          co_await maintain_child(node, new_child);

        parent.erase_rid(child_idx + 1);
        parent.erase_key(child_idx);
        /* node has new max_key update parent with this data */
        co_await maintain_parent(node);

        /* maintain leaf structure */
        if (right_sibling.get_is_leaf()) co_await erase_leaf(right_sibling);
        if (meta_data.get_last_leaf() == right_sibling.get_page_num())
          meta_data.set_last_leaf(node.get_page_num());
        
        release_node(right_sibling);
      }

      /* check if parent has underflow */
      node = parent;
    }
  }

  co_return;
}

/********************************************************************************/

Task<RecId> BTree::get_rid(const IndexId index_id) {
  IndexPageHandler  node = co_await get_node(index_id.page_num);
  const RecId       rid  = node.get_rid(index_id.idx);
  
  co_return rid;
}
  
/********************************************************************************/

Task<IndexId> BTree::lower_bound(const Record key) {
  IndexPageHandler node = co_await get_node(meta_data.get_root_page());

  while (!node.page_hdr.is_leaf) {
    const int32_t key_idx = node.lower_bound(key);
    if (key_idx >= node.get_num_keys())
      co_return (co_await leaf_end());
  
    const RecId rid_resp = node.get_rid(key_idx);
    node = co_await get_node(rid_resp.page_num);
  }

  /* reached leaf node */
  co_return IndexId{node.get_page_num(), 
                    node.lower_bound(key)};
}

/********************************************************************************/

Task<IndexId> BTree::upper_bound(const Record key) {
  IndexPageHandler node = co_await get_node(meta_data.get_root_page());

  while (!node.page_hdr.is_leaf) {
    const int32_t key_idx = node.upper_bound(key);
    if (key_idx >= node.get_num_keys())
      co_return (co_await leaf_end());
  
    const RecId rid_resp = node.get_rid(key_idx);
    node = co_await get_node(rid_resp.page_num);
  }

  /* reached leaf node */
  co_return IndexId{node.get_page_num(), 
                    node.upper_bound(key)};
}

/********************************************************************************/

Task<IndexId> BTree::leaf_end() {
  IndexPageHandler node = co_await get_node(meta_data.get_last_leaf());
  co_return IndexId{meta_data.get_last_leaf(), node.get_num_children()};
}

/********************************************************************************/

IndexId BTree::leaf_begin() {
  return IndexId{meta_data.get_first_leaf(), 0};
}

/********************************************************************************/

Task<IndexPageHandler> BTree::get_node(const int32_t page_num) {
  assert(page_num < meta_data.get_num_pages());

  Handler* handler = co_await disk_manager.read_page(index_pages_fd.fd,
                                                     page_num,
                                                     meta_data.get_key_layout());
  co_return IndexPageHandler{handler, &meta_data};
}

/********************************************************************************/

Task<IndexPageHandler> BTree::create_node() {
  Handler* handler;

  if (meta_data.get_first_free_page() == NO_FREE_PAGE) {
    /* no free pages create a new one, call disk manager */
    handler = co_await disk_manager.create_page(index_pages_fd.fd, 
                                                meta_data.get_num_pages(),
                                                meta_data.get_key_layout());
    meta_data.increase_num_pages();
    co_return IndexPageHandler{handler, &meta_data};
  } else {
    /* we have a page in our index file we can reuse */
    IndexPageHandler node = co_await get_node(meta_data.get_first_free_page());
    meta_data.set_first_free_page(node.get_next_free());
    node.handler_ptr->is_dirty = true;
    co_return node;
  }
}
  
/********************************************************************************/

/* This function ensures that each parent node in the B-tree correctly reflects 
   the maximum key value of its child nodes. In a B-tree, parent nodes store the largest key values
   of their children to guide the search process efficiently. */
Task<void> BTree::maintain_parent(const IndexPageHandler& node) { 
  IndexPageHandler child = node;

  while (child.get_parent() != NO_PARENT) {
    IndexPageHandler parent    = co_await get_node(child.get_parent());

    const int32_t    child_idx = parent.find_child(child.get_page_num());
    const Record     child_key = parent.get_key(child_idx);

    /* all nodes above this point are properly setup */
    if (child_key == child.get_max_key()) break;

    const PageResponse update_child = parent.set_key(child_idx, 
                                                     child.get_max_key());
    assert(update_child == PageResponse::Success);
  }
}

/********************************************************************************/

/* ensure child points to correct parent */
Task<void> BTree::maintain_child(IndexPageHandler& new_parent, 
                                 const int32_t child_idx) 
{
  if (new_parent.get_is_leaf()) co_return;

  const RecId rid_resp = new_parent.get_rid(child_idx);

  const int32_t child_page_num = rid_resp.page_num;
  IndexPageHandler child = co_await get_node(child_page_num);
  child.set_parent(new_parent.get_page_num());
}

/********************************************************************************/

/* erasing a leaf node, we have to maintain that leaf.prev_leaf points to 
   leaf.next_leaf and vice versa */
Task<void> BTree::erase_leaf(IndexPageHandler& leaf) {
  assert(leaf.get_is_leaf());

  IndexPageHandler prev      = co_await get_node(leaf.get_prev_leaf());
  prev.handler_ptr->is_dirty = true;
  prev.set_next_leaf(leaf.get_next_leaf());

  IndexPageHandler next = co_await get_node(leaf.get_next_leaf());
  next.set_prev_leaf(leaf.get_prev_leaf());
}

/********************************************************************************/

/* we have new index page we can use to write to later on */
void BTree::release_node(IndexPageHandler& node) {
  node.set_next_free(meta_data.get_first_free_page());
  meta_data.set_first_free_page(node.get_page_num());
}

