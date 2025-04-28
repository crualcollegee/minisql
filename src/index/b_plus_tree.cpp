#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
        auto root_index = reinterpret_cast<IndexRootsPage*>(buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID));
        root_index->GetRootId(index_id,&root_page_id_);
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);

        if(leaf_max_size == UNDEFINED_SIZE){
          leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId));
        }
        
        if(internal_max_size == UNDEFINED_SIZE){
          internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(page_id_t));
        } 
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  buffer_pool_manager_->DeletePage(current_page_id);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
//Page* FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) 
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  if (IsEmpty()){
    return false;
  }
  auto page_leave_t = reinterpret_cast<BPlusTreeLeafPage*>(FindLeafPage(key,-1,false)->GetData());
  RowId row_t;
  bool flag = page_leave_t->Lookup(key,row_t,processor_);
  if (flag){
    result.push_back(row_t);
  }
  buffer_pool_manager_->UnpinPage(page_leave_t->GetPageId(), false);
  return flag;
}


/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
// 如果当前树是空的，则调用StartNewTree，否则，调用InsertIntoLeaf
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if (IsEmpty()){
    StartNewTree(key,value);
    return true;
  }
  else{
    return InsertIntoLeaf(key,value,transaction);
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
// fetch一个page当作新树的根，调用UpdateRootPageId创建键值对
//然后对根（即叶子）节点进行操作
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  page_id_t page_id_l;
  auto new_page = buffer_pool_manager_->NewPage(page_id_l);
  if (new_page == nullptr) {
    throw("out of memory");
  }
  auto page_leave_t = reinterpret_cast<BPlusTreeLeafPage*>(new_page->GetData());

  page_leave_t->Init(page_id_l,INVALID_PAGE_ID,processor_.GetKeySize(),leaf_max_size_);
  page_leave_t->Insert(key,value,processor_);
  root_page_id_ = page_id_l;
  UpdateRootPageId(1);
  buffer_pool_manager_->UnpinPage(page_id_l,true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
//先调用 FindLeafPage
//Page* FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
//观察是否已经存在，否则插入，如果太多则分裂节点
//BPlusTreeInternalPage* Split(InternalPage *node, Txn *transaction)
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  auto page_t = FindLeafPage(key,INVALID_PAGE_ID,false)->GetData();
  auto leave_page_t = reinterpret_cast<BPlusTreeLeafPage*>(page_t);
  RowId new_row;
  if (leave_page_t->Lookup(key,new_row,processor_)){
    buffer_pool_manager_->UnpinPage(leave_page_t->GetPageId(), false);
    return false;
  }
  else{
    leave_page_t->Insert(key,value,processor_);
    if (leave_page_t->GetSize()>=leaf_max_size_){
      auto next_leave=Split(leave_page_t,transaction);
      InsertIntoParent(leave_page_t, next_leave->KeyAt(0), next_leave, transaction);
    }
    buffer_pool_manager_->UnpinPage(leave_page_t->GetPageId(), false);
    return true;
  }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
//开辟新叶，初始化它，搬运一半过去
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t new_pageid;
  auto new_page = buffer_pool_manager_->NewPage(new_pageid);
  if (new_page == nullptr) {
    throw("out of memory");
  }
  auto internal_page_t = reinterpret_cast<BPlusTreeInternalPage*>(new_page->GetData());
  internal_page_t->Init(new_pageid,node->GetParentPageId(),processor_.GetKeySize(),internal_max_size_);
  node->MoveHalfTo(internal_page_t,buffer_pool_manager_);
  
  buffer_pool_manager_->UnpinPage(new_pageid,true);
  return internal_page_t;
}

//和上面几乎一样，注意设置nextpage指针
BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t new_pageid;
  auto new_page = buffer_pool_manager_->NewPage(new_pageid);
  if (new_page == nullptr) {
    throw("out of memory");
  }
  auto leave_page_t = reinterpret_cast<BPlusTreeLeafPage*>(new_page->GetData());
  leave_page_t->Init(new_pageid,node->GetParentPageId(),processor_.GetKeySize(),leaf_max_size_);
  node->MoveHalfTo(leave_page_t);

  buffer_pool_manager_->UnpinPage(new_pageid,true);
  leave_page_t->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_pageid);
  return leave_page_t;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if (old_node->GetParentPageId()==INVALID_PAGE_ID){
    page_id_t new_page_l;
    auto new_page = buffer_pool_manager_->NewPage(new_page_l);
    if (new_page == nullptr) {
      throw("out of memory");
    }
    auto internal_page_t = reinterpret_cast<BPlusTreeInternalPage*>(new_page->GetData());
    internal_page_t->Init(new_page_l,INVALID_PAGE_ID,processor_.GetKeySize(),internal_max_size_);
    internal_page_t->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
    root_page_id_ = new_page_l;
    old_node->SetParentPageId(new_page_l);
    new_node->SetParentPageId(new_page_l);
    UpdateRootPageId(0); 
    buffer_pool_manager_->UnpinPage(new_page_l,true);
    return;
  }
  else{
    auto parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    auto internal_page_l = reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
    internal_page_l->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
    if (internal_page_l->GetSize()>=internal_max_size_){
      BPlusTreeInternalPage* new_internal_page = Split(internal_page_l,transaction);
      InsertIntoParent(internal_page_l,new_internal_page->KeyAt(0),new_internal_page,transaction);
    }
    buffer_pool_manager_->UnpinPage(old_node->GetParentPageId(),true);
  }

}
//BPlusTreeInternalPage* Split(InternalPage *node, Txn *transaction)

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 * 
删除与输入的 key 相关联的键值对。

如果当前 B+树是空的，直接返回。

如果不是空树，用户需要：

先找到正确的叶子节点（即要删除的目标叶子页）；

在叶子节点中删除对应的 key-value 键值对；

注意，如果删除后导致节点大小小于最小要求（比如不满足半满条件）：

需要考虑**重新分配（Redistribute）**或者

**合并（Merge）**节点，以保证 B+树的平衡性。
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if (IsEmpty()){
    return;
  }
  Page* new_Page;
  new_Page = FindLeafPage(key);
  BPlusTreeLeafPage* page_leave_l = reinterpret_cast<BPlusTreeLeafPage*>(new_Page->GetData());
  page_id_t leave_id_l = page_leave_l->GetPageId();
  if (page_leave_l->KeyAt(0) == key && page_leave_l->GetParentPageId()!=INVALID_PAGE_ID){
    page_id_t parent_id_l = page_leave_l -> GetParentPageId();
    BPlusTreeInternalPage* internal_page_l = reinterpret_cast<BPlusTreeInternalPage*>(buffer_pool_manager_->FetchPage(parent_id_l)->GetData());
    if (!(internal_page_l-> ValueAt(0)==leave_id_l)){
      int key_index_at_parent = internal_page_l->ValueIndex(leave_id_l);
      GenericKey* old_key = internal_page_l->KeyAt(key_index_at_parent);
      memcpy(old_key,key,sizeof(GenericKey));
      bool flag = false;
      while(internal_page_l->KeyAt(0) == key && internal_page_l->GetParentPageId()!=INVALID_PAGE_ID){
        page_id_t new_internal_id_l = internal_page_l -> GetParentPageId();
        BPlusTreeInternalPage* new_internal_page_l = reinterpret_cast<BPlusTreeInternalPage*>(buffer_pool_manager_->FetchPage(new_internal_id_l)->GetData());
        if (!(new_internal_page_l->ValueAt(0)==internal_page_l->GetPageId())){
          int key_index_at_parent_1 = new_internal_page_l->ValueIndex(internal_page_l->GetPageId());
          GenericKey* old_key_1 = new_internal_page_l -> KeyAt(key_index_at_parent_1);
          memcpy(old_key_1,key,sizeof(GenericKey));
          internal_page_l = new_internal_page_l;
          buffer_pool_manager_->UnpinPage(new_internal_page_l->GetPageId(),true);
        }
        else{
          buffer_pool_manager_->UnpinPage(new_internal_page_l->GetPageId(),true);
          break;
        } 
      }
      buffer_pool_manager_->UnpinPage(parent_id_l,true);
    }
    else{
      buffer_pool_manager_->UnpinPage(parent_id_l,false);
    }
  }
  int size_l = page_leave_l->RemoveAndDeleteRecord(key,processor_);
  if (size_l < page_leave_l->GetMinSize()){
    CoalesceOrRedistribute(page_leave_l,transaction);
  }
  page_id_t index_page = page_leave_l->GetPageId();
  buffer_pool_manager_->UnpinPage(index_page,true);
  if (page_leave_l->GetSize()==0){
    buffer_pool_manager_->DeletePage(page_leave_l->GetPageId());
    return;
  }
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 * 
 用户首先需要找到当前节点（node）对应的兄弟节点（邻居节点）。

然后判断：

如果兄弟节点的大小 + 当前节点的大小 > 节点最大容量（MaxSize）：

那么**执行重分配（Redistribute）**操作（从兄弟节点借一部分过来）。

否则：

**执行合并（Merge）**操作（把当前节点和兄弟节点合并成一个）。
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  if (node->IsRootPage()){
    return AdjustRoot(node);
  }
  int this_page_size = node->GetSize();
  page_id_t page_id_l = node->GetPageId();
  page_id_t parent_internal_id = node->GetParentPageId();
  BPlusTreeInternalPage* parent_page_l = reinterpret_cast<BPlusTreeInternalPage*>(buffer_pool_manager_->FetchPage(parent_internal_id)->GetData());
  int index_at_parent = parent_page_l->ValueIndex(page_id_l);

  if (index_at_parent!=0){
    page_id_t left_neighbor_id = parent_page_l->ValueAt(index_at_parent-1);
    N* left_neighbor_page = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(left_neighbor_id)->GetData());
    int left_page_size = left_neighbor_page->GetSize();
    if (this_page_size + left_page_size > left_neighbor_page->GetMaxSize()){
      Redistribute(left_neighbor_page,node,1);
      buffer_pool_manager_->UnpinPage(left_neighbor_id,true);
      buffer_pool_manager_->UnpinPage(parent_internal_id,true);
      return true;
    }
  }
  if (index_at_parent!=parent_page_l->GetSize()-1){
    page_id_t right_neighbor_id = parent_page_l->ValueAt(index_at_parent+1);
    N* right_neighbor_page = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(right_neighbor_id)->GetData());
    int right_page_size = right_neighbor_page->GetSize();
    if (right_page_size + this_page_size > right_neighbor_page->GetMaxSize()){
      Redistribute(right_neighbor_page,node,0);
      buffer_pool_manager_->UnpinPage(right_neighbor_id,true);
      buffer_pool_manager_->UnpinPage(parent_internal_id,true);
      return true;
    }
  }
  if (index_at_parent>0){
    page_id_t left_neighbor_id = parent_page_l->ValueAt(index_at_parent-1);
    N* left_neighbor_page = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(left_neighbor_id)->GetData());
    int left_page_size = left_neighbor_page->GetSize();
    Coalesce(left_neighbor_page,node,parent_page_l,index_at_parent,transaction);
    buffer_pool_manager_->UnpinPage(left_neighbor_id,true);
    buffer_pool_manager_->UnpinPage(parent_internal_id,true);
    return true;
  }
  else{
    page_id_t  right_neighbor_id= parent_page_l->ValueAt(index_at_parent+1);
    N* right_neighbor_page = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(right_neighbor_id)->GetData());
    Coalesce(node,right_neighbor_page,parent_page_l,index_at_parent,transaction);
    buffer_pool_manager_->UnpinPage(parent_internal_id,true);
    buffer_pool_manager_->UnpinPage(right_neighbor_id,true);
    return true;
  }
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 将一个节点（node）的所有键值对全部移动到它的兄弟节点（neighbor_node）中。

然后，通知缓冲池管理器（BufferPoolManager），删除当前节点（即 node 页）。

同时，需要修改父节点（parent）以反映这次合并产生的变化：

父节点中需要删除指向被删节点（node）的记录。

注意：如果父节点因为这次删除也变得太小（比如不再满足最小大小要求），

则需要递归地对父节点继续执行合并（Coalesce）或重分配（Redistribute）。
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  page_id_t node_index = node->GetPageId();
  node->MoveAllTo(neighbor_node);

  parent->Remove(parent->ValueIndex(node_index));
  if (parent->GetSize()<parent->GetMinSize()){
    return CoalesceOrRedistribute<BPlusTree::InternalPage>(parent,transaction);
  }
  return true;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  page_id_t node_index = node->GetPageId();
  node->MoveAllTo(neighbor_node,parent->KeyAt(index),buffer_pool_manager_);

  parent->Remove(parent->ValueIndex(node_index));
  if (parent->GetSize()<parent->GetMinSize()){
    return CoalesceOrRedistribute<BPlusTree::InternalPage>(parent,transaction);
  }
  return true;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 将键值对（key & value）从兄弟节点（neighbor_node）移动到当前节点（node），以重新平衡两个节点的大小。

根据 index 参数的不同：

如果 index == 0：

表示当前节点（node）在左边，兄弟节点在右边；

从兄弟节点的第一个键值对拿过来，插入到当前节点的末尾。

如果 index != 0：

表示当前节点在右边，兄弟节点在左边；

从兄弟节点的最后一个键值对拿过来，插入到当前节点的开头。

使用模板 N 来泛化节点类型，可以是：

叶子节点（LeafPage）

或者内部节点（InternalPage）
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  if (index == 0){
    neighbor_node->MoveFirstToEndOf(node);
    page_id_t page_id_parent = node->GetParentPageId();
    auto fetch_page_l = buffer_pool_manager_->FetchPage(page_id_parent);
    auto parent_internal_page = reinterpret_cast<BPlusTreeInternalPage*>(fetch_page_l->GetData());

    GenericKey* used_key = neighbor_node->KeyAt(0);
    int index_at_parent = parent_internal_page->ValueIndex(neighbor_node->GetPageId());
    parent_internal_page->SetKeyAt(index_at_parent,used_key);

    buffer_pool_manager_->UnpinPage(page_id_parent,true);
  }
  else{
    neighbor_node->MoveLastToFrontOf(node);
    page_id_t page_id_parent = node->GetParentPageId();
    auto fetch_page_l = buffer_pool_manager_->FetchPage(page_id_parent);
    auto parent_internal_page = reinterpret_cast<BPlusTreeInternalPage*>(fetch_page_l->GetData());

    GenericKey* used_key = node->KeyAt(0);
    int index_at_parent = parent_internal_page->ValueIndex(node->GetPageId());
    parent_internal_page->SetKeyAt(index_at_parent,used_key);

    buffer_pool_manager_->UnpinPage(page_id_parent,true);
  }
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  page_id_t page_id_parent = node->GetParentPageId();
  auto fetch_page_l = buffer_pool_manager_->FetchPage(page_id_parent);
  auto parent_internal_page = reinterpret_cast<BPlusTreeInternalPage*>(fetch_page_l->GetData());

  page_id_t neighbor_node_index = neighbor_node->GetPageId();
  if (index == 0){
    // 不必fetch子节点以获取key,父节点已经有key了
    int neighbor_index_at_parent = parent_internal_page->ValueIndex(neighbor_node_index);
    GenericKey* used_key = parent_internal_page->KeyAt(neighbor_index_at_parent);
    neighbor_node->MoveFirstToEndOf(node,used_key,buffer_pool_manager_);
    parent_internal_page->SetKeyAt(neighbor_index_at_parent,neighbor_node->KeyAt(0));
  }
  else{
    int node_index_at_parent = parent_internal_page->ValueIndex(node->GetPageId());
    GenericKey* used_key = neighbor_node->KeyAt(neighbor_node->GetSize()-1);
    neighbor_node->MoveLastToFrontOf(node,parent_internal_page->KeyAt(node_index_at_parent),buffer_pool_manager_);
    parent_internal_page->SetKeyAt(node_index_at_parent,used_key);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 * 情况1：

当你删除了根节点中最后一个元素，但根节点还有最后一个子节点存在时：

需要把根节点指向它的唯一子节点，并更新根页的指向（root page id）。

情况2：

当你删除了整棵B+树的最后一个元素，即整棵树都空了，

那么就应该把根节点也删掉，整棵树成为空树。
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->IsLeafPage()){
    auto leave_page_l = reinterpret_cast<BPlusTreeLeafPage*>(old_root_node);
    if (leave_page_l->GetSize()==0){
      auto root_index_page = reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
      root_index_page->Delete(index_id_);
      root_page_id_ = INVALID_PAGE_ID;
      buffer_pool_manager_->UnpinPage(leave_page_l->GetPageId(),false);
      buffer_pool_manager_->DeletePage(leave_page_l->GetPageId());
      buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,true);
      return true;
    }
    else if (!old_root_node->IsLeafPage()&&old_root_node->GetSize()==1){
      auto root_internal_page = reinterpret_cast<BPlusTreeInternalPage*>(old_root_node);
      page_id_t child_page_id = root_internal_page->ValueAt(0);
      root_page_id_ = child_page_id;

      auto child_page_l =reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(child_page_id)->GetData());
      child_page_l ->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(child_page_id,true);

      UpdateRootPageId(0);
      buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(),false);
      buffer_pool_manager_->DeletePage(old_root_node->GetPageId());

      return true;
    }
    else{
      return false;
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 * 需要首先找到最左边的叶子页（也就是整棵树中最小的元素所在的页）。

然后构造一个索引迭代器（IndexIterator），让它从最小的键开始遍历。
 */
IndexIterator BPlusTree::Begin() {
  GenericKey* null_key;
  BPlusTreeLeafPage* first_leave_page = reinterpret_cast<BPlusTreeLeafPage*>((FindLeafPage(null_key,INVALID_PAGE_ID,true))->GetData());
  page_id_t first_leave_id = first_leave_page->GetPageId();

  return IndexIterator(first_leave_id,buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 * 输入参数：一个下界的 key（也叫 low key）。

需要首先找到包含这个 key 的叶子页。

然后从这个 key 开始构造一个索引迭代器。
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  BPlusTreeLeafPage* des_leave_page = reinterpret_cast<BPlusTreeLeafPage*>((FindLeafPage(key,INVALID_PAGE_ID,false))->GetData());
  page_id_t des_leave_id = des_leave_page->GetPageId();
  
  return IndexIterator(des_leave_id,buffer_pool_manager_,des_leave_page->KeyIndex(key,processor_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 * 构造一个表示B+树中键值对末尾（即遍历结束位置）的索引迭代器（IndexIterator）。

这个迭代器用于指示遍历完成，比如在循环中作为终止条件。
 */
IndexIterator BPlusTree::End() {
  return IndexIterator();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
//先把根节点fetch过来，->data之后转化成B_plus_tree_page
//然后不断向下寻找，注意每循环一次要unpin一次上次打开的page
//注意leftmost
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if (IsEmpty())return nullptr;
  auto tree_page_t = reinterpret_cast<BPlusTreePage*>((buffer_pool_manager_->FetchPage(root_page_id_))->GetData());
  
  page_id_t value_t = root_page_id_;
  while (!tree_page_t->IsLeafPage()){
    buffer_pool_manager_->UnpinPage(value_t,false);
    auto tree_internal_t = reinterpret_cast<BPlusTreeInternalPage*>(tree_page_t);
    if (!leftMost){
      value_t = tree_internal_t->Lookup(key,processor_);
    }
    else{
      value_t = tree_internal_t->ValueAt(0);
    }
    tree_page_t = reinterpret_cast<BPlusTreePage*>((buffer_pool_manager_->FetchPage(value_t))->GetData());
  }
  auto ans = buffer_pool_manager_->FetchPage(value_t);
  buffer_pool_manager_->UnpinPage(value_t,false);
  return ans;
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page isdefined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
// 改变 root-index 键值对 类，根据insert record 来决定是
// 插入还是更新
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto root_index_t = reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  if (insert_record){
    root_index_t->Insert(index_id_,root_page_id_);
  }
  else{
    root_index_t->Update(index_id_,root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}