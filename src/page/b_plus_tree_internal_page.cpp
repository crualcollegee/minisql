#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

// 一个internal_page 表示一个节点，包含多个 generic_key ，一个generic_key表示一个key和value
#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))  //表示每个键值对占用的字节数，即键的大小加上指向子节点的页面 ID 的大小。
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());    // getkeysize()是b_plus_tree_page 定义的 key_size_
}

page_id_t InternalPage::ValueAt(int index) const {  // value 指的是 指向的 页面id
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int begin = 1;
  int end = GetSize();
  int mid;
  while (begin < end){
    mid = (begin + end)/2;
    if (KM.CompareKeys(key,KeyAt(mid))>=0){
      begin = mid+1;
    }
    else if (KM.CompareKeys(key,KeyAt(mid))<0)
    {
      end = mid;
    }
  }
  return ValueAt(begin-1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  SetKeyAt(1, new_key);
  SetValueAt(1,new_value);
  SetValueAt(0,old_value);
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int index = ValueIndex(old_value);
  int size_1 = GetSize();
  for (int i=size_1; i > index + 1; i--){
    GenericKey* key_1 = KeyAt(i - 1);
    page_id_t value_1 = ValueAt(i - 1);
    SetKeyAt(i, key_1);
    SetValueAt(i, value_1); 
  }
  SetKeyAt(index+1, new_key);
  SetValueAt(index+1 ,new_value);
  SetSize(size_1+1);
  return size_1+1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  char *src_1 = reinterpret_cast<char*>(KeyAt(0));
  for (int i=0; i<size-size/2; i++){
    src_1 += pair_size;
  }
  void* src = reinterpret_cast<void*>(src_1);
  recipient->CopyNFrom(src,size/2,buffer_pool_manager);
  SetSize(size - size/2);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
// 移动size个到当前internalpage的最后，注意调用函数
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  char* new_page = reinterpret_cast<char*>(src);
  int index = GetSize();
  for (int i=0;i<size;i++){
    PairCopy(PairPtrAt(index),src,1);
    new_page += sizeof(pair_size);
    SetSize(GetSize()+1);
    index ++;  
  }
}
//void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  int size_t = GetSize();
  for (int i=index ;i<size_t-1 ; i++){
    SetKeyAt(i, KeyAt(i+1));
    SetValueAt(i, ValueAt(i+1));
  }
  SetSize(size_t-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
//当前internalpage只有一个节点了，删去它
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  if (GetSize()!=1){
    return INVALID_PAGE_ID;
  }
  else{
    page_id_t ans = ValueAt(0);
    SetSize(0);
    return ans;
  }
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
// 将当前的internalpage 全部移到recipent中去，注意加分隔层，还要在内存池中完全删除这个page
// 可以得出规律:一旦将 一块只有key没有value的generickey传到其他page的时候（即传出第一页），就要加分隔层
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);

  GenericKey* new_key = KeyAt(1);
  recipient->CopyNFrom(reinterpret_cast<void*>(new_key), GetSize()-1, buffer_pool_manager);
  SetSize(0);
  buffer_pool_manager->DeletePage(this->GetPageId());
}
//void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
//void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
// 加分隔层和调用remove
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
  Remove(0);                                  
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
// 设定一个generickey到当前page的最后，并且从内存池中读出这个generickey对应的子树，设定其父亲为当前internalpage
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int size_1 = GetSize();
  SetKeyAt(size_1, key);
  SetValueAt(size_1, value);
  SetSize(size_1+1);

  InternalPage* new_page =reinterpret_cast<InternalPage*>((buffer_pool_manager)->FetchPage(value)); 
  new_page -> SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
// 将当前页面的最后一个键值对移到 "recipient" 页面头部。
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  int size_t = GetSize();
  page_id_t new_page = ValueAt(size_t-1);
  Remove(size_t-1);
  recipient->CopyFirstFrom(new_page, buffer_pool_manager);
  recipient->SetKeyAt(1, middle_key);
}

/*
 * 将一个条目添加到开头。
 * 由于这是一个内部页面，移动的条目（即子页面）的父页面需要更新。
 * 因此，我需要通过修改其父页面 ID 来“收养”它，并且需要使用 BufferPoolManager 来持久化更新。
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int size_t = GetSize();
  for (int i=0; i<size_t; i++){
    GenericKey* new_key = KeyAt(i);
    page_id_t new_value = ValueAt(i);
    SetKeyAt(i+1,new_key);
    SetValueAt(i+1,new_value);
  }
  SetValueAt(0,value);
  InternalPage* new_page =reinterpret_cast<InternalPage*>((buffer_pool_manager)->FetchPage(value)); 
  new_page -> SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
}
