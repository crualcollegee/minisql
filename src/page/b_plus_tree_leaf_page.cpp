#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
  SetSize(0);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  int begin = 0;
  int end = GetSize();
  int mid;
  while (begin < end){
    mid = (begin + end)/2;
    if (KM.CompareKeys(key,KeyAt(mid))>0){
      begin = mid+1;
    }
    else if (KM.CompareKeys(key,KeyAt(mid))<0)
    {
      end = mid;
    }
    else if (KM.CompareKeys(key,KeyAt(mid))==0){
      return mid;
    }
  }
  return begin;
}


/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int page_insert = KeyIndex(key,KM);
  int size_t = GetSize();
  for (int i=size_t; i>page_insert; i--){
    PairCopy(KeyAt(i),KeyAt(i-1),1);
  }
  SetKeyAt(page_insert,key);
  SetValueAt(page_insert,value);
  SetSize(size_t+1);
  return size_t+1;
}
// int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int size_t = GetSize();
  char *src_l = reinterpret_cast<char*>(KeyAt(0));
  for (int i=0; i<size_t/2; i++){
    src_l += pair_size;
  }
  void* src = reinterpret_cast<void*>(src_l);
  recipient->CopyNFrom(src,size_t/2);
  SetSize(size_t - size_t/2);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  src = reinterpret_cast<char*> (src);
  int index = GetSize();
  for (int i=0;i<size;i++){
    PairCopy(PairPtrAt(index), src, 1);
    src += pair_size;
    SetSize(GetSize()+1);
    index ++;
  }
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int keyindex_t = KeyIndex(key,KM);
  if (keyindex_t == GetSize()){
    return false;
  }
  else if (KM.CompareKeys(key,KeyAt(keyindex_t))!=0){
    return false;
  }
  else{
    value=ValueAt(keyindex_t);
    return true;
  }
}
//int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  RowId value_t;
  bool isSuccess = Lookup(key,value_t,KM);
  if (!isSuccess){
    return GetSize();
  }
  else{
    //Paircopy多个可能有内存重叠的危险
    int index_t = KeyIndex(key,KM);
    char* char_index = reinterpret_cast<char*>(KeyAt(index_t));
    for (int i=index_t;i<GetSize()-1;i++){
      PairCopy(char_index,char_index+pair_size,1);
      char_index+=pair_size;
    }
    SetSize(GetSize()-1);
    return GetSize();
  }
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  GenericKey* new_key = KeyAt(0);
  recipient->CopyNFrom(new_key,GetSize());
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  char* char_index = reinterpret_cast<char*>(KeyAt(0));
  recipient->CopyNFrom(char_index,1);
  for (int i=0;i<GetSize()-1;i++){
    PairCopy(char_index,char_index+pair_size,1);
    char_index+=pair_size;
  }
  SetSize(GetSize()-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  int size_t = GetSize();
  SetKeyAt(size_t,key);
  SetValueAt(size_t,value);
  SetSize(size_t+1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  GenericKey* new_key = KeyAt(GetSize()-1);
  RowId new_row = ValueAt(GetSize()-1);
  recipient->CopyFirstFrom(new_key,new_row);
  SetSize(GetSize()-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  char* last_key = reinterpret_cast<char*>(KeyAt(GetSize()-1));
  for (int i=0;i<GetSize();i++){
    PairCopy(last_key+pair_size,last_key,1);
    last_key-=pair_size;
  }
  SetKeyAt(0,key);
  SetValueAt(0,value);
  SetSize(GetSize()+1);
}