#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn)
    : table_heap_(table_heap), current_rid_(rid), txn_(txn), current_row_(nullptr) {
      if (current_rid_ == RowId{0}) {
        auto page_id = table_heap_->GetFirstPageId();
        if (page_id == INVALID_PAGE_ID) {
          current_rid_ = RowId{-1};
          return;
        }
        // get first page
        auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
        if (page == nullptr) {
          DLOG(ERROR) << "Failed to fetch page";
        }
        // get first rid
        auto got = page->GetFirstTupleRid(&current_rid_);
        table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
        if (!got) {
          //DLOG(ERROR) << "No tuple found in first table";
          current_rid_ = RowId{-1};
        }
      } 
}

TableIterator::TableIterator(const TableIterator &other)
    : table_heap_(other.table_heap_), current_rid_(other.current_rid_), txn_(other.txn_), current_row_(nullptr) {
  if (other.current_row_ != nullptr) {
    current_row_ = new Row(*other.current_row_);
  }
}

TableIterator::~TableIterator() {
  if (current_row_ != nullptr) {
    delete current_row_;
    current_row_ = nullptr;
  }
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return current_rid_ == itr.current_rid_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  if (current_row_ == nullptr) {
    current_row_ = new Row(current_rid_);  // 分配新的 Row 对象
    table_heap_->GetTuple(current_row_, txn_);  // 填充 Row 数据
  }
  return *current_row_;  // 返回引用
}

Row *TableIterator::operator->() {
  Row *row = new Row(current_rid_);
  table_heap_->GetTuple(row, txn_);
  return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (this != &itr) {
    table_heap_ = itr.table_heap_;
    current_rid_ = itr.current_rid_;
    txn_ = itr.txn_;
    
  }
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  if (current_row_ != nullptr) {
    delete current_row_;  // 释放当前行
    current_row_ = nullptr;
  }

  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(current_rid_.GetPageId()));
  if (page == nullptr) {
    DLOG(ERROR) << "Failed to fetch page";
  }
  bool is_next = page->GetNextTupleRid(current_rid_, &current_rid_);
  table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  if (is_next) {
    return *this;
  }
  // get next page
  if (page->GetNextPageId() == INVALID_PAGE_ID) {
    current_rid_ = RowId{-1};
    return *this;
  }
  page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
  if (page == nullptr) {
    DLOG(ERROR) << "Failed to fetch page";
  }
  // get first rid
  bool is_first = page->GetFirstTupleRid(&current_rid_);
  table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  if (!is_first) current_rid_ = RowId{-1};
  return *this;
}


// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator temp(*this); 
  ++(*this); 
  return temp; 
}
