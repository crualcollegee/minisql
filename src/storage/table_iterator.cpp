#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn)
    : table_heap_(table_heap), current_rid_(rid), txn_(txn), current_row_(nullptr) {
  if (current_rid_.Get() != INVALID_ROWID.Get()) {
    current_row_ = new Row(current_rid_);
    if (!table_heap_->GetTuple(current_row_, txn_)) {
      delete current_row_;
      current_row_ = nullptr;
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
  ASSERT(current_row_ != nullptr, "Dereferencing an invalid iterator.");
  return *current_row_;
}

Row *TableIterator::operator->() {
  ASSERT(current_row_ != nullptr, "Accessing an invalid iterator.");
  return current_row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (this != &itr) {
    table_heap_ = itr.table_heap_;
    current_rid_ = itr.current_rid_;
    txn_ = itr.txn_;

    if (current_row_ != nullptr) {
      delete current_row_;
    }

    if (itr.current_row_ != nullptr) {
      current_row_ = new Row(*itr.current_row_);
    } else {
      current_row_ = nullptr;
    }
  }
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  // 获取当前页
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(current_rid_.GetPageId()));
  if (page == nullptr) {
    current_rid_ = RowId{-1};
    return *this;
  }

  // 尝试获取下一条 tuple 的 rid
  if (page->GetNextTupleRid(current_rid_, &current_rid_)) {
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return *this;
  }

  // 获取下一页 id
  auto next_page_id = page->GetNextPageId();
  table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);

  // 如果没有下一页，迭代器结束
  if (next_page_id == INVALID_PAGE_ID) {
    current_rid_ = RowId{-1};
    return *this;
  }

  // 取下一页
  page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
  if (page == nullptr) {
    current_rid_ = RowId{-1};
    return *this;
  }

  // 获取下一页的第一条 tuple
  if (!page->GetFirstTupleRid(&current_rid_)) {
    current_rid_ = RowId{-1};
  }

  table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return *this;
}


// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator temp(*this); 
  ++(*this); 
  return temp; 
}
