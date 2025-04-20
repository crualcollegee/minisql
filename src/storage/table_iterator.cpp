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
  ASSERT(current_row_ != nullptr, "Incrementing an invalid iterator.");

  RowId next_rid;
  // Attempt to move to the next tuple by incrementing the RowId directly.
  current_rid_ = RowId(current_rid_.Get() + 1);

  // Check if the new RowId is valid and fetch the corresponding tuple.
  if (!table_heap_->GetTuple(current_row_, txn_)) {
    // If the tuple is invalid, set the iterator to the end.
    current_rid_ = RowId(INVALID_ROWID);
    delete current_row_;
    current_row_ = nullptr;
  } else {
    // Update the current_row_ with the new RowId.
    current_row_->SetRowId(current_rid_);
  }
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator temp(*this); // Use the copy constructor to create a temporary iterator
  ++(*this); // Increment the current iterator
  return temp; // Return the temporary iterator
}
