#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  // 获取行的序列化大小
  uint32_t row_size = row.GetSerializedSize(schema_);
  // 如果行的大小超过页面大小，则无法插入
  if (row_size >= PAGE_SIZE) {
    return false;
  }

  // 从第一个页面开始尝试插入
  page_id_t current_page_id = first_page_id_;
  while (current_page_id != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
    if (page == nullptr) {
      //printf("Error: Unable to fetch page %d\n", current_page_id);
      return false; // 无法获取页面
    }

    page->WLatch(); // 加写锁
    RowId rid;
    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      // 插入成功，设置行的 RowId 并释放页面
      row.SetRowId(rid);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(current_page_id, true);
      return true;
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(current_page_id, false);

    // 尝试下一个页面
    current_page_id = page->GetNextPageId();
  }

  // 如果所有页面都无法插入，则创建新页面
  auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(current_page_id));
  if (new_page == nullptr) {
    printf("Error: Unable to create new page\n");
    return false; // 无法创建新页面
  }

  new_page->Init(current_page_id, first_page_id_, log_manager_, txn); // 初始化新页面
  RowId rid;
  new_page->WLatch();
  bool success = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  new_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(current_page_id, true);

  if (success) {
    row.SetRowId(rid);
    first_page_id_ = current_page_id; // 更新表的第一个页面 ID
  }

  return success;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &new_row, const RowId &rid, Txn *txn) {
  // 获取包含旧记录的页面
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false; // 无法获取页面
  }

  page->WLatch(); // 加写锁

  // 创建一个旧记录对象，用于存储旧记录数据
  Row old_row(rid);

  // 调用页面的 UpdateTuple 方法尝试更新记录
  bool success = page->UpdateTuple(new_row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if (success) {
    // 更新成功，设置新记录的 RowId
    new_row.SetRowId(rid);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
    return true;
  }

  // 如果页面无法容纳更新后的数据，则需要删除旧记录并插入新记录
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);

  // 标记旧记录为删除
  if (!MarkDelete(rid, txn)) {
    return false; // 删除失败
  }

  // 插入新记录
  if (!InsertTuple(new_row, txn)) {
    // 如果插入新记录失败，回滚删除操作
    RollbackDelete(rid, txn);
    return false;
  }

  return true;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  ASSERT(row != nullptr, "Row pointer cannot be null.");

  // 获取包含目标记录的页面
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr) {
    return false; // 无法获取页面
  }

  page->RLatch(); // 加读锁

  // 调用页面的 GetTuple 方法尝试读取记录
  bool success = page->GetTuple(row, schema_, txn, lock_manager_);

  page->RUnlatch(); // 释放读锁
  buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);

  return success;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

TableIterator TableHeap::Begin(Txn *txn) {
  // 获取第一个有效的记录的 RowId
  RowId first_rid;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (page == nullptr) {
    return End(); // 如果无法获取页面，返回结束迭代器
  }

  page->RLatch(); // 加读锁
  bool has_tuple = page->GetFirstTupleRid(&first_rid); // 获取第一个有效记录的 RowId
  page->RUnlatch(); // 释放读锁
  buffer_pool_manager_->UnpinPage(first_page_id_, false);

  if (!has_tuple) {
    return End(); // 如果没有有效记录，返回结束迭代器
  }

  return TableIterator(this, first_rid, txn);
}

TableIterator TableHeap::End() {
  // 返回一个无效的迭代器
  return TableIterator(this, RowId(INVALID_ROWID), nullptr);
}
