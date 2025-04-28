#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  // 获取行的序列化大小
  uint32_t row_size = row.GetSerializedSize(schema_);
  if (row_size >= PAGE_SIZE) {
    // 行太大，直接拒绝
    return false;
  }

  // 在现有 page_free_space_ 中查找可容纳行的页面
  auto page_iter = std::find_if(
      page_free_space_.begin(), page_free_space_.end(),
      [row_size](const auto &pair) {
        return pair.second >= row_size + TablePage::SIZE_TUPLE;
      });

  TablePage *target_page = nullptr;

  if (page_iter == page_free_space_.end()) {
    // 没找到 → 创建新页
    page_id_t new_page_id;
    auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
    if (new_page == nullptr) {
      return false; // 创建失败
    }

    // 链接新页到链表尾
    page_id_t prev_page_id = INVALID_PAGE_ID;
    if (!page_free_space_.empty()) {
      prev_page_id = page_free_space_.rbegin()->first;
    }

    new_page->Init(new_page_id, prev_page_id, log_manager_, txn);

    // 如果存在前页，设置其 next_page_id
    if (prev_page_id != INVALID_PAGE_ID) {
      auto prev_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(prev_page_id));
      prev_page->WLatch();
      prev_page->SetNextPageId(new_page_id);
      prev_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(prev_page_id, true);
    }

    // 更新 target_page 指针
    target_page = new_page;
  } else {
    // 找到已有 page，直接取出来
    target_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_iter->first));
  }

  // 插入行
  target_page->WLatch();
  bool insert_success = target_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  target_page->WUnlatch();

  if (!insert_success) {
    // 插入失败，释放页面
    buffer_pool_manager_->UnpinPage(target_page->GetPageId(), false);
    return false;
  }

  // 更新该页面的剩余空间记录
  page_free_space_[target_page->GetTablePageId()] = target_page->GetFreeSpaceRemaining();

  // 释放页面
  buffer_pool_manager_->UnpinPage(target_page->GetPageId(), true);

  return true;
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
