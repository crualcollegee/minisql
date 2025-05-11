#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  return sizeof(CATALOG_METADATA_MAGIC_NUM) + sizeof(index_meta_pages_.size()) + sizeof(table_meta_pages_.size()) +
         index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t)) +
         table_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
  LogManager *log_manager, bool init)
: buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if (!init) {
    auto page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(reinterpret_cast<char *>(page->GetData()));
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
    for(auto iter :catalog_meta_->table_meta_pages_){//对于每一个page
      auto table_meta_page = buffer_pool_manager_->FetchPage(iter.second);
      TableMetadata *table_metadata = nullptr;
      TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_metadata);
      table_names_[table_metadata->GetTableName()] = table_metadata->GetTableId();//设置table_names
      TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_metadata->GetFirstPageId(), table_metadata->GetSchema(),log_manager_, lock_manager_);//创建table_heap
      auto table_info = TableInfo::Create();
      table_info->Init(table_metadata, table_heap);
      tables_[table_metadata->GetTableId()] = table_info;
      if(table_metadata->GetTableId() >= next_table_id_){
        next_table_id_ = table_metadata->GetTableId() + 1;
      }
  }
  for(auto iter : catalog_meta_->index_meta_pages_){
    auto index_meta_page = buffer_pool_manager_->FetchPage(iter.second);
    IndexMetadata *index_meta_data = nullptr;
    IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta_data);
    index_names_[tables_[index_meta_data->GetTableId()]->GetTableName()][index_meta_data->GetIndexName()] = index_meta_data->GetIndexId();
    IndexInfo *index_info = IndexInfo::Create();
    index_info->Init(index_meta_data, tables_[index_meta_data->GetTableId()], buffer_pool_manager_);
    indexes_[index_meta_data->GetIndexId()] = index_info;
    if(index_meta_data->GetIndexId() >= next_index_id_){
      next_index_id_ = index_meta_data->GetIndexId() + 1;
    }
    }
  }
  else{
  catalog_meta_ = CatalogMeta::NewInstance();
  }
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  (void)txn;  // Mark txn as unused to suppress the warning
  if (table_names_.count(table_name) > 0) {
    return DB_TABLE_ALREADY_EXIST;
  }

  Schema *schema_copy ;
  schema_copy = schema_copy->DeepCopySchema(schema);
  table_id_t table_id = catalog_meta_->GetNextTableId();
  page_id_t meta_page_id, table_page_id;
  auto table_heap = TableHeap::Create(buffer_pool_manager_, schema_copy,txn, log_manager_, lock_manager_);
  table_page_id = table_heap ->GetFirstPageId();
  // 创建 TableMetadata
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, table_page_id, schema_copy);

  // 分配元数据页面
  Page *meta_page = buffer_pool_manager_->NewPage(meta_page_id);
  if (meta_page == nullptr) {
    delete schema_copy;
    return DB_FAILED;
  }

  // 分配表数据页面
  Page *table_page = buffer_pool_manager_->NewPage(table_page_id);
  if (table_page == nullptr) {
    delete schema_copy;
    buffer_pool_manager_->UnpinPage(meta_page_id, false);
    return DB_FAILED;
  }

  // 序列化元数据
  table_meta->SerializeTo(meta_page->GetData());
  // 初始化 TableInfo
  TableHeap* tableheap ;
  tableheap = tableheap -> Create(buffer_pool_manager_, schema_copy, nullptr,nullptr,nullptr);
  table_info = TableInfo::Create();
  table_info->Init(table_meta, tableheap);
  // 更新目录数据结构
  table_names_[table_name] = table_id;
  tables_[table_id] = table_info;
  // 更新系统目录元数据
  Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if (catalog_meta_page == nullptr) {
    delete schema_copy;
    buffer_pool_manager_->UnpinPage(meta_page_id, false);
    buffer_pool_manager_->UnpinPage(table_page_id, false);
    return DB_FAILED;
  }
  catalog_meta_->table_meta_pages_[table_id] = meta_page_id;
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());

  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  // 释放页面
  buffer_pool_manager_->UnpinPage(meta_page_id, true);
  buffer_pool_manager_->UnpinPage(table_page_id, true);

  return DB_SUCCESS;
}
/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto table = table_names_.find(table_name);
  if (table == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  auto table_id = table->second;
  table_info = tables_[table_id];
  if (table_info == nullptr) {
    return DB_TABLE_NOT_EXIST;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for(auto iter : tables_){
    tables.push_back(iter.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
  const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
  const string &index_type) {
//table是否存在
    auto table = table_names_.find(table_name);
    if (table == table_names_.end()) {
      return DB_TABLE_NOT_EXIST;
    }
    auto table_id = table->second;
    auto table_info = tables_[table_id];
    if (table_info == nullptr) {
      return DB_TABLE_NOT_EXIST;
    }
    //index是否存在
    auto iter = index_names_.find(table_name);
    if (iter != index_names_.end()) {
      auto index = iter->second.find(index_name);
    if (index != iter->second.end()) {
    return DB_INDEX_ALREADY_EXIST;
    }
    }
    //创建index
    index_id_t index_id = catalog_meta_->GetNextIndexId();
    index_info = index_info->Create();
    std::vector<uint32_t> key_map;
    Schema *schema_ = table_info->GetSchema();
    for (auto &column_name : index_keys) {
      uint32_t index;
      if(schema_->GetColumnIndex(column_name, index) == DB_COLUMN_NAME_NOT_EXIST) {
        return DB_COLUMN_NAME_NOT_EXIST;
      }
      key_map.push_back(index);
    }
    page_id_t index_page_id, meta_page_id = 0;
    Page *meta_page = buffer_pool_manager_->NewPage(meta_page_id);
    Page *index_page = buffer_pool_manager_->NewPage(index_page_id);
    IndexMetadata *index_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map);
    index_meta->SerializeTo(meta_page->GetData());
    //更新index_info
    index_info->Init(index_meta, table_info, buffer_pool_manager_);
    Row key;
    for (auto it = table_info->GetTableHeap()->Begin(nullptr); it != table_info->GetTableHeap()->End(); it++) {
      auto row = *it;
      row.GetKeyFromRow(table_info->GetSchema(), index_info->GetIndexKeySchema(), key);
      index_info->GetIndex()->InsertEntry(key, row.GetRowId(), txn);
    }
    //init index
    index_names_[table_name][index_name] = index_id;
    indexes_[index_id] = index_info;
    //更新catalog meta data
    catalog_meta_ ->index_meta_pages_[index_id] = meta_page_id;
    Page *catalog_meta_page_ = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    char* buf = catalog_meta_page_->GetData();
    catalog_meta_->SerializeTo(buf);
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  auto index_table = index_names_.find(table_name);
  if (index_table == index_names_.end())return DB_TABLE_NOT_EXIST;
  auto index = index_table->second.find(index_name);
  if (index == index_table->second.end())return DB_INDEX_NOT_FOUND;
  auto index_id = index->second;
  index_info = indexes_.find(index_id)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  auto index_table = index_names_.find(table_name);
  if (index_table == index_names_.end())return DB_TABLE_NOT_EXIST;
  for(auto iter : index_table->second){
    auto index = indexes_.find(iter.second);
    if(index != indexes_.end()){
      indexes.push_back(index->second);
    }
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  auto table = table_names_.find(table_name);
  if (table == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  auto table_id = table->second;
  auto iter = tables_.find(table_id);
  if (iter == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  auto page_id = iter->second->GetRootPageId();
  buffer_pool_manager_->DeletePage(page_id);
  delete iter->second;
  tables_.erase(iter);
  table_names_.erase(table_name);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  auto index_table = index_names_.find(table_name);
  if (index_table == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  auto index = index_table->second.find(index_name);
  if (index == index_table->second.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  auto index_id = index->second;
  auto iter = indexes_.find(index_id);
  if (iter == indexes_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  page_id_t page_id = catalog_meta_->index_meta_pages_[index_id];
  buffer_pool_manager_->DeletePage(page_id);
  delete iter->second;
  indexes_.erase(iter);
  index_names_[table_name].erase(index_name);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  if(buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) return DB_SUCCESS;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if(tables_.count(table_id) > 0) return DB_TABLE_ALREADY_EXIST;
  auto iter = tables_.find(table_id);
  auto page = buffer_pool_manager_->FetchPage(page_id);
  auto table_meta = reinterpret_cast<TableMetadata *>(page->GetData());
  TableMetadata *table_metadata = nullptr;
  TableMetadata::DeserializeFrom(page->GetData(), table_metadata);
  if (table_metadata == nullptr) {
    return DB_FAILED;
  }
  auto table_heap = TableHeap::Create(buffer_pool_manager_, table_metadata->GetFirstPageId(), table_metadata->GetSchema(), log_manager_, lock_manager_);
  auto table_info = TableInfo::Create();
  table_info->Init(table_metadata, table_heap);
  tables_[table_id] = table_info;
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if(indexes_.count(index_id) > 0) return DB_INDEX_ALREADY_EXIST;
  auto iter = indexes_.find(index_id);
  if (iter != indexes_.end()) {
    return DB_INDEX_ALREADY_EXIST;
  }
  auto page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    return DB_FAILED;
  }
  auto index_meta = reinterpret_cast<IndexMetadata *>(page->GetData());
  if (index_meta == nullptr) {
    return DB_FAILED;
  }
  IndexMetadata *index_metadata = nullptr;
  IndexMetadata::DeserializeFrom(page->GetData(), index_metadata);
  if (index_metadata == nullptr) {
    return DB_FAILED;
  }
  auto index_info = IndexInfo::Create();
  index_info->Init(index_metadata, tables_[index_metadata->GetTableId()], buffer_pool_manager_);
  indexes_[index_id] = index_info;
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto iter = tables_.find(table_id);
  if (iter == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = iter->second;
  return DB_SUCCESS;
}