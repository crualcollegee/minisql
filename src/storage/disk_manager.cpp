#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  // ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 *
    tips:
     1. 检查超界问题
     2. 检查每个extend
     3. 根据物理地址将一个bitmap读入内存
     4. 分配页
     5. 写回硬盘
     6. 没有空间的话新建extent
 */
page_id_t DiskManager::AllocatePage() {
    DiskFileMetaPage* meta_page = reinterpret_cast<DiskFileMetaPage*>(meta_data_);
    bool sign = false;
    if (meta_page->GetAllocatedPages()>=MAX_VALID_PAGE_ID){
        return INVALID_PAGE_ID;
    }
    uint32_t locate_Bitmap;
    for (locate_Bitmap=0;locate_Bitmap<meta_page->GetExtentNums();locate_Bitmap++){
        if (meta_page->GetExtentUsedPage(locate_Bitmap)<BITMAP_SIZE ){
            sign =true;
            break;
        }
    }
    int locate_physics = 1 + (1 + BITMAP_SIZE) * locate_Bitmap;
    char* page_data = new char[sizeof(BitmapPage<PAGE_SIZE>)];
    ReadPhysicalPage(locate_physics,page_data);
    BitmapPage<PAGE_SIZE>* Bitpage=reinterpret_cast<BitmapPage<PAGE_SIZE>*>(page_data);
    uint32_t page_offset;
    if (!(Bitpage->AllocatePage(page_offset))){
        throw std::exception();
    }
    WritePhysicalPage(locate_physics,page_data);
    delete[] page_data;
    meta_page->num_allocated_pages_++;
    meta_page->extent_used_page_[locate_Bitmap]++;
    if (!sign){
        meta_page->num_extents_++;
    }
    return locate_Bitmap * BITMAP_SIZE + page_offset;
}

/*
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
    DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
    if (logical_page_id >= MAX_VALID_PAGE_ID){
        throw std::exception();
    }
    int locate_physics =( logical_page_id / BITMAP_SIZE ) * (1 + BITMAP_SIZE) + 1;
    // 将逻辑地址转化为物理地址非常的关键
    uint32_t page_offset = logical_page_id % BITMAP_SIZE;
    char* page_data = new char[sizeof(BitmapPage<PAGE_SIZE>)];
    ReadPhysicalPage(locate_physics,page_data);
    BitmapPage<PAGE_SIZE>* Bitpage=reinterpret_cast<BitmapPage<PAGE_SIZE>*>(page_data);
    if (!Bitpage->DeAllocatePage(page_offset)){
        throw std::exception();
    }
    WritePhysicalPage(locate_physics,page_data);
    delete[] page_data;
    meta_page->num_allocated_pages_--;
    uint32_t locate_Bitmap;
    locate_Bitmap = locate_physics / BITMAP_SIZE;
    meta_page->extent_used_page_[locate_Bitmap]--;
}

/*
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
    if (logical_page_id >= MAX_VALID_PAGE_ID){
        throw std::exception();
    }
    int locate_physics =( logical_page_id / BITMAP_SIZE ) * (1 + BITMAP_SIZE) + 1;
    uint32_t page_offset = logical_page_id % BITMAP_SIZE;
    char* page_data = new char[sizeof(BitmapPage<PAGE_SIZE>)];
    ReadPhysicalPage(locate_physics,page_data);
    BitmapPage<PAGE_SIZE>* Bitpage=reinterpret_cast<BitmapPage<PAGE_SIZE>*>(page_data);
    delete[] page_data;
    return Bitpage->IsPageFree(page_offset);
}

/*
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
    if (logical_page_id >= MAX_VALID_PAGE_ID){
        throw std::exception();
    }
    return ( logical_page_id / BITMAP_SIZE ) + logical_page_id + 2;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}