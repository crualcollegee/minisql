#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}


Page *BufferPoolManager::FetchPage(page_id_t page_id) {
    if (page_id >= MAX_VALID_PAGE_ID){
        return nullptr;
    }
    if (page_table_.find(page_id) != page_table_.end()){
        frame_id_t new_frame = page_table_[page_id];
        pages_[new_frame].pin_count_++;
        replacer_->Pin(new_frame);
        return &(pages_[new_frame]);
    }
    if (!free_list_.empty()){
        frame_id_t new_frame = free_list_.front();
        free_list_.pop_front();
        latch_.lock();
        disk_manager_->ReadPage(page_id,pages_[new_frame].GetData());
        latch_.unlock();
        pages_[new_frame].pin_count_ = 1;
        pages_[new_frame].page_id_ = page_id;
        page_table_.erase(pages_[new_frame].page_id_);
        // 添加新映射
        page_table_.insert({page_id,new_frame});
        return &(pages_[page_id]);
    }
    else{
        frame_id_t new_frame;
        bool isSuccess = replacer_->Victim(&new_frame);
        if (!isSuccess)return nullptr;
        if (pages_[new_frame].IsDirty()){
            latch_.lock();
            disk_manager_->WritePage(pages_[new_frame].GetPageId(),pages_[new_frame].GetData());
            latch_.unlock();
            pages_[new_frame].is_dirty_=false;
        }
        latch_.lock();
        disk_manager_->ReadPage(page_id,pages_[new_frame].GetData());
        latch_.unlock();
        pages_[new_frame].pin_count_ = 1;
        pages_[new_frame].page_id_ = page_id;
        page_table_.erase(pages_[new_frame].page_id_);
        page_table_.insert({page_id,new_frame});
        return &(pages_[page_id]);
    }

  return nullptr;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
    frame_id_t new_frame;
    if (!free_list_.empty()){
        page_id = AllocatePage();
        if (page_id == INVALID_PAGE_ID) {
            return nullptr;
        }
        new_frame = free_list_.front();
        free_list_.pop_front();
    }
    else{
        bool isSuccess = replacer_->Victim(&new_frame);
        if (!isSuccess)return nullptr;
        page_id = AllocatePage();
        //这一步要放在后面isSuccess的后面，不然有问题
        if (page_id == INVALID_PAGE_ID) {
            return nullptr;
        }
        if (pages_[new_frame].IsDirty()){
            latch_.lock();
            disk_manager_->WritePage(pages_[new_frame].GetPageId(),pages_[new_frame].GetData());
            latch_.unlock();
            pages_[new_frame].is_dirty_=false;
        }
        page_table_.erase(pages_[new_frame].page_id_);
    }
    pages_[new_frame].ResetMemory();
    page_table_[page_id] = new_frame;
    pages_[new_frame].pin_count_ = 1;
    pages_[new_frame].page_id_ = page_id;
    return &(pages_[new_frame]);
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
    frame_id_t delete_frame;
    if (page_table_.find(page_id)==page_table_.end()){
        DeallocatePage(page_id);
        return true;
    }
    else{
        delete_frame = page_table_[page_id];
        if (pages_[delete_frame].pin_count_!=0){
            return false;
        }
        else{
            pages_[delete_frame].ResetMemory();
            pages_[delete_frame].page_id_ = INVALID_PAGE_ID;
            pages_[delete_frame].is_dirty_ = false;
            page_table_.erase(page_id);
            replacer_->Unpin(delete_frame);
            free_list_.push_back(delete_frame);
            DeallocatePage(page_id);
            return true;
        }
    }
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    if (page_table_.find(page_id)==page_table_.end()){
        return false;
    }
    else{
        frame_id_t unpin_frame = page_table_[page_id];
        if (pages_[unpin_frame].pin_count_==0)return true;
        pages_[unpin_frame].pin_count_--;
        if (pages_[unpin_frame].pin_count_==0){
            replacer_->Unpin(unpin_frame);
        }
        return true;
    }
}


bool BufferPoolManager::FlushPage(page_id_t page_id) {
    if (page_table_.find(page_id)==page_table_.end()){
        return false;
    }
    else{
        frame_id_t flush_frame = page_table_[page_id];
        if (pages_[flush_frame].pin_count_!=0){
            return false;
        }
        else{
            latch_.lock();
            disk_manager_->WritePage(pages_[flush_frame].GetPageId(),pages_[flush_frame].GetData());
            latch_.unlock();
            return true;
        }
    }
  return false;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}