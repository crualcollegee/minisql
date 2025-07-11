#ifndef MINISQL_DISK_FILE_META_PAGE_H
#define MINISQL_DISK_FILE_META_PAGE_H

#include <cstdint>

#include "page/bitmap_page.h"

static constexpr page_id_t MAX_VALID_PAGE_ID = (PAGE_SIZE - 8) / 4 * BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
// -8 是减去disk meta page 的基础信息，剩下的是记录extent page 的信息；除以4是因为每个extent page需4个字节，而再乘以GetMax..是为了
// 获取所有extent page映射的实际page的大小总和

class DiskFileMetaPage {
 public:
  uint32_t GetExtentNums() { return num_extents_; }

  uint32_t GetAllocatedPages() { return num_allocated_pages_; }

  uint32_t GetExtentUsedPage(uint32_t extent_id) {
    if (extent_id >= num_extents_) {
      return 0;
    }
    return extent_used_page_[extent_id];
  }

 public:
  uint32_t num_allocated_pages_{0};   //所有实际磁盘数据pages
  uint32_t num_extents_{0};  // each extent consists with a bit map and BIT_MAP_SIZE pages
  uint32_t extent_used_page_[0];
};

#endif  // MINISQL_DISK_FILE_META_PAGE_H
