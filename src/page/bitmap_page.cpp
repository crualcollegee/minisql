#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
    if (!IsPageFree(page_offset)){
        return false;
    }
    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;
    bytes[byte_index]|=(1<<bit_index);
    page_allocated_++;
    return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
    if (IsPageFree(page_offset)){
        return false;
    }
    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;
    bytes[byte_index]&=~(1<<bit_index);
    page_allocated_--;
    return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;
    return IsPageFreeLow(byte_index,bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
    return byte[byte_index]&(1<<bit_index)==0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;