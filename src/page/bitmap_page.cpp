#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
    if (next_free_page_>=GetMaxSupportedSize()){
        return false;
    }
    page_offset=next_free_page_;
    uint32_t byte_index=page_offset / 8;
    uint8_t bit_index=page_offset % 8;
    page_allocated_++;
    bytes[byte_index]|=(1<<bit_index);
    uint32_t count=0;

    if (page_allocated_ == GetMaxSupportedSize()) {
        next_free_page_ = GetMaxSupportedSize();
        return true;
    }

    do{
        count++;
        next_free_page_+=1;
        next_free_page_%=GetMaxSupportedSize();
        if (IsPageFree(next_free_page_)){
            return true;
        }
    }while(count!=GetMaxSupportedSize()+3);
    return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
    if (page_offset>=GetMaxSupportedSize())
    {
        return false;
    }
    if (IsPageFree(page_offset)){
        return false;
    }
    page_allocated_--;
    uint32_t byte_index=page_offset / 8;
    uint8_t bit_index=page_offset % 8;
    bytes[byte_index]&=~(1<<bit_index);
     if (page_offset < next_free_page_) {
         next_free_page_ = page_offset;
     }
    return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
    uint32_t byte_index=page_offset / 8;
    uint8_t bit_index=page_offset % 8;
    return IsPageFreeLow(byte_index,bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
    return (bytes[byte_index]&(1<<bit_index))==0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;