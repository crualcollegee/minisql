#include "page/bitmap_page.h"

#include "glog/logging.h"

template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
    uint32_t count=0;
    while(!IsPageFree(next_free_page_)){
        count++;
        next_free_page_++;
        next_free_page_%=GetMaxSupportedSize();
        if (count>GetMaxSupportedSize()){
            return false;
        }
    }

    page_allocated_++;
    page_offset=next_free_page_;
    uint32_t byte_index=page_offset / 8;
    uint8_t bit_index=page_offset % 8;
    bytes[byte_index]|=(1<<bit_index);
    return true;

}


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