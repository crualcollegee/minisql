#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;


bool LRUReplacer::Victim(frame_id_t *frame_id) {
    if (lru_queue.empty()){
        return false;
    }
    *frame_id = lru_queue.back();
    lru_queue.pop_back();
    lru_set.erase(*frame_id);
    lru_size--;
    return true;
}


void LRUReplacer::Pin(frame_id_t frame_id) {
    if (lru_set.find(frame_id)==lru_set.end()){
        return;
    }
    lru_size--;
    lru_queue.remove(frame_id);
    lru_set.erase(frame_id);
}


void LRUReplacer::Unpin(frame_id_t frame_id) {
    if (lru_set.find(frame_id)!=lru_set.end()){
        return;
    }
    lru_size++;
    lru_set.insert(frame_id);
    lru_queue.push_front(frame_id);
}


size_t LRUReplacer::Size() {
  return lru_size;
}