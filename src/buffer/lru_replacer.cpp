//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  free_list_ = {};
  lru_map_ = {};
  capacity_ = num_pages;
  size_ = 0;
  //  lru_map_.reserve(num_pages);
}

// initialization. head and tail are consecutive,

LRUReplacer::~LRUReplacer() = default;

// victim
// find the frame_id according to LRU algorithm
// kick the elment with such frame_id out of the LRU map, size --;
// remove the related MyListNode out of the List;
//

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(frame_list_mutex_);
  if (Size() == 0) {
    return false;
  }

  *frame_id = free_list_.back();
  lru_map_.erase(*frame_id);
  free_list_.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // kick it out of the list
  // 1. find the ListNode*
  // remove(ListNode*) from the list;
  // remove the listnode from the map;
  std::lock_guard<std::mutex> guard(frame_list_mutex_);
  if (lru_map_.find(frame_id) == lru_map_.end()) {
    return;
  }
  auto node2remove = lru_map_[frame_id];

  free_list_.erase(node2remove);
  lru_map_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  // find the related initiate a new listnode for it
  // put it into map;
  //
  // insert into map, list, ?? how to solve it??
  std::lock_guard<std::mutex> guard(frame_list_mutex_);
  if (lru_map_.find(frame_id) != lru_map_.end()) {
    return;
  }
  if (Size() == capacity_) {
    return;
  }
  free_list_.push_front(frame_id);
  lru_map_[frame_id] = free_list_.begin();
  //    delete node2insert;
}

size_t LRUReplacer::Size() { return free_list_.size(); }

}  // namespace bustub
