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
  head_ = new FrameListNode(-1);
  tail_ = new FrameListNode(-1);
  head_->next_ = tail_;
  tail_->prev_ = head_;
  capacity_ = num_pages;
  size_ = 0;
  lru_map_.reserve(num_pages);
}

// initialization. head and tail are consecutive,

LRUReplacer::~LRUReplacer() {
  for (size_t i = 0; i < size_ + 2; ++i) {
    tail_ = head_->next_;
    delete head_;
    head_ = tail_;
  }
}

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
  
  *frame_id = tail_->prev_->l_data_;
  lru_map_.erase(*frame_id);
  size_ --;
  Remove(tail_->prev_);
  return true;
  // shall we remove ?? who has the priviledge to change data member of LRU?? remove, insert? and what/??
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // kick it out of the list
  // 1. find the ListNode*
  // remove(ListNode*) from the list;
  // remove the listnode from the map;
  std::lock_guard<std::mutex> guard(frame_list_mutex_);
  if (lru_map_.count(frame_id) == 0) {
    return;
  }
  auto node2remove = lru_map_[frame_id];

  Remove(node2remove);
  lru_map_.erase(frame_id);
  size_ --;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  // find the related initiate a new listnode for it
  // put it into map;
  //
  // insert into map, list, ?? how to solve it??
  if (lru_map_.count(frame_id) != 0) {
    return;
  }
  auto node2insert = new FrameListNode(frame_id);
  std::lock_guard<std::mutex> guard(frame_list_mutex_);
  lru_map_.insert({frame_id, node2insert});
  size_ ++;
  Insert(node2insert);
  //    delete node2insert;
}

size_t LRUReplacer::Size() { return size_; }

}  // namespace bustub
