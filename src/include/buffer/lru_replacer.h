//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;  // frame_id_t is of type int32_t as defined in common/config.h

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me!

  class FrameListNode {
    friend LRUReplacer;

   private:
    FrameListNode *next_;
    FrameListNode *prev_;
    frame_id_t l_data_;

   public:
    explicit FrameListNode(frame_id_t data) {
      next_ = nullptr;
      prev_ = nullptr;
      l_data_ = data;
    }
    // what kind of operations do we need?
    frame_id_t &GetData() { return l_data_; }
    ~FrameListNode() = default;
  };

  // pin operation kicks related listnode out of the list
  // unpin operation push a listnode into the list
  // let's define that the node before the tailnode is the LRU node
  // each new node would be the next node of the head node;

  std::unordered_map<frame_id_t, FrameListNode *> lru_map_;  // is the declaration correct??
  FrameListNode *head_;
  FrameListNode *tail_;
  // std::vector<FrameListNode<frame_id_t>> frame_vector;
  size_t capacity_;
  size_t size_;
  std::mutex frame_list_mutex_;

  void Remove(FrameListNode *Node2Move) {
    Node2Move->prev_->next_ = Node2Move->next_;
    Node2Move->next_->prev_ = Node2Move->prev_;
  }

  void Insert(FrameListNode *Node2Insert) {
    Node2Insert->prev_ = head_;
    Node2Insert->next_ = head_->next_;
    head_->next_->prev_ = Node2Insert;
    head_->next_ = Node2Insert;
  }
};

}  // namespace bustub
