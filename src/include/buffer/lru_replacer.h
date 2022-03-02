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

  // pin operation kicks related listnode out of the list
  // unpin operation push a listnode into the list
  std::list<frame_id_t> free_list_;
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> lru_map_;  // is the declaration correct??
  size_t capacity_;
  size_t size_;
  std::mutex frame_list_mutex_;
};

}  // namespace bustub
