//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.

  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  const std::lock_guard<std::mutex> lock(latch_);
  // Make sure you call DiskManager::WritePage!
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  if (page_table_.count(page_id) == 0) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];  // find the corresponding frame id,

  // write the page using the diskmanager.
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;// having been written to disk, consistent with data on disk now.

// no need to push into freelist, since there may be several threads pinning the current frame.
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  const std::lock_guard<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    FlushPgImp(pages_[i].page_id_);
  }
}

/**
 * I've not catch the point why this function is useful?? why do we need new PAGE??? NEXT PAGE???
 * A: when hash table is initiated, the directory and bucket page are all buckets, and are both
 * created through the NewPgImp() below. 
 */
Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  const std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_avail = 0;
  page_id_t next_page;

  if (free_list_.empty() && (!replacer_->Victim(&frame_avail))) { 
    // 1. nothing in freelist, nothing available to evict in the buffer bool.
    return nullptr;
  }

  //2. freelis is not empty
  if (!free_list_.empty()) {
    frame_avail = free_list_.back();
    free_list_.pop_back();        // get the frame_id;
  } else {                        // 3.no frames in the freelist, find one with the help of replacer.
                                  // should reuse the code here, but no need for a helper function,
    replacer_->Pin(frame_avail);  // found one from replacer, pin it, since it would later be used.
    // note that frame_avail is got from the first function.
    if (pages_[frame_avail].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_avail].page_id_, pages_[frame_avail].GetData());
    }
    page_table_.erase(pages_[frame_avail].page_id_);
  }

// the data structure we have: pages_, page_table_, free_list, replacer,diskmanager(no need to maintain)
  next_page = AllocatePage();  // get the page id;
  *page_id = next_page;

  pages_[frame_avail].pin_count_ += 1;  // update P's meta data;
  pages_[frame_avail].page_id_ = next_page;
  pages_[frame_avail].is_dirty_ = false;
  pages_[frame_avail].ResetMemory();

  // std::cout << "pin_count of a new page shoudl be 1, actually it is"<<
  //          pages_[frame_avail].pin_count_<< std::endl;

  page_table_.insert({next_page, frame_avail});  // update page_table;

  return &pages_[frame_avail];
  // why allocate page() is called should be further clarified, useful in the case of parallel BPI, I suppose.

}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  const std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id = 0;

  if (page_table_.count(page_id) != 0) {  // it exists in the pagetable
    frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_ += 1;
    replacer_->Pin(frame_id);

    return &pages_[frame_id];
  }   // not in the pagetable, find it from the freelist, then try to victim a frame from replacer.
    
  if (free_list_.empty() ) {
    if (replacer_->Size() == 0) {
      return nullptr;
    } 
              // evict a frame, we have to flush it back to disk before returning the pointer to frame.
    replacer_->Victim(&frame_id);
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData()); 
    }
        // update pagetable;
    page_table_.erase(pages_[frame_id].page_id_);
    replacer_->Pin(frame_id);
             // page found in the replacer.
  } else {  // found in the freelist
    frame_id = free_list_.back();
      free_list_.pop_back();
  }
    // found the frame_id;
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].ResetMemory();

  disk_manager_->ReadPage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
    // update for replacer done solely for cases where the page is found in the replacer.
  page_table_.insert({page_id, frame_id});
  return &pages_[frame_id];
  
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  
  const std::lock_guard<std::mutex> lock(latch_);
  DeallocatePage(page_id);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t frame_id;
  frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }
  if(pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
  }
  page_table_.erase(page_id);

  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].ResetMemory();

  free_list_.push_back(frame_id);
  

  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  const std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];

  if (page->pin_count_ == 0) {
    return false;
  }
  page->pin_count_--;

  if (is_dirty) {
    page->is_dirty_ = true;
  }

  // replacer?
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
