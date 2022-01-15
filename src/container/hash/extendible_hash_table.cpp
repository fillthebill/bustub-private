//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <assert.h>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
      directory_page_id_ = INVALID_PAGE_ID;
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page ->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  // fetch a page using
  HashTableDirectoryPage* result;
  Page* dir_page;
  Page* new_buc_page;


  if (directory_page_id_ != INVALID_PAGE_ID) {
    dir_page = buffer_pool_manager_->FetchPage(directory_page_id_);
// dir_page may be nullptr;
// up to this point, page_id != invalid
    assert(dir_page != nullptr);
    
    result = reinterpret_cast<HashTableDirectoryPage*>(dir_page->GetData()); 
  // no need to unpin here! we want to use the page.
  // if invalid, why do we need to make a bucket page for it ?? really??
    return result;
  }else {
    // directory page not allocated. 
    // directory_page_id = page->getid;
    // 1. allocate a new page for direcotory page.
    // 2. init metadata: SetPageId,
    // 3. allocate the first bucket page for it
    // 4. init metadata: SetBucketPageId

    page_id_t new_dir_page_id;
    dir_page = buffer_pool_manager_->NewPage(&new_dir_page_id);
    if (dir_page == nullptr) return nullptr;
    
    page_id_t new_bucket_page_id;
    new_buc_page = buffer_pool_manager_->NewPage(&new_bucket_page_id);
    if (new_buc_page == nullptr) {
      buffer_pool_manager_->UnpinPage(new_dir_page_id, false);
      return nullptr;
    }
    //  note that a new page's memory has been set to zero.
    //  global depth is initiate to be zero, so we should add one bucketpage in it.
    directory_page_id_ = new_dir_page_id;
    result = reinterpret_cast<HashTableDirectoryPage*>(dir_page->GetData());
    result->SetPageId(directory_page_id_);
    result->SetBucketPageId(0, new_bucket_page_id);

//    buffer_pool_manager_->UnpinPage(new_dir_page_id,true);
    buffer_pool_manager_->UnpinPage(new_bucket_page_id, false);

//  assert(directory_page_id_ != INVALID_PAGE_ID);
//  dir_page = buffer_pool_manager_->FetchPage(directory_page_id_);

// dir_page may be nullptr;
// up to this point, page_id != invalid
//  assert(dir_page != nullptr);

//  result = reinterpret_cast<HashTableDirectoryPage*>(dir_page->GetData()); 
  //  no need to unpin here! we want to use the page.
  //  if invalid, why do we need to make a bucket page for it ?? really??
    return result;

  } 
}

//  what if  the bucket_page_id is illegal, 
//  this is wrong, we need to cast it to HASH_TABLE_BUCKET_TYPE, the return value is of type Page* now, 
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  HashTableDirectoryPage* dir_page = FetchDirectoryPage();
  for (uint32_t page_index = 0; page_index < (1<< dir_page->GetGlobalDepth()); page_index++) {
    if (dir_page->GetBucketPageId(page_index) == bucket_page_id) {
      return reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
    }
  }
  return nullptr;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  //  fetch directory page
  //  find bucket page id
  //  Getvalue from the bucket,
  //  remember to unpin(), since the bucket and directory page actually lies in the buffer pool. 
  table_latch_.RLock();
  HashTableDirectoryPage* dir_page = FetchDirectoryPage();
  page_id_t buc_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE* buc_page = FetchBucketPage(buc_page_id);

  Page* bucket_as_page = reinterpret_cast<Page*>(buc_page);
  bucket_as_page->RLatch();
  bool res = buc_page->GetValue(key,comparator_, result);
  //  buffer_pool_manager_->Unpin(directory_page_id_, false);
  bucket_as_page->RUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(buc_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);

  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  //if full, call split insert;
  
  table_latch_.WLock();// the function fetch directorypage would read the directory page.
  HashTableDirectoryPage* dir_page = FetchDirectoryPage();
  table_latch_.WUnlock();

  table_latch_.RLock();
  page_id_t buc_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE* buc_page = FetchBucketPage(buc_page_id);

  //  what verification check is needed here? since the bucketpage may be delete by someone, 
  //  after release the read lock on table. thus cannot be insert into.

  //  the validity of the pointer to bucket page is coupled with whether the directory page has been changed.
  //  if(buc_page == nullptr) {
   //   std::cout<< "buc_page error"<<"key is "<< key << "value is "<< value <<std::endl;
  //   return false;
  //  }
  bool result = false;
  Page* buc_as_page = reinterpret_cast<Page*>(buc_page);
    buc_as_page->WLatch();

  //  1. insert
  //  need to upgrade the table_latch to write latch.
  //  if simply release the readlock then acquire the write lock
  //  Other threads may change the dir_page and buc_page,
  //  which could make the buc_page we select to be wrong(e.g. after directory expansion, key is rehashed to another buck.).
  //  one way to bypass, is to decide again the bucket to insert and whether the bucket is full after acquisition of the write lock.
  //  if things changed, 
  
  //  of course, taking wlock() all the way through insert() would be correct, but notice that most insert() operations 
  //  won't encounter a full buckage, we need to use readlock in most insert operations to enhance parellelism.

  if (buc_page->IsFull()) {
//    buffer_pool_manager_->Unpin(buc_page);
    //  std::cout<< "before acquisition, warning! a full bucket to split"<<std::endl;
    table_latch_.RUnlock();
    buc_as_page->WUnlatch();
    //  std::cout<< "waiting for write lock"<<std::endl;
    table_latch_.WLock();
    buc_as_page->WLatch();
    //  deadlock here, the reason maybe that, another thread tries to delete, with the writelock, it would only release write lock 
    //  after the write lock on the bucket has been acquired.
    //  std::cout<< "after acquisition, warning! a full bucket to split"<<std::endl;

      if (buc_page_id == KeyToPageId(key, dir_page) && buc_page->IsFull()) {
        result = SplitInsert(buc_page_id, buc_page, key, value);
        //  splitinsert with table latch & buc_page on.
        //  these two locks are released after executing this fuction.
      }else if (buc_page_id != KeyToPageId(key, dir_page)){
        table_latch_.WUnlock();
        buc_as_page->WUnlatch();
      //      std::cout<< "try to insert again"<<std::endl;
        Insert(transaction, key, value);
      }else {
        //   still hash to the same bucket, but not full anymore.
        //  std::cout<< "hashed to the same bucket "<<std::endl;
        buc_page->Insert(key, value, comparator_);
        table_latch_.WUnlock();
        buc_as_page->WUnlatch();
      }

    //  if(result == false) {
          //  std::cout<< "full insertion failed "<<"key is "<< key << "value is "<< value <<std::endl;
    //   }
    //  if(result) {
    //   std::cout<< "split insert success. "<<"key is "<< key << "value is "<< value <<std::endl;
    //  }

  }else {
    
    result = buc_page->Insert(key, value, comparator_);
    buc_as_page->WUnlatch();
      
    if (result == false) {
          //  std::cout<< "non full insertion failed"<<"key is "<< key << "value is "<< value <<std::endl;
    }
    buffer_pool_manager_->UnpinPage(buc_page_id, true);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.RUnlock();

  }
  //  2. upin the bucket page.
  //  table_latch_.WUnlock();
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(page_id_t buc_page_id, HASH_TABLE_BUCKET_TYPE* split_page, const KeyType &key, const ValueType &value) {
  //  invariance: before calling SplitInsert(), write lock of table latch and the split page has been acquired.
  //  before return, these two locks have to be released.

  //  fetch bucket page only. 
  //  if there is room to insert the k-v pair, save a IO cycle without fetching dir_page.
  //  what if there is no room in the buffer pool?
  
  assert(split_page != nullptr);

  // SplitInsert only deals with the case where split_page is full  

  HashTableDirectoryPage* dir_page = FetchDirectoryPage();
  if (dir_page == nullptr) {
      //  std::cout<< "dir_page error"<<"key is "<< key << "value is "<< value <<std::endl;
    return false;
  }
//  when bucket is full, have to split the bucket page.
//  1. if localdepth == global depth, IncrGlobalDepth()
//  2. then, split the bucket;
//  2.1 save content in the bucket, create image bucket, reset content in the split bucket to zero.
//  2.2 re hash elements in the bucket
//  3. call splitinsert() recursively;
  bool SplitResult;
//  how to rehash, 
//  how to find directory index of the image bucket.

  uint32_t split_page_index = KeyToDirectoryIndex(key, dir_page);
  
  
//  array_ = buc_page->BucketCopy(); // return the kv pairs in it;
//  no need to save it temporarily.

  //  split and rehashing until there are free slots for insertion.
  //  directory expasion and local/global depth update are executed in this function.
  //  remember to unpin the image page inside the fuction split_rehash
  while (split_page->IsFull()) {

    if (!(SplitResult = Split_Rehash(dir_page, split_page, split_page_index)) ) 
      //  calling split_rehash with hashtable latch and page lock on.
      //  still acuiqre them after exectution. to be optimized. reconsider where to unpin and unlock.
      {     //  std::cout<< "split_rehash error"<<"key is "<< key << "value is "<< value <<std::endl;

        return false;
      }
     //  std::cout<< "rehash index "<< split_page_index << " id " << dir_page->GetBucketPageId(split_page_index) <<"key is "<< key << "value is "<< value <<std::endl;
  //  std::cout<<"after split ..." <<std::endl;
     //  dir_page->PrintDirectory();

  }
 //  std::cout<< " new index is " << KeyToDirectoryIndex(key, dir_page) <<std::endl;

  page_id_t new_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE* new_page = FetchBucketPage(new_page_id);
  Page* new_page_casted = reinterpret_cast<Page*>(new_page);
  if(new_page == nullptr) return false;
  
  if(new_page_id != buc_page_id) {
    new_page_casted->WLatch();
  }
  new_page->Insert(key, value, comparator_);

  //  if(!output) {
     //  std::cout<< "insert error after hash"<<"key is "<< key << "value is "<< value <<std::endl;
  //  }
  //  result = SplitInsert(transaction, key, value);
  //  in case of thrashing, the buffer pool is really crowded.
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(dir_page->GetBucketPageId(split_page_index), true);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  reinterpret_cast<Page*>(split_page)->WUnlatch();
  if (new_page_id != buc_page_id) {
    new_page_casted->WUnlatch();
  }
  table_latch_.WUnlock();
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Split_Rehash(HashTableDirectoryPage* dir_page,
HASH_TABLE_BUCKET_TYPE* split_page, uint32_t split_index) {
  // invariance: write lock on hashtable and the split page has been acquired before calling Split_rehash();
  // still on after this function. TO Be Optimized.

  // if fail to get image split page, return false;

  // 0. only need to unpin the image_page at the end. dir_page and split_page would be unpinned in caller functions.

  // 1. determine whether the directory page need expasion.
  // increment the local depth.

  uint32_t global_depth = dir_page->GetGlobalDepth();
  uint8_t local_depth = dir_page->GetLocalDepth(split_index);  // local_depth <= 9, 2^9 = 512,

  if (local_depth == global_depth) {
    dir_page->IncrGlobalDepth();
  }

  dir_page->IncrLocalDepth(split_index);

  // 2. get a new page for image_page
  page_id_t image_page_id = 0;

  HASH_TABLE_BUCKET_TYPE* image_page =
  reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>( (buffer_pool_manager_->NewPage(&image_page_id))->GetData());
  // memory reset to zero by BPM.
  Page* image_page_casted = reinterpret_cast<Page*>(image_page);
  image_page_casted->WLatch();

  if (image_page == nullptr) {
    return false;  // new page allocation failure, no need to unpin.
  }

  //  rehashing
  //  for all elements in the bucket,
  //  split_index ====   (in binary format) a,(localdepth -1 bits)
  //  image_index                          1-a,(localdepth -1 bits)

  //  3. rehash into the split page or the image page.

  //  remember to update the depth variable.

  local_depth = dir_page->GetLocalDepth(split_index);
  global_depth = dir_page->GetGlobalDepth();

  //  we've got the local_depth after split,
  //  indexes with a (l-1) bits at the end, would share the same page.
  //  note that, though there is a single image page used to store tuples
  //  indexes with (1-a) (l-1) bits would all point to the image page.
  //  Here, we could also see that before split, indexes pointing to the split and
  //  the image page all point to the split page.

  uint32_t local_mask = (1 << local_depth) -1;  // local_depth bits of 1;
  uint32_t split_start_index = split_index & local_mask;
  uint32_t image_start_index = (1 << (local_depth-1)) ^ split_start_index;

  MappingType* origin_kvpair = split_page->GetData();

  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE && split_page->IsReadable(i); ++i) {
    if ( (Hash(origin_kvpair[i].first) & local_mask) != split_start_index ) {
      image_page->Insert(origin_kvpair[i].first, origin_kvpair[i].second, comparator_);
      split_page->Remove(origin_kvpair[i].first, origin_kvpair[i].second, comparator_);
    }
  }

  //  4. update local depths of indexes previously pointing to the split page.
  //  indexs pointing to the same page have same local bits,
  //  after split, we have (n+1) local bits now, indexes which have the same
  //  lowest (n+1) bits should still point to the split image.
  //  note that the current index of split image may not be the index
  //  pointing to split image which has the smallest index number.
  //  we need to find the smallest index which point to split page.
  //  also need the smallest index which points to image page

  //   then increment by 1<<(n+1), would find the next split/image page.

  //  update local depths and page id which should point to image page now

  uint32_t max_index = (1 << global_depth);
  uint32_t step = (1 << local_depth);

  for (uint32_t k = split_start_index; k < max_index; k += step) {
    dir_page->SetLocalDepth(k, local_depth);
  }
  for (uint32_t m = image_start_index; m < max_index; m += step) {
    dir_page->SetLocalDepth(m, local_depth);
    dir_page->SetBucketPageId(m, image_page_id);
  }

  buffer_pool_manager_->UnpinPage(image_page_id, true);
  image_page_casted->WUnlatch();

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage* dir_page;
  if (directory_page_id_ == INVALID_PAGE_ID) {
    table_latch_.WLock();
    dir_page = FetchDirectoryPage();

    if (dir_page == nullptr) {
      buffer_pool_manager_->UnpinPage(directory_page_id_, false);
      table_latch_.WUnlock();
      return false;    // no room for creating/ saving dir_page.
    }

    table_latch_.WUnlock();
    table_latch_.RLock();
  } else {
    table_latch_.RLock();
    dir_page = FetchDirectoryPage();
    if (dir_page == nullptr) {
      buffer_pool_manager_->UnpinPage(directory_page_id_, false);
      table_latch_.RUnlock();
      return false;     // no room for creating/ saving dir_page.
    }
  }
  // Readlock on hashtable.
  bool merged = false;

  // for some time, one thing concerns me is whether the data member buc_page_ids array in hashtable
  // would change. Please notice that the read lock won't let anyone change data member of hashtable.
  uint32_t buc_index = KeyToDirectoryIndex(key, dir_page);
  page_id_t buc_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE* bucket_page =
  reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>((buffer_pool_manager_->FetchPage(buc_page_id) )->GetData());

  Page* bucket_as_page = reinterpret_cast<Page*>(bucket_page);
  if (bucket_page == nullptr) {
      buffer_pool_manager_->UnpinPage(directory_page_id_, merged);
      buffer_pool_manager_->UnpinPage(buc_page_id, false);
  }
  bucket_as_page->WLatch();

  bool result = bucket_page->Remove(key, value, comparator_);
  // bucket_as_page->WUnlatch();
  uint8_t local_depth = dir_page->GetLocalDepth(buc_index);

  // merge should be done when
// 1. image index and merge index point to different page.
// 2. image index < pow(2, global_index)
// we do not clean the pageid when shrinking directory. but the pageid would be re-allocated when expasion.
  // uint32_t global_depth = dir_page->GetGlobalDepth();
  if (local_depth != 0) {
    uint32_t image_index = buc_index ^ (1 << (local_depth-1));
    // page_id_t image_page_id = dir_page->GetBucketPageId(image_index);
    if (bucket_page->IsEmpty()) {  // here we are still holding the lock on the bucket and the directory.
// unpin inside the Merge function.
// challenge is that there maybe multiple indexes equal to the buc_page_id.
      table_latch_.RUnlock();
      table_latch_.WLock();
      if (bucket_page->IsEmpty()) {
// before merge, we have write lock on hashtable and bucket page.
        Merge(dir_page, buc_index, image_index);
        merged = true;
      }
    }
  }
  // find the split image index.
  // concern the case that another thread insert into bucket_page when merging is called,
  // write lock on bucket is released after merge.
  bucket_as_page->WUnlatch();
//  a  xxxxxx
// (1-a)xxxxxx
  if (merged) {
    while (dir_page->CanShrink()) {
    // each local depth is less than global depth.
      dir_page->DecrGlobalDepth();
    // dir_page->PrintDirectory();
    }
    table_latch_.WUnlock();
  } else {
    table_latch_.RUnlock();
    table_latch_.WLock();
    while (dir_page->CanShrink()) {
    // each local depth is less than global depth.
      dir_page->DecrGlobalDepth();
    // dir_page->PrintDirectory();
    }
    table_latch_.WUnlock();
  }

  if (!result) {
    buffer_pool_manager_->UnpinPage(buc_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, merged);
    return false;
  }

  buffer_pool_manager_->UnpinPage(buc_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, merged);

  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(HashTableDirectoryPage* dir_page, uint32_t merge_index, uint32_t image_index) {
// it's not possible to merge serveral times,
  // one remove resuls in only 1 empty page, which after merging, won't be empty anymore.

  // 1 update the indexes which points to buc_page_id to image_page_id;
  // 2 decrement the local depth of buc_page_id;
  // no need to fetch image page, so no unpin here.
  // shrink the directory page if possible.


//     (a)xxxx     merge_index
  // (1-a)xxxx     image_index

  // there could be multiple indexes which has (1-a)xxxx as its lowest l bits.
  uint32_t global_depth = dir_page->GetGlobalDepth();
  uint8_t local_depth = dir_page->GetLocalDepth(merge_index);
  uint32_t max_index = (1 << global_depth);
  uint32_t merge_start_index = merge_index & ((1 << local_depth)-1);
  uint32_t image_start_index = image_index & ((1 << local_depth)-1);
  page_id_t image_page_id = dir_page->GetBucketPageId(image_index);
  // the first index whose content is the page_id of the page to merge.
  uint32_t step = (1 << local_depth);

  if ( dir_page->GetLocalDepth(image_index) != local_depth || image_index >= (1 << global_depth) ) {
    return;
  }

  assert(dir_page->GetBucketPageId(merge_index) != dir_page->GetBucketPageId(image_index));

    for (uint32_t i = merge_start_index; i < max_index; i+=step) {
      dir_page->SetBucketPageId(i, image_page_id);
      dir_page->DecrLocalDepth(i);
    }

    for (uint32_t i = image_start_index; i < max_index; i+=step) {
      dir_page->DecrLocalDepth(i);
    }
    // dir_page->PrintDirectory();
  buffer_pool_manager_->DeletePage(dir_page->GetBucketPageId(merge_index));
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

