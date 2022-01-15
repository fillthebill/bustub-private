//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {
template <typename KeyType, typename ValueType, typename KeyComparator>
MappingType* HASH_TABLE_BUCKET_TYPE::GetData() {
  return array_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool res = false;
  
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (cmp(key, array_[i].first)==0 && IsReadable(i)) {
      res = true;
      result->push_back(array_[i].second);
    }
  }

  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {

  //  examine duplication
  //  isreadable means that it's valid, incoming pair could be inserted the first position where(!isreadable())
  //  invariance: a valid pair is always readable, an invalid pair is not.
  //  among the invalid ones, they could be occupied by removed elements
  //  if not occupied, that place has not been inserted any element from the beginning.

  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (cmp(array_[i].first, key)==0 && array_[i].second == value && IsReadable(i)) {
      return false;
    }
  }
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsReadable(i)) {
      array_[i].first = key;
      array_[i].second = value;
      SetReadable(i);
      SetOccupied(i);
      return true;
    } 
  }

  return false;

//  the codes below is problematic, duplication does not mean two same kv pair in the bucket.
//  whether a duplicate kv pair exist  
  
//  what a lesson

//  whether the array is full.
//  for(int i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
//  which bit would i looking up??   position in array of chars: (i-1)/8 + 1; which bit?? i-(position-1 <<3), 
//  mask should be(for 8 bits) 1<<(8-(....))
//    int char_rank = (i-1)>>3 + 1;
//   int bit_rank = i-((char_rank-1)<<3);// from 1 to 8, not 0 to 7, mask would be 1<<(8-bit_rank)
//  if(occupied_[char_rank] & (1<<(8-bit_rank)) == 0 ) {
//  array_[i].first = key;
//  array_[i].second = value;
//  occupied_[char_rank] |= (1<<(8-bit_rank));
//  readable_[char_rank] |= (1<<(8-bit_rank));
//  return true; 
// not full, found a non-occupied place, insert successfully.
//  } 
//  }
// return false;full

}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  
  // if kv matching, decide whether isreadeable(),which means that it's a valid pair to be removed
  // then change readable to 0, using setreadable.

  // assume that no valid duplication in the table, which is presumably garanteened by insert;

  // look up the kv pair.
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (cmp(array_[i].first, key) == 0 && array_[i].second == value) {
      // int char_rank = (i-1)>>3 + 1;
      // int bit_rank = i-((char_rank-1)<<3);
      // occupied_[char_rank] &= (~(1<<(8-bit_rank)));
      // readable_[char_rank] &= (~(1<<(8-bit_rank)));

      if (IsReadable(i)) {
        // set not readable 
        uint8_t mask = CalculateMask(i);
        uint32_t char_rank = (i >> 3);
        readable_[char_rank] &= (~mask);
        return true;
      }
    }
  }

  // no valid, matching kv pair found.
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  uint32_t char_rank = (bucket_idx>>3);
  uint8_t mask = CalculateMask(bucket_idx);
  if (mask & occupied_[char_rank]) {
    // not zero after mask, the corresponding bit is one,
    return true;
  }

  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint8_t HASH_TABLE_BUCKET_TYPE::CalculateMask(uint32_t bucket_idx) const {
  uint32_t char_rank = (bucket_idx >> 3);
  uint32_t bit_rank = bucket_idx-(char_rank << 3);  // from 0 to 7;
  uint8_t mask = 1 << (7-bit_rank);
  return mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t char_rank = (bucket_idx >> 3);
  uint8_t mask = CalculateMask(bucket_idx);
  occupied_[char_rank] |= mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  uint32_t char_rank = ((bucket_idx) >> 3);  //  bucket number start from 0, # of chars,
  uint32_t bit_rank = bucket_idx-(char_rank << 3); //  from 0 to 7;
  uint8_t mask = 1 << (7-bit_rank);  //  0 to 7 mapped to 7 to 0;
  if (mask & readable_[char_rank]) {
    //  not zero after mask, the corresponding bit is one, 
    return true;
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t char_rank = (bucket_idx>>3);
  uint8_t mask = CalculateMask(bucket_idx);  // 1 on the bit corresponding the bucket_idx, all other bits are 0;
  readable_[char_rank] |= mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsReadable(i)) {
            return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (IsReadable(i)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
