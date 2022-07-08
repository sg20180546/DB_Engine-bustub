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
  HashTableDirectoryPage *htdp =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager->NewPage(&directory_page_id_));
  htdp->SetPageId(directory_page_id_);
  page_id_t page_id;
  buffer_pool_manager->NewPage(&page_id);
  htdp->SetBucketPageId(0, page_id);
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
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  // return const_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
  return (HashTableDirectoryPage *)(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  // return const_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
  return (HASH_TABLE_BUCKET_TYPE *)(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  // hasing by key , and mask
  bool ret;

  HashTableDirectoryPage *htdp = FetchDirectoryPage();
  uint32_t bucket_idx = GetBucketIdxByKey(htdp, key);

  table_latch_.RLock();
  page_id_t page_id = htdp->GetBucketPageId(bucket_idx);

  Page *page = buffer_pool_manager_->FetchPage(page_id);
  page->RLatch();
  auto htb = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  ret = htb->GetValue(key, comparator_, result);

  page->RUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(page_id, false, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  return ret;
  // return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  bool ret;
  uint32_t bucket_idx;
  page_id_t page_id;
  HashTableDirectoryPage *htdp = FetchDirectoryPage();
  {
    table_latch_.RLock();
    bucket_idx = GetBucketIdxByKey(htdp, key);
    page_id = htdp->GetBucketPageId(bucket_idx);
    table_latch_.RUnlock();
  }

  Page *page = buffer_pool_manager_->FetchPage(page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *htb = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  if (htb->IsFull()) {
    table_latch_.WLock();
    uint32_t gd;
    if (htdp->GetLocalDepth(bucket_idx) == (gd = htdp->GetGlobalDepth())) {
      htdp->IncrGlobalDepth();
      htdp->IncrLocalDepth(bucket_idx);
      gd = GetGlobalDepth();
      for (int i = 1 << (9 - gd); i < 512; i += 1 << (9 - gd + 1)) {
        htdp->SetBucketPageId(i, htdp->GetBucketPageId(i - (1 << (9 - gd))));
      }
    }
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    HASH_TABLE_BUCKET_TYPE *new_htb = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_page->GetData());
    uint32_t new_bucket_idx = bucket_idx + (1 << (9 - gd + 1));

    htdp->SetBucketPageId(new_bucket_idx, new_page_id);
    htdp->SetLocalDepth(new_bucket_idx, (uint8_t)htdp->GetLocalDepth(bucket_idx));
    ret = new_htb->Insert(key, value, comparator_);

    table_latch_.WUnlock();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
    buffer_pool_manager_->UnpinPage(page_id, false, nullptr);
    buffer_pool_manager_->UnpinPage(new_page_id, true, nullptr);
  } else {
    ret = htb->Insert(key, value, comparator_);

    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    page->WUnlatch();
  }

  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // Page* page=buffer_pool_manager_->FetchPage();
  // // for(int i=0;i<)

  // bool ret;
  // page_id_t page_id;
  // Page* page = buffer_pool_manager_->NewPage(&page_id);

  // buffer_pool_manager_->UnpinPage(page_id,true);

  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  bool ret;
  HashTableDirectoryPage *htdp = FetchDirectoryPage();
  uint32_t bucket_idx;
  page_id_t page_id;
  Page *page;
  {
    table_latch_.RLock();
    bucket_idx = GetBucketIdxByKey(htdp, key);
    page_id = htdp->GetBucketPageId(bucket_idx);
    table_latch_.RUnlock();
  }
  page = buffer_pool_manager_->FetchPage(page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *htb = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  ret = htb->Remove(key, value, comparator_);

  if (htb->IsEmpty()) {
    // uint32_t gd=;
    // if(bucket_idx)
    Merge(transaction, key, value);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  }

  buffer_pool_manager_->UnpinPage(page_id, true, nullptr);
  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
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

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::GetBucketIdxByKey(HashTableDirectoryPage *htdp, KeyType key) -> uint32_t {
  uint32_t global_depth = htdp->GetGlobalDepth();
  uint64_t bucket_frame = hash_fn_.GetHash(key) & (htdp->GetGlobalDepthMask());
  return bucket_frame * (1UL << (9 - global_depth));
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
