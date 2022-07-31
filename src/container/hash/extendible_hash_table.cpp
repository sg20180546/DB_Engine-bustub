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
  for (int i = 1; i < DIRECTORY_ARRAY_SIZE; i++) {
    htdp->SetBucketPageId(i, -1);
  }
  //  std::cout<<"Bucekt earr size"<<BUCKET_ARRAY_SIZE<<"\n";
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
  uint32_t ret = static_cast<uint32_t>(hash_fn_.GetHash(key));
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t bucket_idx = GetBucketIdxByKey(dir_page, key);
  return dir_page->GetBucketPageId(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  // return const_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  // return const_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
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
  // std::cout<<"find key "<<key<<" page_id "<<page_id<<"\n\n";
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
  // std::cout<<"inserting "<<key;
  bool ret;
  uint32_t bucket_idx;
  page_id_t page_id;
  HashTableDirectoryPage *htdp = FetchDirectoryPage();

  table_latch_.RLock();
  bucket_idx = GetBucketIdxByKey(htdp, key);
  page_id = htdp->GetBucketPageId(bucket_idx);
  // printf("  bucket idx %u page_id %u\n",bucket_idx,page_id);
  table_latch_.RUnlock();

  Page *page = buffer_pool_manager_->FetchPage(page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *htb = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  // std::cout<<"key "<<key<<"and value "<<value<<" is insert at page id "<<page_id<<"\n";
  if (htb->IsFull()) {
    // printf("is full\n\n\n");
    table_latch_.WLock();
    uint32_t gd;

    if (htdp->GetLocalDepth(bucket_idx) == (gd = htdp->GetGlobalDepth())) {
      // printf("increase global depth : %u++\n",gd);
      for (int i = 0; i < 512; i += 1UL << (9 - gd)) {
        htdp->SetLocalDepth(i + (1UL << (9 - gd - 1)), htdp->GetLocalDepth(i));
      }
      htdp->IncrGlobalDepth();
      gd = htdp->GetGlobalDepth();
      // error point
    }
    htdp->IncrLocalDepth(bucket_idx);
    // printf("insert lock point here 2\n");
    page_id_t new_page_id;
    Page *new_page;
    uint32_t new_bucket_idx = bucket_idx + (1 << (9 - gd));
    // printf("new bucket idx : %u\n\n\n",new_bucket_idx);
    uint32_t ld = htdp->GetLocalDepth(bucket_idx);
    assert(ld != 0);
    htdp->SetLocalDepth(new_bucket_idx, ld);

    // printf("cur bucket idx : %u new bucket idx :%u  local dpeth %u\n",bucket_idx,new_bucket_idx,ld);
    // printf("new local depth %u global dpeth %u\n",htdp->GetLocalDepth(new_bucket_idx),gd);
    if ((new_page_id = htdp->GetBucketPageId(new_bucket_idx)) == -1) {
      new_page = buffer_pool_manager_->NewPage(&new_page_id);
      htdp->SetBucketPageId(new_bucket_idx, new_page_id);
      printf("there is no cacheed page cur page id : %u new page id : %u",page_id,new_page_id);
      std::cout<<"key : "<<key<<"\n";
    } else {
      printf("there is already cached page : %u",new_page_id);
      std::cout<<"key : "<<key<<"\n";
      new_page = buffer_pool_manager_->FetchPage(new_page_id);
    }

    HASH_TABLE_BUCKET_TYPE *htb = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
    HASH_TABLE_BUCKET_TYPE *new_htb = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_page->GetData());
    // printf("insert lock point here 3\n");
    // // copy correct thing to new bucket
    uint64_t i = 0;
    // uint32_t gm= htdp->GetGlobalDepthMask();
    // printf("copying %ld , mask : %u\n",BUCKET_ARRAY_SIZE,gm);
    int moved = 0;

    while (i < BUCKET_ARRAY_SIZE && htb->IsOccupied(i)) {
      if (htb->IsReadable(i)) {
        // printf("%4lu",i);
        KeyType key = htb->KeyAt(i);
        uint32_t tmp = GetBucketIdxByKey(htdp, key);
        // printf("new table %4u ",tmp);
        if (tmp == new_bucket_idx) {
          //  printf("%4lu",i);
          // std::c
          // std::cout<<tmp<<" gd "<<gd<<"\n";
          moved++;
          // std::cout<<key<<"("<<htb->ValueAt(i)<<") ";
          new_htb->Insert(key, htb->ValueAt(i), comparator_);
          htb->RemoveAt(i);
        } else {
          // std::cout<<tmp<<" gd "<<gd<<"\n";
        }
      }
      i++;
    }
    // printf("\nmoved : %d ,new bucket idx %u new page id %u\n",moved,new_bucket_idx,new_page_id);
    // printf("insert lock point here 4\n");
    page->WUnlatch();
    table_latch_.WUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
    buffer_pool_manager_->UnpinPage(page_id, false, nullptr);
    buffer_pool_manager_->UnpinPage(new_page_id, true, nullptr);
    ret = this->Insert(transaction, key, value);

  } else {
    ret = htb->Insert(key, value, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, false, nullptr);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
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
  // uint32_t global_depth;
  uint32_t local_depth;
  page_id_t page_id;
  Page *page;
  table_latch_.RLock();
  bucket_idx = GetBucketIdxByKey(htdp, key);
  page_id = htdp->GetBucketPageId(bucket_idx);
  table_latch_.RUnlock();

  page = buffer_pool_manager_->FetchPage(page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *htb = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  // std::cout<<"remove at "<<key<<" "<<value<<" page_id "<<page_id<<"\n";
  ret = htb->Remove(key, value, comparator_);
  // if(comparator_(key,0)) assert(ret);
  // global_depth = GetGlobalDepth();
  local_depth = htdp->GetLocalDepth(bucket_idx);
  if (local_depth != 0 && htb->IsEmpty()) {
    // printf("is empty at page id :%u , local depth %u, global depth %u\n",page_id,local_depth, global_depth);
    table_latch_.WLock();
    uint32_t buddy_idx;
    uint32_t interval = 10 - local_depth;
   

    // 1 : 0 -> 0 , 256 -> 0
    // 2 : 0 -> 0, 128 -> 0, 256->256 , 384->256
    // printf("bucket idx %u interval : %u\n",bucket_idx,interval);
    if (bucket_idx != (bucket_idx >> interval) << interval) {
      //( buddy idx ||interval|| bucket idx-empty )
      
      buddy_idx = bucket_idx - (1UL << (interval - 1));
      // printf("bucket idx %u buddy idx : %u\n",bucket_idx,buddy_idx);
      if (htdp->GetLocalDepth(buddy_idx) == local_depth) {

        htdp->DecrLocalDepth(buddy_idx);
        htdp->DecrLocalDepth(bucket_idx);
      }
    } else {
      // ( bucket idx-empty ||interval|| buddy idx)
      buddy_idx = bucket_idx + (1UL << (interval - 1));
      if (htdp->GetLocalDepth(buddy_idx) == local_depth) {
        // copy buddy content to bucket content;
        page_id_t buddy_page_id = htdp->GetBucketPageId(buddy_idx);
        Page *buddy_page = buffer_pool_manager_->FetchPage(buddy_page_id);
        buddy_page->RLatch();
        memmove(page->GetData(), buddy_page->GetData(), PAGE_SIZE);
        memset(buddy_page->GetData(), 0, PAGE_SIZE);
        buddy_page->RUnlatch();

        htdp->DecrLocalDepth(bucket_idx);
        htdp->DecrLocalDepth(buddy_idx);
        buffer_pool_manager_->UnpinPage(buddy_page_id, true);
      }
    }

    printf(" it is empty at %u , buddy : %u\n",bucket_idx,buddy_idx);
    htdp->CanShrink();
    // if (htdp->CanShrink()) {
    //   // htdp->DecrGlobalDepth();
    //   // printf("we can shrink!\n");
    // } else{

    // }
    table_latch_.WUnlock();
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
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
  // 00
  uint32_t global_depth = htdp->GetGlobalDepth();  // 2
  uint32_t mask = htdp->GetGlobalDepthMask();      // 110000000000

  uint32_t bucket_frame = Hash(key) & mask;  // 1000000000 or 0000000000000000

  bucket_frame = bucket_frame >> (32 - global_depth);

  uint32_t bucket_idx = bucket_frame * (1UL << (9 - global_depth));  // 1* 1UL<<7 == 1* 128

  uint32_t local_depth = htdp->GetLocalDepth(bucket_idx);
  uint32_t interval = 9 - local_depth;
  uint32_t ret = ((bucket_idx >> interval) << interval);
  // printf("bucket frame : %u local depth %u ret %u ",bucket_frame,local_depth,ret);

  return ret;  // 1000 0000>>7<<7=
  // 1 0000 0000 >>
}
template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::ReverseBit(uint32_t x) -> uint32_t {
  x = ((x >> 1) & 0x55555555u) | ((x & 0x55555555u) << 1);
  x = ((x >> 2) & 0x33333333u) | ((x & 0x33333333u) << 2);
  x = ((x >> 4) & 0x0f0f0f0fu) | ((x & 0x0f0f0f0fu) << 4);
  x = ((x >> 8) & 0x00ff00ffu) | ((x & 0x00ff00ffu) << 8);
  x = ((x >> 16) & 0xffffu) | ((x & 0xffffu) << 16);
  return x;
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
