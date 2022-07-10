//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), num_instances_(num_instances) {
  // Allocate and create individual BufferPoolManagerInstances
  // size_t pool_size_per_instance=pool_size/num_instances;
  bpmi_ = new BufferPoolManagerInstance *[num_instances];
  for (int i = 0; i < static_cast<int>(num_instances); i++) {
    BufferPoolManagerInstance *bpmi =
        new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
    bpmi_[i] = bpmi;
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return pool_size_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManagerInstance * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return bpmi_[page_id % num_instances_];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  Page *ret;
  BufferPoolManagerInstance *bpmi = GetBufferPoolManager(page_id);
  bpmi->Latch();
  ret = bpmi->FetchPage(page_id);
  bpmi->UnLatch();

  return ret;
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  // bool ret;
  BufferPoolManagerInstance *bpmi = GetBufferPoolManager(page_id);

  return bpmi->UnpinPage(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  bool ret;
  BufferPoolManagerInstance *bpmi = GetBufferPoolManager(page_id);

  bpmi->Latch();
  ret = bpmi->FlushPage(page_id);
  bpmi->UnLatch();

  return ret;
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  Page *ret = nullptr;

  for (size_t i = 0; i < num_instances_; i++) {
    BufferPoolManagerInstance *bpmi = GetBufferPoolManager(i);
    bpmi->Latch();
    ret = bpmi->NewPage(page_id);
    bpmi->UnLatch();
    if (ret != nullptr) {
      break;
    }
  }
  return ret;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  bool ret;
  BufferPoolManagerInstance *bpmi = GetBufferPoolManager(page_id);
  bpmi->Latch();
  ret = bpmi->DeletePage(page_id);
  bpmi->UnLatch();
  return ret;
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  for (size_t i = 0; i < num_instances_; i++) {
    BufferPoolManagerInstance *bpmi = GetBufferPoolManager(i);
    bpmi->Latch();
    bpmi->FlushAllPages();
    bpmi->UnLatch();
  }
  // flush all pages from all BufferPoolManagerInstances
}

}  // namespace bustub
