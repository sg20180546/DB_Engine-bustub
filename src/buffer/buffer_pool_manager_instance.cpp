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

  // physical_frame_latch_=new std::mutex[pool_size];
  // thread t;
  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
  // delete physical_frame_latch_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  class Page *page;
  try {
    frame_id_t frame_id = page_table_.at(page_id);
    page = &pages_[frame_id];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page_id, page->GetData());
      // printf("flush page %d: %s\n\n",page_id,page->GetData());
    }
    return true;
  } catch (const std::out_of_range &e) {
    return false;
  }
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  class Page *page;
  frame_id_t frame_id;
  page_id_t page_id;
  for (auto &mapping : page_table_) {
    page_id = mapping.first;
    frame_id = mapping.second;
    page = &pages_[frame_id];
    disk_manager_->WritePage(page_id, page->GetData());
    page->WPinLatch();
    page->UnsetPageIsDirty();
    page->WUnPinLatch();
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  
  frame_id_t frame_id = GetFrameID();
  if (frame_id == -1) {
    return nullptr;
  }


  class Page *page = &pages_[frame_id];
  page_id_t page_id_tmp = page->GetPageId();
  // printf("newpgimp frame n : %d pid %d\n",frame_id,page_id_tmp);
  page_table_.erase(page_id_tmp);

  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  page->ResetMemory();

  page_id_tmp = AllocatePage();
  page_table_.insert({page_id_tmp, frame_id});
  // page->WPinLatch();
  page->SetPageId(page_id_tmp);
  page->ModifyPinCount(1);
  // page->WUnlatch();
  *page_id = page_id_tmp;
  free_list_.remove(frame_id);
  replacer_->Pin(frame_id);

  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t frame_id;
  class Page *page;
  try {
    frame_id = page_table_.at(page_id);
    page = &pages_[frame_id];
  } catch (const std::out_of_range &e) {
  
    frame_id = GetFrameID();
    if (frame_id == -1) {
      return nullptr;
    }

    page = &pages_[frame_id];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }

    page_table_.insert({page_id, frame_id});

    page->WLatch();
    page->ResetMemory();
    page->SetPageId(page_id);
    disk_manager_->ReadPage(page_id, page->GetData());
    page->ModifyPinCount(1);
    page->WUnlatch();
  }

  replacer_->Pin(frame_id);

  return page;
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  class Page *page;
  try {
    // page_table_latch_.lock();
    frame_id = page_table_.at(page_id);

    page = &pages_[frame_id];
    page->RPinLatch();
    int pin_count = page->GetPinCount();
    page->RUnPinLatch();
    if (pin_count != 0) {
      return false;
    }

    if (page->IsDirty()) {
      disk_manager_->WritePage(page_id, page->GetData());
    }
    page->ResetMemory();
    page_table_.erase(page_id);
    free_list_.push_front(frame_id);
    return true;
  } catch (const std::out_of_range &e) {
    // std::cerr << e.what() << '\n';
    return true;
  }
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id;
  class Page *page;
  int pin_count;
  try {
    frame_id = page_table_.at(page_id);
    page = &pages_[frame_id];
    page->WPinLatch();
    pin_count = page->ModifyPinCount(-1);
    if (is_dirty) {
      page->SetPageIsDirty();
    }
    page->WUnPinLatch();
    if (pin_count == 0) {
      replacer_->Unpin(frame_id);
    }
    // if(is_dirty) pages_->is_dirty_
    // pages_[frame_id].pin_count_--;
    // pages_[frame_id].WUnlatch();
    return true;
  } catch (const std::out_of_range &e) {
    return false;
  }
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  // next_page_id_latch_.lock();
  const page_id_t next_page_id = next_page_id_.fetch_add(num_instances_,std::memory_order_relaxed);
  // next_page_id_ += num_instances_;
  // next_page_id_latch_.unlock();
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

auto BufferPoolManagerInstance::GetFrameID() -> frame_id_t {
  frame_id_t ret = -1;

  if (!free_list_.empty()) {
    ret = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&ret);
  } 
  return ret;
}
}  // namespace bustub
