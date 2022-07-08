//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub {

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManagerInstance : public BufferPoolManager {
 public:
  /**
   * Creates a new BufferPoolManagerInstance.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param log_manager the log manager (for testing only: nullptr = disable logging)
   * instance_index_=0, num_instacnce=1
   */
  BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager = nullptr);
  /**
   * Creates a new BufferPoolManagerInstance.
   * @param pool_size the size of the buffer pool
   * @param num_instances total number of BPIs in parallel BPM
   * @param instance_index index of this BPI in the parallel BPM
   * @param disk_manager the disk manager
   * @param log_manager the log manager (for testing only: nullptr = disable logging)
   */
  BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                            DiskManager *disk_manager, LogManager *log_manager = nullptr);

  /**
   * Destroys an existing BufferPoolManagerInstance.
   */
  ~BufferPoolManagerInstance() override;

  /** @return size of the buffer pool */
  auto GetPoolSize() -> size_t override { return pool_size_; }

  /** @return pointer to all the pages in the buffer pool */
  auto GetPages() -> Page * { return pages_; }
  void Latch() { latch_.lock(); }
  void UnLatch() { latch_.unlock(); }

 protected:
  /**
   * Fetch the requested page from the buffer pool.
   * @param page_id id of page to be fetched
   * @return the requested page
   *
   */
  auto FetchPgImp(page_id_t page_id) -> Page * override;

  /**
   * Unpin the target page from the buffer pool.
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page pin count is <= 0 before this call, true otherwise
   *
   * if dirty
   *    page->dirty=true
   * if Page->pin_count==1
   *    Get frame_id by page_id
   *    Call LRUReplacer::Unpin
   */
  auto UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool override;

  /**
   * Flushes the target page to disk.
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   * if pool_size==cur_size
   *    call LRUReplacer::Victim
   *    find page_id in page_table by frame_id
   *    find page in pages_ by page_id
   *    Allocate Pages
   *    replace it, points to allocate pages
   * else if pool_size>cur_size
   *    Allocate Pages
   *    pages_(NULL) points to allocate Pages
   */
  auto FlushPgImp(page_id_t page_id) -> bool override;

  /**
   * Creates a new page in the buffer pool.
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   *
   */
  auto NewPgImp(page_id_t *page_id) -> Page * override;

  /**
   * Deletes a page from the buffer pool.
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  auto DeletePgImp(page_id_t page_id) -> bool override;

  /**
   * Flushes all the pages in the buffer pool to disk.
   */
  void FlushAllPgsImp() override;

  /**
   * Allocate a page on disk.∂
   * @return the id of the allocated page
   */
  auto AllocatePage() -> page_id_t;

  /**
   * Deallocate a page on disk.
   * @param page_id id of the page to deallocate
   */
  void DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    // This is a no-nop right now without a more complex data structure to track deallocated pages
  }

  /**
   * Validate that the page_id being used is accessible to this BPI. This can be used in all of the functions to
   * validate input data and ensure that a parallel BPM is routing requests to the correct BPI
   * @param page_id
   */
  void ValidatePageId(page_id_t page_id) const;

  auto GetFrameID() -> frame_id_t;

  /** Number of pages in the buffer pool. */
  const size_t pool_size_;
  /** How many instances are in the parallel BPM (if present, otherwise just 1 BPI) */
  const uint32_t num_instances_ = 1;
  /** Index of this BPI in the parallel BPM (if present, otherwise just 0) */
  const uint32_t instance_index_ = 0;
  /** Each BPI maintains its own counter for page_ids to hand out, must ensure they mod back to its instance_index_ */
  std::atomic<page_id_t> next_page_id_ = instance_index_;

  /** Array of buffer pool pages. */
  Page *pages_;
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));
  /** Pointer to the log manager. */
  LogManager *log_manager_ __attribute__((__unused__));
  /** Page table for keeping track of buffer pool pages. */
  std::unordered_map<page_id_t, frame_id_t> page_table_;
  /** Replacer to find unpinned pages for replacement. */
  Replacer *replacer_;
  /** List of free pages. */
  std::list<frame_id_t> free_list_;
  /** This latch protects shared data structures. We recommend updating this comment to describe what it protects. */
  std::mutex latch_;
  // size_t cur_pool_size_=0;
  // std::mutex replacer_latch_;
  // std::mutex free_list_latch_;
  // std::mutex page_table_latch_;
  // std::mutex* physical_frame_latch_;
  // std::mutex next_page_id_latch_;
  // std::list<frame_id_t>::iterator list_iter;
};
}  // namespace bustub
