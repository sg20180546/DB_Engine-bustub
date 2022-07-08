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
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  struct LinkedList {
    /* data */
    struct LRUReplacer::LinkedList *next, *prev;
    frame_id_t frame_id_ = -1;
  };

  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  // get Last node in LinkedList, delete it
  auto Victim(frame_id_t *frame_id) -> bool override;

  // delete frame_id in LinkeList
  void Pin(frame_id_t frame_id) override;

  // add frame_id in LinkedList
  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  size_t num_pages_;
  size_t max_pages_;
  struct LRUReplacer::LinkedList *FindNode(frame_id_t frame_id);
  void DeleteNode(struct LRUReplacer::LinkedList *node);
  void AddNode(frame_id_t frame_id);
  struct LinkedList head_;

  // TODO(student): implement me!
  // data structure<frame_id> Last recently used first
  // -> double linked list?
  // last pointer should be need
};

}  // namespace bustub
