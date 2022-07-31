//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(0) {
  // init head_
  head_.next_ = &head_;
  head_.prev_ = &head_;
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  struct LinkedList *node = head_.prev_;
  if (node == &head_) {
    return false;
  }
  (*frame_id) = node->frame_id_;
  DeleteNode(node);
  num_pages_--;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  struct LinkedList *node = FindNode(frame_id);
  if (node == nullptr) {
    return;
  }
  LRUReplacer::DeleteNode(node);
  num_pages_--;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  struct LinkedList *node = FindNode(frame_id);
  if (node == nullptr) {
    AddNode(frame_id);
    num_pages_++;
  }
}

auto LRUReplacer::Size() -> size_t { return num_pages_; }

struct LRUReplacer::LinkedList *LRUReplacer::FindNode(frame_id_t frame_id) {
  struct LinkedList *ret = nullptr;
  struct LinkedList *node = head_.next_;
  while (node->frame_id_ != -1) {
    if (node->frame_id_ == frame_id) {
      ret = node;
      break;
    }
    node = node->next_;
  }
  return ret;
}

void LRUReplacer::DeleteNode(struct LRUReplacer::LinkedList *node) {
  node->prev_->next_ = node->next_;
  node->next_->prev_ = node->prev_;
  delete node;
}

void LRUReplacer::AddNode(frame_id_t frame_id) {
  struct LinkedList *node = new LRUReplacer::LinkedList;
  node->frame_id_ = frame_id;
  node->next_ = head_.next_;
  node->prev_ = &head_;
  head_.next_ = node;
  node->next_->prev_ = node;
}

}  // namespace bustub
