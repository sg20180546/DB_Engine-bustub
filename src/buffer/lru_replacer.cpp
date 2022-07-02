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

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(0), max_pages_(num_pages) {
    // init head_
    head_.next=&head_;
    head_.prev=&head_;
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {

    struct LRUReplacer::LinkedList* node=head_.prev;
    if(node==&head_) return false;
    (*frame_id)=node->frame_id_;
    DeleteNode(node);
    num_pages_--;
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    
    struct LRUReplacer::LinkedList* node=LRUReplacer::FindNode(frame_id);
    if(!node) return;
    LRUReplacer::DeleteNode(node);
    num_pages_--;
}


void LRUReplacer::Unpin(frame_id_t frame_id) {
    struct LRUReplacer::LinkedList* node=LRUReplacer::FindNode(frame_id);
    if(node) return;
    LRUReplacer::AddNode(frame_id);
    num_pages_++;
}

auto LRUReplacer::Size() -> size_t { return num_pages_; }

struct LRUReplacer::LinkedList* LRUReplacer::FindNode(frame_id_t frame_id)
{   
    struct LRUReplacer::LinkedList *ret=NULL,*node= head_.next;
    while(node->frame_id_!=-1)
    {
        if(node->frame_id_==frame_id)
        {
            ret=node;
            break;
        }
        node=node->next;
    }
    return ret;
}

void LRUReplacer::DeleteNode(struct LRUReplacer::LinkedList* node)
{
    node->prev->next=node->next;
    node->next->prev=node->prev;
    delete node;
}

void LRUReplacer::AddNode(frame_id_t frame_id)
{
    struct LRUReplacer::LinkedList* node= new LRUReplacer::LinkedList;
    node->frame_id_=frame_id;
    node->next=head_.next;
    node->prev=&head_;
    head_.next=node;
    node->next->prev=node;
}

}  // namespace bustub
