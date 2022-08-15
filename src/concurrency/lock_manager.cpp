//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>
// #include <mutex>
namespace bustub {

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  std::lock_guard<std::mutex> lock_(latch_);
  if(ValidateTxnBeforeLock(txn)==false){
    return false;
  }
  txn_id_t tid=txn->GetTransactionId();
  auto lock_table_elem=lock_table_.find(rid);
  LockRequestQueue* lock_request_queue;

  if(lock_table_elem!=lock_table_.end()){
    lock_request_queue=&lock_table_elem->second;
    auto reqeust_queue=lock_request_queue->request_queue_;
    reqeust_queue.emplace_back(tid,LockMode::SHARED);
    std::unique_lock<std::mutex> lock_(lock_request_queue->m_);
    // std::shared_lock
    for(auto lock_request: reqeust_queue) {
      if(lock_request.txn_id_==tid){
        lock_request.granted_=true;
        break;
      }
      if(lock_request.lock_mode_==LockMode::EXCLUSIVE){
        if(lock_request.txn_id_>tid) {
          // abort left
          lock_request.granted_=false;
          Transaction* invalid_txn=txn_table_.find(lock_request.txn_id_)->second;
          invalid_txn->SetState(TransactionState::ABORTED);
        }else{
          // ?
          lock_request_queue->cv_.wait(lock_,[&] {
                                    return lock_request.granted_==false;});
        
        }
      }
    }
  }else{
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
    lock_table_elem=lock_table_.find(rid);
    lock_request_queue=&lock_table_elem->second;
    LockRequest lock_request(tid,LockMode::SHARED);
    lock_request.granted_=true;
    lock_request_queue->request_queue_.push_back(lock_request); //error point
  }
  txn_table_.insert({tid,txn});
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  std::lock_guard<std::mutex> lock_(latch_);
  if(ValidateTxnBeforeLock(txn)==false){
    return false;
  }

  txn_id_t tid=txn->GetTransactionId();
  auto lock_table_elem=lock_table_.find(rid);
  LockRequestQueue* lock_request_queue;
  if(lock_table_elem!=lock_table_.end()){
    lock_request_queue=&lock_table_elem->second;
    auto reqeust_queue=lock_request_queue->request_queue_;
    reqeust_queue.push_back(LockRequest(tid,LockMode::EXCLUSIVE));
    std::unique_lock<std::mutex> lock_(lock_request_queue->m_);
    for(auto lock_request:reqeust_queue){
      if(lock_request.txn_id_==tid){
        lock_request.granted_=true;
        break;
      }
      if(lock_request.txn_id_>tid&&lock_request.granted_==true) {
        lock_request.granted_=false;
        Transaction* invalid_txn=txn_table_.find(lock_request.txn_id_)->second;
        invalid_txn->SetState(TransactionState::ABORTED);
      }else{
         lock_request_queue->cv_.wait(lock_,[&] {
                                    return lock_request.granted_==false;});
      }
    }
  }else{
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
    lock_table_elem=lock_table_.find(rid);
    lock_request_queue=&lock_table_elem->second;
    LockRequest lock_request(tid,LockMode::EXCLUSIVE);
    lock_request.granted_=true;
    lock_request_queue->request_queue_.push_back(lock_request);
  }
  txn_table_.insert({tid,txn});
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  if(ValidateTxnBeforeLock(txn)==false){
    return false;
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  if(txn->GetState()==TransactionState::GROWING){
    txn->SetState(TransactionState::SHRINKING);
  }
  
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
