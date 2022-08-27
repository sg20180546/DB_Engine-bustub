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
  std::unique_lock<std::mutex> lock_(latch_);
  if(ValidateTxnBeforeLock(txn)==false){
    return false;
  }

  IsolationLevel policy=txn->GetIsolationLevel();
  if(policy==IsolationLevel::READ_UNCOMMITTED) {
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  txn_id_t tid=txn->GetTransactionId();
  auto lock_table_elem=lock_table_.find(rid);
  LockRequestQueue* lock_request_queue;

  if(lock_table_elem!=lock_table_.end()){
    lock_request_queue=&lock_table_elem->second;
    auto reqeust_queue=lock_request_queue->request_queue_;
    reqeust_queue.emplace_back(tid,LockMode::SHARED);

    size_t i;
    size_t cur_q_size=reqeust_queue.size();
    for(i=0;i<cur_q_size-1; i++) {
      LockRequest* lock_request=&reqeust_queue[i];
      // if(lock_request->txn_id_==tid){
      //   lock_request->granted_=true;
      //   break;
      // }
      if(lock_request->lock_mode_==LockMode::EXCLUSIVE){
        if(lock_request->txn_id_>tid) {
          lock_request->granted_=false;
          Transaction* invalid_txn=txn_table_.find(lock_request->txn_id_)->second;
          invalid_txn->SetState(TransactionState::ABORTED);
        }else{
          // ?
          lock_request_queue->cv_.wait(lock_,[&] {
                                    return lock_request->granted_==false;});
        
        }
      }
    }
    reqeust_queue[i].granted_=true;
  }else{
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
    lock_table_elem=lock_table_.find(rid);
    lock_request_queue=&lock_table_elem->second;
    // LockRequest lock_request(tid,LockMode::SHARED,true);
  
    // lock_request_queue->request_queue_.push_back(lock_request); //error point
    lock_request_queue->request_queue_.emplace_back(tid,LockMode::SHARED,true);
  }
  IsItAborted(txn,lock_request_queue);
  txn_table_.insert({tid,txn});
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock_(latch_);
  if(ValidateTxnBeforeLock(txn)==false){
    return false;
  }
  size_t i;
  txn_id_t tid=txn->GetTransactionId();
  auto lock_table_elem=lock_table_.find(rid);
  LockRequestQueue* lock_request_queue;
  if(lock_table_elem!=lock_table_.end()){
    lock_request_queue=&lock_table_elem->second;
    auto reqeust_queue=lock_request_queue->request_queue_;
    reqeust_queue.push_back(LockRequest(tid,LockMode::EXCLUSIVE));

    size_t cur_q_size=reqeust_queue.size();
    for(i=0;i<cur_q_size-1;i++){
      LockRequest* lock_request=&reqeust_queue[i];

      if(lock_request->txn_id_>tid&&lock_request->granted_==true) {
        lock_request->granted_=false;
        Transaction* invalid_txn=txn_table_.find(lock_request->txn_id_)->second;
        invalid_txn->SetState(TransactionState::ABORTED);
      }else{
         lock_request_queue->cv_.wait(lock_,[&] {
                                    return lock_request->granted_==false;});
      }
    }
    reqeust_queue[i].granted_=true;
  }else{
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
    lock_table_elem=lock_table_.find(rid);
    lock_request_queue=&lock_table_elem->second;
    // LockRequest lock_request(tid,LockMode::EXCLUSIVE,true);
    lock_request_queue->request_queue_.emplace_back(tid,LockMode::EXCLUSIVE,true);
  }
  IsItAborted(txn,lock_request_queue);
  txn_table_.insert({tid,txn});
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock_(latch_);
  if(ValidateTxnBeforeLock(txn)==false){
    return false;
  }
  txn_id_t tid=txn->GetTransactionId();
  auto lock_table_elem=lock_table_.find(rid);
  LockRequestQueue* lock_request_queue;
  LockRequest lock_request;
  if(lock_table_elem!=lock_table_.end()){
    lock_request_queue=&lock_table_elem->second;
    auto reqeust_queue=lock_request_queue->request_queue_;

    for(size_t i=0;i<reqeust_queue.size();i++) {
      LockRequest* lock_request=&reqeust_queue[i];
      if(lock_request->granted_==true){
        if(lock_request->txn_id_==tid) {
          if(lock_request->lock_mode_==LockMode::SHARED){
            IsItAborted(txn,lock_request_queue);
            lock_request->lock_mode_=LockMode::EXCLUSIVE;
            txn->GetSharedLockSet()->erase(rid);
            txn->GetExclusiveLockSet()->emplace(rid);
            return true;
          }else{
            throw TransactionAbortException(tid,AbortReason::UPGRADE_CONFLICT);
          }

        }else if(lock_request->txn_id_>tid){
          lock_request->granted_=false;
          Transaction* invalid_txn=txn_table_.find(lock_request->txn_id_)->second;
          invalid_txn->SetState(TransactionState::ABORTED);
        }else {
         lock_request_queue->cv_.wait(lock_,[&] {
                                    return lock_request->granted_==false;});
        }       
      }
    }
    
  }
  return false;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  TransactionState tstate=txn->GetState();
  
  if(tstate==TransactionState::GROWING){
    txn->SetState(TransactionState::SHRINKING);
  }

  txn_id_t tid=txn->GetTransactionId();
  LockRequestQueue* lock_request_queue;
  LockRequest lock_request;
  
  auto lock_table_elem=lock_table_.find(rid);
  if(lock_table_elem!=lock_table_.end()) {
    lock_request_queue=&lock_table_elem->second;
    auto reqeust_queue=lock_request_queue->request_queue_;
    for(size_t i=0;i<reqeust_queue.size();i++) {
      LockRequest* lock_request=&reqeust_queue[i];
      if(lock_request->txn_id_==tid&&lock_request->granted_==true) {
        lock_request->granted_=false;
        txn->GetSharedLockSet()->erase(rid);
        txn->GetExclusiveLockSet()->erase(rid);
        break;
      }
    }
  }
  // IsItAborted(txn,lock_request_queue);
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
