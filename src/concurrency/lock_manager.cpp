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

#include <utility>
#include <vector>
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"



namespace bustub {

void LockManager::removefromqueue(LockRequestQueue& lck_queue, Transaction* txn, LockMode l) {
  auto lck_queue_iter = lck_queue.request_queue_.begin();
  while (lck_queue_iter != lck_queue.request_queue_.end()) {
    if (lck_queue_iter->txn_id_ == txn->GetTransactionId() && lck_queue_iter->lock_mode_ == l) {
      lck_queue_iter = lck_queue.request_queue_.erase(lck_queue_iter);
    } else {
      lck_queue_iter++;
    }
  }
}

bool LockManager::ABORTED_GUARD(Transaction* txn) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }    // guard against invalid call.
  return true;
}

bool LockManager::ShareChecker(Transaction* txn) {
  auto iso_level = txn->GetIsolationLevel();

  if (iso_level == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    //  1.1 no need for s lock for read_uncommitted, only exclusive lock is required for write.
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING  && iso_level == IsolationLevel::REPEATABLE_READ) {
  // 1.2 for Repeatable Read, no lock when shrinking.
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  return true;
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
// 1. abort transactions which conflict with lock share operation.
  if (!ShareChecker(txn)) return false;
// it's ok for read committed to take share lock in the shrinking phase.

// 2. whether already locked
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) return true;   // already locked, return immediately.
// 3 if locked by other transactions, either abort or waiting on condition variable.

// take the latch_, since we are going to reading/ writing a shared dsa, use unique lock to aid
// with the use of condition variable
// ref:https://www.cplusplus.com/reference/condition_variable/condition_variable/
  latch_.lock();
  LockRequestQueue& lock_queue = lock_table_[rid];
  std::unique_lock<std::mutex> que_lck(lock_queue.queue_latch_);
  latch_.unlock();    // hashtable resizing may invalidate operater[], e.g when finding in a certain bucket.
  bool killed = false;

exam:
  auto lock_request_iter = lock_queue.request_queue_.begin();

  // policy: no exception thrown out, just abort it implicitly.
  // if we find any element on the queue, we are sure that it comes from a nother transaction.

  for (; lock_request_iter != lock_queue.request_queue_.end(); lock_request_iter++) {
    // 1. shared lock granted 2. exclusive lock granted 3. exclusive lock not granted, may lead to a chain of waiting.
    if (lock_request_iter->lock_mode_ == LockMode::EXCLUSIVE) {  // no need to check granted! see test
// wound wait policy: young(larger id) waits old(smaller id), for order of id see transation_manager.h next_txn_id_
      if (lock_request_iter->txn_id_ > txn->GetTransactionId()) {
        killed = true;
        // older would not wait young, abort the YOUNGER transaction.
        // when aborting:
        // 0. release all locks!
        // 1. do not need to concern with ungranted lock on the queue,
        auto txn2abort = TransactionManager::GetTransaction(lock_request_iter->txn_id_);
        txn2abort->SetState(TransactionState::ABORTED);
      }
    }
    // if wound wait criterion cannot be satisfied, abort.
    // abortreason: abort on deadlock. ans:
    // when a lock is unlocked, call wake up(latch), of course the detail should be further refined.
  }

  if (killed) lock_queue.cv_.notify_all();

  lock_request_iter = lock_queue.request_queue_.begin();
  for (; lock_request_iter != lock_queue.request_queue_.end(); lock_request_iter++) {
    // 1. shared lock granted 2. exclusive lock granted 3. exclusive lock not granted, may lead to a chain of waiting.
    if (lock_request_iter->lock_mode_ == LockMode::EXCLUSIVE) {   // no need to check granted! see test
// wound wait policy: young(larger id) waits old(smaller id), for order of id see transation_manager.h next_txn_id_
      if (lock_request_iter->txn_id_ < txn->GetTransactionId()) {
        //  add a corresponding request, then wait on cv_

        lock_queue.request_queue_.push_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED));
        // implicitly, granted == false;
        lock_queue.cv_.wait(que_lck);
        if (!ABORTED_GUARD(txn)) {
          removefromqueue(lock_queue, txn, LockMode::SHARED);
          return false;
        }
        if (!ShareChecker(txn)) return false;
        if(txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) return true;
        goto exam;
        //  when this thread is woken up and scheduled, the process of going through lock_queue should be done again.
      }
    }
  }

// s lock successfully granted.
// latch acquired, cur txn would not be aborted.
  lock_queue.request_queue_.push_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED));
  txn->GetSharedLockSet()->emplace(rid);
  txn->SetState(TransactionState::GROWING);

  lock_queue.queue_latch_.unlock();

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  auto exc_lck_table = txn->GetExclusiveLockSet();
  if (exc_lck_table->find(rid) != exc_lck_table->end()) {
    return true;
  }    // already locked.

  latch_.lock();
  LockRequestQueue& lock_queue = lock_table_[rid];
  std::unique_lock<std::mutex> que_lck(lock_queue.queue_latch_);
  latch_.unlock();
  bool killed = false;

exam:

  auto lck_queue_iter = lock_queue.request_queue_.begin();
  for (; lck_queue_iter != lock_queue.request_queue_.end(); lck_queue_iter++) {
      if (lck_queue_iter->txn_id_ > txn->GetTransactionId()) {
        auto txn2abort = TransactionManager::GetTransaction(lck_queue_iter->txn_id_);
        txn2abort->SetState(TransactionState::ABORTED);
        killed = true;
      }
  }

  if (killed) lock_queue.cv_.notify_all();
  lck_queue_iter = lock_queue.request_queue_.begin();
  for (; lck_queue_iter != lock_queue.request_queue_.end(); lck_queue_iter++) {
      if (lck_queue_iter->txn_id_ < txn->GetTransactionId()) {
        lock_queue.request_queue_.push_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));
        lock_queue.cv_.wait(que_lck);
        if (!ABORTED_GUARD(txn)) {
          removefromqueue(lock_queue, txn, LockMode::EXCLUSIVE);
          return false;
        }
        if (txn->GetState() == TransactionState::SHRINKING) {
          txn->SetState(TransactionState::ABORTED);
          return false;
        }
        if (exc_lck_table->find(rid) != exc_lck_table->end()) {
          return true;
        }    // already locked.
        goto exam;
      }
    //}
  }

  lock_queue.request_queue_.push_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));
  txn->GetExclusiveLockSet()->emplace(rid);
  txn->SetState(TransactionState::GROWING);
  lock_queue.queue_latch_.unlock();
  return true;
}

bool LockManager::UpgradeCheck(Transaction* txn, const RID& rid) {
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  auto sh_lck_set = txn->GetSharedLockSet();
  if (sh_lck_set->find(rid) == sh_lck_set->end()) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  ABORTED_GUARD(txn);

  if (!UpgradeCheck(txn, rid)) return false;

  auto ex_lck_set = txn->GetExclusiveLockSet();
  if (ex_lck_set->find(rid) != ex_lck_set->end()) {
    return true;
  }

  latch_.lock();
  LockRequestQueue& lock_queue = lock_table_[rid];
  std::unique_lock<std::mutex> que_lck(lock_queue.queue_latch_);
  latch_.unlock();
  bool killed = false;

exam:
  auto lck_queue_iter = lock_queue.request_queue_.begin();

  for (; lck_queue_iter != lock_queue.request_queue_.end(); lck_queue_iter++) {
    if (lck_queue_iter->txn_id_ > txn->GetTransactionId()) {
      auto txn2abort = TransactionManager::GetTransaction(lck_queue_iter->txn_id_);
      txn2abort->SetState(TransactionState::ABORTED);
      killed = true;
    }
  }

  lck_queue_iter = lock_queue.request_queue_.begin();

  for (; lck_queue_iter != lock_queue.request_queue_.end(); lck_queue_iter++) {
    if (lck_queue_iter->txn_id_ < txn->GetTransactionId()) {
      lock_queue.request_queue_.push_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));
      lock_queue.cv_.wait(que_lck);
      if (!ABORTED_GUARD(txn)) {
        removefromqueue(lock_queue, txn, LockMode::EXCLUSIVE);
        return false;
      }
      if (!UpgradeCheck(txn, rid)) return false;
      if (ex_lck_set->find(rid) != ex_lck_set->end()) {
        return true;
      }
      goto exam;
    }
  }

  for (lck_queue_iter = lock_queue.request_queue_.begin(); lck_queue_iter != lock_queue.request_queue_.end();) {
    if (lck_queue_iter->txn_id_ == txn->GetTransactionId() && lck_queue_iter->lock_mode_ == LockMode::SHARED) {
      lck_queue_iter = lock_queue.request_queue_.erase(lck_queue_iter);
    } else {
      lck_queue_iter++;
    }
  }

  lock_queue.upgrading_ = false;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  lock_queue.request_queue_.push_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));
  lock_queue.queue_latch_.unlock();
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() != IsolationLevel::READ_COMMITTED) {
     txn->SetState(TransactionState::SHRINKING);
  }   // both both repeatable read/ read (un) commited

  auto sh_lck_set = txn->GetSharedLockSet();
  auto ex_lck_set = txn->GetExclusiveLockSet();
  if (sh_lck_set->find(rid) == sh_lck_set->end() && ex_lck_set->find(rid) == ex_lck_set->end()) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  latch_.lock();
  LockRequestQueue& lock_queue = lock_table_[rid];
  //  auto lock_request_queue = lock_queue.request_queue_;
  lock_queue.queue_latch_.lock();
  latch_.unlock();

// search in the list
  auto lck_queue_iter = lock_queue.request_queue_.begin();
  for (; lck_queue_iter != lock_queue.request_queue_.end();) {
    if (lck_queue_iter->txn_id_ == txn->GetTransactionId()) {
      auto lock_mode = lck_queue_iter->lock_mode_;
      if (lock_mode == LockMode::SHARED) {
        sh_lck_set->erase(rid);
        if (!lock_queue.request_queue_.empty()) {
          lock_queue.cv_.notify_all();
        }
      } else {
        ex_lck_set->erase(rid);
        if (!lock_queue.request_queue_.empty()) {
          lock_queue.cv_.notify_all();
        }
      }
      lck_queue_iter = lock_queue.request_queue_.erase(lck_queue_iter);
    } else {
      lck_queue_iter++;
    }
  }
  lock_queue.queue_latch_.unlock();
  return true;
}

}  // namespace bustub
















