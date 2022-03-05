//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) :
	AbstractExecutor(exec_ctx), plan_(plan),
	table_heap_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()),
	table_iterator_(table_heap_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() {
	// table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
	// remember to get from uniqueptr.
	table_iterator_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
// loop until we find a tuple that satisfies the predicate
// avoid recusion, i.e. call Next when not found, may be too costly.

// considering the locking convention for different isolation level: 
// 1. repeatable read: only release lock after commit. grab the R lock before reading tuples and examining
// whether the predicate is satisfied, if the lock cannot be acquired, 
// 2. read committed: 
//
//
// 3. read uncommitted


// the level3: read-uncommitted, do not have to take any lock, just exectute the seqscan normally.
// read committed would unlock after 

// we do not need to check whether the intended tuple has already been locked in sharead or exclusived mode, 
// the function LockShared() would concern with that....

// whether & where to lock and unlock for transactions with different isolation level??

// read-uncommitted, no lock at all, just read and see whether the predicate is satisfied.
// read-committed, lock before reading, unlock after reading the tuple, (just to review, this policy may result
// in unrepeatable read). Specifically, we could unlock after the prev_tuple is updated, which could be used for
// further modifications.

// in case of repeatable read, same place to lock as read-commmitted, but no need to unlock here.

// All in all, two bool variables are inited to represent these different senarios, correspoding lock
// and unlock operation would be decideds as allowed or not based on these bool variables.

	auto table_schema = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_;
	Tuple prev_tuple;
	RID prev_rid;
	auto predicate = plan_->GetPredicate();

	bool read_uncommitted = false;
	bool read_committed = false;
	auto lck_mgr = exec_ctx_->GetLockManager();
	auto txn = exec_ctx_->GetTransaction();
	if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
		read_uncommitted = true;
	}
	if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
		read_committed = true;
	}

	do {
		// if the end, return false.
		if (table_iterator_ == table_heap_->End())
		return false;

		prev_rid = table_iterator_->GetRid();   // operator-> has been overloaded, return the tuple_.
		if (!read_uncommitted) {   // R_committed or repeatable read, take on share lock.
			if (!lck_mgr->LockShared(txn, prev_rid)) {
				return false;
			}
		}
		prev_tuple = *table_iterator_;
		// const Tuple* prev_tuple = &(*table_iterator_)
		// this would be buggy, after incrementing iterator, the value pointed to has changed.

		if (read_committed) {   // only unlock() in case of r_committed.
			if (!lck_mgr->Unlock(txn, prev_rid)) {
				return false;
			}
		}

		++table_iterator_;
	} while ( predicate != nullptr && !predicate->Evaluate(&prev_tuple, &table_schema).GetAs<bool>() );

// when the predicate is satisfied.
	auto output_schema = plan_->OutputSchema();
	uint32_t col_num = output_schema->GetColumnCount();
	auto columns = output_schema->GetColumns();
	std::vector<Value> values;
	values.reserve(col_num);

	for (auto & col : columns) {
		values.push_back(col.GetExpr()->Evaluate(&prev_tuple, &table_schema));
	}

	*tuple = Tuple(values, output_schema);
	*rid = prev_rid;
	return true;
}

}  // namespace bustub
