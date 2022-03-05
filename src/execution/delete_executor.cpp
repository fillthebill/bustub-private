//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
	table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
	child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
	Tuple t2delete;
	RID r2delete;

// acquire EX lock before delete, EX lock are freed after commit, so we do not need to concern unlock()

	while(1) {
		try {
			if (!child_executor_->Next(&t2delete, &r2delete)) {
				break;
			}
		} catch (Exception& e) {
			throw Exception(ExceptionType::UNKNOWN_TYPE, "child of delete throwed");
		}

		auto txn = exec_ctx_->GetTransaction();
		auto lck_mgr = exec_ctx_->GetLockManager();

// case 1, no lock, then take on ex lock
// case 2, S lock, upgrade.
// case 3, ex locked, nothing need to be done.

		if (txn->IsSharedLocked(r2delete)) {   // case 2 
			if (!lck_mgr->LockUpgrade(txn, r2delete)) {
				// upgrading S to E failed.
				return false;
			}
		} else if (!txn->IsExclusiveLocked(r2delete)) {   // case 1
			if (!lck_mgr->LockExclusive(txn, r2delete)) {
				return false;
			}	
		}
		// otherwise, case 3, nothing need to be done.
		
		table_info_->table_.get()->MarkDelete(r2delete, exec_ctx_->GetTransaction());
		for (auto& index_info_ : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
			index_info_->index_->DeleteEntry
			(t2delete.KeyFromTuple(table_info_->schema_, *(index_info_->index_->GetKeySchema()),
			 index_info_->index_->GetKeyAttrs()),
			 r2delete, exec_ctx_->GetTransaction());
			
			// save the change into writeset for future rollback
			txn->GetIndexWriteSet()->push_back(IndexWriteRecord(r2delete,   // see transaction.h
				table_info_->oid_, WType::DELETE, t2delete, index_info_->index_oid_, exec_ctx_->GetCatalog()));
				  // see catalog.h for the structure of indexinfo
		}
	}
	return false;
}

}  // namespace bustub
