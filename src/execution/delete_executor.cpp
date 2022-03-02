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

	while(1) {
		try {
			if (!child_executor_->Next(&t2delete, &r2delete)) {
				break;
			}
		} catch (Exception& e) {
			throw Exception(ExceptionType::UNKNOWN_TYPE, "child of delete throwed");
		}

		table_info_->table_.get()->MarkDelete(r2delete, exec_ctx_->GetTransaction());
		for (auto& index_info_ : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
			index_info_->index_->DeleteEntry
			(t2delete.KeyFromTuple(table_info_->schema_, *(index_info_->index_->GetKeySchema()),
			 index_info_->index_->GetKeyAttrs()),
			 r2delete, exec_ctx_->GetTransaction());
		}
	}
	return false;
}

}  // namespace bustub
