//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::InsertWithIndexUpdate(Tuple tuple) {
	RID rid;
// insert into the heap and the index
	if(!table_heap_->InsertTuple(tuple, &rid, exec_ctx_->GetTransaction())) {  // rid would be returned implicitly.
		throw Exception(ExceptionType::OUT_OF_MEMORY, "no space for insertion into heap!");  // see exeption.h
	}

	for (auto& index : catalog_->GetTableIndexes(table_info_->name_)) {
		// params:   key, rid, txn
		index->index_->InsertEntry(tuple.KeyFromTuple
			(table_info_->schema_, *(index->index_->GetKeySchema()), index->index_->GetKeyAttrs()),
			rid, exec_ctx_->GetTransaction());
	}
}

void InsertExecutor::Init() {
	catalog_ = exec_ctx_->GetCatalog();
	table_info_ = catalog_->GetTable(plan_->TableOid());
	table_heap_ = table_info_->table_.get();
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
	//  raw insertion or select insertion.
	if (plan_->IsRawInsert()) {
		for (const auto& values : plan_->RawValues()) {
			InsertWithIndexUpdate(Tuple(values, &(table_info_->schema_)));  //  insert a tuple
		}
		return false;
	}
	//  select insertion, exec the child seq scan first, mimic the execution engine.

	std::vector<Tuple> child_output;
	child_executor_->Init();
	try {
		Tuple tuple;
		RID rid;
		while (child_executor_->Next(&tuple, &rid)) {
			child_output.push_back(tuple);
		}
	} catch (Exception &e) {    // just in case it throw.
		throw Exception(ExceptionType::UNKNOWN_TYPE, "child of insert throws");
	}

	for(auto & output : child_output) {
		InsertWithIndexUpdate(output);
	}
	return false;
}

}  // namespace bustub
