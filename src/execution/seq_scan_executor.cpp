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
	//table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get(); 
	// remember to get from uniqueptr.
	//table_iterator_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
// if the end, return false.
	if (table_iterator_ == table_heap_->End())
	return false; 

// select the columns to return, according to output_schema.

	auto output_schema = plan_->OutputSchema();
	auto table_schema = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_;
	std::vector<Value> values;
	uint32_t col_num = output_schema->GetColumnCount();
	values.reserve(col_num);

	auto columns = output_schema->GetColumns();
	const Tuple* prev_tuple = &(*table_iterator_);
	
	for (uint32_t i = 0; i < col_num; i++) {
		values.push_back(columns[i].GetExpr()->Evaluate(prev_tuple, 
			&table_schema) );
	}

	RID prev_rid = table_iterator_->GetRid();   // save previous rid before incrementation.
	++table_iterator_;

// if the predicate is satisfied, return the tuple, otherwise see if the next tuple satisfies such criteria.
	auto predicate = plan_->GetPredicate();
	if ( predicate == nullptr || predicate->Evaluate(prev_tuple,
		&table_schema).GetAs<bool>() ) {
		*tuple = Tuple(values, output_schema);
		*rid = prev_rid;
		return true;
	}
	return Next(tuple, rid);



}


}  // namespace bustub
