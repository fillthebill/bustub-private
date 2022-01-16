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
	table_iterator_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
// loop until we find a tuple that satisfies the predicate
// avoid recusion, i.e. call Next when not found, may be too costly.

	auto table_schema = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_;
	Tuple prev_tuple;
	RID prev_rid;
	auto predicate = plan_->GetPredicate();

	do {
		// if the end, return false.
		if (table_iterator_ == table_heap_->End())
		return false; 

		prev_tuple = *table_iterator_;
		//const Tuple* prev_tuple = &(*table_iterator_)
		// this is buggy, after incrementing iterator, the value pointed to has changed.
		prev_rid = table_iterator_->GetRid();
		++table_iterator_;

	} while ( predicate != nullptr && !predicate->Evaluate(&prev_tuple, &table_schema).GetAs<bool>() );

// when the predicate is satisfied.
	auto output_schema = plan_->OutputSchema();
	uint32_t col_num = output_schema->GetColumnCount();
	auto columns = output_schema->GetColumns();
	
	std::vector<Value> values;
	values.reserve(col_num);

	for ( auto & col : columns ) {
		values.push_back(col.GetExpr()->Evaluate(&prev_tuple, 
			&table_schema) );
	}

	*tuple = Tuple(values, output_schema);
	*rid = prev_rid;
	return true;

}

}  // namespace bustub
