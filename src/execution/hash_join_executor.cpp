//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_child)),
    right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
	left_executor_->Init();
	Tuple left_tuple;
	RID left_rid;
	while (left_executor_->Next(&left_tuple, &left_rid)) {
		HashJoinKey left_key;
		left_key.keys_ = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_executor_->GetOutputSchema());
		if (map_.count(left_key) != 0) {
			map_[left_key].emplace_back(left_tuple);
		} else {
			map_.insert({ left_key, std::vector{left_tuple} });
		} 
	}

	Tuple right_tuple;
	RID right_rid;
	std::vector<Value> output;
	auto left_schema = left_executor_->GetOutputSchema();
	auto right_schema = right_executor_->GetOutputSchema();
	auto output_schema = GetOutputSchema();
	uint32_t col_num = output_schema->GetColumnCount();

	while(right_executor_->Next(&right_tuple, &right_rid)) {
		HashJoinKey right_key;
		right_key.keys_ = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_executor_->GetOutputSchema());
		if(map_.count(right_key) != 0) {
			auto left_tuples = map_.find(right_key)->second;
			for (auto & left_tuple_hashed : left_tuples) // traverse the whole bucket.
			{
				output.reserve(col_num);
				for ( uint32_t i = 0; i < col_num; ++i) {
					output.push_back(output_schema->GetColumn(i).GetExpr()->
						EvaluateJoin(&left_tuple_hashed, left_schema, &right_tuple, right_schema));
				}
				result_.push_back(Tuple(output, GetOutputSchema()));
				output.clear();
			}
		}
	}

	size_ = result_.size();
	cur_id = 0;
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) { 
	if (cur_id == size_) {
		return false;
	}
	*tuple = result_[cur_id];
	*rid = result_[cur_id].GetRid();
	cur_id++;
	return true; 
}

}  // namespace bustub
