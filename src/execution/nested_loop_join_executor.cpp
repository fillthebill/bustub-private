//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
     left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)),
    result_{} {}

void NestedLoopJoinExecutor::Init() {
// for each tuple in the left child, see if it match with tuple in the right child.
	size_ = 0;
	cur_id_ = 0;
	Tuple left_tuple;
	Tuple right_tuple;
	RID left_rid;
	RID right_rid;

	auto out_schema = GetOutputSchema();
	std::vector<Value> temp_result;
	temp_result.reserve(out_schema->GetColumnCount());

	left_executor_ ->Init();
	while (left_executor_->Next(&left_tuple, &left_rid)) {  // see execution/executors/abstract_executor for declaration.
		right_executor_ ->Init();

		while (right_executor_->Next(&right_tuple, &right_rid)) {
			if (plan_ == nullptr || plan_->Predicate()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
			 &right_tuple, right_executor_->GetOutputSchema()).GetAs<bool>() ) {
				for (auto &col : out_schema->GetColumns()) {
					temp_result.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
						&right_tuple, right_executor_->GetOutputSchema()));  // return value based on left/right sideness of cur col.
				}
				result_.push_back(Tuple(temp_result, out_schema));
				temp_result.clear();  // in case lots of tuple to traverse, save memory to some extent by using the same vector.
			}
		}
	}
	size_ = result_.size();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
	if (cur_id_ < size_) {
		*tuple = result_[cur_id_];
		cur_id_++;
		return true;
	}
	return false;
}

}  // namespace bustub
