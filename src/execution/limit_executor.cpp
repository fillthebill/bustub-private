//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
	child_executor_->Init();
	num = 0;
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
	Tuple output_tuple;
	RID output_rid;
	while(1) {
		try {
			if(!child_executor_->Next(&output_tuple, &output_rid)) {
				break;
			}
		} catch (Exception& e) {
			throw Exception(ExceptionType::UNKNOWN_TYPE, "child of limit throwed!");
		}
		if(num < plan_->GetLimit()) {
			num++;
			*tuple = output_tuple;
			*rid = output_rid;
			return true;
		}
		return false;
	}
	return false;
}

}  // namespace bustub
