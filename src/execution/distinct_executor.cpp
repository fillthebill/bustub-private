//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)),
    map_{}, map_iterator_(map_.begin()) {}

void DistinctExecutor::Init() {
	// for each tuple from child executor,
	// see if such distinctkey already exist.
	child_executor_->Init();

	Tuple tuple;
	RID rid;
	while (child_executor_->Next(&tuple, &rid)) {
		auto out_schema = plan_->OutputSchema();
		uint32_t key_num = out_schema->GetColumnCount();
		DistinctKey temp;
		temp.distinct_keys_.reserve(key_num);
		for (uint32_t i = 0; i < key_num; ++i) {
			temp.distinct_keys_.push_back(tuple.GetValue(out_schema, i));
		}

		if(map_.count(temp) == 0) {
			map_.insert({temp, tuple});
		}
	}

	map_iterator_ = map_.begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
	// map_ from key(could be multiple attributes) to values
	if(map_iterator_ == map_.end()) {
		return false;
	}

	*tuple = map_iterator_->second;
	*rid = map_iterator_->second.GetRid();
	map_iterator_++;
	return true;
}

}  // namespace bustub
