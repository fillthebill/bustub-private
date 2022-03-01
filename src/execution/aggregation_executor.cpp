//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)),
     aht_{plan->GetAggregates(), plan->GetAggregateTypes()}, aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
	child_->Init();

	Tuple tuple;
	RID rid;

	while(child_->Next(&tuple, &rid)) {
		aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
	}
	aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
	std::vector<Value> groupby;
	std::vector<Value> aggregate;
	do {
		if (aht_iterator_ == aht_.End()) {
			return false;
		}

		groupby = aht_iterator_.Key().group_bys_;
		aggregate = aht_iterator_.Val().aggregates_;
		++aht_iterator_;
	} while (plan_->GetHaving() != nullptr && !plan_->GetHaving()->EvaluateAggregate(groupby, aggregate).GetAs<bool>());

	std::vector<Value> result;
	uint32_t col_num = plan_->OutputSchema()->GetColumnCount();
	result.reserve(col_num);
	for (auto &col : plan_->OutputSchema()->GetColumns()) {
		result.push_back(col.GetExpr()->EvaluateAggregate(groupby, aggregate));
	}

	*tuple = Tuple(result, plan_->OutputSchema());
	return true;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
