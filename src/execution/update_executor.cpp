//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple t_old;
  Tuple t_new;
  RID rid_;

  auto txn = exec_ctx_->GetTransaction();
  auto lck_mgr = exec_ctx_->GetLockManager();
  // exec the child, and update it.
  while (1) {
    try {
      if (!child_executor_->Next(&t_old, &rid_)) {
        break;
      }
    } catch (Exception& e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "child of update throws exception");
    }

    t_new = GenerateUpdatedTuple(t_old);

    // take on ex lock before update.
    // case 1: s locked, then upgrade
    // case 2: NO lock, take E lock
    // case 3: E locked, nothing.
    // do not  need to consider unlock() here, would be concerned by commit/abort()

    if (txn->IsSharedLocked(rid_)) {
      // case 1
      if (!lck_mgr->LockUpgrade(txn, rid_)) {
        return false;
      }
    } else if(!txn->IsExclusiveLocked(rid_)) {
      if(!lck_mgr->LockExclusive(txn, rid_)) {
        return false;
      }
    }
    // otherwise, already E locked.

    table_info_->table_.get()->UpdateTuple(t_new, rid_, exec_ctx_->GetTransaction());
  
    for(auto & index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      index_info->index_->DeleteEntry(t_old.KeyFromTuple(table_info_->schema_,
       *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs()), rid_, exec_ctx_->GetTransaction());
    
      index_info->index_->InsertEntry(t_new.KeyFromTuple(table_info_->schema_,
        *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs()), rid_, exec_ctx_->GetTransaction());

      IndexWriteRecord index_w_record(rid_, table_info_->oid_, WType::DELETE, t_new, index_info->index_oid_,
        exec_ctx_->GetCatalog());

      index_w_record.old_tuple_ = t_old;
      txn->GetIndexWriteSet()->push_back(index_w_record);

    }

  }

  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
