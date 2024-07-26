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
#include "execution/executors/update_executor.h"

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  // throw NotImplementedException("UpdateExecutor is not implemented");
  child_executor_->Init();
  is_finished_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }

  auto table = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto index = exec_ctx_->GetCatalog()->GetIndex(plan_->GetTableOid());

  auto local_buffer = std::vector<std::tuple<Tuple, RID>>();
  auto target_tuple = Tuple();
  auto target_rid = RID();

  while (child_executor_->Next(&target_tuple, &target_rid)) {
    local_buffer.emplace_back(target_tuple, target_rid);
  }

  auto defered_buffer = std::vector<Tuple>();
  auto is_index_columns_updated = [&](Tuple &old_tuple, Tuple &new_tuple) {
    if (index == nullptr) {
      return false;
    }

    auto old_key = old_tuple.KeyFromTuple(table->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    auto new_key = new_tuple.KeyFromTuple(table->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    return !IsTupleContentEqual(old_key, new_key);
  };

  for (auto &[old_tuple, rid] : local_buffer) {
    auto old_meta = table->table_->GetTupleMeta(rid);

    auto values = std::vector<Value>();
    for (const auto &expr : plan_->target_expressions_) {
      values.emplace_back(expr->Evaluate(&old_tuple, table->schema_));
    }

    auto new_tuple = Tuple(std::move(values), &table->schema_);
    if (is_index_columns_updated(old_tuple, new_tuple)) {
      DeleteTupleImpl(exec_ctx_->GetTransactionManager(), exec_ctx_->GetTransaction(), table, rid, old_meta, old_tuple);
      defered_buffer.emplace_back(new_tuple);
      continue;
    }

    auto new_meta = old_meta;
    new_meta.ts_ = exec_ctx_->GetTransaction()->GetTransactionTempTs();

    table->table_->UpdateTupleInPlace(
        new_meta, new_tuple, rid,
        [&](const bustub::TupleMeta &old_meta, const bustub::Tuple &old_tuple, bustub::RID rid) {
          if (IsWriteWriteConflict(exec_ctx_->GetTransaction(), old_meta, old_tuple, false)) {
            exec_ctx_->GetTransaction()->SetTainted();
            throw ExecutionException("Update failed due to write-write conflict");
          }

          Modify(exec_ctx_->GetTransactionManager(), exec_ctx_->GetTransaction(), &table->schema_, plan_->GetTableOid(),
                 rid, old_meta, old_tuple, new_meta, new_tuple);

          return true;
        });
  }

  for (auto &new_tuple : defered_buffer) {
    auto new_rid =
        InsertTupleImpl(exec_ctx_->GetTransactionManager(), exec_ctx_->GetTransaction(), table, index, new_tuple);
    if (!new_rid) {
      continue;
    }
  }

  *tuple = Tuple({Value(TypeId::INTEGER, static_cast<int32_t>(local_buffer.size()))}, &plan_->OutputSchema());
  is_finished_ = true;
  return true;
}

}  // namespace bustub
