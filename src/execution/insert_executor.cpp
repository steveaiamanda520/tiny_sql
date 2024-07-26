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

#include "execution/executors/insert_executor.h"

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // throw NotImplementedException("InsertExecutor is not implemented");
  child_executor_->Init();
  is_finished_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }

  auto table = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto index = exec_ctx_->GetCatalog()->GetIndex(plan_->GetTableOid());

  size_t inserted = 0;
  auto target_tuple = Tuple();
  auto target_rid = RID();
  while (child_executor_->Next(&target_tuple, &target_rid)) {
    auto target_rid =
        InsertTupleImpl(exec_ctx_->GetTransactionManager(), exec_ctx_->GetTransaction(), table, index, target_tuple);
    if (!target_rid) {
      continue;
    }

    inserted++;
  }

  *tuple = Tuple({Value(TypeId::INTEGER, static_cast<int32_t>(inserted))}, &plan_->OutputSchema());
  is_finished_ = true;
  return true;
}

}  // namespace bustub
