//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/delete_executor.h"

#include <memory>

#include "execution/execution_common.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // throw NotImplementedException("DeleteExecutor is not implemented");
  child_executor_->Init();
  is_finished_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }

  auto table = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());

  auto local_buffer = std::vector<std::tuple<Tuple, RID>>();
  auto target_tuple = Tuple();
  auto target_rid = RID();

  while (child_executor_->Next(&target_tuple, &target_rid)) {
    local_buffer.emplace_back(target_tuple, target_rid);
  }

  for (auto &[old_tuple, rid] : local_buffer) {
    auto old_meta = table->table_->GetTupleMeta(rid);

    DeleteTupleImpl(exec_ctx_->GetTransactionManager(), exec_ctx_->GetTransaction(), table, rid, old_meta, old_tuple);
  }

  *tuple = Tuple({Value(TypeId::INTEGER, static_cast<int32_t>(local_buffer.size()))}, &plan_->OutputSchema());
  is_finished_ = true;
  return true;
}

}  // namespace bustub
