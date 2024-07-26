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

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  Transaction *transaction = exec_ctx_->GetTransaction();

  table_oid_t table_id = plan_->table_oid_;

  transaction->AppendScanPredicate(table_id, plan_->filter_predicate_);
}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  cursor_ =
      std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_->IsEnd()) {
    return false;
  }

  auto table = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());

  while (!cursor_->IsEnd()) {
    auto [meta, value] = cursor_->GetTuple();
    cursor_->operator++();

    auto recovered = std::optional<Tuple>();

    auto is_most_recent = exec_ctx_->GetTransaction()->GetReadTs() >= meta.ts_;
    auto is_own_write = meta.ts_ >= TXN_START_ID && meta.ts_ == exec_ctx_->GetTransaction()->GetTransactionId();
    if (is_most_recent || is_own_write) {
      if (meta.is_deleted_) {
        continue;
      }

      recovered = value;
    } else if (auto undo_link = exec_ctx_->GetTransactionManager()->GetUndoLink(value.GetRid()); undo_link) {
      auto logs = CollectUndoLogs(exec_ctx_->GetTransactionManager(), undo_link.value(),
                                  exec_ctx_->GetTransaction()->GetReadTs());
      if (logs.empty()) {
        continue;
      }

      recovered = ReconstructTuple(&table->schema_, value, meta, logs);
    }

    if (!recovered || (plan_->filter_predicate_ != nullptr &&
                       !plan_->filter_predicate_->Evaluate(&*recovered, table->schema_).GetAs<bool>())) {
      continue;
    }

    *tuple = *recovered;
    *rid = value.GetRid();
    return true;
  }

  return false;
}

}  // namespace bustub