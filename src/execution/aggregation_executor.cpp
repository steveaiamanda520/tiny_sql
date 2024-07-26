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
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();
  aht_iterator_ = std::nullopt;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!aht_iterator_.has_value()) {
    auto target_tuple = Tuple();
    auto target_rid = RID();
    while (child_executor_->Next(&target_tuple, &target_rid)) {
      auto key = MakeAggregateKey(&target_tuple);
      auto value = MakeAggregateValue(&target_tuple);
      aht_.InsertCombine(key, value);
    }

    aht_iterator_ = aht_.Begin();
    if (aht_.Size() == 0 && plan_->output_schema_->GetColumnCount() == 1) {
      auto values = aht_.GenerateInitialAggregateValue().aggregates_;
      *tuple = Tuple(values, &plan_->OutputSchema());
      *rid = tuple->GetRid();
      return true;
    }
  }

  if (*aht_iterator_ == aht_.End()) {
    return false;
  }

  auto key = aht_iterator_->Key();
  auto value = aht_iterator_->Val();
  aht_iterator_->operator++();

  auto values = std::vector<Value>(key.group_bys_.size() + value.aggregates_.size());
  std::copy(key.group_bys_.begin(), key.group_bys_.end(), values.begin());
  std::copy(value.aggregates_.begin(), value.aggregates_.end(), values.begin() + key.group_bys_.size());

  *tuple = Tuple(values, &plan_->OutputSchema());
  *rid = tuple->GetRid();
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
