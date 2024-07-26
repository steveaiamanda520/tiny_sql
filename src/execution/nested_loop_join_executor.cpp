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

#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // throw NotImplementedException("NestedLoopJoinExecutor is not implemented");
  left_executor_->Init();
  left_tuple_ = std::nullopt;
  is_finished_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }

  auto right_tuple_null = [this] {
    auto values = std::vector<Value>();
    for (auto i = 0U; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
    }
    return Tuple(values, &right_executor_->GetOutputSchema());
  }();
  auto join_tuple = [this](const Tuple &left, const Tuple &right) {
    auto values = std::vector<Value>(left_executor_->GetOutputSchema().GetColumnCount() +
                                     right_executor_->GetOutputSchema().GetColumnCount());
    for (auto i = 0U; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values[i] = left.GetValue(&left_executor_->GetOutputSchema(), i);
    }
    for (auto i = 0U; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values[i + left_executor_->GetOutputSchema().GetColumnCount()] =
          right.GetValue(&right_executor_->GetOutputSchema(), i);
    }
    return Tuple(values, &plan_->OutputSchema());
  };

  while (true) {
    if (left_tuple_.has_value()) {
      auto &inner = left_tuple_.value();

      auto right_tuple = Tuple();
      auto right_rid = RID();
      if (!right_executor_->Next(&right_tuple, &right_rid)) {
        if (!std::get<2>(inner) && plan_->GetJoinType() == JoinType::LEFT) {
          *tuple = join_tuple(std::get<0>(inner), right_tuple_null);
          *rid = tuple->GetRid();
          left_tuple_ = std::nullopt;
          return true;
        }

        left_tuple_ = std::nullopt;
        continue;
      }

      if (auto ok = plan_->predicate_->EvaluateJoin(&std::get<0>(inner), left_executor_->GetOutputSchema(),
                                                    &right_tuple, right_executor_->GetOutputSchema());
          !ok.IsNull() && ok.GetAs<bool>()) {
        *tuple = join_tuple(std::get<0>(inner), right_tuple);
        *rid = tuple->GetRid();
        std::get<2>(inner) = true;
        return true;
      }

      continue;
    }

    auto left_tuple = Tuple();
    auto left_rid = RID();
    if (!left_executor_->Next(&left_tuple, &left_rid)) {
      is_finished_ = true;
      return false;
    }

    left_tuple_ = std::make_tuple(left_tuple, left_rid, false);
    right_executor_->Init();
  }

  return false;
}  // namespace bustub

}  // namespace bustub
