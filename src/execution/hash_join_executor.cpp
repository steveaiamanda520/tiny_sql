//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  // throw NotImplementedException("HashJoinExecutor is not implemented");
  left_executor_->Init();
  right_executor_->Init();
  is_finished_ = std::nullopt;
  ht_.clear();
  output_queue_ = std::queue<Tuple>();
  matched_.clear();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!is_finished_.has_value()) {
    auto left_tuple = Tuple();
    auto left_rid = RID();
    while (left_executor_->Next(&left_tuple, &left_rid)) {
      auto keys = MakeHashJoinKeys(left_tuple, left_executor_->GetOutputSchema(), plan_->LeftJoinKeyExpressions());
      for (size_t i = 0; i < keys.size(); ++i) {
        if (ht_.size() <= i) {
          ht_.emplace_back();
        }

        if (ht_[i].find(keys[i]) == ht_[i].end()) {
          ht_[i][keys[i]] = std::vector<Tuple>();
        }
        ht_[i][keys[i]].push_back(left_tuple);
      }

      if (!keys.empty()) {
        matched_[keys[0]] = std::make_tuple(left_tuple, false);
      }
    }

    is_finished_ = false;
    LOG_DEBUG("Hash table size: %lu", ht_.size());
  }

  auto produce_output = [this, tuple, rid] {
    if (output_queue_.empty()) {
      return false;
    }

    *tuple = output_queue_.front();
    output_queue_.pop();
    *rid = tuple->GetRid();
    return true;
  };

  if (*is_finished_) {
    return produce_output();
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
    if (!output_queue_.empty()) {
      return produce_output();
    }

    auto right_tuple = Tuple();
    auto right_rid = RID();
    if (!right_executor_->Next(&right_tuple, &right_rid)) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        for (const auto &[key, inner] : matched_) {
          const auto &[left_tuple, matched] = inner;
          if (!matched) {
            while (!ht_[0][key].empty()) {
              output_queue_.push(join_tuple(ht_[0][key].back(), right_tuple_null));
              ht_[0][key].pop_back();
            }
          }
        }
      }
      is_finished_ = true;

      return produce_output();
    }

    auto keys = MakeHashJoinKeys(right_tuple, right_executor_->GetOutputSchema(), plan_->RightJoinKeyExpressions());
    auto candidates = std::vector<Tuple>();

    for (size_t i = 0; i < keys.size(); i++) {
      if (i == 0) {
        candidates = ht_[i][keys[i]];
      } else {
        auto new_candidates = std::vector<Tuple>();
        for (auto &tuple : candidates) {
          auto expected = MakeHashJoinKeys(tuple, left_executor_->GetOutputSchema(), plan_->LeftJoinKeyExpressions());
          if (expected[i] == keys[i]) {
            new_candidates.push_back(std::move(tuple));
          }
        }
        candidates = new_candidates;
      }

      if (candidates.empty()) {
        goto done;
      }
    }

    for (const auto &tuple : candidates) {
      output_queue_.push(join_tuple(tuple, right_tuple));
      auto keys = MakeHashJoinKeys(tuple, left_executor_->GetOutputSchema(), plan_->LeftJoinKeyExpressions());
      if (!keys.empty()) {
        std::get<1>(matched_[keys[0]]) = true;
      }
    }

  done:;
  }

  return false;
}

auto HashJoinExecutor::MakeHashJoinKeys(const Tuple &tuple, const Schema &schema,
                                        const std::vector<AbstractExpressionRef> &exprs) -> std::vector<HashJoinKey> {
  auto keys = std::vector<HashJoinKey>();
  for (const auto &expr : exprs) {
    keys.emplace_back(HashJoinKey{expr->Evaluate(&tuple, schema)});
  }
  return keys;
}

}  // namespace bustub
