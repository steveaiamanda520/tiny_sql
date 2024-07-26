#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  // throw NotImplementedException("SortExecutor is not implemented");
  child_executor_->Init();
  tuples_.clear();
  cursor_ = std::nullopt;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!cursor_.has_value()) {
    auto child_tuple = Tuple();
    auto child_rid = RID();
    while (child_executor_->Next(&child_tuple, &child_rid)) {
      tuples_.emplace_back(child_tuple);
    }

    std::sort(tuples_.begin(), tuples_.end(), [this](const Tuple &lhs, const Tuple &rhs) {
      for (const auto &[order, expr] : plan_->order_bys_) {
        if (static_cast<bool>(expr->Evaluate(&lhs, child_executor_->GetOutputSchema())
                                  .CompareLessThan(expr->Evaluate(&rhs, child_executor_->GetOutputSchema())))) {
          return order == OrderByType::INVALID || order == OrderByType::DEFAULT || order == OrderByType::ASC;
        }

        if (static_cast<bool>(expr->Evaluate(&lhs, child_executor_->GetOutputSchema())
                                  .CompareGreaterThan(expr->Evaluate(&rhs, child_executor_->GetOutputSchema())))) {
          return order == OrderByType::DESC;
        }
      }
      return false;
    });

    cursor_ = tuples_.cbegin();
  }

  if (*cursor_ == tuples_.cend()) {
    return false;
  }

  *tuple = **cursor_;
  *rid = tuple->GetRid();
  cursor_->operator++();
  return true;
}

}  // namespace bustub
