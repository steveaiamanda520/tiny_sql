#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  // throw NotImplementedException("TopNExecutor is not implemented");
  child_executor_->Init();
  top_entries_.clear();
  cursor_ = std::nullopt;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!cursor_.has_value()) {
    auto compare_tuple = [this](const Tuple &lhs, const Tuple &rhs) {
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
    };
    auto heap = std::priority_queue<Tuple, std::vector<Tuple>, decltype(compare_tuple)>(compare_tuple);

    auto child_tuple = Tuple();
    auto child_rid = RID();
    while (child_executor_->Next(&child_tuple, &child_rid)) {
      heap.push(child_tuple);
      if (heap.size() > plan_->n_) {
        heap.pop();
      }
    }

    while (!heap.empty()) {
      top_entries_.push_back(heap.top());
      heap.pop();
    }
    std::reverse(top_entries_.begin(), top_entries_.end());
    cursor_ = top_entries_.cbegin();
  }

  if (*cursor_ == top_entries_.cend()) {
    return false;
  }

  *tuple = **cursor_;
  *rid = tuple->GetRid();
  cursor_->operator++();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
  // throw NotImplementedException("TopNExecutor is not implemented");
  return top_entries_.size();
}

}  // namespace bustub
