#include "optimizer/optimizer.h"

#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  auto children = plan->GetChildren();
  for (auto &child : children) {
    child = OptimizeSortLimitAsTopN(child);
  }

  auto optimized_plan = plan->CloneWithChildren(children);
  if (optimized_plan->GetType() != PlanType::Limit) {
    return optimized_plan;
  }

  auto limit_plan = dynamic_cast<LimitPlanNode *>(optimized_plan.get());
  if (limit_plan->GetChildAt(0)->GetType() != PlanType::Sort) {
    return optimized_plan;
  }

  auto sort_plan = dynamic_cast<const SortPlanNode *>(limit_plan->GetChildAt(0).get());
  return std::make_unique<TopNPlanNode>(limit_plan->output_schema_, sort_plan->GetChildAt(0), sort_plan->GetOrderBy(),
                                        limit_plan->GetLimit());
}

}  // namespace bustub
