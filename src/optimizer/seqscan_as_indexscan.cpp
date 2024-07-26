#include "optimizer/optimizer.h"

#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  auto children = plan->GetChildren();
  for (auto &child : children) {
    child = OptimizeSeqScanAsIndexScan(child);
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() != PlanType::SeqScan) {
    return optimized_plan;
  }

  auto seq_scan_plan = dynamic_cast<SeqScanPlanNode *>(optimized_plan.get());
  if (seq_scan_plan->filter_predicate_ == nullptr) {
    return optimized_plan;
  }

  auto expr = dynamic_cast<ComparisonExpression *>(seq_scan_plan->filter_predicate_.get());
  if (expr == nullptr || expr->comp_type_ != ComparisonType::Equal) {
    return optimized_plan;
  }

  auto key = dynamic_cast<ColumnValueExpression *>(expr->GetChildAt(0).get());
  auto value = dynamic_cast<ConstantValueExpression *>(expr->GetChildAt(1).get());
  if (key == nullptr || value == nullptr) {
    return optimized_plan;
  }

  auto indexes = catalog_.GetTableIndexes(seq_scan_plan->table_name_);
  auto index = std::find_if(indexes.begin(), indexes.end(), [&](const IndexInfo *index_info) {
    auto index = index_info->index_.get();
    return index->GetKeyAttrs() == std::vector<uint32_t>{key->GetColIdx()};
  });
  if (index == indexes.end()) {
    return optimized_plan;
  }

  return std::make_shared<IndexScanPlanNode>(seq_scan_plan->output_schema_, seq_scan_plan->table_oid_,
                                             (*index)->index_oid_, seq_scan_plan->filter_predicate_, value);
}

}  // namespace bustub
