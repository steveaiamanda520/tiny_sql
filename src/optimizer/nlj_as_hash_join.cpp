#include <algorithm>
#include <memory>
#include <stack>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...

  auto children = plan->GetChildren();
  for (auto &child : children) {
    child = OptimizeNLJAsHashJoin(child);
  }
  auto optimized_plan = plan->CloneWithChildren(children);

  if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
    return optimized_plan;
  }

  const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
  auto optimize_comparison_expression = [](const ComparisonExpression *cmp_expr,
                                           std::shared_ptr<bustub::ColumnValueExpression> *left,
                                           std::shared_ptr<bustub::ColumnValueExpression> *right) -> bool {
    if (cmp_expr->comp_type_ != ComparisonType::Equal) {
      return false;
    }

    const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->children_[0].get());
    if (left_expr == nullptr) {
      return false;
    }

    const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->children_[1].get());
    if (right_expr == nullptr) {
      return false;
    }

    auto left_tuple = std::make_shared<ColumnValueExpression>(left_expr->GetTupleIdx(), left_expr->GetColIdx(),
                                                              left_expr->GetReturnType());
    auto right_tuple = std::make_shared<ColumnValueExpression>(right_expr->GetTupleIdx(), right_expr->GetColIdx(),
                                                               right_expr->GetReturnType());

    if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
      *left = left_tuple;
      *right = right_tuple;
      return true;
    }

    if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
      *left = right_tuple;
      *right = left_tuple;
      return true;
    }

    return false;
  };

  if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get());
      cmp_expr != nullptr) {
    auto left_tuple = std::shared_ptr<ColumnValueExpression>();
    auto right_tuple = std::shared_ptr<ColumnValueExpression>();
    if (!optimize_comparison_expression(cmp_expr, &left_tuple, &right_tuple)) {
      return optimized_plan;
    }

    return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                              std::vector<AbstractExpressionRef>({left_tuple}),
                                              std::vector<AbstractExpressionRef>({right_tuple}),
                                              nlj_plan.GetJoinType());
  }

  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get());
      logic_expr != nullptr) {
    auto left_tuples = std::vector<AbstractExpressionRef>();
    auto right_tuples = std::vector<AbstractExpressionRef>();

    auto stack = std::stack<const LogicExpression *>({logic_expr});
    while (!stack.empty()) {
      const auto *logic_expr = stack.top();
      stack.pop();

      if (logic_expr->logic_type_ != LogicType::And) {
        return optimized_plan;
      }

      for (const auto &child : logic_expr->children_) {
        if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(child.get()); cmp_expr != nullptr) {
          auto left_tuple = std::shared_ptr<ColumnValueExpression>();
          auto right_tuple = std::shared_ptr<ColumnValueExpression>();
          if (optimize_comparison_expression(cmp_expr, &left_tuple, &right_tuple)) {
            left_tuples.push_back(left_tuple);
            right_tuples.push_back(right_tuple);
          }
        } else if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(child.get()); logic_expr != nullptr) {
          stack.push(logic_expr);
        }
      }
    }

    return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                              left_tuples, right_tuples, nlj_plan.GetJoinType());
  }

  return optimized_plan;
}

}  // namespace bustub
