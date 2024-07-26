//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/** HashJoinKey is a simple struct that holds a single key value for a hash join. */
struct HashJoinKey {
  Value key_;

  /**
   * Construct a new HashJoinKey instance.
   * @param key The key value to store in the HashJoinKey.
   * @return `true` if both keys are equal, `false` otherwise.
   */
  auto operator==(const HashJoinKey &other) const -> bool { return key_.CompareEquals(other.key_) == CmpBool::CmpTrue; }
};

}  // namespace bustub

namespace std {
/** Implement hash function for HashJoinKey. */
template <>
struct hash<bustub::HashJoinKey> {
  /**
   * Compute the hash value for a HashJoinKey.
   * @param key The key to hash.
   * @return The hash value for the key.
   */
  auto operator()(const bustub::HashJoinKey &key) const -> size_t { return bustub::HashUtil::HashValue(&key.key_); }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** The left child executor that produces tuples for the join. */
  std::unique_ptr<AbstractExecutor> left_executor_;
  /** The right child executor that produces tuples for the join. */
  std::unique_ptr<AbstractExecutor> right_executor_;
  /** The indicator of whether the delete is finished */
  std::optional<bool> is_finished_;
  /** The hash table used to store the tuples from the left child. */
  std::vector<std::unordered_map<HashJoinKey, std::vector<Tuple>>> ht_;
  /** The output queue used to store the tuples from the right child. */
  std::queue<Tuple> output_queue_;
  /** The hash table used to indicate whether a tuple from the left child is matched. */
  std::unordered_map<HashJoinKey, std::tuple<Tuple, bool>> matched_;

  /**
   * Construct a vector of HashJoinKey from the given tuple and schema.
   * @param tuple The tuple to construct the keys from.
   * @param schema The schema of the tuple.
   * @param exprs The expressions to construct the keys from.
   * @return The vector of HashJoinKey constructed from the tuple.
   */
  auto MakeHashJoinKeys(const Tuple &tuple, const Schema &schema, const std::vector<AbstractExpressionRef> &exprs)
      -> std::vector<HashJoinKey>;
};

}  // namespace bustub
