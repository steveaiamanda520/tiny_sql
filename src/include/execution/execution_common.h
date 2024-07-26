#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

auto ReconstructTupleForLogIndex(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                                 const std::vector<UndoLog> &undo_logs, size_t index) -> std::optional<Tuple>;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

auto CollectUndoLogs(TransactionManager *txn_mgr, UndoLink undo_link, timestamp_t read_ts) -> std::vector<UndoLog>;

void WalkUndoLogs(TransactionManager *txn_mgr, UndoLink undo_link,
                  const std::function<bool(const UndoLink &, const UndoLog &)> &callback);

void Modify(TransactionManager *txn_mgr, Transaction *txn, const Schema *schema, table_oid_t oid, RID rid,
            const TupleMeta &old_meta, const Tuple &old_tuple, const TupleMeta &new_meta, const Tuple &new_tuple);

auto IsWriteWriteConflict(const Transaction *txn, const TupleMeta &meta, const Tuple &tuple, bool is_delete) -> bool;

auto GenerateDiffLog(const Schema *schema, const TupleMeta &old_meta, const Tuple &old_tuple, const TupleMeta &new_meta,
                     const Tuple &new_tuple) -> std::optional<UndoLog>;

auto GenerateNullTupleForSchema(const Schema *schema) -> Tuple;

auto GetUndoLogSchema(const Schema *schema, const UndoLog &log) -> Schema;

auto InsertTupleImpl(TransactionManager *txn_mgr, Transaction *txn, TableInfo *table, IndexInfo *index,
                     Tuple &target_tuple) -> std::optional<RID>;

void DeleteTupleImpl(TransactionManager *txn_mgr, Transaction *txn, TableInfo *table, const RID &rid,
                     const TupleMeta &old_meta, const Tuple &old_tuple);

}  // namespace bustub
