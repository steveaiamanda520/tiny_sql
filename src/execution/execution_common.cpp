#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // UNIMPLEMENTED("not implemented");
  auto recovered = std::optional<Tuple>();
  if (!base_meta.is_deleted_) {
    recovered = base_tuple;
  }

  for (auto &log : undo_logs) {
    if (log.is_deleted_) {
      recovered = std::nullopt;
      continue;
    }

    if (!recovered.has_value()) {
      recovered = GenerateNullTupleForSchema(schema);
    }

    const auto partial_schema = GetUndoLogSchema(schema, log);
    const auto &partial_tuple = log.tuple_;

    auto values = std::vector<Value>();
    size_t modified_offset = 0;
    for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
      if (log.modified_fields_[i]) {
        values.emplace_back(partial_tuple.GetValue(&partial_schema, modified_offset));
        ++modified_offset;
      } else {
        values.emplace_back(recovered->GetValue(schema, i));
      }
    }

    recovered = Tuple(values, schema);
  }

  return recovered;
}

auto ReconstructTupleForLogIndex(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                                 const std::vector<UndoLog> &undo_logs, size_t index) -> std::optional<Tuple> {
  // UNIMPLEMENTED("not implemented");
  auto recovered = std::optional<Tuple>();
  if (!base_meta.is_deleted_) {
    recovered = base_tuple;
  }

  size_t i = 0;
  for (auto &log : undo_logs) {
    i++;
    if (log.is_deleted_) {
      recovered = std::nullopt;
      continue;
    }

    if (!recovered.has_value()) {
      recovered = GenerateNullTupleForSchema(schema);
    }

    const auto partial_schema = GetUndoLogSchema(schema, log);
    const auto &partial_tuple = log.tuple_;

    auto values = std::vector<Value>();
    size_t modified_offset = 0;
    for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
      if (log.modified_fields_[i]) {
        values.emplace_back(partial_tuple.GetValue(&partial_schema, modified_offset));
        ++modified_offset;
      } else {
        values.emplace_back(recovered->GetValue(schema, i));
      }
    }

    recovered = Tuple(values, schema);

    if (i >= index) {
      break;
    }
  }

  return recovered;
}

auto CollectUndoLogs(TransactionManager *txn_mgr, UndoLink undo_link, timestamp_t read_ts) -> std::vector<UndoLog> {
  auto logs = std::vector<UndoLog>();
  WalkUndoLogs(txn_mgr, undo_link, [&](const UndoLink &, const UndoLog &log) {
    logs.emplace_back(log);
    return log.ts_ > read_ts;
  });
  return logs;
}

void WalkUndoLogs(TransactionManager *txn_mgr, UndoLink undo_link,
                  const std::function<bool(const UndoLink &, const UndoLog &)> &callback) {
  while (undo_link.IsValid()) {
    auto log = txn_mgr->GetUndoLog(undo_link);
    if (!callback(undo_link, log)) {
      return;
    }

    undo_link = log.prev_version_;
  }

  auto log = UndoLog{
      .is_deleted_ = true,
      .modified_fields_ = {},
      .tuple_ = {},
      .ts_ = 0,
      .prev_version_ = {},
  };
  callback(undo_link, log);
}

void Modify(TransactionManager *txn_mgr, Transaction *txn, const Schema *schema, table_oid_t oid, RID rid,
            const TupleMeta &old_meta, const Tuple &old_tuple, const TupleMeta &new_meta, const Tuple &new_tuple) {
  auto merge_undo_log = [&](const UndoLog &old_undo_log, const UndoLog &new_undo_log) {
    auto modified_fields = std::vector<bool>(old_undo_log.modified_fields_.size(), false);
    auto modified_values = std::vector<Value>();
    auto modified_columns = std::vector<Column>();

    size_t old_modified_offset = 0;
    auto old_modified_schema = GetUndoLogSchema(schema, old_undo_log);
    size_t new_modified_offset = 0;
    auto new_modified_schema = GetUndoLogSchema(schema, new_undo_log);

    for (size_t i = 0; i < modified_fields.size(); ++i) {
      if (old_undo_log.modified_fields_[i]) {
        modified_fields[i] = true;
        modified_values.emplace_back(old_undo_log.tuple_.GetValue(&old_modified_schema, old_modified_offset));
        modified_columns.emplace_back(old_modified_schema.GetColumn(old_modified_offset));
        ++old_modified_offset;
      }

      if (new_undo_log.modified_fields_[i]) {
        if (!old_undo_log.modified_fields_[i]) {
          modified_fields[i] = true;
          modified_values.emplace_back(new_undo_log.tuple_.GetValue(&new_modified_schema, new_modified_offset));
          modified_columns.emplace_back(new_modified_schema.GetColumn(new_modified_offset));
        }
        ++new_modified_offset;
      }
    }

    auto modified_schema = Schema(modified_columns);
    return UndoLog{
        .is_deleted_ = old_undo_log.is_deleted_,
        .modified_fields_ = modified_fields,
        .tuple_ = Tuple(modified_values, &modified_schema),
        .ts_ = old_undo_log.ts_,
        .prev_version_ = old_undo_log.prev_version_,
    };
  };

  if (old_meta.ts_ >= TXN_START_ID && old_meta.ts_ == txn->GetTransactionTempTs()) {
    auto undo_link = txn_mgr->GetUndoLink(rid);
    if (!undo_link) {
      return;
    }

    auto new_undo_log = GenerateDiffLog(schema, old_meta, old_tuple, new_meta, new_tuple);
    if (!new_undo_log) {
      return;
    }

    auto old_undo_log = txn_mgr->GetUndoLog(*undo_link);
    auto merged_undo_log = merge_undo_log(old_undo_log, *new_undo_log);

    txn->ModifyUndoLog(undo_link->prev_log_idx_, std::move(merged_undo_log));
    return;
  }

  auto diff_log = GenerateDiffLog(schema, old_meta, old_tuple, new_meta, new_tuple);
  if (!diff_log) {
    return;
  }

  auto old_undo_link = txn_mgr->GetUndoLink(rid);
  if (old_undo_link) {
    diff_log->prev_version_ = *old_undo_link;
  }

  auto new_undo_link = txn->AppendUndoLog(std::move(*diff_log));
  txn_mgr->UpdateUndoLink(rid, new_undo_link);
  txn->AppendWriteSet(oid, rid);
}

auto IsWriteWriteConflict(const Transaction *txn, const TupleMeta &meta, const Tuple &tuple, bool is_delete) -> bool {
  // case 1
  // If a tuple is being modified by an uncommitted transaction,
  // no other transactions are allowed to modify it
  if (meta.ts_ >= TXN_START_ID) {
    // a tuple is being modified by an uncommitted transaction
    return meta.ts_ != txn->GetTransactionTempTs();
  }
  if (is_delete) {
    return meta.is_deleted_;
  }
  return meta.ts_ > txn->GetReadTs();
}

auto GenerateDiffLog(const Schema *schema, const TupleMeta &old_meta, const Tuple &old_tuple, const TupleMeta &new_meta,
                     const Tuple &new_tuple) -> std::optional<UndoLog> {
  if (old_meta.is_deleted_ == new_meta.is_deleted_ && IsTupleContentEqual(old_tuple, new_tuple)) {
    return std::nullopt;
  }

  if (!old_meta.is_deleted_ && new_meta.is_deleted_) {
    return UndoLog{
        .is_deleted_ = false,
        .modified_fields_ = std::vector<bool>(schema->GetColumnCount(), true),
        .tuple_ = old_tuple,
        .ts_ = old_meta.ts_,
        .prev_version_ = {},
    };
  }

  if (old_meta.is_deleted_ && !new_meta.is_deleted_) {
    return UndoLog{
        .is_deleted_ = true,
        .modified_fields_ = std::vector<bool>(schema->GetColumnCount(), true),
        .tuple_ = old_tuple,
        .ts_ = old_meta.ts_,
        .prev_version_ = {},
    };
  }

  auto modified_fields = std::vector<bool>(schema->GetColumnCount(), false);
  auto modified_values = std::vector<Value>();
  auto modified_columns = std::vector<Column>();
  for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
    auto old_value = old_tuple.GetValue(schema, i);
    auto new_value = new_tuple.GetValue(schema, i);
    if (old_value.CompareExactlyEquals(new_value)) {
      continue;
    }

    modified_fields[i] = true;
    modified_values.emplace_back(old_value);
    modified_columns.emplace_back(schema->GetColumn(i));
  }

  auto modified_schema = Schema(modified_columns);
  return UndoLog{
      .is_deleted_ = false,
      .modified_fields_ = modified_fields,
      .tuple_ = Tuple(modified_values, &modified_schema),
      .ts_ = old_meta.ts_,
      .prev_version_ = {},
  };
}

auto GenerateNullTupleForSchema(const Schema *schema) -> Tuple {
  auto values = std::vector<Value>();
  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    values.emplace_back(ValueFactory::GetNullValueByType(schema->GetColumn(i).GetType()));
  }
  return {values, schema};
}

auto GetUndoLogSchema(const Schema *schema, const UndoLog &log) -> Schema {
  auto columns = std::vector<Column>();
  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    if (log.modified_fields_[i]) {
      columns.emplace_back(schema->GetColumn(i));
    }
  }
  return Schema(columns);
}

auto InsertTupleImpl(TransactionManager *txn_mgr, Transaction *txn, TableInfo *table, IndexInfo *index,
                     Tuple &target_tuple) -> std::optional<RID> {
  auto insert_new_tuple = [&] {
    auto meta = TupleMeta{.ts_ = txn->GetTransactionTempTs(), .is_deleted_ = false};
    auto target_rid = table->table_->InsertTuple(meta, target_tuple, nullptr, txn, table->oid_);
    if (!target_rid) {
      return target_rid;
    }

    txn_mgr->UpdateUndoLink(*target_rid, std::nullopt);
    txn->AppendWriteSet(table->oid_, *target_rid);
    return target_rid;
  };

  if (index == nullptr) {
    auto target_rid = insert_new_tuple();
    if (!target_rid) {
      return std::nullopt;
    }

    return target_rid;
  }

  auto key = target_tuple.KeyFromTuple(table->schema_, index->key_schema_, index->index_->GetKeyAttrs());
  auto results = std::vector<RID>();
  index->index_->ScanKey(key, &results, txn);
  if (results.empty()) {
    auto target_rid = insert_new_tuple();
    if (!target_rid) {
      return std::nullopt;
    }

    auto ok = index->index_->InsertEntry(key, *target_rid, txn);
    if (!ok) {
      txn->SetTainted();
      throw ExecutionException("Insertion failed due to unique key constraint violation");
    }

    return target_rid;
  }

  auto [old_meta, old_tuple] = table->table_->GetTuple(results[0]);
  if (!old_meta.is_deleted_) {
    txn->SetTainted();
    throw ExecutionException("Insertion failed due to unique key constraint violation");
  }

  auto new_meta = TupleMeta{.ts_ = txn->GetTransactionTempTs(), .is_deleted_ = false};

  table->table_->UpdateTupleInPlace(
      new_meta, target_tuple, results[0],
      [&](const bustub::TupleMeta &old_meta, const bustub::Tuple &old_tuple, bustub::RID rid) {
        if (IsWriteWriteConflict(txn, old_meta, old_tuple, false)) {
          txn->SetTainted();
          throw ExecutionException("Insertion failed due to write-write conflict");
        }

        Modify(txn_mgr, txn, &table->schema_, table->oid_, results[0], old_meta, old_tuple, new_meta, target_tuple);

        return true;
      });

  return results[0];
}

void DeleteTupleImpl(TransactionManager *txn_mgr, Transaction *txn, TableInfo *table, const RID &rid,
                     const TupleMeta &old_meta, const Tuple &old_tuple) {
  auto new_tuple = GenerateNullTupleForSchema(&table->schema_);
  auto new_meta = old_meta;
  new_meta.ts_ = txn->GetTransactionTempTs();
  new_meta.is_deleted_ = true;

  table->table_->UpdateTupleInPlace(
      new_meta, new_tuple, rid,
      [&](const bustub::TupleMeta &old_meta, const bustub::Tuple &old_tuple, bustub::RID rid) {
        if (IsWriteWriteConflict(txn, old_meta, old_tuple, true)) {
          txn->SetTainted();
          throw ExecutionException("Insertion failed due to write-write conflict");
        }

        Modify(txn_mgr, txn, &table->schema_, table->oid_, rid, old_meta, old_tuple, new_meta, new_tuple);

        return true;
      });
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
  //     tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    auto [meta, value] = iter.GetTuple();
    iter.operator++();

    fmt::print(stderr, "RID={}/{} ", value.GetRid().GetPageId(), value.GetRid().GetSlotNum());
    if (meta.ts_ >= TXN_START_ID) {
      fmt::print(stderr, "ts=txn{} ", meta.ts_ ^ TXN_START_ID);
    } else {
      fmt::print(stderr, "ts={} ", meta.ts_);
    }

    if (meta.is_deleted_) {
      fmt::print(stderr, "<del marker> ");
    }
    fmt::println(stderr, "tuple={}", value.ToString(&table_info->schema_));

    auto undo_link = txn_mgr->GetUndoLink(value.GetRid());
    if (!undo_link.has_value()) {
      continue;
    }

    WalkUndoLogs(txn_mgr, undo_link.value(), [&](const UndoLink &undo_link, const UndoLog &log) {
      if (!undo_link.IsValid()) {
        return false;
      }

      if (undo_link.prev_txn_ >= TXN_START_ID) {
        fmt::print(stderr, "  txn{}@{} ", undo_link.prev_txn_ ^ TXN_START_ID, undo_link.prev_log_idx_);
      } else {
        fmt::print(stderr, "  txn{}@{} ", undo_link.prev_txn_, undo_link.prev_log_idx_);
      }

      if (log.is_deleted_) {
        fmt::print(stderr, "<del> ");
      } else {
        const auto partial_schema = GetUndoLogSchema(&table_info->schema_, log);
        const auto &partial_tuple = log.tuple_;

        fmt::print(stderr, "(");

        size_t modified_offset = 0;
        for (size_t i = 0; i < table_info->schema_.GetColumnCount(); ++i) {
          if (log.modified_fields_[i]) {
            auto value = partial_tuple.GetValue(&partial_schema, modified_offset);
            ++modified_offset;

            if (value.IsNull()) {
              fmt::print(stderr, "<NULL>");
            } else {
              fmt::print(stderr, "{}", value.ToString());
            }
          } else {
            fmt::print(stderr, "_");
          }

          if (i != table_info->schema_.GetColumnCount() - 1) {
            fmt::print(stderr, ", ");
          }
        }

        fmt::print(stderr, ") ");
      }
      fmt::println(stderr, "ts={}", log.ts_);

      return true;
    });
  }
}

}  // namespace bustub
