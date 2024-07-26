//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = running_txns_.commit_ts_;

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
  //当前事务是只读事务，直接返回true
  if (txn->GetWriteSets().empty()) {
    return true;
  }
  for (auto &[tran_id, ctxn] : txn_map_) {
    // 不是已提交的事务，跳过
    if (ctxn->state_ != TransactionState::COMMITTED) {
      continue;
    }

    // 事务的提交时间戳小于等于当前事务的读时间戳，跳过
    if (ctxn->GetCommitTs() <= txn->GetReadTs()) {
      continue;
    }

    // 事务是只读事务，跳过
    if (ctxn->GetWriteSets().empty()) {
      continue;
    }

    for (const auto &[table_oid, rids] : ctxn->GetWriteSets()) {
      auto table_info = catalog_->GetTable(table_oid);

      // 当前表上没有扫描谓词，跳过
      auto iter = txn->GetScanPredicates().find(table_oid);
      if (iter == txn->GetScanPredicates().cend()) {
        continue;
      }

      const auto &filter_exprs = iter->second;
      for (auto rid : rids) {
        // 获取rid对应的元数据和元组
        auto rpage_guard = table_info->table_->AcquireTablePageReadLock(rid);
        auto rpage = rpage_guard.As<TablePage>();
        auto [meta, tuple] = table_info->table_->GetTupleWithLockAcquired(rid, rpage);

        std::vector<Tuple> w_tuples;

        // 元数据的时间戳不等于事务的提交时间戳，说明数据已经在事务提交后被改动
        if (meta.ts_ != ctxn->GetCommitTs()) {
          auto undo_link_opt = GetUndoLink(rid);
          UndoLink undo_link = undo_link_opt.value();
          std::vector<UndoLog> undo_logs;
          while (undo_link.IsValid()) {
            auto undo_log = GetUndoLog(undo_link);
            undo_logs.emplace_back(undo_log);
            if (undo_log.ts_ == ctxn->GetCommitTs()) {  // 获取事务提交时和提交后的所有undo_log
              break;
            }
            undo_link = undo_log.prev_version_;
          }

          auto origin_tuple = ReconstructTuple(&table_info->schema_, tuple, meta, undo_logs);
          auto undo_log = GetUndoLog(undo_link);
          undo_link = undo_log.prev_version_;
          if (origin_tuple == std::nullopt) {  //处理元组新插入的场景
            auto undo_log = GetUndoLog(undo_link);
            undo_logs.emplace_back(undo_log);
            origin_tuple = ReconstructTuple(&table_info->schema_, tuple, meta, undo_logs);
            w_tuples.emplace_back(*origin_tuple);
          } else {  //处理元组修改和删除的场景
            w_tuples.emplace_back(*origin_tuple);
            if (undo_link.IsValid()) {
              auto undo_log = GetUndoLog(undo_link);
              undo_logs.emplace_back(undo_log);
              origin_tuple = ReconstructTuple(&table_info->schema_, tuple, meta, undo_logs);
              if (origin_tuple.has_value()) {
                w_tuples.emplace_back(*origin_tuple);
              }
            }
          }
        } else {
          if (!meta.is_deleted_) {  //当前元组还没被删除，添加到改动元组集
            w_tuples.emplace_back(tuple);
          }

          //获取事务提交时的元组，添加到改动元组集
          auto undo_link_opt = GetUndoLink(rid);
          if (undo_link_opt.has_value()) {
            auto undo_log = GetUndoLog(*undo_link_opt);
            auto origin_tuple = ReconstructTuple(&table_info->schema_, tuple, meta, {undo_log});
            if (origin_tuple.has_value()) {
              w_tuples.emplace_back(*origin_tuple);
            }
          }
        }

        // 判断改动元组是否满足扫描谓词
        for (const auto &tuple : w_tuples) {
          for (const auto &expr : filter_exprs) {
            auto value = expr->Evaluate(&tuple, table_info->schema_);
            if (!value.IsNull() && value.GetAs<bool>()) {
              return false;
            }
          }
        }
      }
    }
  }
  return true;
}

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  auto commit_ts = ++last_commit_ts_;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  for (const auto &[table_oid, rids] : txn->write_set_) {
    auto table = catalog_->GetTable(table_oid);
    for (const auto &rid : rids) {
      auto [meta, _] = table->table_->GetTuple(rid);
      meta.ts_ = commit_ts;
      table->table_->UpdateTupleMeta(meta, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_ = commit_ts;

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->GetTransactionId() >= TXN_START_ID) {
    fmt::println(stderr, "000 Abort begin txnid {} ", txn->GetTransactionId() ^ TXN_START_ID);
  } else {
    fmt::println(stderr, "001 Abort begin txnid {} ", txn->GetTransactionId());
  }
  fmt::println(stderr, "000a");
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  size_t i = 0;
  size_t j = 0;
  // TODO(fall2023): Implement the abort logic!
  for (const auto &[table_oid, rids] : txn->write_set_) {
    fmt::println(stderr, "000a 001 {}", i++);
    auto table = catalog_->GetTable(table_oid);
    for (const auto &rid : rids) {
      fmt::println(stderr, "000a 002 {}", j++);
      auto [new_meta, new_tuple] = table->table_->GetTuple(rid);

      auto undo_link = GetUndoLink(rid);
      auto undo_log = UndoLog{
          .is_deleted_ = true,
          .modified_fields_ = {},
          .tuple_ = {},
          .ts_ = 0,
          .prev_version_ = {},
      };
      fmt::println(stderr, "000a 003 {}", j++);
      if (undo_link) {
        fmt::println(stderr, "000a 004 {}", j++);
        undo_log = GetUndoLog(*undo_link);
      }
      fmt::println(stderr, "000a 005 {}", j++);
      auto old_tuple = ReconstructTuple(&table->schema_, new_tuple, new_meta, std::vector<UndoLog>({undo_log}));
      auto old_meta = TupleMeta{
          .ts_ = undo_log.ts_,
          .is_deleted_ = !old_tuple.has_value(),
      };
      fmt::println(stderr, "000a 006 {}", j++);
      if (old_tuple) {
        fmt::println(stderr, "000a 007 {}", j++);
        table->table_->UpdateTupleInPlace(old_meta, *old_tuple, rid);
      } else {
        fmt::println(stderr, "000a 008 {}", j++);
        table->table_->UpdateTupleInPlace(old_meta, GenerateNullTupleForSchema(&table->schema_), rid);
      }
    }
  }
  fmt::println(stderr, "000a 009");
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);

  fmt::println(stderr, "000a 00a");
}

void TransactionManager::GarbageCollection() {
  // UNIMPLEMENTED("not implemented");
  auto watermark = GetWatermark();
  auto table_names = catalog_->GetTableNames();

  auto txn_ref_map = std::unordered_map<txn_id_t, bool>();
  auto lck = std::unique_lock<std::shared_mutex>(txn_map_mutex_);
  for (const auto &[txn_id, _] : txn_map_) {
    txn_ref_map[txn_id] = false;
  }

  for (const auto &table_name : table_names) {
    auto table = catalog_->GetTable(table_name);
    auto iter = table->table_->MakeIterator();
    while (!iter.IsEnd()) {
      auto [meta, value] = iter.GetTuple();
      iter.operator++();

      if (meta.ts_ <= watermark) {
        UpdateUndoLink(value.GetRid(), std::nullopt);
        continue;
      }

      auto undo_link = GetUndoLink(value.GetRid());
      if (!undo_link) {
        continue;
      }

      while (undo_link->IsValid()) {
        auto iter = txn_map_.find(undo_link->prev_txn_);
        if (iter == txn_map_.end()) {
          break;
        }

        txn_ref_map[undo_link->prev_txn_] = true;
        auto undo_log = iter->second->GetUndoLog(undo_link->prev_log_idx_);
        if (undo_log.ts_ <= watermark) {
          auto new_undo_log = undo_log;
          new_undo_log.prev_version_ = {};
          iter->second->ModifyUndoLog(undo_link->prev_log_idx_, new_undo_log);
          break;
        }

        undo_link = undo_log.prev_version_;
      }
    }
  }

  auto txn_should_remove = [&](txn_id_t txn_id, const std::shared_ptr<bustub::Transaction> &txn) {
    auto is_commited_or_aborted =
        txn->state_ == TransactionState::COMMITTED || txn->state_ == TransactionState::ABORTED;
    auto is_not_referenced = !txn_ref_map[txn_id];

    return is_commited_or_aborted && is_not_referenced;
  };

  for (auto iter = txn_map_.begin(); iter != txn_map_.end();) {
    const auto &[txn_id, txn] = *iter;
    if (txn_should_remove(txn_id, txn)) {
      iter = txn_map_.erase(iter);
    } else {
      ++iter;
    }
  }
}

}  // namespace bustub
