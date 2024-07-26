#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!
  if (current_reads_.find(read_ts) == current_reads_.end()) {
    current_reads_[read_ts] = 0;
  }
  ++current_reads_[read_ts];
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  auto iter = current_reads_.find(read_ts);
  if (iter == current_reads_.end()) {
    return;
  }
  --iter->second;
  if (iter->second == 0) {
    current_reads_.erase(iter);
  }
}

}  // namespace bustub
