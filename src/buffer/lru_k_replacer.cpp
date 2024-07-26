//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

#include <algorithm>

#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  auto guard = std::lock_guard(latch_);

  auto evictables = std::vector<std::tuple<frame_id_t, size_t, size_t>>{};
  for (const auto &[fid, node] : node_store_) {
    if (!node.is_evictable_) {
      continue;
    }

    if (node.history_.size() < k_) {
      evictables.emplace_back(fid, std::numeric_limits<size_t>::max(), node.history_.front());
    } else {
      evictables.emplace_back(fid, current_timestamp_ - node.history_.front(), node.history_.front());
    }
  }

  if (evictables.empty()) {
    return false;
  }

  std::sort(evictables.begin(), evictables.end(), [](const auto &lhs, const auto &rhs) {
    if (std::get<1>(lhs) == std::get<1>(rhs)) {
      return std::get<2>(lhs) < std::get<2>(rhs);
    }
    return std::get<1>(lhs) > std::get<1>(rhs);
  });

  *frame_id = std::get<0>(evictables.front());
  auto &node = node_store_[*frame_id];

  node.history_.clear();
  node.is_evictable_ = false;
  node_store_.erase(*frame_id);
  --curr_size_;

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  auto guard = std::lock_guard(latch_);

  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception("Frame ID out of range");
  }

  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_[frame_id] = LRUKNode{};
  }

  auto &node = node_store_[frame_id];
  if (node.history_.size() == k_) {
    node.history_.pop_front();
  }

  node.history_.push_back(current_timestamp_);
  ++current_timestamp_;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  auto guard = std::lock_guard(latch_);

  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception("Frame ID out of range");
  }

  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }

  auto &node = node_store_[frame_id];
  if (node.is_evictable_ == set_evictable) {
    return;
  }

  node.is_evictable_ = set_evictable;
  if (set_evictable) {
    ++curr_size_;
  } else {
    --curr_size_;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  auto guard = std::lock_guard(latch_);

  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception("Frame ID out of range");
  }

  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }

  auto &node = node_store_[frame_id];
  if (!node.is_evictable_) {
    throw Exception("Frame is not evictable");
  }

  node.history_.clear();
  node.is_evictable_ = false;
  node_store_.erase(frame_id);
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
