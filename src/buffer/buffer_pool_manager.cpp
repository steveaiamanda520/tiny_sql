//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  auto guard = std::lock_guard(latch_);

  frame_id_t frame_id = AllocateFrame();
  if (frame_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  *page_id = AllocatePage();
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_[*page_id] = frame_id;

  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  auto guard = std::lock_guard(latch_);

  if (auto iter = page_table_.find(page_id); iter != page_table_.end()) {
    auto frame_id = iter->second;

    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id, access_type);
    replacer_->SetEvictable(frame_id, false);

    return &pages_[frame_id];
  }

  frame_id_t frame_id = AllocateFrame();
  if (frame_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  ReadFrame(frame_id, page_id);
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, false);
  page_table_[page_id] = frame_id;

  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  auto guard = std::lock_guard(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }

  --pages_[frame_id].pin_count_;
  pages_[frame_id].is_dirty_ |= is_dirty;

  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  auto guard = std::lock_guard(latch_);

  BUSTUB_ENSURE(page_id != INVALID_PAGE_ID, "Page id is ensured to be valid.");  // NOLINT

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  auto frame_id = page_table_[page_id];
  WriteFrame(frame_id, page_id);

  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  auto guard = std::lock_guard(latch_);

  for (const auto &[page_id, frame_id] : page_table_) {
    WriteFrame(frame_id, page_id);
    pages_[frame_id].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  auto guard = std::lock_guard(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }

  page_table_.erase(page_id);
  free_list_.emplace_back(frame_id);
  replacer_->Remove(frame_id);

  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

auto BufferPoolManager::AllocateFrame() -> frame_id_t {
  frame_id_t frame_id = INVALID_PAGE_ID;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    if (pages_[frame_id].IsDirty()) {
      WriteFrame(frame_id, pages_[frame_id].GetPageId());
    }

    page_table_.erase(pages_[frame_id].GetPageId());
  }

  return frame_id;
}

void BufferPoolManager::ReadFrame(frame_id_t frame_id, page_id_t page_id) {
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  auto request = DiskRequest{
      .is_write_ = false,
      .data_ = pages_[frame_id].GetData(),
      .page_id_ = page_id,
      .callback_ = std::move(promise),
  };

  disk_scheduler_->Schedule(std::move(request));
  future.get();
}

void BufferPoolManager::WriteFrame(frame_id_t frame_id, page_id_t page_id) {
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  auto request = DiskRequest{
      .is_write_ = true,
      .data_ = pages_[frame_id].GetData(),
      .page_id_ = page_id,
      .callback_ = std::move(promise),
  };

  disk_scheduler_->Schedule(std::move(request));
  future.get();
}

}  // namespace bustub
