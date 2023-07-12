//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  return NewPgImpInternal(page_id);
}

auto BufferPoolManagerInstance::NewPgImpInternal(page_id_t *page_id) -> Page * {
  auto new_fid = AllocFrameIdInternal();
  if (new_fid == -1) {
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }

  replacer_->RecordAccess(new_fid);
  replacer_->SetEvictable(new_fid, false);
  auto new_pid = AllocatePage();
  page_table_->Insert(new_pid, new_fid);

  pages_[new_fid].WLatch();
  pages_[new_fid].page_id_ = new_pid;
  pages_[new_fid].pin_count_ = 1;
  pages_[new_fid].is_dirty_ = false;
  pages_[new_fid].WUnlatch();

  *page_id = new_pid;

  return &pages_[new_fid];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id_t fid; page_table_->Find(page_id, fid)) {
    pages_[fid].WLatch();
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    ++pages_[fid].pin_count_;
    pages_[fid].WUnlatch();
    return &pages_[fid];
  }
  // not found in page_table_, need to fetch from disk
  auto new_fid = AllocFrameIdInternal();
  if (new_fid == -1) {
    return nullptr;
  }

  replacer_->RecordAccess(new_fid);
  replacer_->SetEvictable(new_fid, false);
  page_table_->Insert(page_id, new_fid);

  pages_[new_fid].WLatch();
  pages_[new_fid].page_id_ = page_id;
  pages_[new_fid].pin_count_ = 1;
  pages_[new_fid].is_dirty_ = false;
  disk_manager_->ReadPage(page_id, pages_[new_fid].GetData());
  pages_[new_fid].WUnlatch();

  return &pages_[new_fid];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  auto find_res = page_table_->Find(page_id, fid);
  pages_[fid].WLatch();
  if (!find_res || pages_[fid].GetPinCount() == 0) {
    pages_[fid].WUnlatch();
    return false;
  }
  if (--pages_[fid].pin_count_ == 0) {
    replacer_->SetEvictable(fid, true);
  }
  pages_[fid].is_dirty_ = pages_[fid].is_dirty_ || is_dirty;
  pages_[fid].WUnlatch();

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return FlushPgImpInternal(page_id);
}

auto BufferPoolManagerInstance::FlushPgImpInternal(page_id_t page_id) -> bool {
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "page_id cannot be INVALID_PAGE_ID");
  frame_id_t fid;
  if (!page_table_->Find(page_id, fid)) {
    return false;
  }
  pages_[fid].WLatch();
  disk_manager_->WritePage(page_id, pages_[fid].GetData());
  pages_[fid].is_dirty_ = false;
  pages_[fid].WUnlatch();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    pages_[i].WLatch();
    if (pages_[i].GetPageId() != INVALID_PAGE_ID && pages_[i].IsDirty()) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
    pages_[i].WUnlatch();
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (!page_table_->Find(page_id, fid)) {
    return true;
  }

  pages_[fid].WLatch();
  if (pages_[fid].GetPinCount() > 0) {
    pages_[fid].WUnlatch();
    return false;
  }
  replacer_->Remove(fid);
  if (pages_[fid].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[fid].GetData());
  }
  page_table_->Remove(page_id);
  pages_[fid].page_id_ = INVALID_PAGE_ID;
  pages_[fid].pin_count_ = 0;
  pages_[fid].is_dirty_ = false;
  pages_[fid].WUnlatch();

  free_list_.push_back(fid);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::AllocFrameIdInternal() -> frame_id_t {
  if (free_list_.empty() && replacer_->Size() == 0) {
    return -1;
  }
  frame_id_t new_fid;
  if (free_list_.empty()) {
    // evict a page from replacer_
    BUSTUB_ASSERT(replacer_->Evict(&new_fid), "replacer should evict successfully");
    pages_[new_fid].WLatch();
    auto evict_pid = pages_[new_fid].GetPageId();
    if (pages_[new_fid].IsDirty()) {
      BUSTUB_ASSERT(evict_pid != INVALID_PAGE_ID, "page_id cannot be INVALID_PAGE_ID");
      frame_id_t fid;
      page_table_->Find(evict_pid, fid);
      disk_manager_->WritePage(evict_pid, pages_[fid].GetData());
      pages_[fid].is_dirty_ = false;
    }
    pages_[new_fid].WUnlatch();
    BUSTUB_ASSERT(page_table_->Remove(evict_pid), "remove evict_pid from page_table should success");
  } else {
    new_fid = free_list_.back();
    free_list_.pop_back();
  }
  return new_fid;
}

}  // namespace bustub
