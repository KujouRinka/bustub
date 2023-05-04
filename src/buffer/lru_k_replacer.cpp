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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return EvictInternal(frame_id);
}

auto LRUKReplacer::EvictInternal(frame_id_t *frame_id) -> bool {
  if (evictable_cnt_ == 0) {
    *frame_id = -1;
    return false;
  }
  for (auto it = history_list_.begin(); it != history_list_.end(); ++it) {
    if (it->evictable_) {
      *frame_id = it->frame_id_;
      rec_map_.erase(it->frame_id_);
      history_list_.erase(it);
      --evictable_cnt_;
      return true;
    }
  }
  for (auto it = buffer_list_.begin(); it != buffer_list_.end(); ++it) {
    if (it->evictable_) {
      *frame_id = it->frame_id_;
      rec_map_.erase(it->frame_id_);
      buffer_list_.erase(it);
      --evictable_cnt_;
      return true;
    }
  }
  UNIMPLEMENTED("evict");
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  assert(static_cast<size_t>(frame_id) < replacer_size_);
  auto it = rec_map_.find(frame_id);
  if (it != rec_map_.end()) {
    // already memo
    auto &rec = it->second;
    if (rec->visit_cnt_ < k_) {
      // in history_list_, add cnt and check whether moves to buffer_list_
      // history_list_ used FIFO
      if (++rec->visit_cnt_ == k_) {
        buffer_list_.push_back(*rec);
        history_list_.erase(rec);
        it->second = prev(buffer_list_.end());
      }
    } else {
      // in buffer_list_, move it to head
      // buffer_list_ uses LRU
      buffer_list_.push_back(*rec);
      buffer_list_.erase(rec);
      it->second = prev(buffer_list_.end());
    }
    return;
  }
  // newcomer
  if (buffer_list_.size() + history_list_.size() == replacer_size_) {
    // frame_id_t id;
    // BUSTUB_ASSERT(EvictInternal(&id), "cannot evict");
  }
  auto to_add = FrameRec{frame_id, 1, false};
  history_list_.push_back(to_add);
  rec_map_.emplace(frame_id, prev(history_list_.end()));
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto it = rec_map_.find(frame_id);
  if (it == rec_map_.end()) {
    // not found
    return;
  }
  auto &rec = it->second;
  if ((set_evictable ^ rec->evictable_) == 0) {
    return;
  }
  rec->evictable_ = set_evictable;
  if (set_evictable) {
    ++evictable_cnt_;
  } else {
    --evictable_cnt_;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto it = rec_map_.find(frame_id);
  if (it == rec_map_.end()) {
    return;
  }
  auto &rec = it->second;
  assert(rec->evictable_);
  if (rec->visit_cnt_ < k_) {
    // in history_list_
    history_list_.erase(rec);
  } else {
    // in buffer_list_
    buffer_list_.erase(rec);
  }
  rec_map_.erase(it);
  --evictable_cnt_;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return evictable_cnt_;
}

}  // namespace bustub
