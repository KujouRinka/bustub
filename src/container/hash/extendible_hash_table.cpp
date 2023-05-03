//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  assert(bucket_size_ > 0);
  dir_.push_back(std::make_shared<Bucket>(bucket_size_, 0, global_depth_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  if (static_cast<size_t>(dir_index) >= dir_.size()) {
    return -1;
  }
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto dir_idx = IndexOf(key);
  auto bucket_p = dir_[dir_idx];
  return bucket_p->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto dir_idx = IndexOf(key);
  auto bucket_p = dir_[dir_idx];
  return bucket_p->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  while (true) {
    auto dir_idx = IndexOf(key);
    auto bucket_p = dir_[dir_idx];
    // do insert
    // if failed, do split
    // if failed, do expand
    if (bucket_p->Insert(key, value)) {
      break;
    }
    if (SplitBucket(bucket_p)) {
      continue;
    }
    if (!ExpandDirs()) {
      UNREACHABLE("ExpandDirs failed\n");
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::SplitBucket(std::shared_ptr<Bucket> bucket) -> bool {
  if (bucket->GetDepth() == global_depth_) {
    return false;
  }
  // TODO(rinka): performance optimization
  // collect idx point to bucket.
  auto bucket_depth = bucket->GetDepth();
  auto bucket0 = std::make_shared<Bucket>(bucket_size_, bucket->GetSelfHash(), bucket_depth + 1);
  auto bucket1 =
      std::make_shared<Bucket>(bucket_size_, (1 << (bucket_depth)) | bucket->GetSelfHash(), bucket_depth + 1);
  for (size_t i = 0; i < (1U << (global_depth_ - bucket_depth)); ++i) {
    auto idx = (i << bucket_depth) | bucket->GetSelfHash();
    dir_[idx] = (i & 1) == 0U ? bucket0 : bucket1;
  }
  ++num_buckets_;
  // redistribute
  for (auto &item : bucket->GetItems()) {
    auto idx = IndexOf(item.first);
    assert(dir_[idx]->Insert(item.first, item.second));
  }
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::ExpandDirs() -> bool {
  if (global_depth_ == sizeof(size_t) * 8) {
    return false;
  }
  // TODO(rinka): performance optimization
  std::vector<std::shared_ptr<Bucket>> new_dir(2 << global_depth_);
  for (size_t i = 0; i < new_dir.size(); ++i) {
    auto idx = i & ((1 << global_depth_) - 1);
    new_dir[i] = dir_[idx];
  }
  dir_ = std::move(new_dir);
  ++global_depth_;
  return true;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, size_t self_hash, int depth)
    : size_(array_size), self_hash_(self_hash), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto entry = std::find_if(list_.begin(), list_.end(), [&key](const auto &pair) { return pair.first == key; });
  if (entry == list_.end()) {
    return false;
  }
  value = entry->second;
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto entry = std::find_if(list_.begin(), list_.end(), [&key](const auto &pair) { return pair.first == key; });
  if (entry == list_.end()) {
    return false;
  }
  list_.erase(entry);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto entry = std::find_if(list_.begin(), list_.end(), [&key](const auto &pair) { return pair.first == key; });
  if (entry != list_.end()) {
    // do update
    entry->second = value;
  } else if (!IsFull()) {
    // do insert
    list_.emplace_back(key, value);
  } else {
    // full
    return false;
  }
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::GetSelfHash() const -> size_t {
  return self_hash_;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
