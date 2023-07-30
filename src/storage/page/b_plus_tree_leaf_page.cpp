//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetLSN();
  // set all pairs to invalid
  for (int i = 0; i < max_size; ++i) {
    array_[i].first = KeyType{};
    array_[i].second = ValueType{};
  }
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyForSearch(const KeyType &key) -> MappingType {
  return MappingType{key, ValueType{}};
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LowerBoundOfKey(const KeyType &key, const KeyComparator &comparator) -> int {
  auto begin = array_;
  auto end = array_ + GetSize();
  auto it = std::lower_bound(begin, end, KeyForSearch(key), [comparator](const auto &lhs, const auto &rhs) {
    return comparator(lhs.first, rhs.first) < 0;
  });
  return std::distance(begin, it);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::UpperBoundOfKey(const KeyType &key, const KeyComparator &comparator) -> int {
  auto begin = array_;
  auto end = array_ + GetSize();
  auto it = std::upper_bound(begin, end, KeyForSearch(key), [comparator](const auto &lhs, const auto &rhs) {
    return comparator(lhs.first, rhs.first) < 0;
  });
  return std::distance(begin, it);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::EqualRangeOfKey(const KeyType &key, const KeyComparator &comparator)
    -> std::vector<ValueType> {
  auto begin = array_;
  auto end = array_ + GetSize();
  auto it = std::equal_range(begin, end, KeyForSearch(key), [comparator](const auto &lhs, const auto &rhs) {
    return comparator(lhs.first, rhs.first) < 0;
  });
  std::vector<ValueType> result;
  result.reserve(std::distance(it.first, it.second));
  std::for_each(it.first, it.second, [&result](const auto &mapping) { result.push_back(mapping.second); });
  return std::move(result);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> bool {
  return false;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
