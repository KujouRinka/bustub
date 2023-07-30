//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetLSN();
  // set all the pairs to invalid
  for (int i = 0; i < max_size; i++) {
    array_[i].first = KeyType();
    array_[i].second = INVALID_PAGE_ID;
  }
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index + 1].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index + 1].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index + 1].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyForSearch(const KeyType &key) -> MappingType {
  return MappingType(key, INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LowerBoundOfKey(const KeyType &key, const KeyComparator &comparator) -> int {
  auto begin = array_ + 1;
  auto end = array_ + 1 + GetSize();
  auto it = std::lower_bound(begin, end, KeyForSearch(key), [comparator](const MappingType &a, const MappingType &b) {
    return comparator(a.first, b.first) < 0;
  });
  return std::distance(begin, it);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpperBoundOfKey(const KeyType &key, const KeyComparator &comparator) -> int {
  auto begin = array_ + 1;
  auto end = array_ + 1 + GetSize();
  auto it = std::upper_bound(begin, end, KeyForSearch(key), [comparator](const MappingType &a, const MappingType &b) {
    return comparator(a.first, b.first) < 0;
  });
  return std::distance(begin, it);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                            const KeyComparator &comparator) {
  UNIMPLEMENTED("BPlusTreeInternalPage::Insert() is not implemented.");
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) {
  UNIMPLEMENTED("BPlusTreeInternalPage::Remove() is not implemented.");
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
