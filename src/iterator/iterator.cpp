#include "../../include/iterator/iterator.h"
#include <iostream>
#include <memory>
#include <tuple>
#include <vector>

namespace tiny_lsm {

// *************************** SearchItem ***************************
bool operator<(const SearchItem &a, const SearchItem &b) {
  // TODO: Lab2.2 实现比较规则
  if (a.key_ != b.key_) {
    return a.key_ < b.key_;
  }
  if (a.tranc_id_ != b.tranc_id_) {
    return a.tranc_id_ > b.tranc_id_;
  }
  if (a.level_ < b.level_) {
    return true;
  }
  return a.idx_ < b.idx_;
}

bool operator>(const SearchItem &a, const SearchItem &b) {
  // TODO: Lab2.2 实现比较规则
  return b < a;
}

bool operator==(const SearchItem &a, const SearchItem &b) {
  // TODO: Lab2.2 实现比较规则
  return a.idx_ == b.idx_ && a.key_ == b.key_;
}

// *************************** HeapIterator ***************************
HeapIterator::HeapIterator(std::vector<SearchItem> item_vec,
                           uint64_t max_tranc_id, bool filter_empty)
    : max_tranc_id_(max_tranc_id), filter_empty_(filter_empty) {
  for (auto &item : item_vec) {
    items.push(std::move(item));
  }
  skip_by_tranc_id();
}

HeapIterator::pointer HeapIterator::operator->() const {
  if (!current_item_.has_value()) {
    return nullptr;
  }
  update_current();
  return cached_value_.has_value() ? &cached_value_.value() : nullptr;
}

HeapIterator::value_type HeapIterator::operator*() const {
  if (!current_item_.has_value()) {
    return value_type{};
  }
  update_current();
  return cached_value_.value();
}

BaseIterator &HeapIterator::operator++() {
  skip_by_tranc_id();
  return *this;
}

bool HeapIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab2.2 实现 == 重载
  // 比较当前迭代器的值与other的值是否相等
  const HeapIterator *other_heap = dynamic_cast<const HeapIterator *>(&other);
  if (!other_heap)
    return false;
  if (!current_item_.has_value() && !other_heap->current_item_.has_value())
    return true;
  if (!current_item_.has_value() || !other_heap->current_item_.has_value())
    return false;
  return current_item_->key_ == other_heap->current_item_->key_ &&
         current_item_->value_ == other_heap->current_item_->value_ &&
         current_item_->tranc_id_ == other_heap->current_item_->tranc_id_;
}

bool HeapIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab2.2 实现 != 重载
  const HeapIterator *other_heap = dynamic_cast<const HeapIterator *>(&other);
  if (!other_heap)
    return true;
  if (!current_item_.has_value() && !other_heap->current_item_.has_value())
    return false;
  if (!current_item_.has_value() || !other_heap->current_item_.has_value())
    return true;
  return current_item_->key_ != other_heap->current_item_->key_ ||
         current_item_->value_ != other_heap->current_item_->value_ ||
         current_item_->tranc_id_ != other_heap->current_item_->tranc_id_;
}

bool HeapIterator::top_value_legal() const { return current_item_.has_value(); }

void HeapIterator::skip_by_tranc_id() { advance_to_next(); }

bool HeapIterator::is_end() const { return !current_item_.has_value(); }
bool HeapIterator::is_valid() const { return current_item_.has_value(); }

void HeapIterator::update_current() const {
  // current 缓存了当前键值对的值, 你实现 -> 重载时可能需要
  // TODO: Lab2.2 更新当前缓存值
  if (!current_item_.has_value()) {
    cached_value_.reset();
    return;
  }
  if (!cached_value_.has_value()) {
    cached_value_.emplace(current_item_->key_, current_item_->value_);
  }
}

bool HeapIterator::advance_to_next() {
  cached_value_.reset();
  current_item_.reset();

  while (!items.empty()) {
    std::vector<SearchItem> group;
    group.reserve(4);

    SearchItem top = items.top();
    items.pop();
    std::string current_key = top.key_;
    group.emplace_back(std::move(top));

    while (!items.empty() && items.top().key_ == current_key) {
      group.emplace_back(items.top());
      items.pop();
    }

    auto selected = select_visible_version(group);
    if (!selected.has_value()) {
      continue;
    }

    current_item_.emplace(selected.value());
    return true;
  }
  return false;
}

std::optional<SearchItem> HeapIterator::select_visible_version(
    const std::vector<SearchItem> &candidates) const {
  for (const auto &item : candidates) {
    if (max_tranc_id_ != 0 && item.tranc_id_ > max_tranc_id_) {
      continue;
    }
    if (filter_empty_ && item.value_.empty()) {
      return std::nullopt; // 可见的删除标记意味着整个 key 被移除
    }
    return item;
  }
  return std::nullopt;
}

IteratorType HeapIterator::get_type() const {
  return IteratorType::HeapIterator;
}

uint64_t HeapIterator::get_tranc_id() const { return max_tranc_id_; }
} // namespace tiny_lsm
