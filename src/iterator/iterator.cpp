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
  // TODO: Lab2.2 实现 HeapIterator 构造函数
  for (auto &item : item_vec) {
    items.push(std::move(item));
  }
  current.reset();
  while (!items.empty()) {
    auto top = items.top();
    items.pop();
    while (!items.empty() && top.key_ == items.top().key_) {
      items.pop();
    }
    if (!filter_empty_ || !top.value_.empty()) {
      current = std::make_shared<value_type>(std::move(top.key_),
                                             std::move(top.value_));
      break;
    }
  }
}

HeapIterator::pointer HeapIterator::operator->() const {
  // TODO: Lab2.2 实现 -> 重载
  return current.get(); //用法：iter->first 或 iter->second，等价于(*iter).first
}

HeapIterator::value_type HeapIterator::operator*() const {
  // TODO: Lab2.2 实现 * 重载
  return *current; //用法：auto kv = *iter
}

BaseIterator &HeapIterator::operator++() {
  // TODO: Lab2.2 实现 ++ 重载
  current.reset();
  while (!items.empty()) {
    auto top = items.top();
    items.pop();
    while (!items.empty() && top.key_ == items.top().key_) {
      items.pop();
    }
    if (filter_empty_ && top.value_.empty()) {
      continue;
    }
    current = std::make_shared<value_type>(std::move(top.key_),
                                           std::move(top.value_));
    break;
  }
  return *this;
}

bool HeapIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab2.2 实现 == 重载
  // 比较当前迭代器的值与other的值是否相等
  const HeapIterator *other_heap = dynamic_cast<const HeapIterator *>(&other);
  if (!other_heap)
    return false;
  if (!current && !other_heap->current)
    return true;
  if (!current || !other_heap->current)
    return false;
  return *current == *(other_heap->current);
}

bool HeapIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab2.2 实现 != 重载
  const HeapIterator *other_heap = dynamic_cast<const HeapIterator *>(&other);
  if (!other_heap)
    return true;
  if (!current && !other_heap->current)
    return false;
  if (!current || !other_heap->current)
    return true;
  return *current != *(other_heap->current);
}

bool HeapIterator::top_value_legal() const {
  // TODO: Lab2.2 判断顶部元素是否合法
  // ? 被删除的值是不合法
  // ? 不允许访问的事务创建或更改的键值对不合法(暂时忽略)
  if (items.top().value_.empty())
    return false;
  return true;
}

void HeapIterator::skip_by_tranc_id() {
  // TODO: Lab2.2 后续的Lab实现, 只是作为标记提醒
}

bool HeapIterator::is_end() const { return !current; }
bool HeapIterator::is_valid() const { return current != nullptr; }

void HeapIterator::update_current() const {
  // current 缓存了当前键值对的值, 你实现 -> 重载时可能需要
  // TODO: Lab2.2 更新当前缓存值
}

IteratorType HeapIterator::get_type() const {
  return IteratorType::HeapIterator;
}

uint64_t HeapIterator::get_tranc_id() const { return max_tranc_id_; }
} // namespace tiny_lsm
