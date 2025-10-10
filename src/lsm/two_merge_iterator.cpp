#include "../../include/lsm/two_merge_iterator.h"
#include <memory>
#include <stdexcept>

namespace tiny_lsm {

TwoMergeIterator::TwoMergeIterator() {}

TwoMergeIterator::TwoMergeIterator(std::shared_ptr<BaseIterator> it_a,
                                   std::shared_ptr<BaseIterator> it_b,
                                   uint64_t max_tranc_id)
    : it_a(std::move(it_a)), it_b(std::move(it_b)),
      max_tranc_id_(max_tranc_id) {
  // 先跳过不可见的事务
  skip_by_tranc_id();
  skip_it_b();              // 跳过与 it_a 重复的 key
  choose_a = choose_it_a(); // 决定使用哪个迭代器
}

bool TwoMergeIterator::choose_it_a() {
  // TODO: Lab 4.4: 实现选择迭代器的逻辑.
  if (!it_a || !it_a->is_valid() || it_a->is_end()) {
    if (!it_b || !it_b->is_valid() || it_b->is_end()) {
      choose_a = false; // 都没了
      return choose_a;
    }
    choose_a = false;
    return choose_a;
  }
  if (!it_b || !it_b->is_valid() || it_b->is_end()) {
    choose_a = true;
    return choose_a;
  }
  const auto &ka = (**it_a).first;
  const auto &kb = (**it_b).first;
  if (ka <= kb)
    choose_a = true;
  else
    choose_a = false;
  return choose_a;
}

void TwoMergeIterator::skip_it_b() {
  if (!it_a->is_end() && !it_b->is_end() && (**it_a).first == (**it_b).first) {
    ++(*it_b);
  }
}

void TwoMergeIterator::skip_by_tranc_id() {
  // TODO: Lab xx
  // auto &tmp_it = choose_a ? it_a : it_b;
  // if (!tmp_it)
  //   return;

  // if (max_tranc_id_ != 0) {
  //   while (!tmp_it->is_end()) {
  //     if (tmp_it->get_tranc_id() > max_tranc_id_)
  //       ++*tmp_it;
  //     else
  //       break;
  //   }
  //   if (tmp_it->is_end()) {
  //     choose_a = false;
  //     skip_it_b();
  //   }
  // } else {
  //   return;
  // }
}

BaseIterator &TwoMergeIterator::operator++() {
  // TODO: Lab 4.4: 实现 ++ 重载
  if (is_end()) {
    throw std::runtime_error("在结尾了不能继续长");
  }

  // 1. 前进当前选择侧
  bool cur_a = choose_it_a(); // 确保 choose_a 已根据当前状态更新
  if (cur_a) {
    if (it_a && !it_a->is_end()) {
      ++*it_a;
    }
  } else {
    if (it_b && !it_b->is_end()) {
      ++*it_b;
    }
  }

  // 2. （可选）事务过滤 - 现在空实现
  skip_by_tranc_id();

  // 3. 去重：如果两个都有效且 key 相同，保留 A（策略），前提 A 还有效
  if (it_a && it_b && it_a->is_valid() && !it_a->is_end() && it_b->is_valid() &&
      !it_b->is_end()) {
    if ((**it_a).first == (**it_b).first) {
      // A 优先，跳过 B
      ++*it_b;
      // 事务过滤
      skip_by_tranc_id();
    }
  }

  // 4. 更新选择
  choose_it_a();

  if (!is_end() && is_valid())
    update_current();

  return *this;
}

bool TwoMergeIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab 4.4: 实现 == 重载
  if (other.get_type() != IteratorType::TwoMergeIterator)
    return false;
  const TwoMergeIterator *other_new =
      dynamic_cast<const TwoMergeIterator *>(&other);

  if (choose_a != other_new->choose_a || it_a != other_new->it_a ||
      it_b != other_new->it_b || current != other_new->current ||
      max_tranc_id_ != other_new->max_tranc_id_)
    return false;
  return true;
}

bool TwoMergeIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab 4.4: 实现 != 重载
  return !(*this == other);
}

BaseIterator::value_type TwoMergeIterator::operator*() const {
  // TODO: Lab 4.4: 实现 * 重载
  update_current();
  return *current;
}

IteratorType TwoMergeIterator::get_type() const {
  return IteratorType::TwoMergeIterator;
}

uint64_t TwoMergeIterator::get_tranc_id() const { return max_tranc_id_; }

bool TwoMergeIterator::is_end() const {
  if (it_a == nullptr && it_b == nullptr) {
    return true;
  }
  if (it_a == nullptr) {
    return it_b->is_end();
  }
  if (it_b == nullptr) {
    return it_a->is_end();
  }
  return it_a->is_end() && it_b->is_end();
}

bool TwoMergeIterator::is_valid() const {
  if (it_a == nullptr && it_b == nullptr) {
    return false;
  }
  if (it_a == nullptr) {
    return it_b->is_valid();
  }
  if (it_b == nullptr) {
    return it_a->is_valid();
  }
  return it_a->is_valid() || it_b->is_valid();
}

TwoMergeIterator::pointer TwoMergeIterator::operator->() const {
  // TODO: Lab 4.4: 实现 -> 重载
  update_current();
  return current.get();
}

void TwoMergeIterator::update_current() const {
  // TODO: Lab 4.4: 实现更新缓存键值对的辅助函数
  if (!(choose_a ? it_a->is_valid() : it_b->is_valid())) {
    throw std::runtime_error("当前选择的迭代器无效");
  }
  auto cur_kv = **(choose_a ? it_a : it_b);
  current = std::make_shared<value_type>(cur_kv.first, cur_kv.second);
}
} // namespace tiny_lsm
