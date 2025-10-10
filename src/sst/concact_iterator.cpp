#include "../../include/sst/concact_iterator.h"
#include <stdexcept>

namespace tiny_lsm {

ConcactIterator::ConcactIterator(std::vector<std::shared_ptr<SST>> ssts,
                                 uint64_t tranc_id)
    : ssts(ssts), cur_iter(nullptr, tranc_id), cur_idx(0),
      max_tranc_id_(tranc_id) {
  if (!this->ssts.empty()) {
    cur_iter = ssts[0]->begin(max_tranc_id_);
  }
}

BaseIterator &ConcactIterator::operator++() {
  // TODO: Lab 4.3 自增运算符重载
  if (is_end()) {
    throw std::runtime_error("bad bad");
  }

  // 先递增当前迭代器
  ++cur_iter;

  // 如果递增后当前迭代器仍然有效且不是结尾，直接返回
  if (cur_iter.is_valid() && !cur_iter.is_end()) {
    return *this;
  }

  // 当前SST已经遍历完，移动到下一个SST
  ++cur_idx;

  // 循环直到找到有效的SST或者遍历完所有SST
  while (cur_idx < ssts.size()) {
    cur_iter = ssts[cur_idx]->begin(max_tranc_id_);

    // 如果新SST的迭代器有效且不是结尾，跳出循环
    if (cur_iter.is_valid() && !cur_iter.is_end()) {
      break;
    }

    // 如果新SST是空的，继续找下一个
    ++cur_idx;
  }

  return *this;
}

bool ConcactIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab 4.3 比较运算符重载
  if (other.get_type() != IteratorType::ConcactIterator) {
    return false;
  }
  auto other2 = dynamic_cast<const ConcactIterator &>(other);
  if (ssts != other2.ssts || cur_iter != other2.cur_iter ||
      cur_idx != other2.cur_idx || max_tranc_id_ != other2.max_tranc_id_) {
    return false;
  }
  return true;
}

bool ConcactIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab 4.3 比较运算符重载
  return !(*this == other);
}

ConcactIterator::value_type ConcactIterator::operator*() const {
  // TODO: Lab 4.3 解引用运算符重载
  return {cur_iter->first, cur_iter->second};
}

IteratorType ConcactIterator::get_type() const {
  return IteratorType::ConcactIterator;
}

uint64_t ConcactIterator::get_tranc_id() const { return max_tranc_id_; }

bool ConcactIterator::is_end() const {
  return cur_iter.is_end() || !cur_iter.is_valid();
}

bool ConcactIterator::is_valid() const {
  return !cur_iter.is_end() && cur_iter.is_valid();
}

ConcactIterator::pointer ConcactIterator::operator->() const {
  // TODO: Lab 4.3 ->运算符重载
  return cur_iter.operator->();
}

std::string ConcactIterator::key() { return cur_iter.key(); }

std::string ConcactIterator::value() { return cur_iter.value(); }
} // namespace tiny_lsm
