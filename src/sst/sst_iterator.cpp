#include "../../include/sst/sst_iterator.h"
#include "../../include/sst/sst.h"
#include <cstddef>
#include <optional>
#include <stdexcept>

namespace toni_lsm {

// predicate返回值:
//   0: 谓词
//   >0: 不满足谓词, 需要向右移动
//   <0: 不满足谓词, 需要向左移动
std::optional<std::pair<SstIterator, SstIterator>> sst_iters_monotony_predicate(
    std::shared_ptr<SST> sst, uint64_t tranc_id,
    std::function<int(const std::string &)> predicate) {
  // TODO: Lab 3.7 实现谓词查询功能
  return {};
}

SstIterator::SstIterator(std::shared_ptr<SST> sst, uint64_t tranc_id)
    : m_sst(sst), m_block_idx(0), m_block_it(nullptr), max_tranc_id_(tranc_id) {
  if (m_sst) {
    seek_first();
  }
}

SstIterator::SstIterator(std::shared_ptr<SST> sst, const std::string &key,
                         uint64_t tranc_id)
    : m_sst(sst), m_block_idx(0), m_block_it(nullptr), max_tranc_id_(tranc_id) {
  if (m_sst) {
    seek(key);
  }
}

void SstIterator::set_block_idx(size_t idx) { m_block_idx = idx; }
void SstIterator::set_block_it(std::shared_ptr<BlockIterator> it) {
  m_block_it = it;
}

void SstIterator::seek_first() {
  // TODO: Lab 3.6 将迭代器定位到第一个key
}

void SstIterator::seek(const std::string &key) {
  // TODO: Lab 3.6 将迭代器定位到指定key的位置
}

std::string SstIterator::key() {
  if (!m_block_it) {
    throw std::runtime_error("Iterator is invalid");
  }
  return (*m_block_it)->first;
}

std::string SstIterator::value() {
  if (!m_block_it) {
    throw std::runtime_error("Iterator is invalid");
  }
  return (*m_block_it)->second;
}

BaseIterator &SstIterator::operator++() {
  // TODO: Lab 3.6 实现迭代器自增
  return *this;
}

bool SstIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab 3.6 实现迭代器比较
  return false;
}

bool SstIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab 3.6 实现迭代器比较
  return false;
}

SstIterator::value_type SstIterator::operator*() const {
  // TODO: Lab 3.6 实现迭代器解引用
  return {};
}

IteratorType SstIterator::get_type() const { return IteratorType::SstIterator; }

uint64_t SstIterator::get_tranc_id() const { return max_tranc_id_; }
bool SstIterator::is_end() const { return !m_block_it; }

bool SstIterator::is_valid() const {
  return m_block_it && !m_block_it->is_end() &&
         m_block_idx < m_sst->num_blocks();
}
SstIterator::pointer SstIterator::operator->() const {
  update_current();
  return &(*cached_value);
}

void SstIterator::update_current() const {
  if (!cached_value && m_block_it && !m_block_it->is_end()) {
    cached_value = *(*m_block_it);
  }
}

std::pair<HeapIterator, HeapIterator>
SstIterator::merge_sst_iterator(std::vector<SstIterator> iter_vec,
                                uint64_t tranc_id) {
  if (iter_vec.empty()) {
    return std::make_pair(HeapIterator(), HeapIterator());
  }

  HeapIterator it_begin;
  for (auto &iter : iter_vec) {
    while (iter.is_valid() && !iter.is_end()) {
      it_begin.items.emplace(
          iter.key(), iter.value(), -iter.m_sst->get_sst_id(), 0,
          tranc_id); // ! 此处的level暂时没有作用, 都作用于同一层的比较
      ++iter;
    }
  }
  return std::make_pair(it_begin, HeapIterator());
}
} // namespace toni_lsm