#include "../../include/sst/sst_iterator.h"
#include "../../include/sst/sst.h"
#include "spdlog/spdlog.h"
#include <cstddef>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
namespace tiny_lsm {

// predicate返回值:
//   0: 谓词
//   >0: 不满足谓词, 需要向右移动
//   <0: 不满足谓词, 需要向左移动
std::optional<std::pair<SstIterator, SstIterator>> sst_iters_monotony_predicate(
    std::shared_ptr<SST> sst, uint64_t tranc_id,
    std::function<int(const std::string &)> predicate) {
  // TODO: Lab 3.7 实现谓词查询功能
  if (!sst || sst->num_blocks() == 0) {
    return std::nullopt;
  }

  const int n = static_cast<int>(sst->num_blocks());
  auto first_key_of = [&](int i) -> const std::string & {
    return sst->meta_entries[i].first_key;
  };
  auto last_key_of = [&](int i) -> const std::string & {
    return sst->meta_entries[i].last_key;
  };

  int l = 0, r = n - 1, first = -1;
  while (l <= r) {
    int m = l + (r - l) / 2;
    if (predicate(last_key_of(m)) > 0) {
      l = m + 1;
      continue;
    }

    if (predicate(first_key_of(m)) < 0) {
      r = m - 1;
      continue;
    }
    first = m;
    r = m - 1;
  }
  if (first == -1) {
    return std::nullopt;
  }

  int last = first;
  l = first;
  r = n - 1;
  while (l <= r) {
    int m = l + (r - l) / 2;
    if (predicate(last_key_of(m)) > 0) {
      l = m + 1;
      continue;
    }

    if (predicate(first_key_of(m)) < 0) {
      r = m - 1;
      continue;
    }
    last = m;
    l = m + 1;
  }

  auto first_block =
      sst->read_block(first)->get_monotony_predicate_iters(tranc_id, predicate);
  if (!first_block.has_value()) {
    return std::nullopt;
  }
  auto last_block =
      sst->read_block(last)->get_monotony_predicate_iters(tranc_id, predicate);
  if (!last_block.has_value()) {
    return std::nullopt;
  }

  SstIterator it_begin(sst, tranc_id);
  it_begin.m_block_idx = first;
  it_begin.m_block_it = first_block->first;

  SstIterator it_end(sst, tranc_id);
  it_end.m_block_idx = last;
  it_end.m_block_it = last_block->second;
  return std::make_pair(it_begin, it_end);
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
  if (!m_sst || m_sst->num_blocks() == 0) {
    m_block_it = nullptr;
    return;
  }

  m_block_idx = 0;
  auto block = m_sst->read_block(m_block_idx);
  m_block_it = std::make_shared<BlockIterator>(block, 0, max_tranc_id_);
}

void SstIterator::seek(const std::string &key) {
  // TODO: Lab 3.6 将迭代器定位到指定key的位置
  if (!m_sst) {
    m_block_it = nullptr;
    return;
  }

  try {
    m_block_idx = m_sst->find_block_idx(key);
    if (m_block_idx == -1 || m_block_idx >= m_sst->num_blocks()) {
      // 置为 end
      m_block_it = nullptr;
      m_block_idx = m_sst->num_blocks();
      return;
    }
    auto block = m_sst->read_block(m_block_idx);
    if (!block) {
      m_block_it = nullptr;
      return;
    }
    m_block_it = std::make_shared<BlockIterator>(block, key, max_tranc_id_);
    if (m_block_it->is_end()) {
      // block 中找不到
      m_block_idx = m_sst->num_blocks();
      m_block_it = nullptr;
      return;
    }
  } catch (const std::exception &) {
    m_block_it = nullptr;
    return;
  }
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
  if (!m_block_it) { // 添加空指针检查
    return *this;
  }
  ++(*m_block_it);
  if (m_block_it->is_end()) {
    m_block_idx++;
    if (m_block_idx < m_sst->num_blocks()) {
      // 读取下一个block
      auto next_block = m_sst->read_block(m_block_idx);
      BlockIterator new_blk_it(next_block, 0, max_tranc_id_);
      (*m_block_it) = new_blk_it;
    } else {
      // 没有下一个block
      m_block_it = nullptr;
    }
  }
  return *this;
}

bool SstIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab 3.6 实现迭代器比较
  if (other.get_type() != IteratorType::SstIterator) {
    return false;
  }
  auto other2 = dynamic_cast<const SstIterator &>(other);
  if (m_sst != other2.m_sst || m_block_idx != other2.m_block_idx) {
    return false;
  }

  if (!m_block_it && !other2.m_block_it) {
    return true;
  }

  if (!m_block_it || !other2.m_block_it) {
    return false;
  }

  return *m_block_it == *other2.m_block_it;
}

bool SstIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab 3.6 实现迭代器比较
  return !(*this == other);
}

SstIterator::value_type SstIterator::operator*() const {
  // TODO: Lab 3.6 实现迭代器解引用
  if (!m_block_it) {
    throw std::runtime_error("Iterator is invalid");
  }
  return (**m_block_it);
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

  // Collect SearchItem entries into a vector and construct HeapIterator via
  // its vector constructor so that `current` gets properly initialized.
  std::vector<SearchItem> items;
  for (auto &iter : iter_vec) {
    while (iter.is_valid() && !iter.is_end()) {
      items.emplace_back(iter.key(), iter.value(),
                         -static_cast<int>(iter.m_sst->get_sst_id()), 0,
                         tranc_id);
      ++iter;
    }
  }
  HeapIterator it_begin(std::move(items), tranc_id,
                        false); // filter_empty=false for compaction
  return std::make_pair(it_begin, HeapIterator());
}
} // namespace tiny_lsm
