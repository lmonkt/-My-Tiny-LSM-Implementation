#include "../../include/lsm/level_iterator.h"
#include "../../include/lsm/engine.h"
#include "../../include/sst/concact_iterator.h"
#include "../../include/sst/sst.h"
#include <memory>
#include <shared_mutex>
#include <string>

// TODO: 需要进行单元测试
namespace tiny_lsm {
Level_Iterator::Level_Iterator(std::shared_ptr<LSMEngine> engine,
                               uint64_t max_tranc_id)
    : engine_(engine), max_tranc_id_(max_tranc_id), rlock_(engine_->ssts_mtx) {
  // 成员变量获取sst读锁

  // 1. 获取内存部分迭代器
  // TODO: 这里最好修改 memtable.begin 使其返回一个指针, 避免多余的内存拷贝
  // auto mem_iter = engine_->memtable.begin(max_tranc_id_);
  // std::shared_ptr<HeapIterator> mem_iter_ptr =
  // std::make_shared<HeapIterator>(); *mem_iter_ptr = mem_iter;
  auto mem_iter = engine_->memtable.begin(max_tranc_id_);
  auto mem_iter_ptr = std::make_shared<HeapIterator>(std::move(mem_iter));
  iter_vec.push_back(mem_iter_ptr);

  // 2. 获取 L0 层的迭代器
  std::vector<SearchItem> item_vec;
  for (auto &sst_id : engine_->level_sst_ids[0]) {
    auto sst = engine_->ssts[sst_id];
    for (auto iter = sst->begin(max_tranc_id_);
      iter.is_valid(); ++iter) {
      // 对同一 key 且同一事务 id 的项，SearchItem 按 level 升序、再按 idx
      // 升序比较。 在 L0 中，较新的 SST 对应的 sst_id 更大。为了让较新的 SST
      // 的记录优先出现， 我们把 SearchItem::idx_ 设为 -sst_id（即将 sst_id
      // 取反），使得原本更大的 sst_id 在 idx 比较中变成更小的值，从而在 heap
      // 中被优先弹出。
      if (max_tranc_id_ != 0 && iter.get_tranc_id() > max_tranc_id_) {
        // 如果开启了事务, 比当前事务 id 更大的记录是不可见的
        continue;
      }
      item_vec.emplace_back(iter.key(), iter.value(), -sst_id, 0,
                            iter.get_tranc_id());
    }
  }
  std::shared_ptr<HeapIterator> l0_iter_ptr =
      std::make_shared<HeapIterator>(item_vec, max_tranc_id);
  iter_vec.push_back(l0_iter_ptr);

  // 3. 获取其他层的迭代器
  for (auto &[level, sst_id_list] : engine_->level_sst_ids) {
    if (level == 0) {
      continue;
    }
    // 为该层一次性创建一个 ConcactIterator（串联本层所有 SST），
    // 而不是在逐步增长的 ssts 前缀上重复创建多个迭代器。
    // 之前的写法会导致 [sst0], [sst0,sst1], [sst0,sst1,sst2] ... 多个前缀迭代器被加入，
    // 既效率低也会引入重复遍历。
    std::vector<std::shared_ptr<SST>> ssts;
    ssts.reserve(sst_id_list.size());
    for (auto sst_id : sst_id_list) {
      ssts.push_back(engine_->ssts[sst_id]);
    }
    if (!ssts.empty()) {
      std::shared_ptr<ConcactIterator> level_i_iter =
          std::make_shared<ConcactIterator>(std::move(ssts), max_tranc_id);
      iter_vec.push_back(level_i_iter);
    }
  }

  while (!is_end()) {
    auto [min_idx, _] = get_min_key_idx();
    cur_idx_ = min_idx;
    update_current();
    auto cached_kv = *cached_value;
    if (cached_kv.second.size() == 0) {
      // 如果当前值为空, 说明当前key已经被删除了
      // 需要跳过这个key
      skip_key(cached_value->first);
      continue;
    } else {
      // 找到一个合法的键值对, 跳出循环
      break;
    }
  }
}

std::pair<size_t, std::string> Level_Iterator::get_min_key_idx() const {
  size_t min_idx = 0;
  std::string min_key = "";
  for (size_t i = 0; i < iter_vec.size(); ++i) {
    if (!iter_vec[i]->is_valid()) {
      // 如果当前迭代器无效, 则跳过
      continue;
    } else if (min_key == "") {
      // 第一次初始化
      min_key = (**iter_vec[i]).first;
      min_idx = i;
    } else if ((**iter_vec[i]).first < (**iter_vec[min_idx]).first) {
      // 更新最小key和索引
      min_key = (**iter_vec[i]).first;
      min_idx = i;
    } else if ((**iter_vec[i]).first == min_key) {
      // key相同时, 事务id大的排前面
      if (max_tranc_id_ != 0) {
        if ((*iter_vec[i]).get_tranc_id() >
            (*iter_vec[min_idx]).get_tranc_id()) {
          min_idx = i;
        }
      }
    }
  }
  return std::make_pair(min_idx, min_key);
}

void Level_Iterator::skip_key(const std::string &key) {
  for (size_t i = 0; i < iter_vec.size(); ++i) {
    while ((*iter_vec[i]).is_valid() && (**iter_vec[i]).first == key) {
      // 如果找到当前key, 则跳过这个key
      ++(*iter_vec[i]);
    }
  }
}

void Level_Iterator::update_current() const {
  if (!(*iter_vec[cur_idx_]).is_valid()) {
    throw std::runtime_error("Level_Iterator is invalid");
  }
  // Ensure cached_value points to the correct value_type
  auto cur_kv = *(*iter_vec[cur_idx_]);
  cached_value = std::make_optional<value_type>(cur_kv.first, cur_kv.second);
}

BaseIterator &Level_Iterator::operator++() {
  // 先跳过和当前 key 相同的部分
  skip_key(cached_value->first);

  // 重新选择key最小的迭代器
  while (!is_end()) {
    auto [min_idx, _] = get_min_key_idx();
    cur_idx_ = min_idx;
    update_current();
    if (cached_value->second.size() == 0) {
      // 如果当前值为空, 说明当前key已经被删除了
      // 需要跳过这个key
      skip_key(cached_value->first);
      continue;
    } else {
      // 找到一个合法的键值对, 跳出循环
      break;
    }
  }
  return *this;
}

bool Level_Iterator::operator==(const BaseIterator &other) const {
  if (other.get_type() != IteratorType::LevelIterator) {
    return false;
  }
  if (other.is_valid() && is_valid()) {
    return (*other).first == cached_value->first &&
           (*other).second == cached_value->second;
  }
  if (!other.is_valid() && !is_valid()) {
    return true;
  }
  return false;
}

bool Level_Iterator::operator!=(const BaseIterator &other) const {
  return !(*this == other);
}

BaseIterator::value_type Level_Iterator::operator*() const {
  if (!cached_value.has_value()) {
    throw std::runtime_error("Level_Iterator is invalid");
  }
  return *cached_value;
}

IteratorType Level_Iterator::get_type() const {
  return IteratorType::LevelIterator;
}

uint64_t Level_Iterator::get_tranc_id() const { return max_tranc_id_; }

bool Level_Iterator::is_end() const {
  for (auto &iter : iter_vec) {
    if ((*iter).is_valid()) {
      return false;
    }
  }
  return true;
}

bool Level_Iterator::is_valid() const { return !is_end(); }

BaseIterator::pointer Level_Iterator::operator->() const {
  update_current();
  return &(*cached_value);
}
} // namespace tiny_lsm
