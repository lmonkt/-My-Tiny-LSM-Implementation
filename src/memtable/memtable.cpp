#include "../../include/memtable/memtable.h"
#include "../../include/config/config.h"
#include "../../include/consts.h"
#include "../../include/iterator/iterator.h"
#include "../../include/skiplist/skiplist.h"
#include "../../include/sst/sst.h"
#include "spdlog/spdlog.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <sys/types.h>
#include <utility>
#include <vector>

namespace tiny_lsm {

class BlockCache;

// MemTable implementation using PIMPL idiom
MemTable::MemTable() : frozen_bytes(0) {
  current_table = std::make_shared<SkipList>();
}
MemTable::~MemTable() = default;

void MemTable::put_(const std::string &key, const std::string &value,
                    uint64_t tranc_id) {
  // TODO: Lab2.1 无锁版本的 put
  current_table->put(key, value, tranc_id);
}

void MemTable::put(const std::string &key, const std::string &value,
                   uint64_t tranc_id) {
  // TODO: Lab2.1 有锁版本的 put
  std::unique_lock<std::shared_mutex> slock1(cur_mtx);
  put_(key, value, tranc_id);
  if (current_table->get_size() >
      TomlConfig::getInstance().getLsmPerMemSizeLimit()) {
    // 冻结当前表还需要获取frozen_mtx的写锁
    std::unique_lock<std::shared_mutex> lock2(frozen_mtx);
    frozen_cur_table_();
    spdlog::debug("MemTable--Current table size exceeded limit. Frozen and "
                  "created new table.");
  }
}

void MemTable::put_batch(
    const std::vector<std::pair<std::string, std::string>> &kvs,
    uint64_t tranc_id) {
  // TODO: Lab2.1 有锁版本的 put_batch
  // ? tranc_id 参数可暂时忽略其逻辑判断, 直接插入即可
  std::unique_lock<std::shared_mutex> slock1(cur_mtx);
  for (auto &[k, v] : kvs) {
    put_(k, v, tranc_id);
  }
  if (current_table->get_size() >
      TomlConfig::getInstance().getLsmPerMemSizeLimit()) {
    // 冻结当前表还需要获取frozen_mtx的写锁
    std::unique_lock<std::shared_mutex> lock2(frozen_mtx);
    frozen_cur_table_();

    spdlog::debug("MemTable--Current table size exceeded limit after batch "
                  "put. Frozen and created new table.");
  }
}

SkipListIterator MemTable::cur_get_(const std::string &key, uint64_t tranc_id) {
  // 检查当前活跃的memtable
  // TODO: Lab2.1 从活跃跳表中查询
  auto result = current_table->get(key, tranc_id);
  if (result.is_valid()) {
    // 只要找到了 key, 不管 value 是否为空都返回
    return result;
  }

  // 没有找到，返回空
  return SkipListIterator{};
}

SkipListIterator MemTable::frozen_get_(const std::string &key,
                                       uint64_t tranc_id) {
  // TODO: Lab2.1 从冻结跳表中查询
  // 检查frozen memtable
  for (auto &tabe : frozen_tables) {
    auto result = tabe->get(key, tranc_id);
    if (result.is_valid()) {
      return result;
    }
  }
  // 都没有找到，返回空
  return SkipListIterator{};
}

SkipListIterator MemTable::get(const std::string &key, uint64_t tranc_id) {
  // TODO: Lab2.1 查询, 建议复用 cur_get_ 和 frozen_get_
  // ? 注意并发控制
  std::shared_lock<std::shared_mutex> slock1(cur_mtx);
  auto tmp = this->cur_get_(key, tranc_id);
  if (tmp.is_valid())
    return tmp;
  slock1.unlock(); // 释放活跃表的锁
  std::shared_lock<std::shared_mutex> slock2(frozen_mtx); // 获取冻结表的锁
  return this->frozen_get_(key, tranc_id);
}

SkipListIterator MemTable::get_(const std::string &key, uint64_t tranc_id) {
  // TODO: Lab2.1 查询, 无锁版本
  auto tmp = this->cur_get_(key, tranc_id);
  if (tmp.is_valid())
    return tmp;
  return this->frozen_get_(key, tranc_id);
}

std::vector<
    std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
MemTable::get_batch(const std::vector<std::string> &keys, uint64_t tranc_id) {
  spdlog::trace("MemTable--get_batch with {} keys", keys.size());

  std::vector<
      std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
      results;
  results.reserve(keys.size());

  // 1. 先获取活跃表的锁
  std::shared_lock<std::shared_mutex> slock1(cur_mtx);
  for (size_t idx = 0; idx < keys.size(); idx++) {
    auto key = keys[idx];
    auto cur_res = cur_get_(key, tranc_id);
    if (cur_res.is_valid()) {
      // 值存在且不为空
      results.emplace_back(
          key, std::make_pair(cur_res.get_value(), cur_res.get_tranc_id()));
    } else {
      // 如果活跃表中未找到，先占位
      results.emplace_back(key, std::nullopt);
    }
  }

  // 2. 如果某些键在活跃表中未找到，还需要查找冻结表
  if (!std::any_of(results.begin(), results.end(), [](const auto &result) {
        return !result.second.has_value();
      })) {
    // ! 最后, 需要把 value 为空的键值对标记为 nullopt
    for (auto &[key, value] : results) {
      if (!value.has_value()) {
        value = std::nullopt;
      }
    }
    return results;
  }

  slock1.unlock(); // 释放活跃表的锁
  std::shared_lock<std::shared_mutex> slock2(frozen_mtx); // 获取冻结表的锁
  for (size_t idx = 0; idx < keys.size(); idx++) {
    if (results[idx].second.has_value()) {
      continue; // 如果在活跃表中已经找到，则跳过
    }
    auto key = keys[idx];
    auto frozen_result = frozen_get_(key, tranc_id);
    if (frozen_result.is_valid()) {
      // 值存在且不为空
      results[idx] =
          std::make_pair(key, std::make_pair(frozen_result.get_value(),
                                             frozen_result.get_tranc_id()));
    } else {
      results[idx] = std::make_pair(key, std::nullopt);
    }
  }

  return results;
}

void MemTable::remove_(const std::string &key, uint64_t tranc_id) {
  // TODO Lab2.1 无锁版本的remove
  put_(key, "", tranc_id);
}

void MemTable::remove(const std::string &key, uint64_t tranc_id) {
  // TODO Lab2.1 有锁版本的remove
  put(key, "", tranc_id);
}

void MemTable::remove_batch(const std::vector<std::string> &keys,
                            uint64_t tranc_id) {
  // TODO Lab2.1 有锁版本的remove_batch
  std::vector<std::pair<std::string, std::string>> tmp;
  for (auto k : keys) {
    tmp.push_back({k, ""});
  }
  put_batch(tmp, tranc_id);
}

void MemTable::clear() {
  spdlog::info("MemTable--clear(): Clearing all tables");

  std::unique_lock<std::shared_mutex> lock1(cur_mtx);
  std::unique_lock<std::shared_mutex> lock2(frozen_mtx);
  frozen_tables.clear();
  current_table->clear();
}

// 将最老的 memtable 写入 SST, 并返回控制类
std::shared_ptr<SST>
MemTable::flush_last(SSTBuilder &builder, std::string &sst_path, size_t sst_id,
                     std::shared_ptr<BlockCache> block_cache) {
  spdlog::debug("MemTable--flush_last(): Starting to flush memtable to SST{}",
                sst_id);

  // 由于 flush 后需要移除最老的 memtable, 因此需要加写锁
  std::unique_lock<std::shared_mutex> lock(frozen_mtx);

  uint64_t max_tranc_id = 0;
  uint64_t min_tranc_id = UINT64_MAX;

  if (frozen_tables.empty()) {
    // 如果当前表为空，直接返回nullptr
    if (current_table->get_size() == 0) {
      spdlog::debug(
          "MemTable--flush_last(): Current table is empty, returning null");

      return nullptr;
    }
    // 将当前表加入到frozen_tables头部
    frozen_tables.push_front(current_table);
    frozen_bytes += current_table->get_size();
    // 创建新的空表作为当前表
    current_table = std::make_shared<SkipList>();
  }

  // 将最老的 memtable 写入 SST
  std::shared_ptr<SkipList> table = frozen_tables.back();
  frozen_tables.pop_back();
  frozen_bytes -= table->get_size();

  std::vector<std::tuple<std::string, std::string, uint64_t>> flush_data =
      table->flush();
  for (auto &[k, v, t] : flush_data) {
    max_tranc_id = std::max(t, max_tranc_id);
    min_tranc_id = std::min(t, min_tranc_id);
    builder.add(k, v, t);
  }
  auto sst = builder.build(sst_id, sst_path, block_cache);

  spdlog::info("MemTable--flush_last(): SST{} built successfully at '{}'",
               sst_id, sst_path);

  return sst;
}

void MemTable::frozen_cur_table_() {
  // TODO: 冻结活跃表
  spdlog::trace("MemTable--frozen_cur_table_(): Freezing current table");
  frozen_bytes += current_table->get_size();
  frozen_tables.push_front(std::move(current_table));
  current_table = std::make_shared<SkipList>();
}

void MemTable::frozen_cur_table() {
  // TODO: 冻结活跃表, 有锁版本
  std::unique_lock<std::shared_mutex> lock1(cur_mtx);
  std::unique_lock<std::shared_mutex> lock2(frozen_mtx);
  frozen_cur_table_();
}

size_t MemTable::get_cur_size() {
  std::shared_lock<std::shared_mutex> slock(cur_mtx);
  return current_table->get_size();
}

size_t MemTable::get_frozen_size() {
  std::shared_lock<std::shared_mutex> slock(frozen_mtx);
  return frozen_bytes;
}

size_t MemTable::get_total_size() {
  std::shared_lock<std::shared_mutex> slock1(cur_mtx);
  std::shared_lock<std::shared_mutex> slock2(frozen_mtx);
  return get_frozen_size() + get_cur_size();
}

HeapIterator MemTable::begin(uint64_t tranc_id) {
  // TODO Lab 2.2 MemTable 的迭代器
  std::shared_lock<std::shared_mutex> slock1(cur_mtx);
  std::shared_lock<std::shared_mutex> slock2(frozen_mtx);
  std::vector<SearchItem> sea;
  for (auto tmp : current_table->flush()) {
    auto [key, value, id] = tmp;
    if (id != 0 && id > tranc_id) {
      continue;
    }
    sea.emplace_back(SearchItem(key, value, 0, 0, id));
  }
  int table_id = 1;

  for (auto tmp : frozen_tables) {
    for (auto tab : (*tmp).flush()) {
      auto [key, value, id] = tab;
      if (id != 0 && id > tranc_id) {
        continue;
      }
      sea.emplace_back(SearchItem(key, value, table_id, 0, id));
    }
    ++table_id;
  }
  return HeapIterator(sea, tranc_id);
}

HeapIterator MemTable::end() {
  // TODO Lab 2.2 MemTable 的迭代器
  std::shared_lock<std::shared_mutex> slock1(cur_mtx);
  std::shared_lock<std::shared_mutex> slock2(frozen_mtx);
  return HeapIterator{};
}

HeapIterator MemTable::iters_preffix(const std::string &preffix,
                                     uint64_t tranc_id) {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wambiguous-reversed-operator"

  // TODO Lab 2.3 MemTable 的前缀迭代器
  std::shared_lock<std::shared_mutex> slock1(cur_mtx);
  std::shared_lock<std::shared_mutex> slock2(frozen_mtx);
  std::vector<SearchItem> sea;
  auto it = current_table->begin_preffix(preffix);
  auto end = current_table->end_preffix(preffix);
  while (it != end) {
    sea.emplace_back(
        SearchItem(it.get_key(), it.get_value(), 0, 0, it.get_tranc_id()));
    ++it;
  }

  int table_id = 1;

  for (const auto &table : frozen_tables) {
    auto it_frozen = table->begin_preffix(preffix);
    auto end_frozen = table->end_preffix(preffix);
    while (it_frozen != end_frozen) {
      sea.emplace_back(SearchItem(it_frozen.get_key(), it_frozen.get_value(),
                                  table_id, 0, it_frozen.get_tranc_id()));
      ++it_frozen;
    }
    ++table_id;
  }
#pragma clang diagnostic pop
  return HeapIterator(sea, tranc_id);
}

std::optional<std::pair<HeapIterator, HeapIterator>>
MemTable::iters_monotony_predicate(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  // TODO Lab 2.3 MemTable 的谓词查询迭代器起始范围
  //这里返回两个HeapIterator，第一个应该是全推进去，第二个就是只有最后一个的
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wambiguous-reversed-operator"

  // TODO Lab 2.3 MemTable 的前缀迭代器
  std::shared_lock<std::shared_mutex> slock1(cur_mtx);
  std::shared_lock<std::shared_mutex> slock2(frozen_mtx);
  std::vector<SearchItem> sea;
  auto opt_range = current_table->iters_monotony_predicate(predicate);
  if (opt_range.has_value()) {
    auto &[start, end] = opt_range.value();
    for (auto tmp = start; tmp != end; ++tmp) {
      sea.emplace_back(
          SearchItem(tmp.get_key(), tmp.get_value(), 0, 0, tmp.get_tranc_id()));
    }
  }

  int table_id = 1;

  for (auto tmp : frozen_tables) {
    auto opt_range = tmp->iters_monotony_predicate(predicate);
    if (opt_range.has_value()) {
      auto &[start, end] = opt_range.value();
      for (auto tmp = start; tmp != end; ++tmp) {
        sea.emplace_back(SearchItem(tmp.get_key(), tmp.get_value(), table_id, 0,
                                    tmp.get_tranc_id()));
      }
      ++table_id;
    }
  }
  if (!sea.empty()) {
    return std::make_optional(std::make_pair(
        HeapIterator(sea, table_id), HeapIterator({sea.back()}, table_id)));
  }
#pragma clang diagnostic pop
  return std::nullopt;
}
} // namespace tiny_lsm
