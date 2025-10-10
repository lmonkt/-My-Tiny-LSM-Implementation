#include "../../include/lsm/engine.h"
#include "../../include/config/config.h"
#include "../../include/consts.h"
#include "../../include/logger/logger.h"
#include "../../include/lsm/level_iterator.h"
#include "../../include/sst/concact_iterator.h"
#include "../../include/sst/sst.h"
#include "../../include/sst/sst_iterator.h"
#include "spdlog/spdlog.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace tiny_lsm {

// *********************** LSMEngine ***********************
LSMEngine::LSMEngine(std::string path) : data_dir(path) {
  // 初始化日志
  init_spdlog_file();
  // TODO: Lab 4.2 引擎初始化
  if (!std::filesystem::exists(data_dir)) {
    if (!std::filesystem::create_directories(data_dir)) {
      SPDLOG_ERROR("Failed to create data directory: {}", data_dir);
      throw std::runtime_error("Failed to create data directory: " + data_dir);
    }
    SPDLOG_INFO("Created data directory: {}", data_dir);
  } else if (!std::filesystem::is_directory(data_dir)) {
    SPDLOG_ERROR("Data path exists but is not a directory: {}", data_dir);
    throw std::runtime_error("Data path is not a directory: " + data_dir);
  }
  // 初始化 BlockCache（容量与K值从配置读取）
  this->block_cache = std::make_shared<BlockCache>(
      TomlConfig::getInstance().getLsmBlockCacheCapacity(),
      TomlConfig::getInstance().getLsmBlockCacheK());
  cur_max_level = 0;

  auto is_digits = [](const std::string &sst_id_string) -> bool {
    if (sst_id_string.empty())
      return false;
    for (char c : sst_id_string) {
      if (!std::isdigit(c))
        return false;
    }
    return true;
  };
  // 可选：预初始化 L0 的容器
  level_sst_ids[0] = std::deque<size_t>();
  // 遍历目录中的每个条目
  bool any_sst_found = false;
  for (const auto &entry : std::filesystem::directory_iterator(data_dir)) {
    if (entry.is_regular_file()) { // 确保是文件
      std::string filename = entry.path().filename().string();
      size_t sst_id = 0;

      // 检查文件名是否以 "sst_" 开头
      if (filename.rfind("sst_", 0) == 0 && filename.size() > 37) {
        // 形如: sst_<32位数字>.<level>
        // 检查分隔符位置
        if (filename.size() <= 37 || filename[36] != '.') {
          spdlog::warn("Skip file (invalid sst name format, missing '.'): {}",
                       filename);
          continue;
        }
        std::string suffix = filename.substr(4, 32); // 32位数字ID
        std::string level = filename.substr(37);     // level 数字

        if (is_digits(suffix)) {
          try {
            // 先转为 unsigned long long，再赋给 size_t
            sst_id = static_cast<size_t>(std::stoull(suffix));
            next_sst_id = std::max(next_sst_id, sst_id);

            if (!is_digits(level)) {
              spdlog::warn("Skip file (invalid level digits): {}", filename);
              continue;
            }
            size_t level_id = static_cast<size_t>(std::stoull(level));
            cur_max_level = std::max(cur_max_level, level_id);
            // 插入到对应 level 的 deque（operator[] 会自动初始化空容器）
            level_sst_ids[level_id].emplace_back(sst_id);

            // 使用静态工厂并传入右值 FileObj（触发移动语义，避免拷贝）
            ssts[sst_id] =
                SST::open(sst_id, FileObj::open(entry.path().string(), false),
                          block_cache);
            any_sst_found = true;
          } catch (const std::invalid_argument &e) {
            std::cerr << "Invalid number format in filename: " << filename
                      << std::endl;
          } catch (const std::out_of_range &e) {
            std::cerr << "Number in filename is out of range: " << filename
                      << std::endl;
          }
        } else {
          std::cout << "File: " << filename << " → Suffix is not numeric: '"
                    << suffix << "'" << std::endl;
        }
      } else {
        // 非 sst_ 开头或长度不匹配的文件，直接跳过
        continue;
      }
    } else {
      std::cerr << "不是文件：" << entry.path().string() << std::endl;
    }
  }
  // 将 next_sst_id 设为当前已存在最大ID的下一个，仅在发现过 sst 文件时自增
  if (any_sst_found) {
    next_sst_id++;
  }
  // 为保证遍历/后续逻辑的确定性：
  // - Level-0：保持“最新优先”，即按 sst_id 从大到小排序（近似创建时间新→旧）
  // - 其他层(L1+)：同层内区间不重叠，按 first_key
  // 升序排序，更利于范围扫描与二分查找
  for (auto &kv : level_sst_ids) {
    auto level = kv.first;
    auto &dq = kv.second;
    if (dq.empty())
      continue;
    if (level == 0) {
      std::sort(dq.begin(), dq.end(), [](size_t a, size_t b) {
        return a > b; // 新的 sst_id 更大，排前面
      });
    } else {
      std::sort(dq.begin(), dq.end(), [&](size_t a, size_t b) {
        const auto &sa = ssts[a];
        const auto &sb = ssts[b];
        // 以 first_key 升序排列；若异常情况缺少元数据，退化为按 id 升序
        if (sa && sb)
          return sa->get_first_key() < sb->get_first_key();
        return a < b;
      });
    }
  }
}
LSMEngine::~LSMEngine() = default;

std::optional<std::pair<std::string, uint64_t>>
LSMEngine::get(const std::string &key, uint64_t tranc_id) {
  // TODO: Lab 4.2 查询
  auto skplit = this->memtable.get(key, tranc_id);
  if (skplit.is_valid()) {
    if (skplit.get_value().empty()) {
      return std::nullopt;
    }
    return std::make_pair(skplit.get_value(), skplit.get_tranc_id());
  }
  std::shared_lock<std::shared_mutex> rlock1(ssts_mtx);
  return this->sst_get_(key, tranc_id);
}

std::vector<
    std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
LSMEngine::get_batch(const std::vector<std::string> &keys, uint64_t tranc_id) {
  // TODO: Lab 4.2 批量查询
  std::vector<
      std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
      res;
  for (auto &k : keys) {
    auto v = this->get(k, tranc_id);
    res.emplace_back(k, v);
  }
  return res;
}

std::optional<std::pair<std::string, uint64_t>>
LSMEngine::sst_get_(const std::string &key, uint64_t tranc_id) {
  // TODO: Lab 4.2 sst 内部查询
  for (auto &kv : level_sst_ids) {
    auto level = kv.first;
    auto &dq = kv.second;
    if (dq.empty())
      continue;
    if (level == 0) {
      for (auto &sst_ids : dq) {
        auto sstits = ssts[sst_ids]->get(key, tranc_id);
        if (sstits.is_valid()) {
          if (sstits->second.empty()) {
            return std::nullopt;
          }
          return std::make_pair(sstits->second, sstits.get_tranc_id());
        }
      }
    } else {
      int l = 0, r = static_cast<int>(dq.size()) - 1, pos = -1;
      while (l <= r) {
        int m = l + (r - l) / 2;
        const auto &first_key = ssts[dq[m]]->get_first_key();
        if (first_key <= key) {
          pos = m;
          l = m + 1;
        } else {
          r = m - 1;
        }
      }
      if (pos != -1) {
        auto sstits = ssts[dq[pos]]->get(key, tranc_id);
        if (sstits.is_valid()) {
          if (sstits->second.empty()) {
            return std::nullopt;
          }
          return std::make_pair(sstits->second, sstits.get_tranc_id());
        }
      }
    }
  }
  return std::nullopt;
}

uint64_t LSMEngine::put(const std::string &key, const std::string &value,
                        uint64_t tranc_id) {
  // TODO: Lab 4.1 插入
  // ? 由于 put 操作可能触发 flush
  // ? 如果触发了 flush 则返回新刷盘的 sst 的 id
  // ? 在没有实现  flush 的情况下，你返回 0即可
  memtable.put(key, value, tranc_id);
  if (memtable.get_frozen_size() >=
      TomlConfig::getInstance().getLsmPerMemSizeLimit()) {
    return this->flush();
  }
  return 0;
}

uint64_t LSMEngine::put_batch(
    const std::vector<std::pair<std::string, std::string>> &kvs,
    uint64_t tranc_id) {
  // TODO: Lab 4.1 批量插入
  // ? 由于 put 操作可能触发 flush
  // ? 如果触发了 flush 则返回新刷盘的 sst 的 id
  // ? 在没有实现  flush 的情况下，你返回 0即可
  uint64_t res = 0;
  for (auto &[k, v] : kvs) {
    res = this->put(k, v, tranc_id);
  }
  return res;
}
uint64_t LSMEngine::remove(const std::string &key, uint64_t tranc_id) {
  // TODO: Lab 4.1 删除
  // ? 在 LSM 中，删除实际上是插入一个空值
  // ? 由于 put 操作可能触发 flush
  // ? 如果触发了 flush 则返回新刷盘的 sst 的 id
  // ? 在没有实现  flush 的情况下，你返回 0即可
  return put(key, "", tranc_id);
}

uint64_t LSMEngine::remove_batch(const std::vector<std::string> &keys,
                                 uint64_t tranc_id) {
  // TODO: Lab 4.1 批量删除
  // ? 在 LSM 中，删除实际上是插入一个空值
  // ? 由于 put 操作可能触发 flush
  // ? 如果触发了 flush 则返回新刷盘的 sst 的 id
  // ? 在没有实现  flush 的情况下，你返回 0即可
  uint64_t res = 0;
  for (auto &k : keys) {
    res = this->put(k, "", tranc_id);
  }
  return res;
}

void LSMEngine::clear() {
  memtable.clear();
  level_sst_ids.clear();
  ssts.clear();
  // 清空当前文件夹的所有内容
  try {
    for (const auto &entry : std::filesystem::directory_iterator(data_dir)) {
      if (!entry.is_regular_file()) {
        continue;
      }
      std::filesystem::remove(entry.path());

      spdlog::info("LSMEngine--"
                   "clear file {} successfully.",
                   entry.path().string());
    }
  } catch (const std::filesystem::filesystem_error &e) {
    // 处理文件系统错误
    spdlog::error("Error clearing directory: {}", e.what());
  }
}

uint64_t LSMEngine::flush() {
  // TODO: Lab 4.1 刷盘形成sst文件
  std::unique_lock<std::shared_mutex> lock(ssts_mtx);
  auto sst_path = this->get_sst_path(next_sst_id, 0);

  size_t block_size = TomlConfig::getInstance().getLsmBlockSize();

  SSTBuilder tmp(block_size, true);
  auto sst_tmp = memtable.flush_last(tmp, sst_path, next_sst_id, block_cache);
  if (!std::filesystem::exists(sst_path)) {
    tmp.build(next_sst_id, sst_path, block_cache);
  }
  level_sst_ids[0].emplace_front(next_sst_id);
  ssts[next_sst_id] = sst_tmp;
  uint64_t flushed_sst_id = next_sst_id++;

  // 检查是否需要触发 compact
  // 从 L0 开始检查每一层是否超过阈值
  size_t ratio = TomlConfig::getInstance().getLsmSstLevelRatio();
  for (size_t level = 0; level <= cur_max_level; ++level) {
    if (level_sst_ids[level].size() >= ratio) {
      SPDLOG_INFO("Level {} has {} SSTs, triggering compaction (threshold: {})",
                  level, level_sst_ids[level].size(), ratio);
      full_compact(level);
      // compact 后可能会影响更高层，但 full_compact 内部已经递归处理了
      break; // 只处理第一个超过阈值的层
    }
  }

  return flushed_sst_id;
}

std::string LSMEngine::get_sst_path(size_t sst_id, size_t target_level) {
  // sst的文件路径格式为: data_dir/sst_<sst_id>，sst_id格式化为32位数字
  std::stringstream ss;
  ss << data_dir << "/sst_" << std::setfill('0') << std::setw(32) << sst_id
     << '.' << target_level;
  return ss.str();
}

std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
LSMEngine::lsm_iters_monotony_predicate(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  // TODO: Lab 4.7 谓词查询
  return std::nullopt;
}

Level_Iterator LSMEngine::begin(uint64_t tranc_id) {
  // TODO: Lab 4.7
  throw std::runtime_error("Not implemented");
}

Level_Iterator LSMEngine::end() {
  // TODO: Lab 4.7
  throw std::runtime_error("Not implemented");
}

void LSMEngine::full_compact(size_t src_level) {
  // TODO: Lab 4.5 负责完成整个 full compact
  // ? 你可能需要控制`Compact`流程需要递归地进行
  std::unique_lock<std::shared_mutex> wlock(ssts_mtx);
  int ratio = TomlConfig::getInstance().getLsmSstLevelRatio();
  size_t high_level = src_level + 1;
  std::vector<size_t> low(level_sst_ids[src_level].begin(),
                          level_sst_ids[src_level].end());
  std::vector<size_t> high(level_sst_ids[high_level].begin(),
                           level_sst_ids[high_level].end());

  if (high.size() >= ratio)
    full_compact(high_level);
  std::vector<std::shared_ptr<SST>> res;
  if (src_level == 0) {
    res = full_l0_l1_compact(low, high);
  } else {
    res = full_common_compact(low, high, high_level);
  }

  auto clear_level = [&](size_t level) {
    for (auto i : level_sst_ids[level]) {
      ssts[i]->del_sst();
    }
    level_sst_ids[level].clear();
  };
  clear_level(src_level);
  clear_level(high_level);

  std::deque<size_t> level_deque;
  for (auto &sst_point : res) {
    auto sst_id = sst_point->get_sst_id();
    level_deque.emplace_back(sst_id);
    ssts[sst_id] = sst_point;
  }
  level_sst_ids[high_level] = level_deque;
}

std::vector<std::shared_ptr<SST>>
LSMEngine::full_l0_l1_compact(std::vector<size_t> &l0_ids,
                              std::vector<size_t> &l1_ids) {
  // TODO: Lab 4.5 负责完成 l0 和 l1 的 full compact
  if (l0_ids.size() == 0)
    return {};
  std::vector<SearchItem> si_tmp;
  size_t max_trancid;
  for (size_t &i : l0_ids) {
    {
      max_trancid = std::max(max_trancid, ssts[i]->get_tranc_id_range().second);
      auto sst_it = ssts[i]->begin(0);
      while (sst_it != ssts[i]->end()) {
        si_tmp.emplace_back(sst_it->first, sst_it->second, i, 0,
                            sst_it.get_tranc_id());
        ++sst_it;
      }
    }
  }
  HeapIterator heap_it(si_tmp, max_trancid);

  std::vector<std::shared_ptr<SST>> sst_vec;
  max_trancid = 0;
  for (size_t &i : l1_ids) {
    max_trancid = std::max(max_trancid, ssts[i]->get_tranc_id_range().second);
    sst_vec.emplace_back(&ssts[i]);
  }
  ConcactIterator con_it(sst_vec, max_trancid);

  TwoMergeIterator tw_it(
      std::make_shared<BaseIterator>(heap_it),
      std::make_shared<BaseIterator>(con_it),
      std::max(heap_it.get_tranc_id(), con_it.get_tranc_id()));
  return this->gen_sst_from_iter(tw_it, get_sst_size(1), 1);
}

std::vector<std::shared_ptr<SST>>
LSMEngine::full_common_compact(std::vector<size_t> &lx_ids,
                               std::vector<size_t> &ly_ids, size_t level_y) {
  // TODO: Lab 4.5 负责完成其他相邻 level 的 full compact
  if (lx_ids.size() == 0)
    return {};

  std::vector<std::shared_ptr<SST>> sst_vec;
  size_t max_trancid = 0;
  for (size_t &i : ly_ids) {
    max_trancid = std::max(max_trancid, ssts[i]->get_tranc_id_range().second);
    sst_vec.emplace_back(&ssts[i]);
  }
  ConcactIterator con_it1(sst_vec, max_trancid);

  sst_vec.clear();
  max_trancid = 0;
  for (size_t &i : ly_ids) {
    max_trancid = std::max(max_trancid, ssts[i]->get_tranc_id_range().second);
    sst_vec.emplace_back(&ssts[i]);
  }
  ConcactIterator con_it2(sst_vec, max_trancid);

  TwoMergeIterator tw_it(
      std::make_shared<BaseIterator>(con_it1),
      std::make_shared<BaseIterator>(con_it2),
      std::max(con_it1.get_tranc_id(), con_it2.get_tranc_id()));
  return this->gen_sst_from_iter(tw_it, get_sst_size(level_y), level_y);
  return {};
}

std::vector<std::shared_ptr<SST>>
LSMEngine::gen_sst_from_iter(BaseIterator &iter, size_t target_sst_size,
                             size_t target_level) {
  // TODO: Lab 4.5 实现从迭代器构造新的 SST
  if (iter.get_type() != IteratorType::TwoMergeIterator)
    throw std::runtime_error("无效的迭代器");
  std::vector<std::shared_ptr<SST>> res;

  auto block_size = TomlConfig::getInstance().getLsmBlockSize();
  SSTBuilder builder(block_size, true);
  std::string last_key;
  bool has_last = false;

  while (!iter.is_end() && iter.is_valid()) {
    auto kv = *iter;
    const auto &k = kv.first;
    const auto &v = kv.second;

    // 去重（防御）
    if (has_last && k == last_key) {
      ++iter;
      continue;
    }

    // 估算写入后大小
    size_t next_est =
        builder.estimated_size() + k.size() + v.size() + 32; // 粗估开销

    // 如果会超并且当前 builder 已经有内容，则先落地当前 SST
    if (builder.estimated_size() > 0 && next_est > target_sst_size) {
      auto path = get_sst_path(next_sst_id, target_level);
      auto sst = builder.build(next_sst_id, path, block_cache);
      res.emplace_back(sst);
      ++next_sst_id;
      // 新建 builder
      builder = SSTBuilder(block_size, true);
    }

    // 写入当前 kv
    builder.add(k, v, iter.get_tranc_id());
    last_key = k;
    has_last = true;

    ++iter;
  }

  // 收尾
  if (builder.estimated_size() > 0) {
    auto path = get_sst_path(next_sst_id, target_level);
    auto sst = builder.build(next_sst_id, path, block_cache);
    res.emplace_back(sst);
    ++next_sst_id;
  }

  return res;
}

size_t LSMEngine::get_sst_size(size_t level) {
  if (level == 0) {
    return TomlConfig::getInstance().getLsmPerMemSizeLimit();
  } else {
    return TomlConfig::getInstance().getLsmPerMemSizeLimit() *
           static_cast<size_t>(std::pow(
               TomlConfig::getInstance().getLsmSstLevelRatio(), level));
  }
}

// *********************** LSM ***********************
LSM::LSM(std::string path)
    : engine(std::make_shared<LSMEngine>(path)),
      tran_manager_(std::make_shared<TranManager>(path)) {
  // TODO: Lab 5.5 控制WAL重放与组件的初始化
}

LSM::~LSM() {
  flush_all();
  tran_manager_->write_tranc_id_file();
}

std::optional<std::string> LSM::get(const std::string &key, bool tranc_off) {
  auto tranc_id = tranc_off ? 0 : tran_manager_->getNextTransactionId();
  auto res = engine->get(key, tranc_id);

  if (res.has_value()) {
    return res.value().first;
  }
  return std::nullopt;
}

std::vector<std::pair<std::string, std::optional<std::string>>>
LSM::get_batch(const std::vector<std::string> &keys) {
  // 1. 获取事务ID
  auto tranc_id = tran_manager_->getNextTransactionId();

  // 2. 调用 engine 的批量查询接口
  auto batch_results = engine->get_batch(keys, tranc_id);

  // 3. 构造最终结果
  std::vector<std::pair<std::string, std::optional<std::string>>> results;
  for (const auto &[key, value] : batch_results) {
    if (value.has_value()) {
      results.emplace_back(key, value->first); // 提取值部分
    } else {
      results.emplace_back(key, std::nullopt); // 键不存在
    }
  }

  return results;
}

void LSM::put(const std::string &key, const std::string &value,
              bool tranc_off) {
  auto tranc_id = tranc_off ? 0 : tran_manager_->getNextTransactionId();
  engine->put(key, value, tranc_id);
}

void LSM::put_batch(
    const std::vector<std::pair<std::string, std::string>> &kvs) {
  auto tranc_id = tran_manager_->getNextTransactionId();
  engine->put_batch(kvs, tranc_id);
}
void LSM::remove(const std::string &key) {
  auto tranc_id = tran_manager_->getNextTransactionId();
  engine->remove(key, tranc_id);
}

void LSM::remove_batch(const std::vector<std::string> &keys) {
  auto tranc_id = tran_manager_->getNextTransactionId();
  engine->remove_batch(keys, tranc_id);
}

void LSM::clear() { engine->clear(); }

void LSM::flush() { auto max_tranc_id = engine->flush(); }

void LSM::flush_all() {
  while (engine->memtable.get_total_size() > 0) {
    auto max_tranc_id = engine->flush();
    tran_manager_->update_max_flushed_tranc_id(max_tranc_id);
  }
}

LSM::LSMIterator LSM::begin(uint64_t tranc_id) {
  return engine->begin(tranc_id);
}

LSM::LSMIterator LSM::end() { return engine->end(); }

std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
LSM::lsm_iters_monotony_predicate(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  return engine->lsm_iters_monotony_predicate(tranc_id, predicate);
}

// 开启一个事务
std::shared_ptr<TranContext>
LSM::begin_tran(const IsolationLevel &isolation_level) {
  // TODO: xx

  return {};
}

void LSM::set_log_level(const std::string &level) { reset_log_level(level); }
} // namespace tiny_lsm
