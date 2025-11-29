#include "../../include/lsm/engine.h"
#include "../../include/lsm/transaction.h"
#include "../../include/utils/files.h"
#include "spdlog/spdlog.h"
#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace tiny_lsm {

inline std::string isolation_level_to_string(const IsolationLevel &level) {
  switch (level) {
  case IsolationLevel::READ_UNCOMMITTED:
    return "READ_UNCOMMITTED";
  case IsolationLevel::READ_COMMITTED:
    return "READ_COMMITTED";
  case IsolationLevel::REPEATABLE_READ:
    return "REPEATABLE_READ";
  case IsolationLevel::SERIALIZABLE:
    return "SERIALIZABLE";
  default:
    return "UNKNOWN";
  }
}

// *********************** TranContext ***********************
TranContext::TranContext(uint64_t tranc_id, std::shared_ptr<LSMEngine> engine,
                         std::shared_ptr<TranManager> tranManager,
                         const enum IsolationLevel &isolation_level)
    : engine_(std::move(engine)), tranManager_(std::move(tranManager)),
      tranc_id_(tranc_id), isolation_level_(isolation_level) {
  // TODO: Lab 5.2 构造函数初始化
  operations.emplace_back(Record::createRecord(tranc_id_));
}

// REVIEW: put() 存在若干问题：
// 1) 在函数开始和结尾处都对 operations 添加了 putRecord，会导致重复的日志记录。
//    建议只在准备写 WAL 时添加一次记录，避免重复写入 WAL/恢复时混乱。
void TranContext::put(const std::string &key, const std::string &value) {
  // TODO: Lab 5.2 put 实现
  switch (isolation_level_) {
  case (IsolationLevel::READ_UNCOMMITTED): {
    rollback_map_[key] = engine_->get(key, 0);

    engine_->memtable.put(key, value, tranc_id_);
    break;
  };
  default:
    temp_map_[key] = value;
  }
  operations.emplace_back(Record::putRecord(tranc_id_, key, value));
}

void TranContext::remove(const std::string &key) {
  // TODO: Lab 5.2 remove 实现
  operations.emplace_back(Record::deleteRecord(tranc_id_, key));
  switch (isolation_level_) {
  case (IsolationLevel::READ_UNCOMMITTED): {
    rollback_map_[key] = engine_->get(key, 0);
    engine_->memtable.remove(key, tranc_id_);
    break;
  };
  default:
    temp_map_[key] = "";
  }
}

std::optional<std::string> TranContext::get(const std::string &key) {
  // TODO: Lab 5.2 get 实现
  switch (isolation_level_) {
  case (IsolationLevel::READ_UNCOMMITTED): {
    auto skipliteator = engine_->memtable.get(key, tranc_id_);
    if (skipliteator.is_valid())
      return skipliteator.get_value();
    else
      return std::nullopt;
  };
  case (IsolationLevel::READ_COMMITTED): {
    if (temp_map_.find(key) != temp_map_.end()) {
      return temp_map_[key];
    } else {
      auto skipliteator = engine_->memtable.get(key, tranc_id_);
      if (skipliteator.is_valid())
        return skipliteator.get_value();
      else
        return std::nullopt;
    }
  }
  default: {
    if (temp_map_.find(key) != temp_map_.end()) {
      return temp_map_[key];
    }
    if (read_map_.find(key) != read_map_.end()) {
      return read_map_[key]->first;
    } else {
      auto skipliteator = engine_->memtable.get(key, tranc_id_);
      if (skipliteator.is_valid()) {
        read_map_[key] = engine_->get(key, 0);
        return skipliteator.get_value();
      } else
        return std::nullopt;
    }
  }
  }
}

bool TranContext::commit(bool test_fail) {
  // TODO: Lab 5.2 commit 实现
  // REVIEW: commit 的重要流程除了把 temp_map_ 应用到 memtable/做冲突检测外，
  // 还需要把 operations（包含 commitRecord）写入
  // WAL（tranManager_->write_to_wal）， 并在 WAL 写入成功后根据 test_fail
  // 决定是否把数据真正写入 memtable（参考实现使用 memtable.put_
  // 在加锁时进行无锁写入）。最后需要调用
  // tranManager_->add_ready_to_flush_tranc_id(tranc_id_,
  // TransactionState::COMMITTED) 或在冲突/回滚时标记为 ABORTED。缺少
  // WAL/TranManager 状态更新会导致 Recover 测试失败。
  if (isCommited || isAborted) { // 防止已提交或已中止的事务再次操作
    return false;
  }

  switch (isolation_level_) {
  case (IsolationLevel::READ_UNCOMMITTED): {
    break;
  }
  case (IsolationLevel::REPEATABLE_READ):
  case (IsolationLevel::SERIALIZABLE): {
    MemTable &memtable = engine_->memtable;
    std::unique_lock<std::shared_mutex> wlock1(memtable.frozen_mtx);
    std::unique_lock<std::shared_mutex> wlock2(memtable.cur_mtx);
    for (auto &[k, v] : temp_map_) {
      auto it = memtable.get_(k, 0);
      if (it.is_valid() && it.get_tranc_id() > this->tranc_id_) {
        isAborted = true;
        return false;
      } else {
        if (tranManager_->get_max_flushed_tranc_id() <= tranc_id_) {
          // sst 中最大的 tranc_id 小于当前 tranc_id, 没有冲突
          continue;
        }
        // 否则要查询具体的key是否冲突
        // ! 注意第二个参数设置为0, 表示忽略事务可见性的查询
        auto res = engine_->sst_get_(k, 0);
        if (res.has_value()) {
          auto [v, tranc_id] = res.value();
          if (tranc_id > tranc_id_) {
            // 数据库中存在相同的 key , 且其 tranc_id 大于当前 tranc_id
            // 表示更晚创建的事务修改了相同的key, 并先提交, 发生了冲突
            // 需要终止事务
            isAborted = true;
            spdlog::warn("TranContext--commit(): SST conflict on key={}, "
                         "aborting transaction ID={}",
                         k, tranc_id_);
            return false;
          }
        }
      }
    }
    // REVIEW: 必须检查 test_fail。如果为 true（模拟崩溃），则不应写入
    // memtable。 之前缺少此检查导致 Recover
    // 测试“意外”通过（因为数据被写入了）， 而实际上在没有 WAL
    // 的情况下，test_fail=true 时数据应该丢失。
    if (!test_fail) {
      for (auto const &[key, value] : temp_map_) {
        memtable.put_(key, value, this->tranc_id_);
      }
      memtable.put_("", "", this->tranc_id_);
    }
    break;
  }
  default: {
    if (!test_fail) {
      for (auto [k, v] : temp_map_) {
        engine_->memtable.put(k, v, tranc_id_);
      }
      engine_->memtable.put("", "", tranc_id_);
    }
    break;
  }
  }
  isCommited = true;
  operations.emplace_back(Record::commitRecord(tranc_id_));
  // NOTE: 这里应当在将 commitRecord push 到 operations 后调用
  bool flag = tranManager_->write_to_wal(operations);
  // 并根据返回值决定接下来的行为（参考实现会在 写入 WAL 成功后把数据 apply 到
  // memtable 或在 test_fail 模式下不 apply 以模拟崩溃）。
  return true;
}

bool TranContext::abort() {
  // TODO: Lab 5.2 abort 实现
  if (isAborted)
    return false;
  switch (isolation_level_) {
  case (IsolationLevel::READ_UNCOMMITTED): {
    for (auto &[k, pair] : rollback_map_) {
      if (!pair)
        continue;
      engine_->memtable.put(k, pair->first, pair->second);
    }
    break;
  };
  default: {
    temp_map_.clear();
  }
  }
  isAborted = true;
  operations.emplace_back(Record::rollbackRecord(tranc_id_));
  // NOTE: 如果希望把 rollback 也记录到 WAL，需要在此处调用
  // tranManager_->write_to_wal。
  return true;
}

enum IsolationLevel TranContext::get_isolation_level() {
  return isolation_level_;
}

// *********************** TranManager ***********************
TranManager::TranManager(std::string data_dir) : data_dir_(data_dir) {
  auto file_path = get_tranc_id_file_path();
  // TODO: Lab 5.2 初始化时读取持久化的事务状态信息
  if (!std::filesystem::exists(file_path)) {
    tranc_id_file_ = FileObj::open(file_path, true);
  } else {
    tranc_id_file_ = FileObj::open(file_path, false);
    read_tranc_id_file();
  }
  init_new_wal();
}

void TranManager::init_new_wal() {
  // TODO: Lab 5.x 初始化 wal
  wal = std::make_shared<WAL>(data_dir_, 1024, max_finished_tranc_id_, 1,
                              1024 * 1024);
}

void TranManager::set_engine(std::shared_ptr<LSMEngine> engine) {
  engine_ = std::move(engine);
}

TranManager::~TranManager() { write_tranc_id_file(); }

void TranManager::write_tranc_id_file() {
  // TODO: Lab 5.2 持久化事务状态信息
  // REVIEW: 参考实现的持久化格式为：
  // [nextTransactionId(uint64)][num_flushed_ids(uint64)][flushed_id1][flushed_id2]...
  // 你的当前实现写入固定 3 个 uint64（next, max_flushed,
  // max_finished），与参考格式不一致。 如果其他模块（如 WAL.recover 或
  // TranManager::read_tranc_id_file）按照参考格式读取，
  // 会导致解析错误/恢复失败。建议改为写入 flushedTrancIds_ 集合的序列化表示。

  size_t total_size = sizeof(uint64_t) * 3;
  std::vector<uint8_t> res(total_size, 0);
  uint8_t *pos = res.data();
  auto id = nextTransactionId_.load();
  memcpy(pos, &id, sizeof(uint64_t));
  pos += sizeof(uint64_t);

  id = max_flushed_tranc_id_.load();
  memcpy(pos, &id, sizeof(uint64_t));
  pos += sizeof(uint64_t);

  id = max_finished_tranc_id_.load();
  memcpy(pos, &id, sizeof(uint64_t));

  tranc_id_file_.write(0, res);
}

void TranManager::read_tranc_id_file() {
  // TODO: Lab 5.2 读取持久化的事务状态信息
  // REVIEW: read 的解析逻辑必须和 write_tranc_id_file
  // 使用相同格式。参考实现会先读 nextTransactionId，再读集合大小，然后读取
  // flushedTrancIds_。当前的固定三 uint64
  // 的方案与参考实现不匹配，会导致恢复失败。若采用参考格式，应在此按集合格式解析。
  if (tranc_id_file_.size() == 0) {
    return;
  }
  const std::vector<uint8_t> &data =
      tranc_id_file_.read_to_slice(0, sizeof(uint64_t) * 3);
  const uint8_t *pos = data.data();
  const uint8_t *end = data.data() + data.size();
  if (pos + sizeof(uint64_t) * 3 > end) {
    throw std::runtime_error("Data too small for header");
  }
  uint64_t num;
  memcpy(&num, pos, sizeof(uint64_t));
  pos += sizeof(uint64_t);
  nextTransactionId_ = num;

  memcpy(&num, pos, sizeof(uint64_t));
  pos += sizeof(uint64_t);
  max_flushed_tranc_id_ = num;

  memcpy(&num, pos, sizeof(uint64_t));
  max_finished_tranc_id_ = num;
}

void TranManager::update_max_finished_tranc_id(uint64_t tranc_id) {
  // TODO: Lab 5.2 更新持久化的事务状态信息
  // REVIEW: 如果你采用的是参考实现的
  // flushedTrancIds_（集合）格式，则更新持久化需 修改集合并写入文件；单纯设置
  // max_finished_tranc_id_ 原子变量可能不足以保持一致性。
  max_finished_tranc_id_.store(tranc_id);
  write_tranc_id_file();
}

void TranManager::update_max_flushed_tranc_id(uint64_t tranc_id) {
  // TODO: Lab 5.2 更新持久化的事务状态信息
  // REVIEW: 同上，若采用 flushedTrancIds_ 集合，需要在此更新集合并写入文件。
  max_flushed_tranc_id_.store(tranc_id);
  write_tranc_id_file();
}

uint64_t TranManager::getNextTransactionId() {
  return nextTransactionId_.fetch_add(1, std::memory_order_relaxed);
}

uint64_t TranManager::get_max_flushed_tranc_id() {
  return max_flushed_tranc_id_.load();
}

uint64_t TranManager::get_max_finished_tranc_id_() {
  return max_finished_tranc_id_.load();
}

std::shared_ptr<TranContext>
TranManager::new_tranc(const IsolationLevel &isolation_level) {
  // TODO: Lab 5.2 事务上下文分配
  // REVIEW: 实现上看起来合理：加锁、分配 tranc_id、创建 TranContext 并登记到
  // activeTrans_ 中。如果要和参考实现行为完全一致，请补充调试日志（可选）并确保
  // getNextTransactionId 的内存顺序与参考一致（这里使用 fetch_add 的 relaxed
  // 也可，但要注意多线程语义）。
  std::lock_guard<std::mutex> lock(mutex_);

  uint64_t tranc_id = getNextTransactionId();

  // 创建事务上下文
  auto context = std::make_shared<TranContext>(
      tranc_id, engine_, shared_from_this(), isolation_level);

  // 记录活跃事务
  activeTrans_[tranc_id] = context;

  return context;
}
std::string TranManager::get_tranc_id_file_path() {
  if (data_dir_.empty()) {
    data_dir_ = "./";
  }
  return data_dir_ + "/tranc_id";
}

std::map<uint64_t, std::vector<Record>> TranManager::check_recover() {
  // TODO: Lab 5.5
  return WAL::recover(data_dir_, max_flushed_tranc_id_);
}

bool TranManager::write_to_wal(const std::vector<Record> &records) {
  // TODO: Lab 5.4
  if (wal == nullptr) {
    return false;
  }
  wal->log(records, true);
  return true;
}

// void TranManager::flusher() {
//   while (flush_thread_running_.load()) {
//     std::this_thread::sleep_for(std::chrono::seconds(1));
//     write_tranc_id_file();
//   }
// }
} // namespace tiny_lsm
