#include "../../include/lsm/engine.h"
#include "../../include/lsm/transaction.h"
#include "../../include/utils/files.h"
#include "spdlog/spdlog.h"
#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <thread>
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
                         const enum IsolationLevel &isolation_level) {
  // TODO: Lab 5.2 构造函数初始化
}

void TranContext::put(const std::string &key, const std::string &value) {
  // TODO: Lab 5.2 put 实现
}

void TranContext::remove(const std::string &key) {
  // TODO: Lab 5.2 remove 实现
}

std::optional<std::string> TranContext::get(const std::string &key) {
  // TODO: Lab 5.2 get 实现
  return {};
}

bool TranContext::commit(bool test_fail) {
  // TODO: Lab 5.2 commit 实现
  return true;
}

bool TranContext::abort() {
  // TODO: Lab 5.2 abort 实现
  return true;
}

enum IsolationLevel TranContext::get_isolation_level() {
  return isolation_level_;
}

// *********************** TranManager ***********************
TranManager::TranManager(std::string data_dir) : data_dir_(data_dir) {
  auto file_path = get_tranc_id_file_path();

  // TODO: Lab 5.2 初始化时读取持久化的事务状态信息
}

void TranManager::init_new_wal() {
  // TODO: Lab 5.x 初始化 wal
}

void TranManager::set_engine(std::shared_ptr<LSMEngine> engine) {
  engine_ = std::move(engine);
}

TranManager::~TranManager() { write_tranc_id_file(); }

void TranManager::write_tranc_id_file() {
  // TODO: Lab 5.2 持久化事务状态信息
}

void TranManager::read_tranc_id_file() {
  // TODO: Lab 5.2 读取持久化的事务状态信息
}

void TranManager::update_max_finished_tranc_id(uint64_t tranc_id) {
  // TODO: Lab 5.2 更新持久化的事务状态信息
}

void TranManager::update_max_flushed_tranc_id(uint64_t tranc_id) {
  // TODO: Lab 5.2 更新持久化的事务状态信息
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
  return nullptr;
}
std::string TranManager::get_tranc_id_file_path() {
  if (data_dir_.empty()) {
    data_dir_ = "./";
  }
  return data_dir_ + "/tranc_id";
}

std::map<uint64_t, std::vector<Record>> TranManager::check_recover() {
  // TODO: Lab 5.5
  return {};
}

bool TranManager::write_to_wal(const std::vector<Record> &records) {
  // TODO: Lab 5.4

  return true;
}

// void TranManager::flusher() {
//   while (flush_thread_running_.load()) {
//     std::this_thread::sleep_for(std::chrono::seconds(1));
//     write_tranc_id_file();
//   }
// }
} // namespace tiny_lsm