// src/wal/wal.cpp

#include "../../include/wal/wal.h"
#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <vector>

namespace tiny_lsm {

// 从零开始的初始化流程
WAL::WAL(const std::string &log_dir, size_t buffer_size,
         uint64_t max_finished_tranc_id, uint64_t clean_interval,
         uint64_t file_size_limit) {
  // TODO Lab 5.4 : 实现WAL的初始化流程
}

WAL::~WAL() {
  // TODO Lab 5.4 : 实现WAL的清理流程
}

std::map<uint64_t, std::vector<Record>>
WAL::recover(const std::string &log_dir, uint64_t max_flushed_tranc_id) {
  // TODO: Lab 5.5 检查需要重放的WAL日志
  return {};
}

void WAL::log(const std::vector<Record> &records, bool force_flush) {
  // TODO Lab 5.4 : 实现WAL的写入流程
}

// commit 时 强制写入
void WAL::flush() {
  // TODO Lab 5.4 : 强制刷盘
  // ? 取决于你的 log 实现是否使用了缓冲区或者异步的实现
}

void WAL::cleaner() {
  // TODO Lab 5.4 : 实现WAL的清理线程
}

} // namespace tiny_lsm