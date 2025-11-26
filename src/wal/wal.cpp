// src/wal/wal.cpp

#include "../../include/wal/wal.h"
#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <vector>

namespace tiny_lsm {

// 从零开始的初始化流程
WAL::WAL(const std::string &log_dir, size_t buffer_size,
         uint64_t max_finished_tranc_id, uint64_t clean_interval,
         uint64_t file_size_limit)
    : file_size_limit_(file_size_limit), buffer_size_(buffer_size),
      max_finished_tranc_id_(max_finished_tranc_id),
      clean_interval_(clean_interval), stop_cleaner_(false) {
  // TODO Lab 5.4 : 实现WAL的初始化流程
  if (!std::filesystem::exists(log_dir)) {
    std::filesystem::create_directories(log_dir);
  }
  active_log_path_ = (std::filesystem::path(log_dir) / "wal.log").string();
  log_file_ = FileObj::open(active_log_path_, true);
  cleaner_thread_ = std::thread(&WAL::cleaner, this);
}

WAL::~WAL() {
  // TODO Lab 5.4 : 实现WAL的清理流程
  // TODO Lab 5.4 : 实现WAL的清理流程

  // 1. 处理线程
  if (cleaner_thread_.joinable()) {
    // 如果有清理线程在运行，等待它结束
    // 注意：这里需要确保cleaner()函数有退出机制
    // 比如通过atomic标志位让cleaner()自己退出
    stop_cleaner_ = true;
    cleaner_thread_.join();
  }
  // 2.获取锁，确保没有其他操作在进行
  std::lock_guard<std::mutex> lock(mutex_);

  // 3. 强制刷新缓冲区中的剩余数据
  if (!log_buffer_.empty()) {
    // 这里应该调用flush()，但注意flush()内部也会获取锁
    // 由于我们已经持有锁，需要避免死锁
    // 可以创建一个不锁的flush实现，或者在这里直接写文件
    flush(); // 假设flush能处理重入问题
  }

  // 4. 文件对象会自己关闭，不需要手动操作
  // FileObj的析构函数会自动关闭文件
}

std::map<uint64_t, std::vector<Record>>
WAL::recover(const std::string &log_dir, uint64_t max_flushed_tranc_id) {
  // TODO: Lab 5.5 检查需要重放的WAL日志
  return {};
}

void WAL::log(const std::vector<Record> &records, bool force_flush) {
  // TODO Lab 5.4 : 实现WAL的写入流程
  std::lock_guard<std::mutex> lock(mutex_);
  if (buffer_size_ < log_buffer_.size() + records.size()) {
    throw std::runtime_error("WAL Buffer File Size has arrived limt");
  }
  log_buffer_.insert(log_buffer_.end(), records.begin(), records.end());
  if (force_flush) {
    flush();
  }
}

// commit 时 强制写入
void WAL::flush() {
  // TODO Lab 5.4 : 强制刷盘
  // ? 取决于你的 log 实现是否使用了缓冲区或者异步的实现
  if (file_size_limit_ < log_file_.size() + log_buffer_.size()) {
    throw std::runtime_error("WAL File Size has arrived limt");
  }
  std::vector<uint8_t> res;
  for (auto &record : log_buffer_) {
    auto data = record.encode();
    res.insert(res.end(), data.begin(), data.end());
  }
  log_file_.write(log_file_.size(), res);
  if (!log_file_.sync()) {
    throw std::runtime_error("WAL File Flush sync raise error");
  };
  log_buffer_.clear();
}

void WAL::cleaner() {
  // TODO Lab 5.4 : 实现WAL的清理线程
  while (!stop_cleaner_) {
    std::this_thread::sleep_for(std::chrono::seconds(clean_interval_));
    // Lab 5.5 再实现真正的清理逻辑
  }
}

} // namespace tiny_lsm
