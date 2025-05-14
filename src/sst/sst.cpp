#include "../../include/sst/sst.h"
#include "../../include/config/config.h"
#include "../../include/consts.h"
#include "../../include/sst/sst_iterator.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <stdexcept>

namespace toni_lsm {

// **************************************************
// SST
// **************************************************

std::shared_ptr<SST> SST::open(size_t sst_id, FileObj file,
                               std::shared_ptr<BlockCache> block_cache) {
  // TODO Lab 3.6 打开一个SST文件, 返回一个描述类

  return nullptr;
}

void SST::del_sst() { file.del_file(); }

std::shared_ptr<SST> SST::create_sst_with_meta_only(
    size_t sst_id, size_t file_size, const std::string &first_key,
    const std::string &last_key, std::shared_ptr<BlockCache> block_cache) {
  auto sst = std::make_shared<SST>();
  sst->file.set_size(file_size);
  sst->sst_id = sst_id;
  sst->first_key = first_key;
  sst->last_key = last_key;
  sst->meta_block_offset = 0;
  sst->block_cache = block_cache;

  return sst;
}

std::shared_ptr<Block> SST::read_block(size_t block_idx) {
  // TODO: Lab 3.6 根据 block 的 id 读取一个 `Block`
  return nullptr;
}

size_t SST::find_block_idx(const std::string &key) {
  // 先在布隆过滤器判断key是否存在
  // TODO: Lab 3.6 二分查找
  // ? 给定一个 `key`, 返回其所属的 `block` 的索引
  // ? 如果没有找到包含该 `key` 的 Block，返回-1
  return 0;
}

SstIterator SST::get(const std::string &key, uint64_t tranc_id) {
  // TODO: Lab 3.6 根据查询`key`返回一个迭代器
  // ? 如果`key`不存在, 返回一个无效的迭代器即可
  throw std::runtime_error("Not implemented");
}

size_t SST::num_blocks() const { return meta_entries.size(); }

std::string SST::get_first_key() const { return first_key; }

std::string SST::get_last_key() const { return last_key; }

size_t SST::sst_size() const { return file.size(); }

size_t SST::get_sst_id() const { return sst_id; }

SstIterator SST::begin(uint64_t tranc_id) {
  // TODO: Lab 3.6 返回起始位置迭代器
  throw std::runtime_error("Not implemented");
}

SstIterator SST::end() {
  // TODO: Lab 3.6 返回终止位置迭代器
  throw std::runtime_error("Not implemented");
}

std::pair<uint64_t, uint64_t> SST::get_tranc_id_range() const {
  return std::make_pair(min_tranc_id_, max_tranc_id_);
}

// **************************************************
// SSTBuilder
// **************************************************

SSTBuilder::SSTBuilder(size_t block_size, bool has_bloom) : block(block_size) {
  // 初始化第一个block
  if (has_bloom) {
    bloom_filter = std::make_shared<BloomFilter>(
        TomlConfig::getInstance().getBloomFilterExpectedSize(),
        TomlConfig::getInstance().getBloomFilterExpectedErrorRate());
  }
  meta_entries.clear();
  data.clear();
  first_key.clear();
  last_key.clear();
}

void SSTBuilder::add(const std::string &key, const std::string &value,
                     uint64_t tranc_id) {
  // TODO: Lab 3.5 添加键值对
}

size_t SSTBuilder::estimated_size() const { return data.size(); }

void SSTBuilder::finish_block() {
  // TODO: Lab 3.5 构建块
  // ? 当 add
  // 函数发现当前的`block`容量超出阈值时，需要将其编码到`data`，并清空`block`
}

std::shared_ptr<SST>
SSTBuilder::build(size_t sst_id, const std::string &path,
                  std::shared_ptr<BlockCache> block_cache) {
  // TODO 3.5 构建一个SST
  return nullptr;
}
} // namespace toni_lsm