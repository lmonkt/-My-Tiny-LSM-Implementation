// include/utils/bloom_filter.cpp

#include "../..//include/utils/bloom_filter.h"
#include <cstring>
#include <functional>
#include <iostream>
#include <numeric>
#include <string>

namespace toni_lsm {

BloomFilter::BloomFilter() {};

// 构造函数，初始化布隆过滤器
// expected_elements: 预期插入的元素数量
// false_positive_rate: 允许的假阳性率
BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate)
    : expected_elements_(expected_elements),
      false_positive_rate_(false_positive_rate) {
  // TODO: Lab 4.9: 初始化数组长度
}

void BloomFilter::add(const std::string &key) {
  // TODO: Lab 4.9: 添加一个记录到布隆过滤器中
}

//  如果key可能存在于布隆过滤器中，返回true；否则返回false
bool BloomFilter::possibly_contains(const std::string &key) const {
  // TODO: Lab 4.9: 检查一个记录是否可能存在于布隆过滤器中
  return false;
}

// 清空布隆过滤器
void BloomFilter::clear() { bits_.assign(bits_.size(), false); }

size_t BloomFilter::hash1(const std::string &key) const {
  std::hash<std::string> hasher;
  return hasher(key);
}

size_t BloomFilter::hash2(const std::string &key) const {
  std::hash<std::string> hasher;
  return hasher(key + "salt");
}

size_t BloomFilter::hash(const std::string &key, size_t idx) const {
  // TODO: Lab 4.9: 计算哈希值
  // ? idx 标识这是第几个哈希函数
  // ? 你需要按照某些方式, 从 hash1 和 hash2 中组合成新的哈希函数
  return 0;
}

// 编码布隆过滤器为 std::vector<uint8_t>
std::vector<uint8_t> BloomFilter::encode() {
  // TODO: Lab 4.9: 编码布隆过滤器
  return std::vector<uint8_t>();
}

// 从 std::vector<uint8_t> 解码布隆过滤器
BloomFilter BloomFilter::decode(const std::vector<uint8_t> &data) {
  BloomFilter bf;
  // TODO: Lab 4.9: 解码布隆过滤器
  return bf;
}
} // namespace toni_lsm