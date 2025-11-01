// include/utils/bloom_filter.cpp

#include "../..//include/utils/bloom_filter.h"
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

namespace tiny_lsm {

BloomFilter::BloomFilter(){};

// 构造函数，初始化布隆过滤器
// expected_elements: 预期插入的元素数量
// false_positive_rate: 允许的假阳性率
BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate)
    : expected_elements_(expected_elements),
      false_positive_rate_(false_positive_rate) {
  // TODO: Lab 4.9: 初始化数组长度
  num_bits_ = -(expected_elements_ * std::log(false_positive_rate_) /
                (std::log(2.0) * std::log(2.0)));
  num_hashes_ =
      static_cast<double>(num_bits_) / expected_elements_ * std::log(2.0);
  bits_.resize(num_bits_, false);
}

void BloomFilter::add(const std::string &key) {
  // TODO: Lab 4.9: 添加一个记录到布隆过滤器中
  for (size_t i = 0; i < num_hashes_; ++i) {
    size_t idx = hash(key, i) % num_bits_;
    bits_[idx] = 1;
  }
}

//  如果key可能存在于布隆过滤器中，返回true；否则返回false
bool BloomFilter::possibly_contains(const std::string &key) const {
  // TODO: Lab 4.9: 检查一个记录是否可能存在于布隆过滤器中
  for (size_t i = 0; i < num_hashes_; ++i) {
    size_t idx = hash(key, i) % num_bits_;
    if (bits_[idx] != 1)
      return false;
  }
  return true;
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
  auto tmp = hash2(key);
  return (hash1(key) + idx * (tmp != 0 ? tmp : 1));
}

// 编码布隆过滤器为 std::vector<uint8_t>
std::vector<uint8_t> BloomFilter::encode() {
  // TODO: Lab 4.9: 编码布隆过滤器
  size_t header_size = sizeof(size_t) + sizeof(double);
  size_t bitmap_bytes = (num_bits_ + 7) / 8; // 向上取整
  size_t total_bytes = header_size + bitmap_bytes;

  std::vector<uint8_t> bytes(total_bytes, 0);
  uint8_t *ptr = bytes.data();

  // 2. 依次编码头部字段，注意指针偏移
  memcpy(ptr, &expected_elements_, sizeof(size_t));
  ptr += sizeof(size_t);
  memcpy(ptr, &false_positive_rate_, sizeof(double));
  ptr += sizeof(double);

  // 3. 编码位图数据
  for (size_t i = 0; i < num_bits_; ++i) {
    if (bits_[i]) {
      // 使用 |= 来设置位，而不是覆盖
      ptr[i / 8] |= (1U << (i % 8));
    }
  }
  return bytes;
}

// 从 std::vector<uint8_t> 解码布隆过滤器
BloomFilter BloomFilter::decode(const std::vector<uint8_t> &data) {
  // TODO: Lab 4.9: 解码布隆过滤器
  if (data.size() < sizeof(size_t) + sizeof(double)) {
    throw std::runtime_error("Encoded data too small for BloomFilter header");
  }
  // 1. 依次解码头部字段
  const uint8_t *ptr = data.data();
  size_t expected_elements;
  double false_positive_rate;

  memcpy(&expected_elements, ptr, sizeof(size_t));
  ptr += sizeof(size_t);
  memcpy(&false_positive_rate, ptr, sizeof(double));
  ptr += sizeof(double);

  // 2. 使用解码出的参数构造一个BloomFilter对象
  //    构造函数会自动计算正确的 num_bits_ 和 num_hashes_
  BloomFilter bf(expected_elements, false_positive_rate);

  // 3. 验证数据长度是否足够存放位图
  size_t bitmap_bytes_expected = (bf.num_bits_ + 7) / 8;
  size_t header_size = sizeof(size_t) + sizeof(double);
  if (data.size() - header_size < bitmap_bytes_expected) {
    throw std::runtime_error("Encoded data too small for bitmap");
  }

  // 4. 解码位图
  // bf.bits_ 已经在构造函数中被 resize 过了
  for (size_t i = 0; i < bf.num_bits_; ++i) {
    // 从ptr（指向位图数据的起始位置）开始解码
    uint8_t byte = ptr[i / 8];
    bf.bits_[i] = (byte >> (i % 8)) & 1;
  }

  return bf;
}
} // namespace tiny_lsm
