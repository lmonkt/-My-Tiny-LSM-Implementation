#include "../../include/block/blockmeta.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <string_view>

namespace tiny_lsm {
BlockMeta::BlockMeta() : offset(0), first_key(""), last_key("") {}

BlockMeta::BlockMeta(size_t offset, const std::string &first_key,
                     const std::string &last_key)
    : offset(offset), first_key(first_key), last_key(last_key) {}

void BlockMeta::encode_meta_to_slice(std::vector<BlockMeta> &meta_entries,
                                     std::vector<uint8_t> &metadata) {
  // TODO: Lab 3.4 将内存中所有`Blcok`的元数据编码为二进制字节数组
  // ? 输入输出都由参数中的引用给定, 你不需要自己创建`vector`
  // 1. 计算所需总大小
  size_t total_size = sizeof(uint32_t); // num_entries
  for (const auto &meta : meta_entries) {
    total_size += sizeof(uint32_t);      // offset
    total_size += sizeof(uint16_t) * 2;  // key length*2
    total_size += meta.first_key.size(); // first_key data
    total_size += meta.last_key.size();  // last_key data
  }
  total_size += sizeof(uint32_t); // hash

  // 2. 预分配空间
  metadata.resize(total_size);
  uint8_t *ptr = metadata.data();

  uint32_t num_entries = static_cast<uint32_t>(meta_entries.size());
  memcpy(ptr, &num_entries, sizeof(uint32_t));
  ptr += sizeof(uint32_t);

  for (auto &meta : meta_entries) {
    uint32_t offset32 = meta.offset;
    memcpy(ptr, &offset32, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // Copy the length of first_key as uint16_t
    uint16_t first_key_len = static_cast<uint16_t>(meta.first_key.size());
    memcpy(ptr, &first_key_len, sizeof(uint16_t));
    ptr += sizeof(uint16_t);

    // Copy the actual first_key string data
    memcpy(ptr, meta.first_key.data(), first_key_len);
    ptr += first_key_len;

    // Copy the length of last_key as uint16_t
    uint16_t last_key_len = static_cast<uint16_t>(meta.last_key.size());
    memcpy(ptr, &last_key_len, sizeof(uint16_t));
    ptr += sizeof(uint16_t);

    // Copy the actual last_key string data
    memcpy(ptr, meta.last_key.data(), last_key_len);
    ptr += last_key_len;
  }

  const uint8_t *data_start = metadata.data() + sizeof(uint32_t);
  const uint8_t *data_end = ptr;
  size_t data_len = data_end - data_start;
  uint32_t hash = std::hash<std::string_view>{}(
      std::string_view(reinterpret_cast<const char *>(data_start), data_len));

  memcpy(ptr, &hash, sizeof(uint32_t));
}

std::vector<BlockMeta>
BlockMeta::decode_meta_from_slice(const std::vector<uint8_t> &metadata) {
  // TODO: Lab 3.4 将二进制字节数组解码为内存中的`Blcok`元数据
  if (metadata.size() < 8) {
    throw std::runtime_error("metadata too short");
  }

  std::vector<BlockMeta> result;
  uint32_t num_entries;
  const uint8_t *ptr = metadata.data();
  memcpy(&num_entries, ptr, sizeof(uint32_t));
  ptr += sizeof(uint32_t);

  // 验证hash
  const uint8_t *data_start = ptr;
  const uint8_t *data_end =
      metadata.data() + metadata.size() - sizeof(uint32_t);
  size_t data_len = data_end - data_start;
  uint32_t stored_hash;
  memcpy(&stored_hash, data_end, sizeof(uint32_t));

  uint32_t computed_hash = std::hash<std::string_view>{}(
      std::string_view(reinterpret_cast<const char *>(data_start), data_len));

  if (stored_hash != computed_hash) {
    throw std::runtime_error("metadata hash mismatch");
  }

  result.reserve(num_entries);
  for (uint32_t i = 0; i < num_entries; ++i) {
    // 检查剩余空间是否足够读取 offset
    if (ptr + sizeof(uint32_t) > data_end) {
      throw std::runtime_error(
          "corrupted metadata: insufficient data for offset");
    }

    uint32_t block_offset;
    memcpy(&block_offset, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 检查剩余空间是否足够读取 first_key_len
    if (ptr + sizeof(uint16_t) > data_end) {
      throw std::runtime_error(
          "corrupted metadata: insufficient data for first_key_len");
    }

    uint16_t first_key_len;
    memcpy(&first_key_len, ptr, sizeof(uint16_t));
    ptr += sizeof(uint16_t);

    // 检查剩余空间是否足够读取 first_key 数据
    if (ptr + first_key_len > data_end) {
      throw std::runtime_error(
          "corrupted metadata: insufficient data for first_key");
    }

    std::string first_key(reinterpret_cast<const char *>(ptr), first_key_len);
    ptr += first_key_len;

    // 检查剩余空间是否足够读取 last_key_len
    if (ptr + sizeof(uint16_t) > data_end) {
      throw std::runtime_error(
          "corrupted metadata: insufficient data for last_key_len");
    }

    uint16_t last_key_len;
    memcpy(&last_key_len, ptr, sizeof(uint16_t));
    ptr += sizeof(uint16_t);

    // 检查剩余空间是否足够读取 last_key 数据
    if (ptr + last_key_len > data_end) {
      throw std::runtime_error(
          "corrupted metadata: insufficient data for last_key");
    }

    std::string last_key(reinterpret_cast<const char *>(ptr), last_key_len);
    ptr += last_key_len;

    result.emplace_back(BlockMeta(block_offset, first_key, last_key));
  }

  return result;
}
} // namespace tiny_lsm
