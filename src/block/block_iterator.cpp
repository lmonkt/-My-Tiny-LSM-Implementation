#include "../../include/block/block_iterator.h"
#include "../../include/block/block.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>

class Block;

namespace tiny_lsm {
BlockIterator::BlockIterator(std::shared_ptr<Block> b, size_t index,
                             uint64_t tranc_id)
    : block(b), current_index(index), tranc_id_(tranc_id),
      cached_value(std::nullopt) {
  skip_by_tranc_id();
}

BlockIterator::BlockIterator(std::shared_ptr<Block> b, const std::string &key,
                             uint64_t tranc_id)
    : block(b), tranc_id_(tranc_id), cached_value(std::nullopt) {
  // TODO: Lab3.2 创建迭代器时直接移动到指定的key位置
  // ? 你需要借助之前实现的 Block 类的成员函数
  if (block) {
    auto idx_opt = block->get_idx_binary(key, tranc_id);
    current_index = idx_opt ? *idx_opt : block->size();
  } else {
    current_index = 0;
  }
}

// BlockIterator::BlockIterator(std::shared_ptr<Block> b, uint64_t tranc_id)
//     : block(b), current_index(0), tranc_id_(tranc_id),
//       cached_value(std::nullopt) {
//   skip_by_tranc_id();
// }

BlockIterator::pointer BlockIterator::operator->() const {
  // TODO: Lab3.2 -> 重载
  if (!cached_value.has_value()) {
    update_current();
  }
  return &cached_value.value();
}

BlockIterator &BlockIterator::operator++() {
  // TODO: Lab3.2 ++ 重载
  // ? 在后续的Lab实现事务后，你可能需要对这个函数进行返修
  ++current_index;
  skip_by_tranc_id();          // 跳过对当前事务不可见的条目
  cached_value = std::nullopt; // 位置改变，清空缓存
  return *this;
}

bool BlockIterator::operator==(const BlockIterator &other) const {
  // TODO: Lab3.2 == 重载
  return block.get() == other.block.get() &&
         current_index == other.current_index;
}

bool BlockIterator::operator!=(const BlockIterator &other) const {
  // TODO: Lab3.2 != 重载
  return !(*this == other);
}

BlockIterator::value_type BlockIterator::operator*() const {
  // TODO: Lab3.2 * 重载
  if (!cached_value.has_value()) {
    update_current();
  }
  return cached_value.value();
}

bool BlockIterator::is_end() const {
  if (!block)
    return true;
  return current_index >= block->size();
}

void BlockIterator::update_current() const {
  // TODO: Lab3.2 更新当前指针
  if (is_end()) {
    cached_value = std::nullopt;
    return;
  }

  // 从 block 中获取当前索引位置的键值对
  size_t offset = block->get_offset_at(current_index);
  std::string key = block->get_key_at(offset);
  std::string value = block->get_value_at(offset);
  cached_value = {key, value};
}

void BlockIterator::skip_by_tranc_id() {
  // TODO: Lab3.2 * 跳过事务ID
  // ? 只是进行标记以供你在后续Lab实现事务功能后修改
  // ? 现在你不需要考虑这个函数
  if (!block)
    return;
  while (!is_end()) {
    size_t offset = block->get_offset_at(current_index); // 索引 -> 偏移
    uint64_t current_tranc_id = block->get_tranc_id_at(offset);
    if (current_tranc_id <= tranc_id_)
      break;
    ++current_index;
  }
  cached_value = std::nullopt;
}
} // namespace tiny_lsm
