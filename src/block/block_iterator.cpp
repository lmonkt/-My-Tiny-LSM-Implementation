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

  auto get_key_at_index = [&](size_t idx) -> std::string {
    size_t off = block->get_offset_at(idx);
    return block->get_key_at(off);
  };
  auto get_tid_at_index = [&](size_t idx) -> uint64_t {
    size_t off = block->get_offset_at(idx);
    return block->get_tranc_id_at(off);
  };

  while (!is_end()) {
    if (tranc_id_ == 0) {
      // 未开启事务：对相邻相同 key 去重，只保留每组第一个（最大 tranc_id）
      if (current_index > 0) {
        std::string prev_key = get_key_at_index(current_index - 1);
        std::string cur_key = get_key_at_index(current_index);
        if (cur_key == prev_key) {
          // 跳过这一组剩余的重复 key
          while (current_index < block->size() &&
                 get_key_at_index(current_index) == prev_key) {
            ++current_index;
          }
          continue; // 继续检查下一组
        }
      }
      break; // 当前是这一组的第一个，保留
    } else {
      // 开启事务视图：每组选择首个 tranc_id <=
      // 视图的版本；若整组不可见，跳过整组 若当前落在组内（与前一个 key
      // 相同），先跳到下一组起点
      if (current_index > 0) {
        std::string prev_key = get_key_at_index(current_index - 1);
        std::string cur_key = get_key_at_index(current_index);
        if (cur_key == prev_key) {
          while (current_index < block->size() &&
                 get_key_at_index(current_index) == prev_key) {
            ++current_index;
          }
          continue;
        }
      }

      // 现在在某组的起点，扫描该组找第一个可见版本
      std::string group_key = get_key_at_index(current_index);
      size_t idx = current_index;
      bool found = false;
      while (idx < block->size() && get_key_at_index(idx) == group_key) {
        if (get_tid_at_index(idx) <= tranc_id_) {
          current_index = idx;
          found = true;
          break;
        }
        ++idx;
      }
      if (found)
        break;

      // 该组在当前视图下不可见，跳到下一组
      current_index = idx;
      continue;
    }
  }
  cached_value = std::nullopt;
}
} // namespace tiny_lsm
