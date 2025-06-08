#include "../../include/skiplist/skiplist.h"
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <tuple>
#include <utility>

namespace tiny_lsm {

// ************************ SkipListIterator ************************
BaseIterator &SkipListIterator::operator++() {
  // TODO: Lab1.2 任务：实现SkipListIterator的++操作符
  if (this->is_end() || !this->is_valid())
    return *this;
  current = current->forward_[0];

  return *this;
}

bool SkipListIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab1.2 任务：实现SkipListIterator的==操作符
  if (this->is_end() || other.is_end()) {
    if (this->is_end() && other.is_end())
      return true;
    else
      return false;
  }
  if (current->key_ != (*other).first || current->value_ != (*other).second)
    return false;
  return true;
}

bool SkipListIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab1.2 任务：实现SkipListIterator的!=操作符
  if (this->is_end() || other.is_end()) {
    if (this->is_end() && other.is_end())
      return false;
    else
      return true;
  }
  if (current->key_ != (*other).first || current->value_ != (*other).second)
    return true;
  return false;
}

SkipListIterator::value_type SkipListIterator::operator*() const {
  // TODO: Lab1.2 任务：实现SkipListIterator的*操作符
  if (!this->is_valid())
    return {"", ""};
  return {current->key_, current->value_};
}

IteratorType SkipListIterator::get_type() const {
  // TODO: Lab1.2 任务：实现SkipListIterator的get_type
  // ? 主要是为了熟悉基类的定义和继承关系
  if (this->is_valid())
    return IteratorType::SkipListIterator;
  return IteratorType::Undefined;
}

bool SkipListIterator::is_valid() const {
  return current && !current->key_.empty();
}
bool SkipListIterator::is_end() const { return current == nullptr; }

std::string SkipListIterator::get_key() const { return current->key_; }
std::string SkipListIterator::get_value() const { return current->value_; }
uint64_t SkipListIterator::get_tranc_id() const { return current->tranc_id_; }

// ************************ SkipList ************************
// 构造函数
SkipList::SkipList(int max_lvl) : max_level(max_lvl), current_level(1) {
  head = std::make_shared<SkipListNode>("", "", max_level, 0);
  dis_01 = std::uniform_int_distribution<>(0, 1);
  dis_level = std::uniform_int_distribution<>(0, (1 << max_lvl) - 1);
  gen = std::mt19937(std::random_device()());
}

int SkipList::random_level() {
  // ? 通过"抛硬币"的方式随机生成层数：
  // ? - 每次有50%的概率增加一层
  // ? - 确保层数分布为：第1层100%，第2层50%，第3层25%，以此类推
  // ? - 层数范围限制在[1, max_level]之间，避免浪费内存
  // TODO: Lab1.1 任务：插入时随机为这一次操作确定其最高连接的链表层数
  int level = 1;
  while (dis_01(gen) == 0 && level < this->max_level) {
    ++level;
  }
  return level;
}

// 插入或更新键值对
void SkipList::put(const std::string &key, const std::string &value,
                   uint64_t tranc_id) {
  spdlog::trace("SkipList--put({}, {}, {})", key, value, tranc_id);
  std::vector<std::shared_ptr<SkipListNode>> update(max_level, nullptr);

  // 先创建一个新节点
  int new_level = std::max(random_level(), current_level);
  auto new_node =
      std::make_shared<SkipListNode>(key, value, new_level, tranc_id);

  // std::unique_lock<std::shared_mutex> lock(rw_mutex);
  auto current = head;

  // 从最高层开始查找插入位置
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward_[i] && *current->forward_[i] < *new_node) {
      current = current->forward_[i];
    }
    spdlog::trace("SkipList--put({}, {}, {}), level{} needs updating", key,
                  value, tranc_id, i);

    update[i] = current;
  }

  // 移动到最底层
  current = current->forward_[0];

  if (current && current->key_ == key && current->tranc_id_ == tranc_id) {
    // 若 key 存在且 tranc_id 相同，更新 value
    size_bytes += value.size() - current->value_.size();
    current->value_ = value;
    current->tranc_id_ = tranc_id;

    spdlog::trace("SkipList--put({}, {}, {}), key and tranc_id_ is the same, "
                  "only update value to {}",
                  key, value, tranc_id, value);

    return;
  }

  // 如果key不存在，创建新节点
  // ! 默认新的 tranc_id 一定比当前的大, 由上层保证
  if (new_level > current_level) {
    for (int i = current_level; i < new_level; ++i) {
      update[i] = head;

      spdlog::trace("SkipList--put({}, {}, {}), update level{} to head", key,
                    value, tranc_id, i);
    }
  }

  // 生成一个随机数，用于决定是否在每一层更新节点
  int random_bits = dis_level(gen);

  size_bytes += key.size() + value.size() + sizeof(uint64_t);

  // 更新各层的指针
  for (int i = 0; i < new_level; ++i) {
    bool need_update = false;
    if (i == 0 || (new_level > current_level) || (random_bits & (1 << i))) {
      // 按照如下顺序判断是否进行更新
      // 1. 第0层总是更新
      // 2. 如果需要创建新的层级, 这个节点需要再之前所有的层级上都更新
      // 3. 否则, 根据随机数的位数按照50%的概率更新
      need_update = true;
    }

    if (need_update) {
      new_node->forward_[i] = update[i]->forward_[i]; // 可能为nullptr
      if (new_node->forward_[i]) {
        new_node->forward_[i]->set_backward(i, new_node);
      }
      update[i]->forward_[i] = new_node;
      new_node->set_backward(i, update[i]);
    } else {
      // 如果不更新当前层，之后更高的层级都不更新
      break;
    }
  }

  current_level = new_level;

  // TODO: Lab1.1  任务：实现插入或更新键值对
  // ? Hint: 你需要保证不同`Level`的步长从底层到高层逐渐增加
  // ? 你可能需要使用到`random_level`函数以确定层数, 其注释中为你提供一种思路
  // ? tranc_id 为事务id, 现在你不需要关注它, 直接将其传递到 SkipListNode
  // 的构造函数中即可
}

// 查找键值对
SkipListIterator SkipList::get(const std::string &key, uint64_t tranc_id) {
  // TODO: Lab1.1 任务：实现查找键值对,
  // TODO: 并且你后续需要额外实现SkipListIterator中的TODO部分(Lab1.2)
  spdlog::trace("SkipList--get({}) called", key);
  // ? 你可以参照上面的注释完成日志输出以便于调试
  // ? 日志为输出到你执行二进制所在目录下的log文件夹
  auto current = head;
  // 从最高层开始查找
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward_[i] && current->forward_[i]->key_ < key) {
      current = current->forward_[i];
    }
  }
  // 移动到最底层
  current = current->forward_[0];
  if (tranc_id == 0) {
    // 如果没有开启事务，直接比较key即可
    // 如果找到匹配的key，返回value
    if (current && current->key_ == key) {
      return SkipListIterator{current};
    }
  } else {
    while (current && current->key_ == key) {
      // 如果开启了事务，只返回小于等于事务id的值
      if (tranc_id != 0) {
        if (current->tranc_id_ <= tranc_id) {
          // 满足事务可见性
          return SkipListIterator{current};

        } else {
          // 否则跳过
          current = current->forward_[0];
        }
      } else {
        // 没有开启事务
        return SkipListIterator{current};
      }
    }
  }
  // 未找到返回空
  spdlog::trace("SkipList--get({}): not found", key);
  return SkipListIterator{};
}

// 删除键值对
// ! 这里的 remove 是跳表本身真实的 remove,  lsm 应该使用 put 空值表示删除,
// ! 这里只是为了实现完整的 SkipList 不会真正被上层调用
void SkipList::remove(const std::string &key) {
  // TODO: Lab1.1 任务：实现删除键值对

  std::vector<std::shared_ptr<SkipListNode>> update(max_level, nullptr);
  auto current = head;
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward_[i] && current->forward_[i]->key_ < key) {
      current = current->forward_[i];
    }
    update[i] = current;
  }

  current = current->forward_[0];

  if (current && current->key_ == key) {
    for (int i = 0; i < current_level; ++i) {
      if (update[i]->forward_[i] != current) {
        break;
      }
      update[i]->forward_[i] = current->forward_[i];
    }
    for (int i = 0; i < current->backward_.size() && i < current_level; ++i) {
      if (current->forward_[i]) {
        current->forward_[i]->set_backward(i, update[i]);
      }
    }
    size_bytes -= (key.size() + current->value_.size() + sizeof(uint64_t));
    while (current_level > 1 && head->forward_[current_level - 1] == nullptr) {
      current_level--;
    }
  }
}

// 刷盘时可以直接遍历最底层链表
std::vector<std::tuple<std::string, std::string, uint64_t>> SkipList::flush() {
  // std::shared_lock<std::shared_mutex> slock(rw_mutex);
  spdlog::debug("SkipList--flush(): Starting to flush skiplist data");

  std::vector<std::tuple<std::string, std::string, uint64_t>> data;
  auto node = head->forward_[0];
  while (node) {
    data.emplace_back(node->key_, node->value_, node->tranc_id_);
    node = node->forward_[0];
  }

  spdlog::debug("SkipList--flush(): Flushed {} entries", data.size());

  return data;
}

size_t SkipList::get_size() {
  // std::shared_lock<std::shared_mutex> slock(rw_mutex);
  return size_bytes;
}

// 清空跳表，释放内存
void SkipList::clear() {
  // std::unique_lock<std::shared_mutex> lock(rw_mutex);
  head = std::make_shared<SkipListNode>("", "", max_level, 0);
  size_bytes = 0;
}

SkipListIterator SkipList::begin() {
  // return SkipListIterator(head->forward[0], rw_mutex);
  return SkipListIterator(head->forward_[0]);
}

SkipListIterator SkipList::end() {
  return SkipListIterator(); // 使用空构造函数
}

// 找到前缀的起始位置
// 返回第一个前缀匹配或者大于前缀的迭代器
SkipListIterator SkipList::begin_preffix(const std::string &preffix) {
  // TODO: Lab1.3 任务：实现前缀查询的起始位置
  auto current = head;
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward_[i] &&
           current->forward_[i]->key_.compare(0, preffix.size(), preffix, 0,
                                              preffix.size()) < 0) {
      current = current->forward_[i];
    }
  }
  current = current->forward_[0];

  return SkipListIterator{current};
}

// 找到前缀的终结位置
SkipListIterator SkipList::end_preffix(const std::string &prefix) {
  // TODO: Lab1.3 任务：实现前缀查询的终结位置
  auto current = head;
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward_[i] &&
           current->forward_[i]->key_.compare(0, prefix.size(), prefix, 0,
                                              prefix.size()) <= 0) {
      current = current->forward_[i];
    }
  }
  current = current->forward_[0];
  return SkipListIterator{current};
}

// ? 这里单调谓词的含义是, 整个数据库只会有一段连续区间满足此谓词
// ? 例如之前特化的前缀查询，以及后续可能的范围查询，都可以转化为谓词查询
// ? 返回第一个满足谓词的位置和最后一个满足谓词的迭代器
// ? 如果不存在, 范围nullptr
// ? 谓词作用于key, 且保证满足谓词的结果只在一段连续的区间内, 例如前缀匹配的谓词
// ? predicate返回值:
// ?   0: 满足谓词
// ?   >0: 不满足谓词, 需要向右移动
// ?   <0: 不满足谓词, 需要向左移动
// ! Skiplist 中的谓词查询不会进行事务id的判断, 需要上层自己进行判断
std::optional<std::pair<SkipListIterator, SkipListIterator>>
SkipList::iters_monotony_predicate(
    std::function<int(const std::string &)> predicate) {
  // TODO: Lab1.3 任务：实现谓词查询的起始位置
  auto current = head;
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward_[i] &&
           (predicate(current->forward_[i]->key_) > 0)) {
      current = current->forward_[i];
    }
  }
  current = current->forward_[0];
  SkipListIterator start(current);
  if (!start.is_end() && start.is_valid()) {
    while (current->forward_[0] &&
           (predicate(current->forward_[0]->key_) >= 0)) {
      current = current->forward_[0];
    }
    SkipListIterator end(current->forward_[0]);

    return std::make_optional(std::make_pair(start, end));

  } else
    return std::nullopt;
}

// ? 打印跳表, 你可以在出错时调用此函数进行调试
void SkipList::print_skiplist() {
  for (int level = 0; level < current_level; level++) {
    std::cout << "Level " << level << ": ";
    auto current = head->forward_[level];
    while (current) {
      std::cout << current->key_;
      current = current->forward_[level];
      if (current) {
        std::cout << " -> ";
      }
    }
    std::cout << std::endl;
  }
  std::cout << std::endl;
}
} // namespace tiny_lsm
