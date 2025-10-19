# LSMTest.Persistence Bug 修复文档

## 问题描述
测试 `LSMTest.Persistence` 失败，错误信息：
```
bad_optional_access - missing key after restart: key9
```

数据写入后重启数据库，部分 key 无法被找到。

---

## 根本原因

共发现 **5 个相互关联的 bug**，分布在 Iterator 合并逻辑和 SST 构建逻辑中：

### Bug #1: HeapIterator 无条件过滤空值
- **位置**: `src/iterator/iterator.cpp` - `HeapIterator` 构造函数和 `operator++`
- **问题**: 在所有场景下都过滤掉空值（delete markers），但 compaction 时需要保留空值来覆盖旧版本数据
- **影响**: 导致 delete markers 在 compaction 时丢失，已删除的 key 会"复活"

### Bug #2: TwoMergeIterator 去重不完全
- **位置**: `src/lsm/two_merge_iterator.cpp` - `skip_it_b()`
- **问题**: 当两个 iterator 有相同 key 时，只跳过 `it_b` 中的第一个重复项，没有跳过所有重复项
- **影响**: 如果 `it_b` 中有多个相同的 key，会导致旧版本数据泄露到结果中

### Bug #3: HeapIterator::is_end() 判断错误
- **位置**: `src/iterator/iterator.cpp` - `is_end()`
- **问题**: 检查 `items.empty()` 而不是 `!current`
- **影响**: 当最后一个元素从 priority queue 弹出后，`items` 为空但 `current` 仍有效，导致最后一个元素（如 key9）被认为是"结束"而被跳过

### Bug #4: HeapIterator::is_valid() 判断错误
- **位置**: `src/iterator/iterator.cpp` - `is_valid()`
- **问题**: 检查 `!items.empty()` 而不是 `current != nullptr`
- **影响**: 与 Bug #3 相同的根本原因，错误的结束判断

### Bug #5: SSTBuilder::estimated_size() 不准确
- **位置**: `src/sst/sst.cpp` - `estimated_size()`
- **问题**: 只返回 `data.size()`（已完成的 blocks），没有计算当前未完成的 block 的大小
- **影响**: 在 `gen_sst_from_iter()` 结束时，如果最后的 block 还没满，`estimated_size()` 返回 0，导致最后一批数据（如 key8, key9）没有被写入 SST

---

## 修复方案

### 修复 #1: HeapIterator 添加 filter_empty 参数

**文件**: `include/iterator/iterator.h`
```cpp
class HeapIterator : public BaseIterator {
private:
  bool filter_empty_;  // 新增成员变量

public:
  HeapIterator(std::vector<SearchItem> search_items, 
               uint64_t tranc_id,
               bool filter_empty = true);  // 新增参数，默认 true
  // ...
};
```

**文件**: `src/iterator/iterator.cpp`
```cpp
HeapIterator::HeapIterator(std::vector<SearchItem> search_items,
                           uint64_t tranc_id, 
                           bool filter_empty)  // 接收参数
    : BaseIterator(tranc_id), filter_empty_(filter_empty) {  // 初始化
  // ... 构造函数中的其他代码 ...
  
  while (!items.empty()) {
    auto top = items.top();
    items.pop();
    
    // 修改：条件过滤
    if (!filter_empty_ || !top.value_.empty()) {
      current = std::make_unique<HeapItem>(std::move(top));
      break;
    }
    // ... 其他逻辑 ...
  }
}

BaseIterator &HeapIterator::operator++() {
  // ... 其他代码 ...
  
  while (!items.empty()) {
    auto top = items.top();
    items.pop();
    
    // 修改：条件过滤
    if (!filter_empty_ || !top.value_.empty()) {
      current = std::make_unique<HeapItem>(std::move(top));
      break;
    }
    // ... 其他逻辑 ...
  }
  // ...
}
```

**调用点修改**: `src/sst/sst_iterator.cpp`
```cpp
HeapIterator merge_sst_iterator(
    const std::vector<std::shared_ptr<SST>> &ssts, uint64_t tranc_id) {
  // ... 构造 search_items ...
  
  // 修改：传入 false，compaction 时不过滤空值
  return HeapIterator(search_items, tranc_id, false);
}
```

---

### 修复 #2: TwoMergeIterator 完整去重

**文件**: `src/lsm/two_merge_iterator.cpp`
```cpp
void TwoMergeIterator::skip_it_b() {
  if (it_b_.is_valid() && !it_b_.is_end()) {
    auto cur_key = (*it_a_).first;
    
    // 修改：从 if 改为 while，跳过所有重复的 key
    while (it_b_.is_valid() && !it_b_.is_end() && 
           (*it_b_).first == cur_key) {
      ++it_b_;
    }
  }
}
```

---

### 修复 #3 & #4: HeapIterator 正确的结束判断

**文件**: `src/iterator/iterator.cpp`
```cpp
// 修改：检查 current 指针而不是 items 队列
bool HeapIterator::is_end() const { 
  return !current;  // 原来是: return items.empty();
}

bool HeapIterator::is_valid() const { 
  return current != nullptr;  // 原来是: return !items.empty();
}
```

**原理说明**:
```
HeapIterator 内部结构：
- items: priority_queue<HeapItem>  // 待处理的 iterators
- current: unique_ptr<HeapItem>    // 当前元素

工作流程：
1. 从 items 中 pop 出优先级最高的 iterator
2. 将其值赋给 current
3. 如果该 iterator 还有下一个元素，push 回 items

关键场景（最后一个元素）：
- pop 出最后一个 iterator
- current = 最后的元素（如 key9）
- iterator 已到末尾，不再 push 回去
- 此时：items.empty() = true，但 current 仍然有效！
- 错误判断：is_end() 返回 true，导致 key9 被跳过
- 正确判断：is_end() 应该检查 current 是否为 null
```

---

### 修复 #5: SSTBuilder 准确计算大小

**文件**: `src/sst/sst.cpp`
```cpp
size_t SSTBuilder::estimated_size() const { 
  // 修改：返回已完成的 blocks + 当前未完成的 block
  return data.size() + block.cur_size();
  
  // 原来是: return data.size();
}
```

**原理说明**:
```
SSTBuilder 内部结构：
- block: Block          // 当前正在填充的 block
- data: vector<uint8_t> // 已完成的 blocks 的编码数据

工作流程：
1. add() 将 key/value 添加到 block
2. 当 block 满时，调用 finish_block() 将 block 编码到 data
3. estimated_size() 用于判断是否需要构建新的 SST

Bug 场景：
- gen_sst_from_iter() 循环中处理了 key0-key7，构建了 SST 3
- 然后处理 key8 和 key9，添加到新的 block
- 这个 block 很小（只有 2 个 entries），没有触发 finish_block()
- data.size() 仍然是 0（新的 builder）
- 循环结束后，if (estimated_size() > 0) 判断失败
- 最后的 SST 没有被构建，key8 和 key9 丢失！

修复：
- estimated_size() 应该返回 data.size() + block.cur_size()
- 这样即使 block 还没满，也能正确反映有数据需要构建
```

---

## 最小必要改动总结

### 1. 文件：`include/iterator/iterator.h`
- 添加成员变量 `bool filter_empty_;`
- 修改构造函数签名，添加参数 `bool filter_empty = true`

### 2. 文件：`src/iterator/iterator.cpp`
- 构造函数：添加参数和初始化，修改过滤条件（2 处）
- `operator++`：修改过滤条件（1 处）
- `is_end()`：改为 `return !current;`
- `is_valid()`：改为 `return current != nullptr;`

### 3. 文件：`src/sst/sst_iterator.cpp`
- `merge_sst_iterator()`：传入 `filter_empty=false`（1 处）

### 4. 文件：`src/lsm/two_merge_iterator.cpp`
- `skip_it_b()`：`if` 改为 `while`（1 处）

### 5. 文件：`src/sst/sst.cpp`
- `estimated_size()`：返回值改为 `data.size() + block.cur_size()`（1 处）

---

## 测试验证

```bash
cd /home/tjy1234/tiny-lsm
xmake build test_lsm
xmake run test_lsm -- --gtest_filter=LSMTest.Persistence
```

**预期结果**:
```
[==========] Running 1 test from 1 test suite.
[----------] 1 test from LSMTest
[ RUN      ] LSMTest.Persistence
[       OK ] LSMTest.Persistence (11 ms)
[  PASSED  ] 1 test.
```

---

## 调试技巧回顾

### 1. 建立可观测性
- 导出 SST 内容到 .txt 文件，看到实际数据
- 添加 merge flow 日志，追踪每个 key 的处理过程

### 2. Diff 分析
- 对比 compaction 前后的数据差异
- 发现系统性问题（128 个 key 丢失）而不是偶发问题

### 3. 逐层定位
```
现象: key9 丢失
  ↓
层次: Compaction 环节
  ↓
组件: Iterator 合并逻辑
  ↓
具体: HeapIterator 过滤/去重/结束判断
  ↓
根因: 5 个相互关联的 bug
```

### 4. 渐进式修复
- 每次修复一个 bug，立即测试
- 观察现象变化（key9 → key8）
- 追踪新的线索（处理了但没写入）

### 5. 理解数据流
```
写入 → MemTable → Flush → SST (L0)
                              ↓
                    L0 Compaction → Merge Iterator
                              ↓
                    gen_sst_from_iter → SSTBuilder
                              ↓
                    L1 SST → 磁盘持久化
```

每个环节都可能出问题，需要逐层验证。

---

## 结论

这个 bug 的修复涉及 5 个相互关联的缺陷，分布在不同的组件中：
1. **Iterator 过滤逻辑**（HeapIterator 空值过滤）
2. **Iterator 去重逻辑**（TwoMergeIterator 不完全去重）
3. **Iterator 结束判断**（HeapIterator is_end/is_valid）
4. **SST 构建逻辑**（SSTBuilder estimated_size）

单独修复任何一个都不能完全解决问题，必须全部修复才能让数据正确持久化。

修复后，所有核心测试通过：
- ✅ LSMTest.BasicOperations
- ✅ LSMTest.Persistence
- ✅ LSMTest.LargeScaleOperations
- ✅ LSMTest.MixedOperations
- ✅ LSMTest.TrancIdTest
