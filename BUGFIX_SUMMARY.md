# LSMTest.Persistence Bug 修复总结（精简版）

## 问题
数据写入后重启数据库，部分 key（如 key9）无法被找到。

---

## 5 个 Bug 及修复

### 1️⃣ HeapIterator 无条件过滤空值 → 添加 filter_empty 参数
```cpp
// include/iterator/iterator.h + src/iterator/iterator.cpp
class HeapIterator {
  bool filter_empty_;  // 新增
public:
  HeapIterator(..., bool filter_empty = true);  // 新增参数
};

// 构造函数和 operator++ 中
if (!filter_empty_ || !top.value_.empty()) {  // 条件过滤
  current = std::make_unique<HeapItem>(std::move(top));
}

// src/sst/sst_iterator.cpp - compaction 时不过滤
return HeapIterator(search_items, tranc_id, false);
```

### 2️⃣ TwoMergeIterator 只跳过一个重复项 → 改为 while 循环
```cpp
// src/lsm/two_merge_iterator.cpp
void TwoMergeIterator::skip_it_b() {
  while (it_b_.is_valid() && !it_b_.is_end() &&  // if → while
         (*it_b_).first == cur_key) {
    ++it_b_;
  }
}
```

### 3️⃣ 4️⃣ HeapIterator 错误的结束判断 → 检查 current 而不是 items
```cpp
// src/iterator/iterator.cpp
bool is_end() const { 
  return !current;  // 原: return items.empty();
}

bool is_valid() const { 
  return current != nullptr;  // 原: return !items.empty();
}
```

**原理**: 最后一个元素 pop 出后，`items` 为空但 `current` 仍有效！

### 5️⃣ SSTBuilder::estimated_size() 只算已完成的 blocks → 加上当前 block
```cpp
// src/sst/sst.cpp
size_t SSTBuilder::estimated_size() const { 
  return data.size() + block.cur_size();  // 原: return data.size();
}
```

**原理**: 最后几个 key 在未满的 block 中，`data.size()=0` 导致最后的 SST 没有被构建！

## 涉及文件（5 个）
1. `include/iterator/iterator.h` - 添加 filter_empty_ 成员
2. `src/iterator/iterator.cpp` - 修改 HeapIterator（4 处改动）
3. `src/sst/sst_iterator.cpp` - 传入 filter_empty=false
4. `src/lsm/two_merge_iterator.cpp` - if 改 while
5. `src/sst/sst.cpp` - estimated_size() 加上 block.cur_size()

## 调试过程
1. **导出 SST 内容** → 发现 128 个 key 丢失（系统性问题）
2. **添加 merge flow 日志** → 发现 key9 没有被处理
3. **修复 Bug #1, #2** → key9 仍然丢失
4. **深入分析日志** → key9 被处理了但在此之前循环就结束了
5. **修复 Bug #3, #4** → 缺失 key 变成 key8（进步了！）
6. **再次分析日志** → key8, key9 都被处理但没有写入 SST
7. **追踪 estimated_size()** → 发现没有计算当前 block
8. **修复 Bug #5** → 测试通过！✅

## 核心洞察
```
错误信号的演变：
key9 丢失 → (修复 is_end) → key8 丢失 → (修复 estimated_size) → 全部通过

每次修复后现象变化，说明方向正确！
```

## 验证
```bash
xmake build test_lsm
xmake run test_lsm -- --gtest_filter=LSMTest.Persistence
# [  PASSED  ] LSMTest.Persistence ✅
```

---

## 附加说明

### 调试功能
为了方便定位此类问题，代码中保留了 SST 导出功能，但通过环境变量控制：

```bash
# 默认关闭（正常运行，零性能开销）
xmake run test_lsm

# 需要导出调试信息时启用
LSM_EXPORT_SST=1 xmake run test_lsm
```

导出的 `.txt` 文件位于 `build/linux/x86_64/debug/exports/`，包含每个 SST 的详细内容。

详见：[DEBUG_EXPORT_SST.md](DEBUG_EXPORT_SST.md)

