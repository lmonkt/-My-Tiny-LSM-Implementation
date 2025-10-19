# SST 导出功能使用说明

## 概述

为了方便调试 LSM-tree 的持久化和 compaction 逻辑，代码中集成了 SST 导出功能。该功能可以将 SST 文件内容导出为人类可读的 `.txt` 文件。

**重要：** 该功能通过环境变量控制，默认关闭，不会影响正常运行性能。

---

## 启用方式

### 方法 1：临时启用（推荐用于测试）

```bash
# 设置环境变量后运行测试
LSM_EXPORT_SST=1 xmake run test_lsm

# 或者运行你的程序
LSM_EXPORT_SST=1 ./your_program
```

### 方法 2：在当前 shell 会话中启用

```bash
# 导出环境变量（在当前终端有效）
export LSM_EXPORT_SST=1

# 运行程序
xmake run test_lsm

# 关闭导出功能
unset LSM_EXPORT_SST
```

### 方法 3：在代码中检查

```cpp
// 检查环境变量是否设置
if (std::getenv("LSM_EXPORT_SST")) {
    // 导出功能已启用
}
```

---

## 导出文件格式

导出的文件位于：`build/linux/x86_64/debug/exports/`

### 文件命名规则

- **刷盘生成的 SST**：`sst_<sst_id>.0.txt`
  - 例如：`sst_00000000000000000000000000000001.0.txt`
  
- **加载的 SST**：`sst_<sst_id>.<level>.loaded.txt`
  - 例如：`sst_00000000000000000000000000000006.1.loaded.txt`
  
- **Compaction 生成的 SST**：`sst_<sst_id>.<level>.txt`
  - 例如：`sst_00000000000000000000000000000007.1.txt`

### 文件内容示例

```
sst_id:	1
level:	0
file_size:	1024
num_blocks:	2
first_key:	key0
last_key:	key99
tranc_id_range:	1-1
sources:	(memtable/flush)
key0	1	value0
key1	1	value1
key10	1	value10
...
entry_count:	100
```

**字段说明：**
- `sst_id`：SST 文件 ID
- `level`：所在层级（0 为 L0，1 为 L1，以此类推）
- `sources`：来源信息
  - `(memtable/flush)`：从 MemTable 刷盘生成
  - `1,2,3`：由 SST 1、2、3 compaction 生成
- 数据行格式：`key\ttranc_id\tvalue`

---

## 使用场景

### 1. 调试数据丢失问题

```bash
# 启用导出
export LSM_EXPORT_SST=1

# 运行测试
xmake run test_lsm -- --gtest_filter=LSMTest.Persistence

# 检查导出的文件
ls -lh build/linux/x86_64/debug/exports/

# 查看某个 SST 的内容
cat build/linux/x86_64/debug/exports/sst_00000000000000000000000000000001.0.txt

# 对比 compaction 前后的数据
diff <(sort sst_*.original.txt) <(sort sst_*.loaded.txt)
```

### 2. 分析 Compaction 流程

```bash
# 启用导出后运行
LSM_EXPORT_SST=1 ./your_program

# 查看某个 SST 的来源
grep "sources:" exports/sst_*.txt

# 示例输出：
# sst_00000000000000000000000000000006.1.txt:sources:	1,2,3,4,5
# 表示 SST 6 是由 SST 1,2,3,4,5 compaction 生成的
```

### 3. 验证数据完整性

```bash
# 导出所有 SST
LSM_EXPORT_SST=1 xmake run test_lsm

# 统计每个文件的 entry 数量
grep "entry_count:" exports/*.txt

# 提取所有 key
grep -v "^[a-z_]*:" exports/sst_*.txt | cut -f1 | sort | uniq
```

---

## 性能影响

| 场景 | LSM_EXPORT_SST=0 (默认) | LSM_EXPORT_SST=1 (启用) |
|------|-------------------------|-------------------------|
| 正常运行 | ✅ 无性能影响 | ⚠️ 文件 I/O 开销 |
| 测试速度 | ✅ 快速 | ⚠️ 约慢 20-30% |
| 磁盘使用 | ✅ 仅 SST 文件 | ⚠️ 额外 .txt 文件 |

**建议：**
- ✅ **生产环境**：不设置环境变量（默认关闭）
- ✅ **日常开发**：不设置环境变量
- ✅ **调试问题**：临时设置 `LSM_EXPORT_SST=1`

---

## 实现原理

代码中的三个关键位置：

### 1. 加载 SST 时导出（engine.cpp:86-102）

```cpp
if (std::getenv("LSM_EXPORT_SST")) {
    sst->export_to_txt(ss_exp.str(), level, {});
}
```

### 2. Flush 时导出（engine.cpp:513-525）

```cpp
if (std::getenv("LSM_EXPORT_SST")) {
    new_sst->export_to_txt(ss.str(), 0, {});
}
```

### 3. Compaction 时导出（engine.cpp:587-604）

```cpp
if (std::getenv("LSM_EXPORT_SST")) {
    for (auto &new_sst : new_ssts) {
        new_sst->export_to_txt(ss.str(), src_level + 1, sources);
    }
}
```

---

## 常见问题

### Q: 为什么我的 exports 目录是空的？

A: 确保设置了环境变量 `LSM_EXPORT_SST=1`。

### Q: 导出文件太多，怎么清理？

```bash
# 清理所有导出文件
rm -rf build/linux/x86_64/debug/exports/*.txt

# 只保留最新的 5 个文件
cd build/linux/x86_64/debug/exports/
ls -t *.txt | tail -n +6 | xargs rm -f
```

### Q: 如何在 CI/CD 中使用？

```yaml
# GitHub Actions 示例
- name: Run tests with SST export
  run: LSM_EXPORT_SST=1 xmake run test_lsm
  
- name: Upload SST exports as artifacts
  uses: actions/upload-artifact@v2
  with:
    name: sst-exports
    path: build/linux/x86_64/debug/exports/*.txt
```

### Q: 性能测试时要关闭导出吗？

A: **是的！** 性能测试时务必不设置 `LSM_EXPORT_SST` 环境变量，避免文件 I/O 影响测试结果。

---

## 总结

✅ **优点：**
- 默认关闭，零性能开销
- 按需启用，方便调试
- 无需修改代码即可控制
- 导出格式清晰易读

⚠️ **注意事项：**
- 仅用于调试和测试
- 生产环境不要启用
- 注意磁盘空间占用

💡 **最佳实践：**
```bash
# 调试时
LSM_EXPORT_SST=1 xmake run test_lsm -- --gtest_filter=YourTest

# 性能测试时（不设置环境变量）
xmake run benchmark

# 清理调试文件
rm -rf build/linux/x86_64/debug/exports/*.txt
```
