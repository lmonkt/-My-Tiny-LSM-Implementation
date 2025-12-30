# Toni-LSM 实现
个人对 **Tiny-LSM 实验教程**（https://tonixwd.github.io/toni-lsm/book/）完成的成果，纯当一个存档。

## 📈 基准测试结果
在本地环境（WSL2、Ubuntu 22.04 系统、12600K 处理器）中，使用 `redis-benchmark` 工具测得如下性能数据：

| 命令 | 每秒查询率（QPS）—— 本实现 |
| :--- | :--- |
| **SET** | 111,482 |
| **GET** | 91,743 |
| **INCR** | 85,836 |
| **SADD** | 72,411 |
| **HSET** | 62,893 |

## 📜 致谢与许可证
- 基本代码来自开源项目 [Vanilla-Beauty/tiny-lsm](https://github.com/Vanilla-Beauty/tiny-lsm)，感谢大佬付出
