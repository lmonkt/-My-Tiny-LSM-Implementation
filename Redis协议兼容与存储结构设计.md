# Redis 数据结构映射方案：逻辑对象到物理 KV 的深度解析

> 本文档详细阐述 Tiny-LSM 如何将 Redis 的五种数据结构（String, Hash, Set, ZSet, List）高效地映射到 LSM-Tree 的 KV 存储引擎。涵盖架构设计、编码规则、操作复杂度和特殊优化机制。

---

## 第一部分：系统架构总览

### 1.1 逻辑层与物理层的映射关系

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Redis 兼容层架构                               │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │          应用层：Redis 命令（SET, HSET, ZADD 等）          │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                              ↓                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │    RedisWrapper：命令解析、数据结构转换                     │    │
│  │  - 命令: HSET, ZADD, LPUSH, SADD 等                        │    │
│  │  - 任务: 构造 Meta Key、字段 Key、索引 Key                  │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                              ↓                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │   编码层：将 Redis 对象编码为 LSM KV 对                    │    │
│  │  - Meta Key：存储元数据（字段列表、元素计数等）             │    │
│  │  - Data Key：存储实际数据（字段值、元素等）                │    │
│  │  - Index Key：存储索引（用于快速查询，如 ZSet 的分数索引）│    │
│  └─────────────────────────────────────────────────────────────┘    │
│                              ↓                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │      LSMEngine：存储引擎核心                                │    │
│  │  - Put/Get：单行操作                                        │    │
│  │  - PutBatch/RemoveBatch：批量操作（提供原子性）            │    │
│  │  - Memtable, SST, Compaction 等                            │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### 1.2 原子性与隔离性的层次划分（关键概念澄清）

在讨论「批量操作的原子性」时，必须区分**两个独立的概念**：

```
内存隔离层（Isolation）
  └─ unique_lock<shared_mutex> 保证：同一时刻只有一个线程执行该操作
     └─ 任何其他读/写线程无法看到中间状态 ✅

LSM 持久化层（Durability Atomicity）  
  └─ PutBatch() 保证：这批 KV 对要么全部写入 WAL，要么全部不写
     └─ 即使进程在此时崩溃，系统启动时也能完全恢复或回滚 ✅

示例：HSET user:1000 name alice age 30
  第 1-3 步（GET 查询）→ 获取内存读写锁 → 操作序列化 → 内存隔离性 ✅
  第 4-5 步（PUT_BATCH）→ LSM 一组 KV 的原子落盘 → 持久化原子性 ✅
  综合结果：完整的 ACID 保证 ✅
```

**为工业级系统补充说明**：
- **内存层**：`unique_lock` 确保操作的**排他执行**，任何读者无法看到中间状态
- **磁盘层**：`PutBatch` 确保一组 KV 的**原子提交**，不存在部分成功的情况  
- **崩溃恢复**：若操作未完全持久化，系统启动时通过 WAL 恢复至一致状态

这是 LSM-Tree 的核心设计精髓：**在内存中快速序列化，在磁盘上原子落盘**。

---

### 1.3 前缀规范与命名空间隔离

Tiny-LSM Redis 层通过**前缀隔离**来区分不同的数据类型和键的不同部分：

```
配置来源：include/config/config.h 和 config.toml

┌────────────────────────────────────────────────────────────────────┐
│                     前缀规范（Prefix Convention）                   │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  数据类型       │  前缀                  │  示例                   │
│  ─────────────────────────────────────────────────────────────  │
│  Hash Meta     │  h_（可配置）         │  h_user:1000            │
│  Hash Field    │  f_（可配置）         │  f_user:1000:name       │
│  Set           │  SET:（固定）         │  SET:fruits:apple       │
│  Sorted Set    │  ZSET_（固定）        │  ZSET_students          │
│  List Meta     │  M:（固定）           │  M:REDIS_LIST_mylist    │
│  List Data     │  D:（固定）           │  D:REDIS_LIST_mylist    │
│  过期时间      │  expire_（固定）      │  expire_key_name        │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

---

## 第二部分：String（字符串）

### 2.1 数据结构与 KV 映射

```
┌─────────────────────────────────────────────────────────────┐
│                    String 类型的映射方案                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Redis 侧（逻辑对象）      │   LSM 侧（物理 KV 对）        │
│  ───────────────────────────────────────────────────────  │
│                                                             │
│      Key: "username"         │  Key: "username"            │
│      Value: "alice"          │  Value: "alice"             │
│                              │  （完全一致映射）            │
│                                                             │
│  + TTL: 3600 秒             │  Key: "expire_username"     │
│                              │  Value: "1704902400"        │
│                              │  （UNIX 时间戳）             │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 编码规则

**String 的 KV 对最简单，遵循以下规则**：

| 字段 | 规则 | 示例 |
|------|------|------|
| **Key** | 原始键名 | `username` |
| **Value** | 原始字符串值 | `alice` |
| **TTL Key** | `expire_` + 原始键 | `expire_username` |
| **TTL Value** | UNIX 时间戳（秒） | `1704902400` |

**时间戳编码**：
```cpp
// src/redis_wrapper/redis_wrapper.cpp
std::string expire_key = get_explire_key(key);  // "expire_" + key
time_t expire_time = std::time(nullptr) + seconds;
lsm->put(expire_key, std::to_string(expire_time));
```

### 2.3 操作复杂度分析

| 操作 | 底层调用 | 原子性 | 说明 |
|------|---------|--------|------|
| **SET key value** | 1× Put | 单行原子 | 直接写入主键 |
| **GET key** | 1× Get | 读一致 | 读主键；检查 TTL；过期则删除 |
| **EXPIRE key seconds** | 1× Put | 单行原子 | 更新 TTL Key |
| **TTL key** | 2× Get | 读一致 | 读主键 + 读 TTL Key |
| **INCR key** | 2× Get + 1× Put | 需锁定 | 读→递增→写（使用 unique_lock） |

### 2.4 TTL 处理流程

```
SET key value EXPIRE 3600
└── redis_set(key, value)
    └── lsm->put(key, value)
└── redis_expire(key, 3600)
    ├── 获取当前时间戳 t_now
    ├── 计算过期时间戳 t_expire = t_now + 3600
    └── lsm->put(expire_key, to_string(t_expire))

GET key
└── redis_get(key)
    ├── auto expire_value = lsm->get(expire_key)
    ├── if (expire_value && stoll(*expire_value) <= now) {
    │   ├── lsm->remove(key)
    │   ├── lsm->remove(expire_key)
    │   └── return nil
    │ }
    └── return lsm->get(key)
```

---

## 第三部分：Hash（哈希表）

### 3.1 数据结构与 KV 映射

```
┌─────────────────────────────────────────────────────────────────┐
│               Hash 类型的字段分离存储方案                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Redis 侧：HSET user:1000 name alice age 30 city NYC           │
│                                                                 │
│  user:1000 = { "name": "alice", "age": "30", "city": "NYC" }   │
│                                                                 │
│  ↓↓↓ 映射到 LSM 的三层结构 ↓↓↓                                   │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  第 1 层：Meta Key（元数据）                               │ │
│  │  ─────────────────────────────────────────────────────   │ │
│  │  Key:   h_user:1000                                       │ │
│  │  Value: "name|age|city"  （字段列表，用 | 分隔）          │ │
│  │                                                           │ │
│  │  作用：记录所有字段名，便于 HKEYS, HGETALL 等操作        │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  第 2 层：Field Keys（字段值）                             │ │
│  │  ─────────────────────────────────────────────────────   │ │
│  │  Key:   f_user:1000:name         Value: "alice"          │ │
│  │  Key:   f_user:1000:age          Value: "30"             │ │
│  │  Key:   f_user:1000:city         Value: "NYC"            │ │
│  │                                                           │ │
│  │  作用：每个字段独立存储，支持单字段查询和更新            │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  第 3 层：TTL Key（过期时间）                              │ │
│  │  ─────────────────────────────────────────────────────   │ │
│  │  Key:   expire_h_user:1000                               │ │
│  │  Value: "1704902400"  （UNIX 时间戳）                    │ │
│  │                                                           │ │
│  │  作用：控制整个哈希表的过期时间                          │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 编码规则详解

#### Meta Key 的构造

```cpp
// include/redis_wrapper/redis_wrapper.h
inline std::string get_hash_meta_key(const std::string &key) {
  return TomlConfig::getInstance().getRedisHashValuePreffix() + key;
}
// 假设前缀为 "h_"，则：
// get_hash_meta_key("user:1000") → "h_user:1000"
```

#### Field Key 的构造

```cpp
inline std::string get_hash_filed_key(const std::string &key,
                                      const std::string &field) {
  const std::string &prefix = TomlConfig::getInstance().getRedisFieldPrefix();
  char sep = TomlConfig::getInstance().getRedisFieldSeparator();
  return prefix + key + sep + field;
}
// 假设前缀为 "f_"，分隔符为 ":"，则：
// get_hash_filed_key("user:1000", "name") → "f_user:1000:name"
```

#### Meta Value 的编码（字段列表）

```cpp
// src/redis_wrapper/redis_wrapper.cpp
std::string get_hash_value_from_fields(const std::vector<std::string> &fields) {
  char sep = TomlConfig::getInstance().getRedisFieldSeparator();
  std::string out;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (i) out.push_back(sep);
    out += fields[i];
  }
  return out;
}
// 示例：
// 输入: {"name", "age", "city"}，分隔符 ":"
// 输出: "name:age:city"
```

### 3.3 HSET 操作的完整流程

```
HSET user:1000 name alice age 30
└── redis_hset_batch(key, [{name, alice}, {age, 30}])
    │
    ├─ 第一步：获取写锁
    │  └── wlock = unique_lock<shared_mutex>(redis_mtx)
    │
    ├─ 第二步：检查 TTL 和清理过期数据
    │  ├── metaKey = "h_user:1000"
    │  ├── expireKey = "expire_h_user:1000"
    │  └── if (expired) { remove_all_fields_and_meta() }
    │
    ├─ 第三步：查询现有字段
    │  ├── Query: lsm->get(metaKey)
    │  ├── 如果不存在 → fields_list = {}
    │  └── 如果存在 → fields_list = parse(meta_value)
    │
    ├─ 第四步：逐字段处理（检测新增字段）
    │  ├── for each (field, value) in {name:alice, age:30}:
    │  │   ├── fieldKey = "f_user:1000:name"
    │  │   ├── Check: lsm->get(fieldKey) ?
    │  │   ├── NOT_EXIST → added_count++, add to to_put
    │  │   └── EXIST → skip (返回 0，已存在)
    │  │
    │  └── added_count = 2  (两个新字段)
    │
    ├─ 第五步：批量写入
    │  ├── to_put = [
    │  │     (f_user:1000:name, alice),
    │  │     (f_user:1000:age, 30),
    │  │     (h_user:1000, "name:age")  ← 更新元数据
    │  │   ]
    │  └── lsm->put_batch(to_put)  ← 原子性保证
    │
    └── 返回 ":2\r\n"  (新增 2 个字段)
```

### 3.4 操作复杂度分析

| 操作 | 底层调用 | 原子性 | 说明 |
|------|---------|--------|------|
| **HSET key f v** | 1× Get(Meta) + 1× Get(Field) + 2× Put | PutBatch 原子 | 查询字段是否存在，然后写入 |
| **HGET key field** | 1× Get(Meta) + 1× Get(Field) | 读一致 | 先查 Meta 确认过期，再读字段 |
| **HDEL key field** | 1× Get(Field) + 1× Remove | 单行原子 | 检查字段存在后删除 |
| **HKEYS key** | 1× Get(Meta) | 读一致 | 解析元数据字符串，提取字段名 |
| **HLEN key** | 1× Get(Meta) | 读一致 | 计算元数据中分隔符的个数 |

### 3.5 内存布局示例

```
Redis 命令序列：
HSET user:1000 name alice age 30 city NYC
HSET user:1000 country US

LSM 中的实际存储：
┌────────────────────────────────────────┐
│ Key: h_user:1000                       │
│ Value: "name:age:city:country"         │
│ （字段列表，随 HSET 次数递增）         │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│ Key: f_user:1000:name                  │
│ Value: "alice"                         │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│ Key: f_user:1000:age                   │
│ Value: "30"                            │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│ Key: f_user:1000:city                  │
│ Value: "NYC"                           │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│ Key: f_user:1000:country               │
│ Value: "US"                            │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│ Key: expire_h_user:1000                │
│ Value: "1704902400"（可选，若设置 TTL）│
└────────────────────────────────────────┘

总计：1 个 Meta Key + 4 个 Field Key + 1 个 Expire Key = 6 行 KV 对
```

---

## 第四部分：Set（无序集合）

### 4.1 数据结构与 KV 映射

```
┌────────────────────────────────────────────────────────────────┐
│           Set 类型的元素独立存储方案                           │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  Redis 侧：SADD fruits apple banana orange                    │
│  fruits = {"apple", "banana", "orange"}                       │
│                                                                │
│  ↓↓↓ 映射到 LSM ↓↓↓                                            │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │ 第 1 层：Set Meta Key（集合计数）                         │ │
│  │ ──────────────────────────────────────────────────────  │ │
│  │ Key:   SET:fruits_meta                                  │ │
│  │ Value: "3"   （集合中元素个数）                         │ │
│  │                                                          │ │
│  │ 注：前缀带 "_meta" 后缀，与普通元素区分                 │ │
│  └──────────────────────────────────────────────────────────┘ │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │ 第 2 层：Set Element Keys（集合元素）                     │ │
│  │ ──────────────────────────────────────────────────────  │ │
│  │ Key:   SET:fruits:apple      Value: ""（空字符串）      │ │
│  │ Key:   SET:fruits:banana     Value: ""                  │ │
│  │ Key:   SET:fruits:orange     Value: ""                  │ │
│  │                                                          │ │
│  │ 作用：每个元素一个 KV 对，值为空（仅记录存在性）       │ │
│  │       利用 LSM 的前缀查询高效遍历所有元素               │ │
│  └──────────────────────────────────────────────────────────┘ │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │ 第 3 层：TTL Key（过期时间）                              │ │
│  │ ──────────────────────────────────────────────────────  │ │
│  │ Key:   expire_SET:fruits                                │ │
│  │ Value: "1704902400"                                    │ │
│  └──────────────────────────────────────────────────────────┘ │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### 4.2 编码规则

```cpp
// SET 元素的前缀构造
inline std::string get_set_key(const std::string &key) {
  return TomlConfig::getInstance().getRedisSetPrefix() + key;
  // 假设前缀为 "SET:"
  // get_set_key("fruits") → "SET:fruits"
}

// Meta Key 的构造（集合计数）
inline std::string get_set_meta_key(const std::string &set_key) {
  return get_set_key(set_key) + "_meta";
  // get_set_meta_key("fruits") → "SET:fruits_meta"
}

// 单个元素的完整 Key
std::string element_key = get_set_key("fruits") + ":" + "apple";
// Result: "SET:fruits:apple"
```

### 4.3 SADD 操作的完整流程

```
SADD fruits apple banana
└── redis_sadd(args = [fruits, apple, banana])
    │
    ├─ 第一步：获取写锁并检查 TTL
    │  ├── wlock = unique_lock<shared_mutex>(redis_mtx)
    │  └── Check expire_SET:fruits, cleanup if expired
    │
    ├─ 第二步：批量查询现有元素
    │  ├── to_query = [SET:fruits_meta, SET:fruits:apple, SET:fruits:banana]
    │  ├── batch_res = lsm->get_batch(to_query)
    │  │
    │  ├── batch_res[0] = (SET:fruits_meta, "2")  ← 当前集合大小
    │  ├── batch_res[1] = (SET:fruits:apple, "")   ← 存在
    │  └── batch_res[2] = (SET:fruits:banana, "")  ← 不存在
    │
    ├─ 第三步：遍历结果，决定是否添加
    │  ├── added_count = 0
    │  ├── for i in 1..2:
    │  │   ├── if (batch_res[i].second == null) {
    │  │   │   ├── to_put.push((SET:fruits:banana, ""))
    │  │   │   └── added_count++
    │  │   │ }
    │  │   └── else { skip （元素已存在）}
    │  │
    │  └── added_count = 1
    │
    ├─ 第四步：更新集合大小
    │  ├── if (added_count > 0) {
    │  │   ├── new_size = 2 + 1 = 3
    │  │   └── to_put.push((SET:fruits_meta, "3"))
    │  │ }
    │
    ├─ 第五步：批量写入（原子性）
    │  ├── to_put = [
    │  │     (SET:fruits:banana, ""),
    │  │     (SET:fruits_meta, "3")
    │  │   ]
    │  └── lsm->put_batch(to_put)
    │
    └── 返回 ":1\r\n"  (新增 1 个元素)
```

### 4.4 操作复杂度分析

| 操作 | 底层调用 | 原子性 | 说明 |
|------|---------|--------|------|
| **SADD set elem** | 1× Get(Meta) + 1× Get(Elem) + 2× Put | PutBatch 原子 | 检查元素是否存在，则更新大小 |
| **SREM set elem** | 1× Get(Elem) + 1× Remove(Elem) + 1× Put(Meta) | RemoveBatch 原子 | 删除元素，更新计数 |
| **SISMEMBER set elem** | 1× Get(Elem) | 读一致 | 简单查询 |
| **SCARD set** | 1× Get(Meta) | 读一致 | 读取集合大小 |
| **SMEMBERS set** | 1× Get(Meta) + N× Get(Elem) | 读一致 | 读 Meta 获取元素个数，然后遍历前缀查询 |

### 4.5 前缀查询优化

```cpp
// 获取集合所有元素的伪代码
std::string set_key = "SET:fruits";
// 使用前缀查询（LSM 原生支持）
auto [begin_iter, end_iter] = lsm->lsm_iters_monotony_predicate(
    0,
    [set_key](const std::string &key) {
        // 过滤条件：key 以 "SET:fruits:" 开头，但不包括 "SET:fruits_meta"
        if (key.compare(0, set_key.size() + 1, set_key + ":") == 0) {
            return 0;  // 在范围内
        }
        return (key < set_key) ? -1 : 1;  // 范围外
    }
);

// 遍历迭代器得到所有元素
for (; begin_iter != end_iter; ++begin_iter) {
    std::string element = begin_iter->first;  // e.g., "SET:fruits:apple"
    // 提取元素名：element.substr(set_key.size() + 1)
}
```


---

## 第五部分：Sorted Set（有序集合）

### 5.1 数据结构与 KV 映射

```
┌─────────────────────────────────────────────────────────────────────┐
│          ZSet 类型的双索引存储方案（Score→Member & Member→Score）  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Redis 侧：ZADD students 100 tom 90 jerry 85 kate                  │
│  students = {tom:100, jerry:90, kate:85}                           │
│                                                                     │
│  ↓↓↓ 映射到 LSM 的多层索引 ↓↓↓                                      │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 第 1 层：Meta Key（集合大小）                                 │  │
│  │ ──────────────────────────────────────────────────────────  │  │
│  │ Key:   ZSET_students                                         │  │
│  │ Value: "3"   （成员个数）                                   │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 第 2 层：Member→Score 索引（快速查询成员的分数）             │  │
│  │ ──────────────────────────────────────────────────────────  │  │
│  │ Key:   ZSET_students:MEMBER:tom       Value: "100"          │  │
│  │ Key:   ZSET_students:MEMBER:jerry     Value: "90"           │  │
│  │ Key:   ZSET_students:MEMBER:kate      Value: "85"           │  │
│  │                                                               │  │
│  │ 作用：给定成员名，直接查询分数                              │  │
│  │       时间复杂度 O(log N)（LSM Get）                        │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 第 3 层：Score→Member 索引（范围查询、排序）                │  │
│  │ ──────────────────────────────────────────────────────────  │  │
│  │ Key:   ZSET_students:SCORE:000085:kate      Value: "kate"   │  │
│  │ Key:   ZSET_students:SCORE:000090:jerry     Value: "jerry"  │  │
│  │ Key:   ZSET_students:SCORE:000100:tom       Value: "tom"    │  │
│  │                                                               │  │
│  │ 作用：按分数顺序遍历（前缀查询），实现 ZRANGE              │  │
│  │       时间复杂度 O(log N + K)（K 是返回元素数）             │  │
│  │       分数使用**左侧补零**确保字典序等于数值序             │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 第 4 层：TTL Key（过期时间）                                 │  │
│  │ ──────────────────────────────────────────────────────────  │  │
│  │ Key:   expire_ZSET_students                                  │  │
│  │ Value: "1704902400"                                         │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.2 编码规则详解

ZSet 的分数编码采用**左侧补零**，确保字典序等于数值序：

```cpp
inline std::string encode_score_padded(const std::string &raw_score) {
  if (raw_score.size() > 6)
    throw std::runtime_error("Score too long");
  std::string res = raw_score;
  res.insert(res.begin(), 6 - res.size(), '0');
  return res;
}
```

### 5.3 操作复杂度

| 操作 | 复杂度 | 说明 |
|------|--------|------|
| **ZADD** | O(log N) | 查询旧分数 + 删除旧索引 + 添加新索引 |
| **ZSCORE** | O(log N) | 查询 Member→Score 索引 |
| **ZRANGE** | O(log N + K) | 前缀查询 Score 索引，K 为返回元素数 |

---

## 第六部分：List（双端队列）

### 6.1 数据结构与 KV 映射

```
┌──────────────────────────────────────────────────────────────────────┐
│           List 类型的双端队列存储方案（Head/Tail 序列号）           │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Redis 侧：LPUSH mylist a b c                                        │
│           RPUSH mylist d e                                          │
│  mylist = [c, b, a, d, e]  (从左到右)                               │
│                                                                      │
│  ↓↓↓ 映射到 LSM 的四层结构 ↓↓↓                                       │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ 第 1 层：Meta Key（元数据，24 字节）                           │ │
│  │ ─────────────────────────────────────────────────────────── │ │
│  │ Key:   M:REDIS_LIST_mylist                                   │ │
│  │ Value: [Head: 8B][Tail: 8B][Length: 8B]                     │ │
│  │        (大端序编码，8 字节 × 3 = 24 字节)                   │ │
│  │                                                               │ │
│  │ 初始值：Head = Tail = 0x8000000000000000 (中位数)           │ │
│  │        Length = 0                                            │ │
│  │                                                               │ │
│  │ 作用：记录队列的头尾指针和长度，支持 O(1) 查询元素个数     │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ 第 2 层：Data Keys（队列元素）                                │ │
│  │ ─────────────────────────────────────────────────────────── │ │
│  │                                                               │ │
│  │ LPUSH c b a 序列（从左往右添加）：                           │ │
│  │   Head 递减：0x8000... → 0x7FFF... → 0x7FFE...               │ │
│  │   Key: D:REDIS_LIST_mylist:0x7FFE...   Value: "c"          │ │
│  │   Key: D:REDIS_LIST_mylist:0x7FFF...   Value: "b"          │ │
│  │   Key: D:REDIS_LIST_mylist:0x8000...   Value: "a"          │ │
│  │                                                               │ │
│  │ RPUSH d e 序列（从右往左添加）：                             │ │
│  │   Tail 递增：0x8000... → 0x8001... → 0x8002...               │ │
│  │   Key: D:REDIS_LIST_mylist:0x8001...   Value: "d"          │ │
│  │   Key: D:REDIS_LIST_mylist:0x8002...   Value: "e"          │ │
│  │                                                               │ │
│  │ 作用：每个元素独立存储，序列号决定位置，支持 O(1) 访问     │ │
│  │       通过前缀查询高效遍历所有元素                          │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ 第 3 层：TTL Key（过期时间）                                  │ │
│  │ ─────────────────────────────────────────────────────────── │ │
│  │ Key:   expire_M:REDIS_LIST_mylist                            │ │
│  │ Value: "1704902400"  (UNIX 时间戳)                          │ │
│  │                                                               │ │
│  │ 作用：控制整个列表的过期时间                                │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### 6.2 序列号设计与大端序编码

List 使用 **Head/Tail 序列号** 实现 O(1) 的两端操作：

```
序列号的数学设计：

初始值：INITIAL_ID = 0x8000000000000000  (最高位为 1，代表中位数)
  - 这样设计可以让 Head 和 Tail 向两个方向无限扩展

LPUSH 操作：Head--  (向前递减)
  Head = INITIAL_ID = 0x8000000000000000
  Head = INITIAL_ID - 1 = 0x7FFFFFFFFFFFFFFF  (还有 2^63 - 1 个位置)

RPUSH 操作：Tail++  (向后递增)
  Tail = INITIAL_ID = 0x8000000000000000
  Tail = INITIAL_ID + 1 = 0x8000000000000001  (还有 2^63 - 1 个位置)

容量：
  - Head 可递减约 2^63 次（向负方向）
  - Tail 可递增约 2^63 次（向正方向）
  - 总容量约 9.2 × 10^18 个元素
  - 完全满足业界需求
```

#### ⚠️ 关键：序列号必须使用大端序（Big-Endian）编码

**为什么必须是大端序？**

```
问题场景：假设 x86 小端序存储序列号

LPUSH 后的两个序列号：
  seq1 = 0x00FF (十进制 255)   → 小端序存储为 [FF, 00]
  seq2 = 0x0100 (十进制 256)   → 小端序存储为 [00, 01]

LSM 中的字典序比较：
  [00, 01] < [FF, 00]  ❌（错误！256 排在 255 前面）

LRANGE mylist 0 -1 结果：
  错误顺序：[元素@256, 元素@255]  ❌

---

解决方案：使用大端序（Big-Endian）编码

  seq1 = 0x00FF (十进制 255)   → 大端序存储为 [00, FF]
  seq2 = 0x0100 (十进制 256)   → 大端序存储为 [01, 00]

LSM 中的字典序比较：
  [00, FF] < [01, 00]  ✅（正确！255 < 256）

LRANGE mylist 0 -1 结果：
  正确顺序：[元素@255, 元素@256]  ✅
```

**实现保证**：
```cpp
// src/redis_wrapper/redis_wrapper.cpp
void EncodeFixed64(uint64_t value, std::string &dst) {
  // 严格按大端序编码（网络字节序）
  unsigned char buf[8];
  buf[0] = (value >> 56) & 0xff;  // 最高字节
  buf[1] = (value >> 48) & 0xff;
  buf[2] = (value >> 40) & 0xff;
  buf[3] = (value >> 32) & 0xff;
  buf[4] = (value >> 24) & 0xff;
  buf[5] = (value >> 16) & 0xff;
  buf[6] = (value >> 8) & 0xff;
  buf[7] = value & 0xff;           // 最低字节
  dst.append(reinterpret_cast<const char*>(buf), 8);
}

// 结果：uint64_t 转为 8 字节大端序字符串
// 保证字典序 == 数值序 ✅
```

**关键结论**：List 的 Data Key `D:REDIS_LIST_<name>:<seq_8bytes>` 中的序列号**必须采用 Big-Endian 编码**，否则 LRANGE 扫描会返回乱序结果。这是 LSM 结构对**字典序**的硬需求。

### 6.3 编码规则详解

#### Meta Key 的构造

```cpp
// include/redis_wrapper/redis_wrapper.h
inline std::string get_list_meta_key(const std::string &key) {
  return "M:REDIS_LIST_" + key;
}
// get_list_meta_key("mylist") → "M:REDIS_LIST_mylist"
```

#### Data Key 的构造

```cpp
inline std::string get_list_data_key(const std::string &key, 
                                     uint64_t seq_id) {
  std::string seq_encoded;
  EncodeFixed64(seq_id, seq_encoded);  // 大端序编码
  return "D:REDIS_LIST_" + key + ":" + seq_encoded;
}
// get_list_data_key("mylist", 0x8000000000000000)
// → "D:REDIS_LIST_mylist:<8字节大端序>"
```

#### Meta Value 的编码（Head/Tail/Length）

```cpp
// src/redis_wrapper/redis_wrapper.cpp
std::string encode_list_meta(uint64_t head, uint64_t tail, uint64_t len) {
  std::string result;
  EncodeFixed64(head, result);   // 8 字节
  EncodeFixed64(tail, result);   // 8 字节
  EncodeFixed64(len, result);    // 8 字节
  return result;  // 总共 24 字节
}

// 示例：
// head=0x7FFF..., tail=0x8001..., len=5
// 返回：24 字节的二进制串
```

#### Meta Value 的解码

```cpp
struct ListMeta {
  uint64_t head;
  uint64_t tail;
  uint64_t length;
};

ListMeta decode_list_meta(const std::string &meta_value) {
  if (meta_value.size() != 24) {
    throw std::runtime_error("Invalid list meta size");
  }
  
  uint64_t head = DecodeFixed64(meta_value.substr(0, 8));
  uint64_t tail = DecodeFixed64(meta_value.substr(8, 8));
  uint64_t len = DecodeFixed64(meta_value.substr(16, 8));
  
  return {head, tail, len};
}
```

### 6.4 LPUSH 操作的完整流程

```
LPUSH mylist c b a
└── redis_lpush(args = [mylist, c, b, a])
    │
    ├─ 第一步：获取写锁并检查 TTL
    │  ├── wlock = unique_lock<shared_mutex>(redis_mtx)
    │  ├── metaKey = "M:REDIS_LIST_mylist"
    │  ├── expireKey = "expire_M:REDIS_LIST_mylist"
    │  └── Check expire_key, cleanup if expired
    │
    ├─ 第二步：查询现有元数据
    │  ├── Query: lsm->get(metaKey)
    │  ├── if (not_exist) {
    │  │   ├── head = INITIAL_ID = 0x8000000000000000
    │  │   ├── tail = INITIAL_ID
    │  │   ├── length = 0
    │  │ }
    │  └── else {
    │      └── Decode: head, tail, length from meta_value
    │    }
    │
    ├─ 第三步：为每个新元素生成序列号（递减）
    │  ├── to_put = []
    │  ├── for each elem in [c, b, a]:
    │  │   ├── seq = head - 1
    │  │   ├── head = seq
    │  │   ├── dataKey = "D:REDIS_LIST_mylist:" + encode_seq(seq)
    │  │   ├── to_put.push((dataKey, elem))
    │  │
    │  ├── seq_a = head - 3 = 0x7FFFFFFFFFFFFFFF
    │  ├── seq_b = head - 2 = 0x8000000000000000 - 2
    │  ├── seq_c = head - 1 = 0x8000000000000000 - 1
    │  └── head_final = 0x8000000000000000 - 3
    │
    ├─ 第四步：更新元数据
    │  ├── new_length = length + 3
    │  ├── new_meta = encode_list_meta(head_final, tail, new_length)
    │  └── to_put.push((metaKey, new_meta))
    │
    ├─ 第五步：批量写入（原子性）
    │  ├── to_put = [
    │  │     (D:REDIS_LIST_mylist:<seq_c>, "c"),
    │  │     (D:REDIS_LIST_mylist:<seq_b>, "b"),
    │  │     (D:REDIS_LIST_mylist:<seq_a>, "a"),
    │  │     (M:REDIS_LIST_mylist, <新元数据>)
    │  │   ]
    │  └── lsm->put_batch(to_put)
    │
    └── 返回 ":3\r\n"  (列表现在包含 3 个元素)
```

### 6.5 RPUSH 操作的完整流程

```
RPUSH mylist d e
└── redis_rpush(args = [mylist, d, e])
    │
    ├─ 第一步：获取写锁并检查 TTL
    │  ├── wlock = unique_lock<shared_mutex>(redis_mtx)
    │  └── Check expire_key, cleanup if expired
    │
    ├─ 第二步：查询现有元数据（同 LPUSH）
    │  ├── Query: lsm->get(metaKey)
    │  └── Decode: head, tail, length
    │
    ├─ 第三步：为每个新元素生成序列号（递增）
    │  ├── to_put = []
    │  ├── for each elem in [d, e]:
    │  │   ├── seq = tail + 1
    │  │   ├── tail = seq
    │  │   ├── dataKey = "D:REDIS_LIST_mylist:" + encode_seq(seq)
    │  │   ├── to_put.push((dataKey, elem))
    │  │
    │  ├── seq_d = tail + 1 = 0x8000000000000001
    │  ├── seq_e = tail + 2 = 0x8000000000000002
    │  └── tail_final = 0x8000000000000002
    │
    ├─ 第四步：更新元数据
    │  ├── new_length = length + 2
    │  ├── new_meta = encode_list_meta(head, tail_final, new_length)
    │  └── to_put.push((metaKey, new_meta))
    │
    ├─ 第五步：批量写入（原子性）
    │  ├── to_put = [
    │  │     (D:REDIS_LIST_mylist:<seq_d>, "d"),
    │  │     (D:REDIS_LIST_mylist:<seq_e>, "e"),
    │  │     (M:REDIS_LIST_mylist, <新元数据>)
    │  │   ]
    │  └── lsm->put_batch(to_put)
    │
    └── 返回 ":5\r\n"  (列表现在包含 5 个元素)
```

### 6.6 LPOP 操作的完整流程

```
LPOP mylist
└── redis_lpop(key = mylist)
    │
    ├─ 第一步：获取写锁并检查 TTL
    │  ├── wlock = unique_lock<shared_mutex>(redis_mtx)
    │  └── Check expire_key, cleanup if expired
    │
    ├─ 第二步：查询元数据
    │  ├── Query: lsm->get(metaKey)
    │  ├── if (not_exist || length == 0) {
    │  │   └── return nil  // 列表为空
    │  │ }
    │  └── Decode: head, tail, length
    │
    ├─ 第三步：获取头部元素
    │  ├── // 下一个要删除的序列号是 head + 1
    │  ├── seq_to_delete = head + 1
    │  ├── dataKey = "D:REDIS_LIST_mylist:" + encode_seq(seq_to_delete)
    │  ├── elem_value = lsm->get(dataKey)
    │  ├── if (not_exist) {
    │  │   └── throw std::runtime_error("List corruption!")
    │  │ }
    │
    ├─ 第四步：更新元数据并删除元素
    │  ├── to_remove = [dataKey]
    │  ├── new_head = head + 1
    │  ├── new_length = length - 1
    │  ├── new_meta = encode_list_meta(new_head, tail, new_length)
    │  │
    │  ├── to_put = [(metaKey, new_meta)]
    │  └── lsm->remove_batch(to_remove) + lsm->put_batch(to_put)
    │
    └── 返回 elem_value  (e.g., "$1\r\nc\r\n")
```

### 6.7 LRANGE 操作的完整流程

```
LRANGE mylist 0 2  // 获取前 3 个元素
└── redis_lrange(key, start=0, stop=2)
    │
    ├─ 第一步：获取读锁并检查 TTL
    │  ├── rlock = shared_lock<shared_mutex>(redis_mtx)
    │  └── Check expire_key, return empty if expired
    │
    ├─ 第二步：查询元数据
    │  ├── Query: lsm->get(metaKey)
    │  ├── if (not_exist) {
    │  │   └── return empty_array
    │  │ }
    │  └── Decode: head, tail, length
    │
    ├─ 第三步：规范化索引
    │  ├── // Redis LRANGE 支持负数索引
    │  ├── if (start < 0) start += length
    │  ├── if (stop < 0) stop += length
    │  ├── start = max(0, start)
    │  ├── stop = min(length - 1, stop)
    │  ├── if (start > stop) {
    │  │   └── return empty_array
    │  │ }
    │  │
    │  ├── // 例：LRANGE mylist 0 2，length=5
    │  ├── // 需要获取第 0, 1, 2 个元素（共 3 个）
    │  └── count = stop - start + 1 = 3
    │
    ├─ 第四步：构造序列号范围
    │  ├── // head 指向最左边元素的前一个
    │  ├── // 第 0 个元素的序列号 = head + 1
    │  ├── // 第 1 个元素的序列号 = head + 2
    │  ├── // 第 2 个元素的序列号 = head + 3
    │  ├──
    │  ├── first_seq = head + 1 + start
    │  ├── last_seq = head + 1 + stop
    │  │
    │  ├── // 生成要查询的所有 Key
    │  ├── to_get = []
    │  ├── for i in 0..count-1:
    │  │   ├── seq = first_seq + i
    │  │   ├── dataKey = "D:REDIS_LIST_mylist:" + encode_seq(seq)
    │  │   └── to_get.push(dataKey)
    │
    ├─ 第五步：批量查询所有元素
    │  ├── results = lsm->get_batch(to_get)
    │  ├── values = extract_values(results)
    │  │
    │  └── // 输出顺序严格按序列号升序（因为大端序编码）
    │
    └── 返回数组格式 *3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n
```

### 6.8 操作复杂度分析

| 操作 | 底层调用 | 原子性 | 说明 |
|------|---------|--------|------|
| **LPUSH/RPUSH** | 1× Get(Meta) + N× Put + 1× Put(Meta) | PutBatch 原子 | 批量插入 N 个元素，更新元数据 |
| **LPOP/RPOP** | 1× Get(Meta) + 1× Get(Data) + 1× Remove + 1× Put(Meta) | 原子 | 读头/尾元素，删除，更新元数据 |
| **LLEN** | 1× Get(Meta) | 读一致 | 解码元数据获取长度 |
| **LRANGE** | 1× Get(Meta) + K× Get(Data) | 读一致 | K 为返回元素数 |
| **LINDEX** | 1× Get(Meta) + 1× Get(Data) | 读一致 | 计算序列号，直接查询 |

### 6.9 内存布局示例

```
Redis 命令序列：
LPUSH mylist c b a
RPUSH mylist d e
LRANGE mylist 0 -1

Meta 元数据：
┌───────────────────────────────────────────────────────┐
│ Key: M:REDIS_LIST_mylist                              │
│ Value: [Head (8B)][Tail (8B)][Length (8B)]           │
│        [0x7FFE...][0x8002...][0x00000005]            │
│                                                       │
│ 含义：                                                 │
│   Head = 0x7FFE... (最左元素前一个位置)              │
│   Tail = 0x8002... (最右元素)                        │
│   Length = 5 (共 5 个元素)                           │
└───────────────────────────────────────────────────────┘

Data 数据按序列号递增顺序排列（字典序）：
┌───────────────────────────────────────────────────────┐
│ Key: D:REDIS_LIST_mylist:0x7FFE...  Value: "c"       │
│ (序列号最小，对应 LPUSH 的第一个元素)                 │
└───────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────┐
│ Key: D:REDIS_LIST_mylist:0x7FFF...  Value: "b"       │
└───────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────┐
│ Key: D:REDIS_LIST_mylist:0x8000...  Value: "a"       │
│ (序列号为中位数)                                      │
└───────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────┐
│ Key: D:REDIS_LIST_mylist:0x8001...  Value: "d"       │
│ (序列号递增，对应 RPUSH)                              │
└───────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────┐
│ Key: D:REDIS_LIST_mylist:0x8002...  Value: "e"       │
│ (序列号最大，对应 RPUSH 的最后一个元素)               │
└───────────────────────────────────────────────────────┘

TTL (可选)：
┌───────────────────────────────────────────────────────┐
│ Key: expire_M:REDIS_LIST_mylist                       │
│ Value: "1704902400"  (UNIX 时间戳)                   │
└───────────────────────────────────────────────────────┘

LRANGE mylist 0 -1 返回顺序：
  通过 LSM 前缀查询 "D:REDIS_LIST_mylist:"
  按字典序（=序列号升序）扫描所有 Data Key
  顺序得到：[c, b, a, d, e]  ✅
```

### 6.10 关键设计特性总结

| 特性 | 实现方式 | 优势 |
|------|---------|------|
| **两端 O(1) 操作** | Head/Tail 序列号各自递减/递增 | 支持 LPUSH/RPUSH/LPOP/RPOP 均为 O(1) |
| **范围查询** | 前缀查询 + 序列号范围 | LRANGE 可 O(log N + K) 完成 |
| **有序性保证** | 大端序编码序列号 | LSM 字典序等于元素逻辑序 |
| **容量扩展** | 2^63 方向各 2^63 | 约 9.2 × 10^18 元素容量 |
| **原子性** | PutBatch/RemoveBatch | 多元素批量操作不会看到中间状态 |
| **TTL 支持** | 独立 expire_key | 统一的过期时间管理机制 |

---

## 第七部分：TTL 与过期时间管理深度讨论

### 7.1 惰性删除 vs 主动删除（Lazy vs Active Expiration）

Tiny-LSM 的 TTL 实现目前采用**惰性删除**策略，但工业级系统应理解两者的权衡：

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        过期数据清理策略对比                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  惰性删除（Lazy Expiration）                                                 │
│  ───────────────────────────────────────────────────────────────────────   │
│  触发条件：用户访问 KEY 时                                                  │
│  当前实现：GET/HGET 等读操作时检查过期时间戳                               │
│  ```cpp                                                                   │
│  auto expire_value = lsm->get(expire_key);                              │
│  if (is_expired(expire_value)) {                                        │
│    lsm->remove(key);  // ← 此时才实际删除                                │
│  }                                                                       │
│  ```                                                                      │
│                                                                             │
│  优点：                                                                      │
│    ✅ 无后台线程开销，CPU 占用低                                            │
│    ✅ 操作延迟 O(1)，仅增加一次 Get 调用                                     │
│    ✅ 当前 Tiny-LSM 的实现方式                                              │
│                                                                             │
│  缺点 ⚠️：                                                                   │
│    ❌ 未被访问的过期数据永久占用磁盘空间                                    │
│    ❌ 写多读少场景：100 万 Key 设 TTL，读取 1% → 99 万条垃圾数据永驻      │
│    ❌ LSM 的 Compaction 无法自动清理（需要 Filter）                       │
│    ❌ 最坏情况：磁盘被过期数据填满                                          │
│                                                                             │
│  问题案例：                                                                  │
│    SET key value EX 3600  (100 万次)                                      │
│    → 1 小时后，如果这些 Key 都未被读取                                    │
│    → Memtable 中有 100 万条 expire_key 记录                              │
│    → Flush 到 SST 时，仍然占用 SST 空间                                   │
│    → 后续 Compaction 无法判断是否过期 (没有 Filter)                      │
│    → 这 100 万条 Tombstone 永驻 LSM                                       │
│                                                                             │
│  ─────────────────────────────────────────────────────────────────────────│
│                                                                             │
│  主动删除（Active Expiration）                                              │
│  ───────────────────────────────────────────────────────────────────────   │
│  触发条件：后台定时任务或 Compaction 过程中                                │
│  实现方式：独立线程定期扫描，或在 Compaction Filter 中删除                 │
│                                                                             │
│  优点：                                                                      │
│    ✅ 过期数据及时清理，无限堆积风险                                        │
│    ✅ 磁盘占用可控，生产环境必须（存储成本考虑）                           │
│    ✅ 结合 Compaction Filter 最高效（成本 O(log N)）                       │
│                                                                             │
│  缺点 ⚠️：                                                                   │
│    ❌ 需要后台线程持续运行，CPU 占用高                                      │
│    ❌ 扫描成本 O(N)，可能卡顿业务线程                                       │
│    ❌ Filter 在每次 Compaction 都执行，可能频繁触发                        │
│    ❌ 与 Compaction 争抢 I/O 资源                                           │
│                                                                             │
│  ─────────────────────────────────────────────────────────────────────────│
│                                                                             │
│  混合方案（推荐）✨                                                          │
│  ───────────────────────────────────────────────────────────────────────   │
│  日常路径：惰性删除（低成本）                                               │
│    GET key → 检查过期 → 删除（如果过期）                                  │
│                                                                             │
│  后台路径：Compaction Filter（自动清理）                                    │
│    Compaction 遍历 KV 时 → 判断 expire_key 是否超期                       │
│    → 跳过过期数据，避免持久化到新 SST                                     │
│    → 成本最低（复用 Compaction 的 I/O）                                     │
│                                                                             │
│  结果：                                                                      │
│    ✅ 日常操作低延迟（惰性删除）                                            │
│    ✅ 后台自动清理（Compaction Filter）                                    │
│    ✅ 磁盘占用受控（无垃圾堆积）                                            │
│    ✅ 成本最优（不需独立扫描线程）                                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 当前 Tiny-LSM 的状态与改进方向

**当前实现（✅ 完成）：**
- 所有数据类型统一的 TTL 存储（`expire_<key>` → UNIX 时间戳）
- 惰性删除在读取时生效（GET/HGET/ZADD 等）
- 支持 EXPIRE / TTL 命令

**现有缺陷（⚠️ 已识别）：**
- ❌ 未被访问的过期数据无法清理
- ❌ 长期不读的 Key 会导致磁盘垃圾堆积
- ❌ Compaction 时仍保留过期 Tombstone 到新 SST

**工业级改进方向（🎯 优先级）：**

**优先级 🔴 高 - Compaction Filter（推荐立即实现）**
```cpp
class ExpireFilter : public CompactionFilter {
  bool ShouldRemove(const std::string &key, const std::string &value) override {
    // 判断 Key 类型
    if (key.find("expire_") == 0) {
      // 检查是否过期
      auto expire_time = stoll(value);
      if (expire_time <= time(nullptr)) {
        return true;  // ✅ 物理删除过期数据，避免持久化到新 SST
      }
    }
    return false;
  }
};

// 在 Compaction 时应用
compact_options.filter = std::make_unique<ExpireFilter>();
```
**成本**：低（复用现有 Compaction 流程）  
**效果**：高（完全消除过期数据堆积）

**优先级 🟡 中 - 后台定时扫描（可选）**
```cpp
void BackgroundExpireCleanup() {
  while (running_) {
    // 每 60 秒扫描一次
    auto expired_keys = FindExpiredKeys(batch_size=1000);
    if (!expired_keys.empty()) {
      lsm->remove_batch(expired_keys);
    }
    sleep(60);
  }
}
```
**成本**：中（后台线程 CPU 占用）  
**效果**：中（定期清理，但不如 Filter 彻底）

**优先级 🟢 低 - Bloom Filter 优化（future work）**
- 为过期数据单独建立 Bloom Filter
- Compaction 时快速定位过期 Key
- 进一步减少 Filter 的扫描成本

### 7.3 最佳实践建议

对于**生产环境**，建议采用以下方案：

1. **立即实现**：Compaction Filter  
   - 在 Compaction 时自动清理过期数据
   - 零额外 CPU 成本（利用现有 Compaction）
   - 效果明显（防止垃圾堆积）

2. **监控指标**：添加以下指标观测  
   ```
   - 过期数据占比 = expired_keys / total_keys
   - 磁盘浪费比 = expired_data_size / total_sstable_size
   - Compaction 清理数 = keys_removed_by_filter
   ```

3. **参数调优**：  
   ```
   - Compaction 触发频率（如 10 GB SST 大小）
   - TTL 默认值（建议不超过 24 小时）
   - Scan 批次大小（如后台扫描时）
   ```

---

## 第八部分：总结与性能对比

### 7.1 五种数据类型快速对比

```
┌─────────────┬──────────┬───────────────┬──────────────────┐
│ 类型        │ Meta Key │ 主要索引      │ 典型操作复杂度    │
├─────────────┼──────────┼───────────────┼──────────────────┤
│ String      │ 原始键   │ （无）        │ Get/Set: O(1)    │
│ Hash        │ 字段列表 │ 字段 Key      │ HGET: O(1)       │
│ Set         │ 元素计数 │ 元素 Key      │ SADD: O(1)       │
│ ZSet        │ 元素计数 │ Score索引     │ ZADD: O(logN)    │
│ List        │ Head/Tail│ 序列号        │ LPUSH: O(1)      │
└─────────────┴──────────┴───────────────┴──────────────────┘
```

### 7.2 设计关键点总结

1. **前缀隔离**：通过前缀区分不同数据类型和元数据，利用 LSM 的有序性
2. **批量原子性**：使用 PutBatch/RemoveBatch 保证多个 KV 对的原子修改
3. **双索引设计**（ZSet）：Member→Score 快速查询，Score→Member 支持排序
4. **序列号扩展**（List）：中位数初始值，两端可无限扩展
5. **TTL 统一管理**：所有类型都支持过期时间，异步清理

