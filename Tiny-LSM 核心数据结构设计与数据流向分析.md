> 这是一份基于 C++ 项目视角的深度分析，专注于**数据结构设计**和**数据流向**两个角度，忽略事务隔离级别、缓存池细节和布隆过滤器。

---

## 第一部分：静态结构与层级关系

### 宏观架构图

Tiny-LSM 采用**经典的 LSM Tree 分层架构**，由内存组件（MemTable）和多层磁盘组件（SST Level 0 到 Level N）组成。WAL（Write-Ahead Log）贯穿写入流程始终。

```mermaid
graph TB
    subgraph Memtable ["内存层（MemTable）"]
        CurrentTable["Current SkipList<br/>（活跃表）"]
        FrozenTables["Frozen SkipList List<br/>（冻结表队列）"]
    end

    subgraph Disk ["磁盘层（SST Levels）"]
        L0["Level 0 SST<br/>（可能重叠）"]
        L1["Level 1 SST<br/>（不重叠）"]
        L2["Level 2+ SST<br/>（不重叠）"]
    end

    subgraph WAL ["预写日志"]
        WalLog["WAL File<br/>（操作序列）"]
    end

    subgraph Engine ["LSMEngine 管理"]
        MemTable
        Disk
        LevelMap["level_sst_ids Map<br/>（层级-SST映射）"]
        SstMap["ssts Map<br/>（SST ID-对象映射）"]
        BlockCache["BlockCache<br/>（块缓存）"]
    end

    subgraph Lsm ["用户界面"]
        LsmWrapper["LSM 包装类"]
        TranManager["TranManager<br/>（事务管理）"]
    end

    Engine -->|管理| MemTable
    Engine -->|管理| Disk
    Engine -->|维护| LevelMap
    Engine -->|维护| SstMap
    Engine -->|使用| BlockCache
    LsmWrapper -->|使用| Engine
    LsmWrapper -->|使用| TranManager
    TranManager -->|写入| WalLog
    MemTable -->|Flush| L0

    style Memtable fill:#e1f5ff
    style Disk fill:#f3e5f5
    style WAL fill:#fff3e0
    style Engine fill:#f1f8e9
    style Lsm fill:#ede7f6
```

**关键组件说明：**

| 组件 | 位置     | 职责                                          | 指针类型              |
| ------ | ---------- | ----------------------------------------------- | ----------------------- |
| `LSMEngine`     | 核心引擎 | 管理 MemTable、SST、编码、压缩流程            | `shared_ptr<LSMEngine>`                      |
| `MemTable`     | 内存     | 持有 Current SkipList 和 Frozen SkipList 队列 | 成员变量              |
| `Current SkipList`     | 内存     | 接收当前写入的键值对                          | `shared_ptr<SkipList>`                      |
| `Frozen SkipList List`     | 内存     | 冻结的已满表队列，等待 Flush                  | `list<shared_ptr<SkipList>>`                      |
| `SST (Level 0~N)`     | 磁盘     | 分层存储的排序集合表                          | `shared_ptr<SST>` 存于 `ssts` Map            |
| `WAL`     | 磁盘     | 记录所有写操作以支持崩溃恢复                  | 通过 `TranManager` 访问            |
| `Block`     | 磁盘     | SST 内部的数据单元                            | `shared_ptr<Block>` 通过 BlockCache 管理 |
| `BlockCache`     | 内存     | 缓存最近访问的 Block                          | `shared_ptr<BlockCache>`                      |

---

### 迭代器设计图

Tiny-LSM 使用**多层迭代器组合**模式。从最底层的 `BlockIterator` 开始，逐级封装成更高层的迭代器，最终形成用户可见的全局 `Level_Iterator`。

#### 迭代器继承关系与组合结构



```mermaid
graph TD
    %% 定义基类
    Base["BaseIterator<br/>(Interface / Abstract Class)"]

    %% 定义子类
    subgraph Basic ["基础单元"]
        Sst["SstIterator"]
        Skip["SkipListIterator"]
    end

    subgraph Combinators ["组合器 (Combinators)"]
        Heap["HeapIterator<br/>(N路合并)"]
        Two["TwoMergeIterator<br/>(双路合并)"]
        Concat["ConcactIterator<br/>(串联)"]
    end

    subgraph TopLevel ["顶层封装"]
        Level["Level_Iterator<br/>(用户入口)"]
    end

    %% 继承关系
    Base -->|继承| Sst
    Base -->|继承| Skip
    Base -->|继承| Heap
    Base -->|继承| Two
    Base -->|继承| Concat
    Base -->|继承| Level

    %% 样式
    style Base fill:#f9f9f9,stroke:#333,stroke-width:2px
    style Basic fill:#e1f5fe
    style Combinators fill:#fff9c4
    style TopLevel fill:#e8f5e9
```



```mermaid
graph TD
    User["用户 (User Code)"]

    subgraph "最外层：统一视图"
        LI["Level_Iterator<br/>(负责合并 内存 与 磁盘)"]
    end

    subgraph "中间层：合并与排序"
        MemHeap["HeapIterator (内存部分)<br/>合并 Current 与 Frozen"]
        L0Heap["HeapIterator (L0 层)<br/>合并重叠的 SST"]
        L1Concat["ConcactIterator (L1+ 层)<br/>串联不重叠的 SST"]
    end

    subgraph "基础层：文件/内存访问"
        SkiplistIter["SkipListIterator"]
        SstIter["SstIterator"]
    end

    subgraph "底层：物理数据"
        BlockIter["BlockIterator<br/>(解析 Block 数据块)"]
    end

    %% 组装关系
    User -->|持有| LI
    
    LI -->|Merge| MemHeap
    LI -->|Merge| L0Heap
    LI -->|Merge| L1Concat

    MemHeap -->|管理多个| SkiplistIter
    
    L0Heap -->|管理多个| SstIter
    L1Concat -->|管理多个| SstIter

    SstIter -->|持有| BlockIter

    %% 样式
    linkStyle default stroke-width:2px,fill:none,stroke:#666
    style User fill:#ffffff,stroke:#333
    style LI fill:#e8f5e9,stroke:#2e7d32
    style MemHeap fill:#fff9c4,stroke:#fbc02d
    style L0Heap fill:#fff9c4,stroke:#fbc02d
    style L1Concat fill:#e1f5fe,stroke:#0277bd
    style BlockIter fill:#ffebee,stroke:#c62828
```

#### 迭代器组合流程图（全量 Scan 场景）

当执行 `LSMEngine::begin(tranc_id)` 进行全量扫描时，迭代器的构建与使用流程如下：

```mermaid
sequenceDiagram
    participant User as 用户代码
    participant Engine as LSMEngine
    participant Li as Level_Iterator
    participant MemIter as HeapIterator<br/>MemTable
    participant L0Iter as L0 Merge
    participant L1PlusIter as L1+ Merge

    User->>Engine: begin(tranc_id)
    Note over Engine: 构建多层迭代器
    
    Engine->>MemIter: create HeapIterator<br/>for current + frozen
    Note over MemIter: 包含所有 SkipListIterator
    
    Engine->>L0Iter: create L0 迭代器<br/>（可能重叠）
    Note over L0Iter: 使用 HeapIterator 合并所有 L0 SST
    
    Engine->>L1PlusIter: create L1+ 迭代器<br/>（不重叠）
    Note over L1PlusIter: 使用 ConcactIterator 串联
    
    Engine->>Li: create Level_Iterator
    Note over Li: 内部包含:<br/>- HeapIterator MemTable<br/>- L0 Merge Iterator<br/>- L1+ Merge Iterator
    
    User->>Li: operator++()
    activate Li
    Li->>Li: find_min_key_idx()
    Note over Li: 比较三层迭代器的当前值<br/>选择最小的 key
    Li->>Li: skip_key(min_key)
    Note over Li: 跳过相同 key 的旧版本
    deactivate Li
```

#### BlockIterator 与 SstIterator 的关系

```mermaid
graph LR
    subgraph "SST 文件结构"
        Disk["SST File<br/>（磁盘）"]
    end

    subgraph "内存中的迭代器链"
        SST["SST 对象<br/>（启动时预加载）"]
        SstIt["SstIterator<br/>（当前 Block Index）"]
        BlockIt["BlockIterator<br/>（当前 Block 内位置）"]
        Block["Block 对象<br/>（BlockCache 缓存）"]
    end

    Disk -->|LSMEngine 启动<br/>一次性 SST::open| SST
    SST -->|Get 时<br/>二分查找 Key Range| Block
    SST -->|构造| SstIt
    SstIt -->|持有| BlockIt
    BlockIt -->|指向| Block
    Block -->|read_entry| Entry["Entry<br/>key, value, tranc_id"]

    style Disk fill:#fff3e0
    style SST fill:#e1f5fe
    style SstIt fill:#f3e5f5
    style BlockIt fill:#e8f5e9
    style Block fill:#fff9c4
    style Entry fill:#c8e6c9

    classDef pointer fill:#b3e5fc,stroke:#01579b
    class SstIt,BlockIt pointer
```

---

## 第二部分：核心操作的数据流向

### Put 流程

#### 完整的 Put 操作时序图

```mermaid
sequenceDiagram
    box "接口与事务层" #fff9c4
    participant User as 用户代码
    participant LSM as LSM
    participant TranMgr as TranManager
    end

    box "存储引擎核心" #e8f5e9
    participant WAL as WAL File
    participant Engine as LSMEngine
    participant MemTable as MemTable
    participant SkipList as SkipList
    end

    User->>LSM: put(key, value)
    Note over LSM: 或 get TranContext

    alt 事务隔离
        User->>TranMgr: begin_tran()
        TranMgr->>TranMgr: create TranContext
        User->>TranMgr: put(key, value)<br/>via TranContext
        Note over TranMgr: READ_UNCOMMITTED: 直接写入 MemTable<br/>其他隔离级别: 缓存到 temp_map_
    else 非事务模式
        LSM->>Engine: put(key, value, tranc_id=0)
    end

    Note over Engine: 1️⃣ 先写 Memtable（内存）
    Engine->>MemTable: put(key, value, tranc_id)
    MemTable->>SkipList: insert(key, value, tranc_id)
    Note over SkipList: SkipList 内部维护版本链

    Note over Engine: 2️⃣ 检查 MemTable 大小
    alt MemTable 超出阈值
        Note over Engine: 触发 Flush 流程
        
        Engine->>MemTable: frozen_cur_table()
        Note over MemTable: Current SkipList 移入 Frozen 队列
        
        Engine->>MemTable: flush_last()
        Note over MemTable: 冻结表 -> SSTBuilder -> SST 文件
        
        Note over Engine: 3️⃣ 刷盘前写 WAL（如果使用事务）
        alt 事务模式
            Engine->>WAL: 在 TranContext::commit 时写 WAL
            Note over WAL: 记录 BeginRecord + PutRecord + CommitRecord
        else 非事务模式
            Note over Engine: LSM 默认模式未显式写 WAL
        end
        
        Engine->>Engine: level_sst_ids[0] 添加新 SST
        Note over Engine: 触发可能的 Compaction
    else MemTable 未满
        Note over Engine: 返回 0（未触发 Flush）
    end

    User->>User: 操作完成
```

#### Put 核心流程说明

| 步骤 | 组件        | 操作               | 说明                            |
| ------ | ------------- | -------------------- | --------------------------------- |
| 1    | LSMEngine   | `put(key, value, tranc_id)`                   | 接收 Put 请求                   |
| 2    | MemTable    | `put(key, value, tranc_id)`                   | 写入 Current SkipList           |
| 3    | MemTable    | 检查大小           | 若超过阈值，冻结 Current        |
| 4    | MemTable    | 冻结与刷盘         | 冻结表转为 SST 文件             |
| 5    | WAL（可选） | 记录操作           | 在 TranContext::commit 时写入   |
| 6    | LSMEngine   | 更新 level_sst_ids | SST 添加到 Level 0              |
| 7    | LSMEngine   | 压缩判定           | 若 L0 超过阈值，触发 Compaction |

**关键点：**

- **WAL 与 Memtable 顺序**：在内存模式下，Memtable 先写；在事务模式下，`TranContext::commit` 先写 WAL，后写 Memtable。
- **Tombstone（删除标记）** ：Remove 操作本质上也是 Put，值为空字符串，标记删除。
- **内存指针**：`Current SkipList` 是 `shared_ptr<SkipList>`，超大后冻结并添加至 Frozen 队列。

---

### Get 流程

#### Get 的多阶段读取流程

```mermaid
flowchart TB
    Start["LSMEngine::get<br/>key, tranc_id"] 
    Note0["📌 预备：所有 SST 对象已在<br/>LSMEngine 启动时加载完毕<br/>Get 只查询预加载的 Map"]
    
    subgraph ReadPhase1 ["阶段 1：读 MemTable（内存）"]
        Mem1["MemTable::get<br/>key, tranc_id"]
        Cur["查询 Current SkipList<br/>（SkipListIterator）"]
        Frozen["查询 Frozen SkipList 队列<br/>（HeapIterator 多路合并）"]
        MResult{"找到有效值?"}
        Return1["返回值"]
    end

    subgraph ReadPhase2 ["阶段 2：读 L0（可能重叠）"]
        L0Loop["遍历 L0 SST<br/>（从新到旧）"]
        L0Find["SST::get<br/>find_block_idx -> Block<br/>-> BlockIterator 查询"]
        L0Result{"找到有效值?"}
        Return2["返回值"]
    end

    subgraph ReadPhase3 ["阶段 3：读 L1+（不重叠）"]
        Invariant["✅ 关键不变性<br/>L1+ 层 Key Range 完全分离<br/>（由 Compaction 保证）"]
        L1Find["二分查找对应 SST<br/>（比较 first_key/last_key）"]
        L1SST["单个 SST::get<br/>（同 L0 流程）"]
        L1Result{"找到有效值?"}
        Return3["返回值"]
    end

    NotFound["返回 NULL"]
    End["结束"]

    Start --> Note0
    Note0 --> Mem1
    Mem1 --> Cur
    Cur --> MResult
    MResult -->|是| Return1
    MResult -->|否| Frozen
    Frozen --> MResult
    
    Return1 --> End
    
    MResult -->|未找到| L0Loop
    L0Loop --> L0Find
    L0Find --> L0Result
    L0Result -->|是| Return2
    L0Result -->|否，继续遍历| L0Loop
    Return2 --> End
    
    L0Result -->|未在 L0 找到| Invariant
    Invariant --> L1Find
    L1Find --> L1SST
    L1SST --> L1Result
    L1Result -->|是| Return3
    L1Result -->|否| NotFound
    Return3 --> End
    NotFound --> End

    style ReadPhase1 fill:#c8e6c9
    style ReadPhase2 fill:#ffccbc
    style ReadPhase3 fill:#d1c4e9
    style Start fill:#fff9c4
    style End fill:#f3e5f5
    style Note0 fill:#fff59d,stroke:#f57f17,stroke-width:2px
    style Invariant fill:#c8e6c9,stroke:#00897b,stroke-width:2px
```

#### Get 中的迭代器使用详解

> **关键架构洞察**：
>
> - **Read Path（简单 Get）中只使用 HeapIterator 和 ConcactIterator**，不涉及 TwoMergeIterator
> - **TwoMergeIterator 专用于 Compaction 和范围查询**，属于写路径或高级查询的优化
> - **SST 对象预加载**：LSMEngine 启动时一次性加载所有 SST 元数据，Get 操作只做查询，避免重复 I/O
> - **L1+ 不重叠保证**：是 ConcactIterator 能正确工作的前提，也是 LSM Tree 性能的关键

```mermaid
graph TB
    subgraph S1 [阶段 1：MemTable 读取]
        Mem["MemTable"]
        Cur["Current SkipList"]
        Frozen["Frozen SkipList Queue"]
        MemHeap["HeapIterator<br/>（管理 Current + Frozen）"]
        SKI["SkipListIterator"]
        Mem -->|包含| Cur
        Mem -->|包含| Frozen
        MemHeap -->|管理| SKI
        %% 修复点：移除了原本悬空的箭头，或指向具体节点
        MemHeap -.->|用于范围查询/scan| MemHeap 
    end

    subgraph S2 [阶段 2：L0 SST 读取（可重叠）]
        L0SSTs["多个 SST（L0）"]
        L0Merge["HeapIterator 或 TwoMergeIterator<br/>（多路合并）"]
        SstIt["SstIterator"]
        BlockIt["BlockIterator"]
        Blk["Block"]
        L0SSTs -->|包含| SstIt
        L0Merge -->|管理多个| SstIt
        SstIt -->|持有| BlockIt
        BlockIt -->|指向| Blk
    end

    subgraph S3 [阶段 3：L1+ SST 读取（不重叠）]
        L1SSTs["多个 SST（L1+）"]
        L1Merge["ConcactIterator<br/>（串联，不重叠）"]
        SstIt2["SstIterator"]
        BlockIt2["BlockIterator"]
        Blk2["Block"]
        L1SSTs -->|包含| SstIt2
        L1Merge -->|串联| SstIt2
        SstIt2 -->|持有| BlockIt2
        BlockIt2 -->|指向| Blk2
    end

    subgraph S4 [全局层]
        LI["Level_Iterator<br/>（用户可见）"]
        LI -->|内部包含| MemHeap
        LI -->|内部包含| L0Merge
        LI -->|内部包含| L1Merge
    end

    %% 样式修复：使用 subgraph 的 ID 进行着色
    style S1 fill:#c8e6c9
    style S2 fill:#ffccbc
    style S3 fill:#d1c4e9
    style S4 fill:#f3e5f5
```

#### 关键问题解答

**Q1: TwoMergeIterator 和 HeapIterator 分别在什么场景下介入？**

| 迭代器 | 场景             | 原因                                                           | 在 Read Path 中使用？         |
| -------- | ------------------ | ---------------------------------------------------------------- | ------------------------------- |
| **TwoMergeIterator**       | 合并两个有序序列 | Compaction 中合并 Lx 和 Ly；范围查询中合并 MemTable 和磁盘部分 | ❌ **否**（仅 Compaction/范围查询） |
| **HeapIterator**       | 合并多个有序序列 | 多路堆合并：L0 中多个重叠 SST、MemTable 的 Current + Frozen    | ✅ **是**（L0、MemTable）           |
| **ConcactIterator**       | 串联多个**不重叠**序列     | L1+ 中的 SST 串联（已保证无重叠，无需比较）                    | ✅ **是**（L1+ 层）                 |

**Q2: Get 流程中如何保证 MVCC 可见性？**

- 每个 Entry 都带有 `tranc_id` 时间戳
- `BlockIterator` 和 `SstIterator` 在遍历时，通过 `skip_by_tranc_id()` 跳过不可见版本
- `HeapIterator` 通过 `select_visible_version()` 在多个版本中选择对当前事务可见的版本

**Q3: Block 是如何被缓存和释放的？**

- SST 读取 Block 时调用 `SST::read_block(block_idx)`
- 请求被转发到 `BlockCache`
- BlockCache 采用 LRU 策略，超出容量时自动淘汰旧 Block
- Block 通过 `shared_ptr` 管理生命周期

**Q4: SST 对象何时加载？Get 流程中是否每次都打开 SST 文件？**

- **SST 对象加载时机**：LSMEngine 启动时（构造函数中）一次性遍历数据目录，通过 `SST::open()` 加载所有 SST 文件的元数据（索引、Footer）到内存
- **Get 流程中的查询**：直接从预加载的 `ssts` Map 中获取 SST 对象，然后查询其索引和 Block，**不需要重复打开文件**
- **性能影响**：这种设计大幅减少 I/O，只需在启动时加载一次，之后的 Block 访问通过 BlockCache 进一步优化

**Q5: ConcactIterator 使用的关键前提是什么？**

系统**必须严格保证 L1+ 层 SST 之间的 Key Range 不重叠**，这是 Compaction 算法必须维护的核心不变性（Invariant）。如果违反此不变性，ConcactIterator 会导致：

- **数据漏读**：相同 Key 存在于多个 SST，但由于串联特性只扫描第一个 SST，后续版本无法被访问
- **版本混乱**：多版本控制（MVCC）在不重叠的假设下工作，违反会导致可见性错误

因此，**L1+ 层的分离性（Separation）是 LSM Tree 架构的关键约束**，由 Compaction 算法严格保证。

---

### Remove 流程

#### Remove 操作的实现原理

```mermaid
graph TB
    Start["LSMEngine::remove<br/>key, tranc_id"]
    
    Decision{"操作模式"}
    
    subgraph NonTransaction ["非事务模式"]
        NT1["MemTable::remove<br/>key, tranc_id"]
        NT2["put(key, '', tranc_id)<br/>写入空值（Tombstone）"]
        NT3["返回 flush 状态"]
    end
    
    subgraph Transaction ["事务模式<br/>（REPEATABLE_READ/SERIALIZABLE）"]
        T1["TranContext::remove<br/>key"]
        T2["temp_map_[key] = ''<br/>缓存删除操作"]
        T3["Record::deleteRecord<br/>添加到 operations"]
        T4["TranContext::commit"]
        T5["冲突检测"]
        T6["MemTable::put_<br/>key, '', tranc_id"]
        T7["TranManager::write_to_wal<br/>写入 Begin+Delete+Commit"]
        T8["返回提交结果"]
    end

    Start --> Decision
    Decision -->|不使用事务| NonTransaction
    Decision -->|使用事务| Transaction
    
    NT1 --> NT2
    NT2 --> NT3
    
    T1 --> T2
    T2 --> T3
    T3 --> T4
    T4 --> T5
    T5 --> T6
    T6 --> T7
    T7 --> T8
    
    NT3 --> End["结束"]
    T8 --> End

    style Start fill:#fff9c4
    style NonTransaction fill:#ffccbc
    style Transaction fill:#c8e6c9
    style End fill:#f3e5f5
```

#### Tombstone（墓碑）的生命周期

```mermaid
sequenceDiagram
    box "用户与引擎" #fff9c4
    participant User as 用户代码
    participant Engine as LSMEngine
    end
    
    box "内存层" #c8e6c9
    participant MemTable as MemTable
    participant SkipList as SkipList
    end
    
    box "持久化层" #fff3e0
    participant SST as SST文件
    participant Compaction as Compaction
    end

    User->>Engine: remove(key, tranc_id)
    
    Engine->>MemTable: put(key, '', tranc_id)
    Note over MemTable: 插入空值作为 Tombstone
    
    MemTable->>SkipList: insert(key, '', tranc_id)
    Note over SkipList: SkipList 中保存 key 和空值<br/>以及版本号 tranc_id
    
    Note over Engine: MemTable 变满后 Flush
    
    Engine->>SST: 将 SkipList 转为 SST
    Note over SST: SST 中记录 (key, '', tranc_id)<br/>Tombstone 与普通键值对<br/>存储方式相同
    
    Note over Compaction: L0/L1 Compaction 时
    Compaction->>Compaction: 比较版本链
    
    alt 同一 Key 有多个版本
        Note over Compaction: 选择最新的 Tombstone<br/>或有效值
        Compaction->>Compaction: 若最新为 Tombstone<br/>则丢弃整条链<br/>不写入新 SST
    else 该 Key 无更新
        Note over Compaction: Tombstone 通过<br/>保留到更高层级
    end
    
    Note over SST: Tombstone 在 Get 时<br/>被识别为空值返回 NULL
```

#### Remove 核心流程说明

| 步骤 | 组件        | 操作               | 说明                                      |
| ------ | ------------- | -------------------- | ------------------------------------------- |
| 1    | LSMEngine   | `remove(key, tranc_id)`                   | 接收 Remove 请求                          |
| 2    | MemTable    | `put(key, '', tranc_id)`                   | 写入空值（Tombstone）                     |
| 3    | SkipList    | 版本链管理         | 与普通 Put 相同，空值表示删除             |
| 4    | WAL（可选） | 记录删除操作       | TranContext 中记录为 `deleteRecord`                     |
| 5    | 刷盘        | SST 存储 Tombstone | Tombstone 以空值形式保存                  |
| 6    | Compaction  | 清理 Tombstone     | 在最新版本为 Tombstone 时，丢弃整条版本链 |
| 7    | Get 流程    | 识别空值           | 返回 NULL 而非错误                        |

**关键点：**

- **本质上是 Put**：Remove 就是 `put(key, '', tranc_id)`
- **Tombstone 标记**：空字符串 `''` 表示已删除
- **MVCC 兼容**：多个事务可看到不同版本的 Tombstone
- **压缩时清理**：Compaction 在最新版本为 Tombstone 时，整条版本链被丢弃

---

## 总结与设计要点

### 数据结构设计原则

1. **内存层（MemTable）**

   - 采用 SkipList 实现，支持 O(log n) 查询和范围扫描
   - Current + Frozen 队列设计，支持无锁或低锁并发
   - 通过 `shared_ptr` 管理生命周期
2. **磁盘层（SST）**

   - 多层不同特性：L0 可重叠，L1+ 不重叠
   - Block 粒度存储，支持部分读取
   - BlockCache 缓存热数据
   - **SST 对象预加载**：启动时一次性加载所有 SST 元数据到内存，减少 Get 流程 I/O
3. **迭代器体系**

   - BaseIterator 抽象接口，支持多态
   - 自底向上：BlockIterator → SstIterator → 合并迭代器 → Level_Iterator
   - 合并策略：

     - **HeapIterator（堆合并）** ：用于 Read Path 合并多个有序序列（L0、MemTable）
     - **TwoMergeIterator（双路合并）** ：仅用于 Compaction 和范围查询，优化两层 SST 的合并
     - **ConcactIterator（串联）** ：用于 L1+ 不重叠层的零成本串联
4. **数据流向**

   - **写入路径**：User → LSMEngine → MemTable → SkipList → (Flush) → SST → (Compaction) → 高层 SST
   - **读取路径**：User → Level_Iterator → MemTable/L0/L1+ 多层查询 → 返回（所有 SST 对象已预加载）
   - **删除路径**：Remove = Put(tombstone)，通过版本控制实现逻辑删除

### C++ 内存管理特点

- **shared_ptr**：用于所有共享所有权的对象（SST、Block、SkipList）
- **unique_ptr**：用于独占所有权的组件（如 FileObj）
- **stack allocation**：迭代器通常栈分配，减少堆分配开销
- **RAII**：所有资源通过构析函数自动释放

### 架构不变性（Invariants）

| 不变性 | 位置     | 作用                                        | 维护者                        |
| -------- | ---------- | --------------------------------------------- | ------------------------------- |
| **L0 可重叠**       | Level 0  | 快速接收新数据，无需排序                    | Flush 过程                    |
| **L1+ 不重叠**       | Level 1~ | 支持 ConcactIterator 高效串联，二分查找定位 | Compaction 过程               |
| **同层 SST 有序**       | 所有层   | 支持二分查找和迭代器组合                    | SST 构建算法                  |
| **版本可见性**       | 所有层   | MVCC 支持，tranc_id 时间戳控制              | 所有迭代器的 skip_by_tranc_id |

---

## 附录：术语速查

| 术语            | 中文           | 解释                                                 |
| ----------------- | ---------------- | ------------------------------------------------------ |
| LSM Tree        | 日志结构合并树 | 分层存储结构，内存层 + 多层磁盘层                    |
| MemTable        | 内存表         | 内存中的活跃键值存储，采用 SkipList 实现             |
| SST             | 排序字符串表   | 磁盘上的排序数据单元                                 |
| Block           | 块             | SST 内部的数据单元，通常 4KB 大小                    |
| Tombstone       | 墓碑           | 标记删除的特殊键值（空值），用于表示逻辑删除         |
| Compaction      | 压缩           | 后台任务，合并多层 SST 以提高查询效率和回收空间      |
| Level 0         | 0 层           | 新刷入的 SST，可能相互重叠                           |
| Level 1+        | 1+ 层          | 已排序的 SST 层，相同层内不重叠，相邻层也有分离      |
| MVCC            | 多版本并发控制 | 通过 tranc_id 时间戳管理版本，支持并发读写           |
| WAL             | 预写日志       | 崩溃恢复的日志，记录所有写操作                       |
| ConcactIterator | 串联迭代器     | （原项目拼写，应为 Concatenate）用于串联不重叠的 SST |
