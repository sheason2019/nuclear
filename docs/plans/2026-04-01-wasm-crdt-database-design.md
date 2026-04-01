# WASM CRDT NoSQL Database Design Document

## 1. 项目概述

基于Rust编写的WASM嵌入式NoSQL数据库，使用GraphQL语法进行查询，以CRDT作为底层数据实现，支持两个数据库之间的数据同步，并通过最终一致性确保合并后的内容。

## 2. 需求分析

### 2.1 功能需求

| 需求项 | 描述 |
|--------|------|
| 用途场景 | 混合场景，支持浏览器端离线优先应用和主流WASM Runtime（如Wasmtime）的任意设备 |
| 数据规模 | 可能超过1GB，长期运行的大型数据库，类似SQLite |
| 同步频率 | 实时/近实时同步 |
| 冲突解决 | 最后写入获胜（LWW） |
| 存储机制 | WASM直接使用文件系统，浏览器使用OPFS或File System Access API |
| GraphQL查询能力 | 基本CRUD和查询、高级查询功能（过滤、排序、分页、聚合）、实时订阅 |
| API风格 | 函数式调用，但使用GraphQL语法 |
| 性能要求 | 高性能（查询延迟<100ms，同步延迟<1s，支持高并发） |

### 2.2 非功能需求

- 跨平台兼容性：浏览器和WASM Runtime
- 离线支持：离线操作，上线后自动同步
- 数据持久化：可靠的存储机制
- 类型安全：编译时检查

## 3. 架构设计

### 3.1 整体架构

采用混合方法，整体架构分为五个核心层：

1. **核心CRDT引擎**（自定义实现）
2. **存储层**（抽象接口）
3. **GraphQL解析层**（使用现有库）
4. **同步引擎**（自定义实现）
5. **API层**（函数式调用）

### 3.2 核心CRDT引擎

#### 3.2.1 数据类型

- `LWWRegister<T>`: 最后写入获胜寄存器
- `LWWMap<K, V>`: 最后写入获胜映射
- `GSet<T>`: 只增集合
- `ORSet<T>`: 观察移除集合
- `PNCounter`: 正负计数器

#### 3.2.2 向量时钟

- 每个节点维护向量时钟 `[node_id: counter]`
- 用于因果关系跟踪和冲突检测
- 支持节点动态加入/离开

#### 3.2.3 存储结构

- 每个CRDT类型存储为独立文件
- 使用二进制序列化（bincode）
- 支持增量快照，避免全量持久化

#### 3.2.4 内存管理

- 使用内存映射文件处理大数据
- LRU缓存热点数据
- 支持惰性加载，按需读取

### 3.3 存储层

#### 3.3.1 核心接口

```rust
trait Storage {
    async fn open(&self, path: &str, options: OpenOptions) -> Result<FileHandle>;
    async fn close(&self, handle: FileHandle) -> Result<()>;
    async fn read(&self, handle: FileHandle, offset: u64, buf: &mut [u8]) -> Result<usize>;
    async fn write(&self, handle: FileHandle, offset: u64, buf: &[u8]) -> Result<usize>;
    async fn sync(&self, handle: FileHandle) -> Result<()>;
    async fn size(&self, handle: FileHandle) -> Result<u64>;
}
```

#### 3.3.2 跨平台实现

- **WASI实现**：直接使用标准文件API
- **浏览器实现**：使用OPFS的FileHandle API

#### 3.3.3 性能优化

- 内存映射大文件（mmap）
- 批量写入，减少系统调用
- 异步I/O，非阻塞操作

### 3.4 GraphQL解析层

#### 3.4.1 解析器框架

- 使用`async-graphql`作为解析器
- 支持查询、变更、订阅
- 类型安全，编译时检查

#### 3.4.2 Schema生成

- 动态生成GraphQL Schema
- 基于CRDT数据结构自动推导类型
- 支持自定义标量和枚举
- **宽松类型设计**：核心字段（id、时间戳）收敛到 `_meta` 字段，用户数据扁平化存储
- 支持 JSON 标量类型，允许无 schema 数据
- **单一类型，集合区分**：所有Record使用相同结构，通过collection字段区分不同类型
- **关联查询支持**：通过GraphQL嵌套字段实现关联查询，应用层解析关联数据

#### 3.4.3 查询能力

- 基本CRUD：
  ```graphql
  # 查询用户集合
  query {
    records(collection: "users") {
      name
      email
      _meta {
        id
        created_at
      }
    }
  }
  
  # 创建记录
  mutation {
    createRecord(collection: "users", data: {name: "Alice", email: "alice@example.com"}) {
      _meta { id }
    }
  }
  ```
- 关联查询（嵌套查询）：
  ```graphql
  # 查询用户及其关联的订单
  query {
    records(collection: "users") {
      name
      orders {  # 嵌套查询，应用层解析关联
        _meta { id }
        total
        status
      }
      _meta { id }
    }
  }
  ```
- 高级过滤：`query { records(collection: "users", where: {age_gt: 18}) { name } }`
- 排序分页：`query { records(collection: "users", orderBy: {name: ASC}, first: 10) { name } }`
- 聚合：`query { records_aggregate(collection: "users") { count } }`

#### 3.4.4 实时订阅

- WebSocket支持
- 订阅数据变更：
  ```graphql
  # 订阅用户集合的更新
  subscription {
    recordUpdated(collection: "users") {
      name
      email
      _meta {
        id
        updated_at
      }
    }
  }
  
  # 订阅所有集合的更新
  subscription {
    recordUpdated {
      _meta {
        id
        collection
        updated_at
      }
      data
    }
  }
  ```
- 过滤订阅条件

### 3.5 同步引擎

#### 3.5.1 同步协议

- 基于WebSocket的双向通信
- 增量同步：只传输变更数据
- 支持断点续传，网络恢复后继续同步

#### 3.5.2 冲突检测与解决

- 使用向量时钟检测冲突
- LWW（最后写入获胜）策略
- 冲突日志记录，支持审计

#### 3.5.3 离线支持

- 离线操作队列
- 本地变更记录
- 上线后自动同步

#### 3.5.4 性能优化

- 批量同步，减少网络请求
- 压缩传输，节省带宽
- 并行同步，提高吞吐量

### 3.6 API层

#### 3.6.1 核心API

```rust
// 创建/打开数据库
let db = Database::open("mydb").await?;

// 执行GraphQL查询（指定集合）
let result = db.query(r#"
    query {
        records(collection: "users") {
            name
            email
            _meta { id }
        }
    }
"#).await?;

// 执行GraphQL变更
let result = db.mutation(r#"
    mutation {
        createRecord(
            collection: "users", 
            data: {name: "Alice", email: "alice@example.com"}
        ) {
            _meta { id }
        }
    }
"#).await?;

// 关联查询（嵌套查询）
let result = db.query(r#"
    query {
        records(collection: "users") {
            name
            orders {
                _meta { id }
                total
                status
            }
        }
    }
"#).await?;

// 订阅数据变更
let mut stream = db.subscribe(r#"
    subscription {
        recordUpdated(collection: "users") {
            name
            _meta { id updated_at }
        }
    }
"#).await?;
while let Some(event) = stream.next().await {
    println!("{:?}", event);
}
```

#### 3.6.2 类型安全

- 编译时GraphQL验证
- 自动生成Rust类型
- 错误处理完善

#### 3.6.3 异步支持

- 全异步API
- 支持tokio/async-std
- 非阻塞操作

#### 3.6.4 配置选项

```rust
let db = Database::builder()
    .path("mydb")
    .sync_interval(Duration::from_secs(1))
    .cache_size(1024 * 1024 * 100) // 100MB
    .build()
    .await?;
```

## 4. 技术栈

| 组件 | 技术选择 |
|------|----------|
| 语言 | Rust |
| 编译目标 | WASM |
| GraphQL解析 | async-graphql |
| 序列化 | bincode, serde |
| 异步运行时 | tokio |
| WebSocket | tokio-tungstenite |
| 存储 | 文件系统（WASI/OPFS） |

## 5. 项目结构

```
nuclear/
├── Cargo.toml
├── src/
│   ├── lib.rs           # 库入口
│   ├── core/            # 核心CRDT引擎
│   │   ├── mod.rs
│   │   ├── lww.rs       # LWW寄存器和映射
│   │   ├── set.rs       # 集合类型
│   │   ├── counter.rs   # 计数器类型
│   │   └── clock.rs     # 向量时钟
│   ├── storage/         # 存储层
│   │   ├── mod.rs
│   │   ├── trait.rs     # 存储接口
│   │   ├── wasi.rs      # WASI实现
│   │   └── browser.rs   # 浏览器实现
│   ├── graphql/         # GraphQL解析层
│   │   ├── mod.rs
│   │   ├── schema.rs    # Schema生成
│   │   ├── resolver.rs  # 解析器
│   │   └── subscription.rs # 订阅
│   ├── sync/            # 同步引擎
│   │   ├── mod.rs
│   │   ├── protocol.rs  # 同步协议
│   │   ├── conflict.rs  # 冲突解决
│   │   └── offline.rs   # 离线支持
│   └── api/             # API层
│       ├── mod.rs
│       ├── database.rs  # 数据库API
│       └── builder.rs   # 构建器模式
├── tests/               # 测试
├── examples/            # 示例
└── docs/                # 文档
    └── plans/
        └── 2026-04-01-wasm-crdt-database-design.md
```

## 6. 开发阶段

### 阶段1：核心CRDT引擎
- 实现基本CRDT类型
- 向量时钟
- 序列化/反序列化

### 阶段2：存储层
- 存储接口设计
- WASI和浏览器实现
- 性能优化

### 阶段3：GraphQL解析层
- async-graphql集成
- Schema生成
- 查询和订阅

### 阶段4：同步引擎
- 同步协议
- 冲突解决
- 离线支持

### 阶段5：API层和集成
- 统一API设计
- 集成测试
- 性能优化

## 7. 风险评估

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| WASM性能瓶颈 | 高 | 内存映射、批量操作优化 |
| 浏览器兼容性 | 中 | 优先使用OPFS，提供回退方案 |
| CRDT复杂性 | 高 | 从简单类型开始，逐步扩展 |
| 同步一致性 | 高 | 充分测试向量时钟和LWW策略 |
