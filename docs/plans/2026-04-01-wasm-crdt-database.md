# WASM CRDT Database Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 创建一个基于Rust的WASM嵌入式NoSQL数据库，使用GraphQL语法查询，CRDT数据同步，支持最终一致性。

**Architecture:** 混合方法：核心CRDT引擎自定义实现，GraphQL解析使用async-graphql库。存储层使用文件句柄API，支持WASI和浏览器环境。

**Tech Stack:** Rust, WASM, async-graphql, bincode, serde, tokio, WebSocket

---

### Task 1: 项目初始化

**Files:**
- Create: `Cargo.toml`
- Create: `src/lib.rs`
- Create: `src/core/mod.rs`
- Create: `src/storage/mod.rs`
- Create: `src/graphql/mod.rs`
- Create: `src/sync/mod.rs`
- Create: `src/api/mod.rs`

**Step 1: 创建Cargo.toml**

```toml
[package]
name = "nuclear"
version = "0.1.0"
edition = "2021"
description = "WASM CRDT NoSQL Database with GraphQL"

[lib]
crate-type = ["cdylib", "rlib"]

[dependencies]
wasm-bindgen = "0.2"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
bincode = "1.3"
async-graphql = "7.0"
tokio = { version = "1.0", features = ["full"] }
thiserror = "1.0"
anyhow = "1.0"

[dev-dependencies]
wasm-bindgen-test = "0.3"
```

**Step 2: 创建lib.rs**

```rust
pub mod core;
pub mod storage;
pub mod graphql;
pub mod sync;
pub mod api;

pub use api::Database;
```

**Step 3: 创建模块文件**

创建空的模块文件：
- `src/core/mod.rs`
- `src/storage/mod.rs`
- `src/graphql/mod.rs`
- `src/sync/mod.rs`
- `src/api/mod.rs`

**Step 4: 验证项目结构**

Run: `cargo check`
Expected: 编译通过，无错误

**Step 5: 提交**

```bash
git add .
git commit -m "feat: initialize project structure"
```

---

### Task 2: 向量时钟实现

**Files:**
- Create: `src/core/clock.rs`
- Modify: `src/core/mod.rs`

**Step 1: 编写测试**

```rust
// src/core/clock.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_clock_increment() {
        let mut clock = VectorClock::new();
        clock.increment("node1");
        assert_eq!(clock.get("node1"), 1);
    }

    #[test]
    fn test_vector_clock_merge() {
        let mut clock1 = VectorClock::new();
        clock1.increment("node1");
        
        let mut clock2 = VectorClock::new();
        clock2.increment("node2");
        
        clock1.merge(&clock2);
        assert_eq!(clock1.get("node1"), 1);
        assert_eq!(clock1.get("node2"), 1);
    }

    #[test]
    fn test_vector_clock_compare() {
        let mut clock1 = VectorClock::new();
        clock1.increment("node1");
        
        let mut clock2 = VectorClock::new();
        clock2.increment("node1");
        clock2.increment("node1");
        
        assert!(clock1 < clock2);
    }
}
```

**Step 2: 运行测试验证失败**

Run: `cargo test clock::tests`
Expected: FAIL with "VectorClock not defined"

**Step 3: 实现VectorClock**

```rust
// src/core/clock.rs
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorClock {
    clocks: HashMap<String, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self {
            clocks: HashMap::new(),
        }
    }

    pub fn increment(&mut self, node_id: &str) {
        let counter = self.clocks.entry(node_id.to_string()).or_insert(0);
        *counter += 1;
    }

    pub fn get(&self, node_id: &str) -> u64 {
        self.clocks.get(node_id).copied().unwrap_or(0)
    }

    pub fn merge(&mut self, other: &VectorClock) {
        for (node_id, &counter) in &other.clocks {
            let entry = self.clocks.entry(node_id.clone()).or_insert(0);
            *entry = (*entry).max(counter);
        }
    }

    pub fn happens_before(&self, other: &VectorClock) -> bool {
        let mut at_least_one_less = false;
        
        // 检查所有节点
        let all_nodes: std::collections::HashSet<_> = 
            self.clocks.keys().chain(other.clocks.keys()).collect();
        
        for node in all_nodes {
            let self_count = self.get(node);
            let other_count = other.get(node);
            
            if self_count > other_count {
                return false;
            }
            if self_count < other_count {
                at_least_one_less = true;
            }
        }
        
        at_least_one_less
    }

    pub fn concurrent(&self, other: &VectorClock) -> bool {
        !self.happens_before(other) && !other.happens_before(self)
    }
}

impl PartialOrd for VectorClock {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.happens_before(other) {
            Some(std::cmp::Ordering::Less)
        } else if other.happens_before(self) {
            Some(std::cmp::Ordering::Greater)
        } else if self == other {
            Some(std::cmp::Ordering::Equal)
        } else {
            None // concurrent
        }
    }
}
```

**Step 4: 更新模块文件**

```rust
// src/core/mod.rs
pub mod clock;
pub use clock::VectorClock;
```

**Step 5: 运行测试验证通过**

Run: `cargo test clock::tests`
Expected: PASS

**Step 6: 提交**

```bash
git add src/core/clock.rs src/core/mod.rs
git commit -m "feat: implement vector clock"
```

---

### Task 3: LWW寄存器实现

**Files:**
- Create: `src/core/lww.rs`
- Modify: `src/core/mod.rs`

**Step 1: 编写测试**

```rust
// src/core/lww.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lww_register_set() {
        let mut reg = LWWRegister::new("node1");
        reg.set("value1");
        assert_eq!(reg.get(), Some(&"value1"));
    }

    #[test]
    fn test_lww_register_merge_later_wins() {
        let mut reg1 = LWWRegister::new("node1");
        reg1.set("value1");
        
        let mut reg2 = LWWRegister::new("node2");
        std::thread::sleep(std::time::Duration::from_millis(10));
        reg2.set("value2");
        
        reg1.merge(&reg2);
        assert_eq!(reg1.get(), Some(&"value2"));
    }

    #[test]
    fn test_lww_register_merge_equal_timestamp() {
        let mut reg1 = LWWRegister::new("node1");
        reg1.set("value1");
        
        let mut reg2 = LWWRegister::new("node2");
        reg2.set("value2");
        
        // 相同时间戳，node_id大的获胜
        reg1.merge(&reg2);
        assert_eq!(reg1.get(), Some(&"value2"));
    }
}
```

**Step 2: 运行测试验证失败**

Run: `cargo test lww::tests`
Expected: FAIL with "LWWRegister not defined"

**Step 3: 实现LWWRegister**

```rust
// src/core/lww.rs
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LWWRegister<T> {
    value: T,
    timestamp: u64,
    node_id: String,
}

impl<T: Clone + PartialEq> LWWRegister<T> {
    pub fn new(node_id: &str) -> Self {
        Self {
            value: T::default(), // 需要T实现Default
            timestamp: 0,
            node_id: node_id.to_string(),
        }
    }

    pub fn set(&mut self, value: T) {
        self.value = value;
        self.timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
    }

    pub fn get(&self) -> Option<&T> {
        if self.timestamp == 0 {
            None
        } else {
            Some(&self.value)
        }
    }

    pub fn merge(&mut self, other: &LWWRegister<T>) {
        if other.timestamp > self.timestamp {
            self.value = other.value.clone();
            self.timestamp = other.timestamp;
            self.node_id = other.node_id.clone();
        } else if other.timestamp == self.timestamp && other.node_id > self.node_id {
            self.value = other.value.clone();
            self.node_id = other.node_id.clone();
        }
    }
}
```

**Step 4: 更新模块文件**

```rust
// src/core/mod.rs
pub mod clock;
pub mod lww;
pub use clock::VectorClock;
pub use lww::LWWRegister;
```

**Step 5: 运行测试验证通过**

Run: `cargo test lww::tests`
Expected: PASS

**Step 6: 提交**

```bash
git add src/core/lww.rs src/core/mod.rs
git commit -m "feat: implement LWW register"
```

---

### Task 4: LWW映射实现

**Files:**
- Create: `src/core/lww_map.rs`
- Modify: `src/core/mod.rs`

**Step 1: 编写测试**

```rust
// src/core/lww_map.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lww_map_insert() {
        let mut map = LWWMap::new("node1");
        map.insert("key1", "value1");
        assert_eq!(map.get("key1"), Some(&"value1"));
    }

    #[test]
    fn test_lww_map_merge() {
        let mut map1 = LWWMap::new("node1");
        map1.insert("key1", "value1");
        
        let mut map2 = LWWMap::new("node2");
        map2.insert("key1", "value2");
        map2.insert("key2", "value3");
        
        map1.merge(&map2);
        assert_eq!(map1.get("key2"), Some(&"value3"));
    }

    #[test]
    fn test_lww_map_remove() {
        let mut map = LWWMap::new("node1");
        map.insert("key1", "value1");
        map.remove("key1");
        assert_eq!(map.get("key1"), None);
    }
}
```

**Step 2: 运行测试验证失败**

Run: `cargo test lww_map::tests`
Expected: FAIL with "LWWMap not defined"

**Step 3: 实现LWWMap**

```rust
// src/core/lww_map.rs
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use super::LWWRegister;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LWWMap<K, V> {
    entries: HashMap<K, LWWRegister<Option<V>>>,
    node_id: String,
}

impl<K: Clone + std::hash::Hash + Eq, V: Clone + PartialEq> LWWMap<K, V> {
    pub fn new(node_id: &str) -> Self {
        Self {
            entries: HashMap::new(),
            node_id: node_id.to_string(),
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        let entry = self.entries.entry(key).or_insert_with(|| LWWRegister::new(&self.node_id));
        entry.set(Some(value));
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key).and_then(|reg| reg.get().and_then(|v| v.as_ref()))
    }

    pub fn remove(&mut self, key: K) {
        let entry = self.entries.entry(key).or_insert_with(|| LWWRegister::new(&self.node_id));
        entry.set(None);
    }

    pub fn merge(&mut self, other: &LWWMap<K, V>) {
        for (key, other_reg) in &other.entries {
            let entry = self.entries.entry(key.clone()).or_insert_with(|| LWWRegister::new(&self.node_id));
            entry.merge(other_reg);
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.entries.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.values().filter_map(|reg| reg.get().and_then(|v| v.as_ref()))
    }
}
```

**Step 4: 更新模块文件**

```rust
// src/core/mod.rs
pub mod clock;
pub mod lww;
pub mod lww_map;
pub use clock::VectorClock;
pub use lww::LWWRegister;
pub use lww_map::LWWMap;
```

**Step 5: 运行测试验证通过**

Run: `cargo test lww_map::tests`
Expected: PASS

**Step 6: 提交**

```bash
git add src/core/lww_map.rs src/core/mod.rs
git commit -m "feat: implement LWW map"
```

---

### Task 5: 存储层接口

**Files:**
- Create: `src/storage/trait.rs`
- Create: `src/storage/error.rs`
- Modify: `src/storage/mod.rs`

**Step 1: 定义错误类型**

```rust
// src/storage/error.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("File not found: {0}")]
    NotFound(String),
    
    #[error("Permission denied: {0}")]
    PermissionDenied(String),
    
    #[error("Invalid handle")]
    InvalidHandle,
    
    #[error("WASM error: {0}")]
    WasmError(String),
}
```

**Step 2: 定义存储接口**

```rust
// src/storage/trait.rs
use async_trait::async_trait;
use super::error::StorageError;

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug, Clone)]
pub struct FileHandle(u64);

#[derive(Debug, Clone)]
pub struct OpenOptions {
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub truncate: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            read: true,
            write: true,
            create: true,
            truncate: false,
        }
    }
}

#[async_trait]
pub trait Storage: Send + Sync {
    async fn open(&self, path: &str, options: OpenOptions) -> Result<FileHandle>;
    async fn close(&self, handle: FileHandle) -> Result<()>;
    async fn read(&self, handle: FileHandle, offset: u64, buf: &mut [u8]) -> Result<usize>;
    async fn write(&self, handle: FileHandle, offset: u64, buf: &[u8]) -> Result<usize>;
    async fn sync(&self, handle: FileHandle) -> Result<()>;
    async fn size(&self, handle: FileHandle) -> Result<u64>;
}
```

**Step 3: 更新模块文件**

```rust
// src/storage/mod.rs
pub mod error;
pub mod r#trait;
pub use error::StorageError;
pub use r#trait::{Storage, FileHandle, OpenOptions, Result};
```

**Step 4: 验证编译**

Run: `cargo check`
Expected: 编译通过

**Step 5: 提交**

```bash
git add src/storage/trait.rs src/storage/error.rs src/storage/mod.rs
git commit -m "feat: define storage trait and error types"
```

---

### Task 6: WASI存储实现

**Files:**
- Create: `src/storage/wasi.rs`
- Modify: `src/storage/mod.rs`

**Step 1: 编写测试**

```rust
// src/storage/wasi.rs
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wasi_storage_write_read() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let handle = storage.open("test.bin", OpenOptions::default()).await.unwrap();
        storage.write(handle, 0, b"hello").await.unwrap();
        
        let mut buf = [0u8; 5];
        let bytes = storage.read(handle, 0, &mut buf).await.unwrap();
        assert_eq!(bytes, 5);
        assert_eq!(&buf, b"hello");
        
        storage.close(handle).await.unwrap();
    }

    #[tokio::test]
    async fn test_wasi_storage_size() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let handle = storage.open("test.bin", OpenOptions::default()).await.unwrap();
        storage.write(handle, 0, b"hello world").await.unwrap();
        
        let size = storage.size(handle).await.unwrap();
        assert_eq!(size, 11);
        
        storage.close(handle).await.unwrap();
    }
}
```

**Step 2: 运行测试验证失败**

Run: `cargo test wasi::tests`
Expected: FAIL with "WasiStorage not defined"

**Step 3: 实现WasiStorage**

```rust
// src/storage/wasi.rs
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::fs::{File, OpenOptions as TokioOpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};

use super::{Storage, FileHandle, OpenOptions, Result, StorageError};

pub struct WasiStorage {
    base_path: PathBuf,
    files: Arc<RwLock<HashMap<u64, File>>>,
    next_handle: Arc<RwLock<u64>>,
}

impl WasiStorage {
    pub fn new(base_path: impl AsRef<Path>) -> Self {
        Self {
            base_path: base_path.as_ref().to_path_buf(),
            files: Arc::new(RwLock::new(HashMap::new())),
            next_handle: Arc::new(RwLock::new(1)),
        }
    }
}

#[async_trait::async_trait]
impl Storage for WasiStorage {
    async fn open(&self, path: &str, options: OpenOptions) -> Result<FileHandle> {
        let full_path = self.base_path.join(path);
        
        let mut open_options = TokioOpenOptions::new();
        open_options.read(options.read);
        open_options.write(options.write);
        open_options.create(options.create);
        open_options.truncate(options.truncate);
        
        let file = open_options.open(&full_path).await?;
        
        let mut handle_id = self.next_handle.write().await;
        let handle = FileHandle(*handle_id);
        *handle_id += 1;
        
        self.files.write().await.insert(handle.0, file);
        
        Ok(handle)
    }

    async fn close(&self, handle: FileHandle) -> Result<()> {
        self.files.write().await.remove(&handle.0)
            .ok_or(StorageError::InvalidHandle)?;
        Ok(())
    }

    async fn read(&self, handle: FileHandle, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let mut files = self.files.write().await;
        let file = files.get_mut(&handle.0)
            .ok_or(StorageError::InvalidHandle)?;
        
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let bytes = file.read(buf).await?;
        Ok(bytes)
    }

    async fn write(&self, handle: FileHandle, offset: u64, buf: &[u8]) -> Result<usize> {
        let mut files = self.files.write().await;
        let file = files.get_mut(&handle.0)
            .ok_or(StorageError::InvalidHandle)?;
        
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        file.write_all(buf).await?;
        Ok(buf.len())
    }

    async fn sync(&self, handle: FileHandle) -> Result<()> {
        let files = self.files.read().await;
        let file = files.get(&handle.0)
            .ok_or(StorageError::InvalidHandle)?;
        
        file.sync_data().await?;
        Ok(())
    }

    async fn size(&self, handle: FileHandle) -> Result<u64> {
        let files = self.files.read().await;
        let file = files.get(&handle.0)
            .ok_or(StorageError::InvalidHandle)?;
        
        let metadata = file.metadata().await?;
        Ok(metadata.len())
    }
}
```

**Step 4: 更新模块文件**

```rust
// src/storage/mod.rs
pub mod error;
pub mod r#trait;
pub mod wasi;
pub use error::StorageError;
pub use r#trait::{Storage, FileHandle, OpenOptions, Result};
pub use wasi::WasiStorage;
```

**Step 5: 运行测试验证通过**

Run: `cargo test wasi::tests`
Expected: PASS

**Step 6: 提交**

```bash
git add src/storage/wasi.rs src/storage/mod.rs
git commit -m "feat: implement WASI storage"
```

---

### Task 7: GraphQL Schema生成

**Files:**
- Create: `src/graphql/schema.rs`
- Create: `src/graphql/scalars.rs`
- Modify: `src/graphql/mod.rs`

**Step 1: 定义自定义标量**

```rust
// src/graphql/scalars.rs
use async_graphql::*;
use serde::{Deserialize, Serialize};

/// JSON标量类型，用于存储无schema数据
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Json(pub serde_json::Value);

#[Scalar]
impl ScalarType for Json {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::Object(_) | Value::List(_) | Value::String(_) | 
            Value::Number(_) | Value::Boolean(_) | Value::Null => {
                Ok(Json(serde_json::to_value(&value)?))
            }
        }
    }

    fn to_value(&self) -> Value {
        serde_json::from_value(self.0.clone()).unwrap_or(Value::Null)
    }
}

/// DateTime标量类型
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DateTime(pub chrono::DateTime<chrono::Utc>);

#[Scalar]
impl ScalarType for DateTime {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(s) = value {
            let dt = chrono::DateTime::parse_from_rfc3339(&s)?
                .with_timezone(&chrono::Utc);
            Ok(DateTime(dt))
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_rfc3339())
    }
}
```

**Step 2: 定义元数据类型**

```rust
// src/graphql/schema.rs
use async_graphql::*;
use super::scalars::{Json, DateTime};

/// 元数据类型，包含核心字段
#[derive(SimpleObject, Debug, Clone)]
pub struct Meta {
    pub id: ID,
    pub created_at: DateTime,
    pub updated_at: DateTime,
}

/// 通用记录类型，用户数据扁平化
#[derive(Debug, Clone)]
pub struct Record {
    pub data: serde_json::Value,
    pub meta: Meta,
}

#[Object]
impl Record {
    /// 动态解析用户定义的字段
    async fn field(&self, name: String) -> Option<Json> {
        self.data.get(&name).map(|v| Json(v.clone()))
    }
    
    /// 返回完整数据
    async fn data(&self) -> Json {
        Json(self.data.clone())
    }
    
    /// 返回元数据
    async fn _meta(&self) -> &Meta {
        &self.meta
    }
}
```

**Step 3: 定义查询根**

```rust
// src/graphql/schema.rs
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// 查询记录列表
    async fn records(
        &self,
        ctx: &Context<'_>,
        collection: String,
        filter: Option<Json>,
        order_by: Option<Json>,
        first: Option<i32>,
        offset: Option<i32>,
    ) -> Result<Vec<Record>> {
        // 实现查询逻辑
        // 1. 从CRDT引擎获取指定集合的数据
        // 2. 应用过滤条件
        // 3. 应用排序和分页
        // 4. 返回Record列表
        todo!()
    }

    /// 查询单个记录
    async fn record(
        &self, 
        ctx: &Context<'_>, 
        collection: String,
        id: ID
    ) -> Result<Option<Record>> {
        // 实现单个记录查询
        todo!()
    }

    /// 聚合查询
    async fn records_aggregate(
        &self, 
        ctx: &Context<'_>,
        collection: String
    ) -> Result<RecordsAggregate> {
        // 实现聚合查询
        todo!()
    }
}

#[derive(SimpleObject)]
pub struct RecordsAggregate {
    pub count: i32,
}

/// 关联查询支持
#[Object]
impl Record {
    /// 嵌套字段解析，支持关联查询
    /// 例如：用户记录中嵌套订单数据
    async fn resolve_field(
        &self,
        ctx: &Context<'_>,
        field_name: String,
    ) -> Result<Option<Json>> {
        // 1. 检查是否是关联字段
        // 2. 如果是关联字段，查询关联数据
        // 3. 返回关联数据
        
        // 关联字段配置示例：
        // - users.orders -> 查询 orders 集合中 user_id 等于当前记录 id 的记录
        // - orders.user -> 查询 users 集合中 id 等于当前记录 user_id 的记录
        
        // 关联配置可以从数据库配置中读取，或通过约定命名规则推断
        todo!()
    }
}
```

**Step 4: 定义变更根**

```rust
// src/graphql/schema.rs
pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// 创建记录
    async fn create_record(
        &self, 
        ctx: &Context<'_>,
        collection: String,
        data: Json
    ) -> Result<Record> {
        // 实现创建记录
        todo!()
    }

    /// 更新记录
    async fn update_record(
        &self, 
        ctx: &Context<'_>,
        collection: String,
        id: ID,
        data: Json
    ) -> Result<Option<Record>> {
        // 实现更新记录
        todo!()
    }

    /// 删除记录
    async fn delete_record(
        &self, 
        ctx: &Context<'_>,
        collection: String,
        id: ID
    ) -> Result<bool> {
        // 实现删除记录
        todo!()
    }
}
```

**Step 5: 定义订阅根**

```rust
// src/graphql/schema.rs
pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    /// 订阅记录创建
    async fn record_created(
        &self, 
        ctx: &Context<'_>,
        collection: String
    ) -> impl Stream<Item = Record> {
        // 实现记录创建订阅
        todo!()
    }

    /// 订阅记录更新
    async fn record_updated(
        &self, 
        ctx: &Context<'_>,
        collection: String
    ) -> impl Stream<Item = Record> {
        // 实现记录更新订阅
        todo!()
    }

    /// 订阅记录删除
    async fn record_deleted(
        &self, 
        ctx: &Context<'_>,
        collection: String
    ) -> impl Stream<Item = ID> {
        // 实现记录删除订阅
        todo!()
    }
}
```

**Step 6: 更新模块文件**

```rust
// src/graphql/mod.rs
pub mod schema;
pub mod scalars;
pub use schema::{QueryRoot, MutationRoot, SubscriptionRoot, Record, Meta};
pub use scalars::{Json, DateTime};
```

**Step 7: 验证编译**

Run: `cargo check`
Expected: 编译通过（有todo!警告）

**Step 8: 提交**

```bash
git add src/graphql/schema.rs src/graphql/scalars.rs src/graphql/mod.rs
git commit -m "feat: define GraphQL schema with flat data and _meta"
```

---

### Task 8: 数据库核心实现

**Files:**
- Create: `src/api/database.rs`
- Create: `src/api/builder.rs`
- Modify: `src/api/mod.rs`

**Step 1: 定义数据库结构**

```rust
// src/api/database.rs
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::core::{VectorClock, LWWMap};
use crate::storage::Storage;
use crate::graphql::{QueryRoot, MutationRoot, SubscriptionRoot};

pub struct Database<S: Storage> {
    storage: Arc<S>,
    /// 集合存储：collection_name -> Collection
    collections: Arc<RwLock<LWWMap<String, Collection>>>,
    node_id: String,
    clock: Arc<RwLock<VectorClock>>,
    /// 关联配置：collection.field -> (target_collection, foreign_key)
    relations: Arc<RwLock<std::collections::HashMap<String, RelationConfig>>>,
}

struct Collection {
    /// 记录数据：record_id -> record_data
    data: LWWMap<String, RecordData>,
}

struct RecordData {
    /// 用户定义的数据（扁平化）
    fields: serde_json::Value,
    /// 元数据
    meta: RecordMeta,
}

struct RecordMeta {
    id: String,
    created_at: u64,
    updated_at: u64,
    /// 向量时钟用于冲突解决
    clock: VectorClock,
}

/// 关联配置
struct RelationConfig {
    /// 目标集合
    target_collection: String,
    /// 外键字段（在目标集合中）
    foreign_key: String,
    /// 当前集合中的引用字段
    local_key: String,
}

impl<S: Storage> Database<S> {
    pub async fn open(storage: S, node_id: String) -> Result<Self, StorageError> {
        Ok(Self {
            storage: Arc::new(storage),
            collections: Arc::new(RwLock::new(LWWMap::new(&node_id))),
            node_id,
            clock: Arc::new(RwLock::new(VectorClock::new())),
            relations: Arc::new(RwLock::new(std::collections::HashMap::new())),
        })
    }

    /// 执行GraphQL查询
    pub async fn query(&self, query: &str) -> Result<serde_json::Value, StorageError> {
        // 1. 解析GraphQL查询
        // 2. 提取collection参数
        // 3. 查询指定集合的数据
        // 4. 处理嵌套字段（关联查询）
        // 5. 返回结果
        todo!()
    }

    /// 执行GraphQL变更
    pub async fn mutation(&self, mutation: &str) -> Result<serde_json::Value, StorageError> {
        // 1. 解析GraphQL变更
        // 2. 提取collection和data参数
        // 3. 执行创建/更新/删除操作
        // 4. 更新向量时钟
        // 5. 返回结果
        todo!()
    }

    /// 订阅GraphQL变更
    pub async fn subscribe(&self, subscription: &str) -> Result<impl futures::Stream<Item = serde_json::Value>, StorageError> {
        // 1. 解析GraphQL订阅
        // 2. 提取collection参数
        // 3. 监听指定集合的变更
        // 4. 返回变更流
        todo!()
    }

    /// 注册关联关系
    pub async fn register_relation(
        &self,
        collection: &str,
        field: &str,
        target_collection: &str,
        foreign_key: &str,
        local_key: &str,
    ) -> Result<(), StorageError> {
        let mut relations = self.relations.write().await;
        let key = format!("{}.{}", collection, field);
        relations.insert(key, RelationConfig {
            target_collection: target_collection.to_string(),
            foreign_key: foreign_key.to_string(),
            local_key: local_key.to_string(),
        });
        Ok(())
    }

    /// 解析关联查询
    async fn resolve_relation(
        &self,
        collection: &str,
        record_id: &str,
        field: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        let relations = self.relations.read().await;
        let key = format!("{}.{}", collection, field);
        
        if let Some(config) = relations.get(&key) {
            // 1. 获取当前记录的local_key值
            // 2. 查询目标集合中foreign_key匹配的记录
            // 3. 返回关联数据
            todo!()
        } else {
            Ok(None)
        }
    }
}
```

**Step 2: 定义数据库构建器**

```rust
// src/api/builder.rs
use std::time::Duration;
use crate::storage::Storage;
use super::database::Database;

pub struct DatabaseBuilder<S: Storage> {
    storage: S,
    node_id: Option<String>,
    sync_interval: Duration,
    cache_size: usize,
    relations: Vec<RelationConfig>,
}

/// 构建时的关联配置
pub struct RelationConfig {
    pub collection: String,
    pub field: String,
    pub target_collection: String,
    pub foreign_key: String,
    pub local_key: String,
}

impl<S: Storage> DatabaseBuilder<S> {
    pub fn new(storage: S) -> Self {
        Self {
            storage,
            node_id: None,
            sync_interval: Duration::from_secs(1),
            cache_size: 1024 * 1024 * 100, // 100MB
            relations: Vec::new(),
        }
    }

    pub fn node_id(mut self, node_id: String) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub fn sync_interval(mut self, interval: Duration) -> Self {
        self.sync_interval = interval;
        self
    }

    pub fn cache_size(mut self, size: usize) -> Self {
        self.cache_size = size;
        self
    }

    /// 添加关联配置
    pub fn relation(mut self, config: RelationConfig) -> Self {
        self.relations.push(config);
        self
    }

    /// 批量添加关联配置
    pub fn relations(mut self, configs: Vec<RelationConfig>) -> Self {
        self.relations.extend(configs);
        self
    }

    pub async fn build(self) -> Result<Database<S>, StorageError> {
        let node_id = self.node_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let db = Database::open(self.storage, node_id).await?;
        
        // 注册所有关联配置
        for config in self.relations {
            db.register_relation(
                &config.collection,
                &config.field,
                &config.target_collection,
                &config.foreign_key,
                &config.local_key,
            ).await?;
        }
        
        Ok(db)
    }
}
```

**Step 3: 更新模块文件**

```rust
// src/api/mod.rs
pub mod database;
pub mod builder;
pub use database::Database;
pub use builder::DatabaseBuilder;
```

**Step 4: 验证编译**

Run: `cargo check`
Expected: 编译通过（有todo!警告）

**Step 5: 提交**

```bash
git add src/api/database.rs src/api/builder.rs src/api/mod.rs
git commit -m "feat: implement database core and builder"
```

---

### Task 9: 同步引擎基础

**Files:**
- Create: `src/sync/protocol.rs`
- Create: `src/sync/conflict.rs`
- Modify: `src/sync/mod.rs`

**Step 1: 定义同步协议**

```rust
// src/sync/protocol.rs
use serde::{Deserialize, Serialize};
use crate::core::VectorClock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncMessage {
    /// 请求同步
    SyncRequest {
        from: String,
        clock: VectorClock,
    },
    
    /// 同步响应
    SyncResponse {
        from: String,
        clock: VectorClock,
        changes: Vec<DataChange>,
    },
    
    /// 数据变更通知
    ChangeNotification {
        from: String,
        changes: Vec<DataChange>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataChange {
    pub collection: String,
    pub key: String,
    pub value: Option<serde_json::Value>,
    pub clock: VectorClock,
    pub timestamp: u64,
}
```

**Step 2: 定义冲突解决**

```rust
// src/sync/conflict.rs
use crate::core::VectorClock;
use super::protocol::DataChange;

pub struct ConflictResolver;

impl ConflictResolver {
    pub fn resolve(change1: &DataChange, change2: &DataChange) -> DataChange {
        // LWW策略：时间戳大的获胜
        if change1.timestamp > change2.timestamp {
            change1.clone()
        } else if change2.timestamp > change1.timestamp {
            change2.clone()
        } else {
            // 时间戳相同，node_id大的获胜
            if change1.clock > change2.clock {
                change1.clone()
            } else {
                change2.clone()
            }
        }
    }
}
```

**Step 3: 更新模块文件**

```rust
// src/sync/mod.rs
pub mod protocol;
pub mod conflict;
pub use protocol::{SyncMessage, DataChange};
pub use conflict::ConflictResolver;
```

**Step 4: 验证编译**

Run: `cargo check`
Expected: 编译通过

**Step 5: 提交**

```bash
git add src/sync/protocol.rs src/sync/conflict.rs src/sync/mod.rs
git commit -m "feat: implement sync protocol and conflict resolver"
```

---

### Task 10: 集成测试

**Files:**
- Create: `tests/integration_test.rs`

**Step 1: 编写集成测试**

```rust
// tests/integration_test.rs
use nuclear::*;
use nuclear::storage::WasiStorage;
use tempfile::tempdir;

#[tokio::test]
async fn test_database_crud() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = Database::open(storage, "node1".to_string()).await.unwrap();
    
    // 创建用户
    let result = db.mutation(r#"
        mutation {
            create_user(input: {name: "Alice", email: "alice@example.com"}) {
                id
                name
            }
        }
    "#).await.unwrap();
    
    // 查询用户
    let result = db.query(r#"
        query {
            users {
                id
                name
                email
            }
        }
    "#).await.unwrap();
    
    assert!(result.get("data").is_some());
}

#[tokio::test]
async fn test_database_sync() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let storage1 = WasiStorage::new(dir1.path());
    let storage2 = WasiStorage::new(dir2.path());
    
    let db1 = Database::open(storage1, "node1".to_string()).await.unwrap();
    let db2 = Database::open(storage2, "node2".to_string()).await.unwrap();
    
    // 在db1中创建数据
    db1.mutation(r#"
        mutation {
            create_user(input: {name: "Bob", email: "bob@example.com"}) {
                id
            }
        }
    "#).await.unwrap();
    
    // 同步到db2
    // 实现同步逻辑
    
    // 验证db2中有数据
    let result = db2.query("query { users { name } }").await.unwrap();
    // 验证结果
}
```

**Step 2: 运行测试**

Run: `cargo test --test integration_test`
Expected: 测试运行（可能有未实现的部分）

**Step 3: 提交**

```bash
git add tests/integration_test.rs
git commit -m "test: add integration tests"
```

---

### Task 11: 示例和文档

**Files:**
- Create: `examples/basic_usage.rs`
- Create: `README.md`

**Step 1: 创建示例**

```rust
// examples/basic_usage.rs
use nuclear::*;
use nuclear::storage::WasiStorage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建存储
    let storage = WasiStorage::new("./data");
    
    // 打开数据库
    let db = Database::builder(storage)
        .node_id("node1".to_string())
        .build()
        .await?;
    
    // 创建用户
    let result = db.mutation(r#"
        mutation {
            create_user(input: {name: "Alice", email: "alice@example.com"}) {
                id
                name
            }
        }
    "#).await?;
    
    println!("Created user: {:?}", result);
    
    // 查询用户
    let result = db.query(r#"
        query {
            users {
                id
                name
                email
            }
        }
    "#).await?;
    
    println!("Users: {:?}", result);
    
    // 订阅更新
    let mut stream = db.subscribe(r#"
        subscription {
            user_updated {
                id
                name
            }
        }
    "#).await?;
    
    while let Some(event) = stream.next().await {
        println!("User updated: {:?}", event);
    }
    
    Ok(())
}
```

**Step 2: 创建README**

```markdown
# Nuclear - WASM CRDT Database

A WASM-based embedded NoSQL database with GraphQL query language and CRDT synchronization.

## Features

- **WASM Compatible**: Runs in browsers and WASI runtimes
- **GraphQL Queries**: Full GraphQL support with queries, mutations, and subscriptions
- **CRDT Synchronization**: Automatic conflict resolution with LWW strategy
- **Real-time Sync**: WebSocket-based real-time synchronization
- **Offline Support**: Works offline with automatic sync when online

## Quick Start

```rust
use nuclear::*;
use nuclear::storage::WasiStorage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = WasiStorage::new("./data");
    let db = Database::builder(storage)
        .node_id("node1".to_string())
        .build()
        .await?;
    
    // GraphQL query
    let result = db.query("query { users { name } }").await?;
    println!("{:?}", result);
    
    Ok(())
}
```

## Architecture

- **Core CRDT Engine**: Custom implementation with LWW registers and vector clocks
- **Storage Layer**: File handle API with WASI and browser support
- **GraphQL Layer**: async-graphql for parsing and execution
- **Sync Engine**: WebSocket-based real-time synchronization
- **API Layer**: Clean, functional interface

## License

MIT
```

**Step 3: 提交**

```bash
git add examples/basic_usage.rs README.md
git commit -m "docs: add example and README"
```

---

## 完成

计划已保存到 `docs/plans/2026-04-01-wasm-crdt-database.md`。

两种执行选项：

**1. 子代理驱动（当前会话）** - 我为每个任务分派新的子代理，任务间进行审查，快速迭代

**2. 并行会话（单独）** - 打开新会话使用 executing-plans，批量执行带检查点

请选择哪种方式？
