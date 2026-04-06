use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::page::{FileHeader, Page, PageHeader, PageNumber, PageType, PAGE_SIZE, PAGE_HEADER_SIZE, CELL_POINTER_SIZE};
use super::buffer_pool::{BufferPoolConfig, SharedBufferPool};
use super::{Storage, StorageError};

#[derive(Debug, Clone)]
pub struct RecordLocation {
    pub page_number: PageNumber,
    pub cell_index: u16,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub deleted: bool,
}

/// 元数据序列化结构
#[derive(Serialize, Deserialize)]
struct CollectionMetadata {
    collection_roots: HashMap<String, PageNumber>,
    collection_pages: HashMap<String, Vec<PageNumber>>,
}

pub struct PageManager {
    buffer_pool: SharedBufferPool,
    collection_roots: HashMap<String, PageNumber>,
    collection_pages: HashMap<String, Vec<PageNumber>>,
    /// 文件是否已初始化（防止重复加载元数据）
    metadata_loaded: bool,
}

impl PageManager {
    pub fn new(buffer_pool: SharedBufferPool) -> Self {
        Self {
            buffer_pool,
            collection_roots: HashMap::new(),
            collection_pages: HashMap::new(),
            metadata_loaded: false,
        }
    }

    /// 尝试从元数据页面加载集合映射（仅在首次调用时执行）
    pub async fn load_metadata_if_needed(&mut self) -> Result<(), StorageError> {
        if self.metadata_loaded {
            return Ok(());
        }
        self.metadata_loaded = true;

        // 读取 FileHeader 获取 metadata_page 编号（直接从文件读取）
        let file_header = self.buffer_pool.read_file_header().await
            .map_err(|e| StorageError::WasmError(format!("Failed to read file header: {}", e)))?;

        let metadata_page = match file_header {
            Some(h) => h.metadata_page,
            None => return Ok(()), // 新文件，无 FileHeader
        };

        // 直接从文件读取元数据（绕过 Page 缓存，避免 Page.data 双用途问题）
        let (storage, file_path) = {
            let pool = self.buffer_pool.inner.read().await;
            (pool.storage_and_path().0.clone(), pool.storage_and_path().1.to_string())
        };
        let options = super::OpenOptions {
            read: true, write: false, create: false, truncate: false,
        };
        let handle = storage.open(&file_path, options).await
            .map_err(|e| StorageError::WasmError(format!("Failed to open file: {}", e)))?;
        let offset = metadata_page * PAGE_SIZE + 32;
        let mut buf = vec![0u8; PAGE_SIZE as usize - 32];
        let bytes_read = storage.read(handle, offset, &mut buf).await
            .map_err(|e| StorageError::WasmError(format!("Failed to read metadata: {}", e)))?;
        storage.close(handle).await
            .map_err(|e| StorageError::WasmError(format!("Failed to close file: {}", e)))?;

        if bytes_read < 4 {
            return Ok(());
        }

        let meta_len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
        if meta_len == 0 || 4 + meta_len > bytes_read {
            return Ok(());
        }

        let metadata: CollectionMetadata = bincode::deserialize(&buf[4..4 + meta_len]).map_err(|e| StorageError::WasmError(format!("Failed to deserialize metadata: {}", e)))?;

        self.collection_roots = metadata.collection_roots;
        self.collection_pages = metadata.collection_pages;

        Ok(())
    }

    /// 将集合映射持久化到元数据页面（直接写文件，绕过 Page 缓存）
    /// 格式：[meta_len: 4 bytes LE][bincode: meta_len bytes]，从文件偏移 page*4096+32 开始
    async fn save_metadata(&mut self) -> Result<(), StorageError> {
        let metadata = CollectionMetadata {
            collection_roots: self.collection_roots.clone(),
            collection_pages: self.collection_pages.clone(),
        };

        let meta_data = bincode::serialize(&metadata)
            .map_err(|e| StorageError::WasmError(format!("Failed to serialize metadata: {}", e)))?;

        let file_header = self.buffer_pool.read_file_header().await
            .map_err(|e| StorageError::WasmError(format!("Failed to read file header: {}", e)))?;

        let metadata_page = match file_header {
            Some(h) => h.metadata_page,
            None => return Ok(()),
        };

        // 直接写入文件：metadata_page * PAGE_SIZE + 32
        let mut buf = vec![0u8; PAGE_SIZE as usize - 32];
        let meta_len = meta_data.len() as u32;
        buf[..4].copy_from_slice(&meta_len.to_le_bytes());
        buf[4..4 + meta_data.len()].copy_from_slice(&meta_data);

        let (storage, file_path) = {
            let pool = self.buffer_pool.inner.read().await;
            (pool.storage_and_path().0.clone(), pool.storage_and_path().1.to_string())
        };
        let options = super::OpenOptions {
            read: false,
            write: true,
            create: false,
            truncate: false,
        };
        let handle = storage.open(&file_path, options).await
            .map_err(|e| StorageError::WasmError(format!("Failed to open file: {}", e)))?;
        let offset = metadata_page * PAGE_SIZE + 32;
        storage.write(handle, offset, &buf).await
            .map_err(|e| StorageError::WasmError(format!("Failed to write metadata: {}", e)))?;
        storage.sync(handle).await
            .map_err(|e| StorageError::WasmError(format!("Failed to sync metadata: {}", e)))?;
        storage.close(handle).await
            .map_err(|e| StorageError::WasmError(format!("Failed to close file: {}", e)))?;

        Ok(())
    }

    pub async fn initialize_collection(&mut self, collection: &str) -> Result<(), StorageError> {
        if self.collection_roots.contains_key(collection) {
            return Ok(());
        }
        let root_page = self.buffer_pool.allocate_page(PageType::Data).await?;
        self.collection_roots.insert(collection.to_string(), root_page);
        self.collection_pages.insert(collection.to_string(), vec![root_page]);
        // 同步 FileHeader
        self.buffer_pool.sync_file_header().await?;
        self.save_metadata().await
    }

    pub async fn insert(&mut self, collection: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        if !self.collection_roots.contains_key(collection) {
            self.initialize_collection(collection).await?;
        }

        // Check if key already exists and delete old version
        if let Some(loc) = self.find_record(collection, key).await? {
            self.delete_at_location(&loc).await?;
        }

        // Find a page with enough space
        let page_number = self.find_page_with_space(collection).await?;

        // Write the record
        {
            let page = self.buffer_pool.get_page(page_number).await?;
            let mut page = page; // clone
            page.write_record(key, value)
                .map_err(|e| StorageError::WasmError(format!("Failed to write record: {}", e)))?;
            self.buffer_pool.write_page(page).await?;
        }

        Ok(())
    }

    pub async fn get(&self, collection: &str, key: &[u8]) -> Result<Option<Record>, StorageError> {
        if let Some(loc) = self.find_record(collection, key).await? {
            let page = self.buffer_pool.get_page(loc.page_number).await?;
            let (record_key, record_value, deleted) = page.read_record_by_index(loc.cell_index)
                .map_err(|e| StorageError::WasmError(format!("Failed to read record: {}", e)))?;
            Ok(Some(Record {
                key: record_key,
                value: record_value,
                deleted,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn delete(&mut self, collection: &str, key: &[u8]) -> Result<bool, StorageError> {
        if let Some(loc) = self.find_record(collection, key).await? {
            self.delete_at_location(&loc).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn scan_collection(&self, collection: &str) -> Result<Vec<Record>, StorageError> {
        let mut records = Vec::new();
        if let Some(pages) = self.collection_pages.get(collection) {
            for &page_number in pages {
                let page = self.buffer_pool.get_page(page_number).await?;
                for result in page.iter_records() {
                    let (key, value, deleted) = result
                        .map_err(|e| StorageError::WasmError(format!("Failed to iterate: {}", e)))?;
                    records.push(Record { key, value, deleted });
                }
            }
        }
        Ok(records)
    }

    pub async fn count_records(&self, collection: &str) -> Result<usize, StorageError> {
        let records = self.scan_collection(collection).await?;
        Ok(records.iter().filter(|r| !r.deleted).count())
    }

    pub async fn flush(&self) -> Result<(), StorageError> {
        self.buffer_pool.flush().await
    }

    async fn find_record(&self, collection: &str, key: &[u8]) -> Result<Option<RecordLocation>, StorageError> {
        if let Some(pages) = self.collection_pages.get(collection) {
            for &page_number in pages {
                let page = self.buffer_pool.get_page(page_number).await?;
                for cell_index in page.iter_cells() {
                    let (record_key, _, deleted) = page.read_record_by_index(cell_index)
                        .map_err(|e| StorageError::WasmError(format!("Failed to read: {}", e)))?;
                    if record_key == key && !deleted {
                        return Ok(Some(RecordLocation { page_number, cell_index }));
                    }
                }
            }
        }
        Ok(None)
    }

    async fn delete_at_location(&mut self, loc: &RecordLocation) -> Result<(), StorageError> {
        let page = self.buffer_pool.get_page(loc.page_number).await?;
        let mut page = page;
        page.delete_record_by_index(loc.cell_index)
            .map_err(|e| StorageError::WasmError(format!("Failed to delete: {}", e)))?;
        self.buffer_pool.write_page(page).await?;
        Ok(())
    }

    async fn find_page_with_space(&mut self, collection: &str) -> Result<PageNumber, StorageError> {
        let min_free = 64u16;
        if let Some(pages) = self.collection_pages.get(collection) {
            for &page_number in pages {
                let page = self.buffer_pool.get_page(page_number).await?;
                if page.header.free_space() >= min_free {
                    return Ok(page_number);
                }
            }
        }
        // Allocate new page
        let new_page = self.buffer_pool.allocate_page(PageType::Data).await?;
        if let Some(pages) = self.collection_pages.get_mut(collection) {
            pages.push(new_page);
        }
        // 同步 FileHeader 并保存元数据
        self.buffer_pool.sync_file_header().await?;
        self.save_metadata().await?;
        Ok(new_page)
    }
}

pub struct SharedPageManager {
    inner: Arc<RwLock<PageManager>>,
}

impl SharedPageManager {
    pub fn new(buffer_pool: SharedBufferPool) -> Self {
        Self {
            inner: Arc::new(RwLock::new(PageManager::new(buffer_pool))),
        }
    }

    /// 加载元数据（启动时调用）
    pub async fn load_metadata(&self) -> Result<(), StorageError> {
        self.inner.write().await.load_metadata_if_needed().await
    }

    pub async fn initialize_collection(&self, collection: &str) -> Result<(), StorageError> {
        self.inner.write().await.initialize_collection(collection).await
    }

    pub async fn insert(&self, collection: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.inner.write().await.insert(collection, key, value).await
    }

    pub async fn get(&self, collection: &str, key: &[u8]) -> Result<Option<Record>, StorageError> {
        self.inner.read().await.get(collection, key).await
    }

    pub async fn delete(&self, collection: &str, key: &[u8]) -> Result<bool, StorageError> {
        self.inner.write().await.delete(collection, key).await
    }

    pub async fn scan_collection(&self, collection: &str) -> Result<Vec<Record>, StorageError> {
        self.inner.read().await.scan_collection(collection).await
    }

    pub async fn count_records(&self, collection: &str) -> Result<usize, StorageError> {
        self.inner.read().await.count_records(collection).await
    }

    pub async fn flush(&self) -> Result<(), StorageError> {
        self.inner.read().await.flush().await
    }

    /// 获取所有集合名称
    pub async fn collection_names(&self) -> Vec<String> {
        self.inner.read().await.collection_roots.keys().cloned().collect()
    }
}

impl Clone for SharedPageManager {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub struct PageStorageEngine {
    pub buffer_pool: SharedBufferPool,
    pub page_manager: SharedPageManager,
}

impl PageStorageEngine {
    pub async fn new(
        storage: Arc<dyn Storage>,
        file_path: &str,
        config: BufferPoolConfig,
    ) -> Result<Self, StorageError> {
        let buffer_pool = SharedBufferPool::new(storage, file_path, config);
        buffer_pool.initialize().await?;
        let page_manager = SharedPageManager::new(buffer_pool.clone());
        // 从元数据页面恢复集合映射
        page_manager.load_metadata().await?;
        Ok(Self {
            buffer_pool,
            page_manager,
        })
    }

    pub async fn flush(&self) -> Result<(), StorageError> {
        self.page_manager.flush().await
    }

    pub async fn stats(&self) -> super::buffer_pool::BufferPoolStats {
        self.buffer_pool.stats().await
    }
}

impl Clone for PageStorageEngine {
    fn clone(&self) -> Self {
        Self {
            buffer_pool: self.buffer_pool.clone(),
            page_manager: self.page_manager.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::WasiStorage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_page_manager_initialize_collection() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();
        let config = BufferPoolConfig { max_pages: 10, ..Default::default() };
        let engine = PageStorageEngine::new(storage, &file_path, config).await.unwrap();
        engine.page_manager.initialize_collection("users").await.unwrap();
        let count = engine.page_manager.count_records("users").await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_page_manager_insert_get() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();
        let config = BufferPoolConfig { max_pages: 10, ..Default::default() };
        let engine = PageStorageEngine::new(storage, &file_path, config).await.unwrap();
        engine.page_manager.insert("users", b"user1", b"Alice").await.unwrap();
        let record = engine.page_manager.get("users", b"user1").await.unwrap();
        assert!(record.is_some());
        assert_eq!(record.unwrap().value, b"Alice");
    }

    #[tokio::test]
    async fn test_page_manager_delete() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();
        let config = BufferPoolConfig { max_pages: 10, ..Default::default() };
        let engine = PageStorageEngine::new(storage, &file_path, config).await.unwrap();
        engine.page_manager.insert("users", b"user1", b"Alice").await.unwrap();
        let deleted = engine.page_manager.delete("users", b"user1").await.unwrap();
        assert!(deleted);
        let record = engine.page_manager.get("users", b"user1").await.unwrap();
        assert!(record.is_none());
    }

    #[tokio::test]
    async fn test_page_manager_multiple_records() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();
        let config = BufferPoolConfig { max_pages: 10, ..Default::default() };
        let engine = PageStorageEngine::new(storage, &file_path, config).await.unwrap();
        for i in 0..10 {
            let key = format!("user{}", i);
            let value = format!("User{}", i);
            engine.page_manager.insert("users", key.as_bytes(), value.as_bytes()).await.unwrap();
        }
        let count = engine.page_manager.count_records("users").await.unwrap();
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_page_manager_scan() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();
        let config = BufferPoolConfig { max_pages: 10, ..Default::default() };
        let engine = PageStorageEngine::new(storage, &file_path, config).await.unwrap();
        engine.page_manager.insert("users", b"user1", b"Alice").await.unwrap();
        engine.page_manager.insert("users", b"user2", b"Bob").await.unwrap();
        let records = engine.page_manager.scan_collection("users").await.unwrap();
        assert_eq!(records.len(), 2);
    }

    #[tokio::test]
    async fn test_page_manager_flush() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();
        let config = BufferPoolConfig { max_pages: 10, ..Default::default() };
        let engine = PageStorageEngine::new(storage, &file_path, config).await.unwrap();
        engine.page_manager.insert("users", b"user1", b"Alice").await.unwrap();
        engine.page_manager.flush().await.unwrap();
    }

    #[tokio::test]
    async fn test_page_manager_multiple_collections() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();
        let config = BufferPoolConfig { max_pages: 20, ..Default::default() };
        let engine = PageStorageEngine::new(storage, &file_path, config).await.unwrap();
        engine.page_manager.insert("users", b"user1", b"Alice").await.unwrap();
        engine.page_manager.insert("posts", b"post1", b"Hello").await.unwrap();
        let users_count = engine.page_manager.count_records("users").await.unwrap();
        let posts_count = engine.page_manager.count_records("posts").await.unwrap();
        assert_eq!(users_count, 1);
        assert_eq!(posts_count, 1);
    }

    #[tokio::test]
    async fn test_page_manager_stats() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();
        let config = BufferPoolConfig { max_pages: 10, ..Default::default() };
        let engine = PageStorageEngine::new(storage, &file_path, config).await.unwrap();
        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            engine.page_manager.insert("test", key.as_bytes(), value.as_bytes()).await.unwrap();
        }
        let stats = engine.stats().await;
        assert!(stats.cached_pages > 0);
        assert!(stats.dirty_pages > 0);
    }

    #[tokio::test]
    async fn test_page_manager_metadata_persists_across_restart() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();

        // 写入数据到两个集合
        {
            let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
            let config = BufferPoolConfig { max_pages: 10, ..Default::default() };
            let engine = PageStorageEngine::new(storage, &file_path, config).await.unwrap();
            engine.page_manager.insert("users", b"user1", b"Alice").await.unwrap();
            engine.page_manager.insert("posts", b"post1", b"Hello").await.unwrap();
            engine.flush().await.unwrap();
        }

        // 重新打开，验证集合映射已恢复
        {
            let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
            let config = BufferPoolConfig { max_pages: 10, ..Default::default() };
            let engine = PageStorageEngine::new(storage, &file_path, config).await.unwrap();

            let names = engine.page_manager.collection_names().await;
            assert!(names.contains(&"users".to_string()));
            assert!(names.contains(&"posts".to_string()));

            let record = engine.page_manager.get("users", b"user1").await.unwrap();
            assert!(record.is_some());
            assert_eq!(record.unwrap().value, b"Alice");

            let record = engine.page_manager.get("posts", b"post1").await.unwrap();
            assert!(record.is_some());
            assert_eq!(record.unwrap().value, b"Hello");
        }
    }

    #[tokio::test]
    async fn test_buffer_pool_next_page_number_survives_restart() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();

        // 分配多个页面
        {
            let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
            let config = BufferPoolConfig { max_pages: 10, ..Default::default() };
            let engine = PageStorageEngine::new(storage, &file_path, config).await.unwrap();
            engine.page_manager.insert("test", b"key1", b"val1").await.unwrap();
            engine.page_manager.insert("test", b"key2", b"val2").await.unwrap();
            engine.flush().await.unwrap();
        }

        // 重新打开，验证 next_page_number 已恢复（不会覆盖已有数据）
        {
            let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
            let config = BufferPoolConfig { max_pages: 10, ..Default::default() };
            let engine = PageStorageEngine::new(storage, &file_path, config).await.unwrap();
            // 插入新记录，不应覆盖旧数据
            engine.page_manager.insert("test", b"key3", b"val3").await.unwrap();
            let count = engine.page_manager.count_records("test").await.unwrap();
            assert_eq!(count, 3);
            let r1 = engine.page_manager.get("test", b"key1").await.unwrap();
            assert_eq!(r1.unwrap().value, b"val1");
            let r3 = engine.page_manager.get("test", b"key3").await.unwrap();
            assert_eq!(r3.unwrap().value, b"val3");
        }
    }
}
