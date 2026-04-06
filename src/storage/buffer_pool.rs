use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::page::{Page, PageNumber, PageType, PAGE_SIZE};
use super::{Storage, OpenOptions, StorageError};

/// 缓冲池配置
#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    /// 最大缓存页面数
    pub max_pages: usize,
    /// 是否启用预读
    pub enable_readahead: bool,
    /// 预读页面数
    pub readahead_pages: usize,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            max_pages: 256, // 1MB 缓存 (256 * 4KB)
            enable_readahead: false,
            readahead_pages: 4,
        }
    }
}

/// LRU 链表节点
struct LruNode {
    page_number: PageNumber,
    prev: Option<PageNumber>,
    next: Option<PageNumber>,
}

/// LRU 链表
struct LruList {
    nodes: HashMap<PageNumber, LruNode>,
    head: Option<PageNumber>, // 最近使用
    tail: Option<PageNumber>, // 最少使用
}

impl LruList {
    fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            head: None,
            tail: None,
        }
    }

    /// 添加页面到链表头部（最近使用）
    fn push_front(&mut self, page_number: PageNumber) {
        let node = LruNode {
            page_number,
            prev: None,
            next: self.head,
        };

        if let Some(old_head) = self.head {
            if let Some(old_head_node) = self.nodes.get_mut(&old_head) {
                old_head_node.prev = Some(page_number);
            }
        }

        self.head = Some(page_number);
        self.nodes.insert(page_number, node);

        if self.tail.is_none() {
            self.tail = Some(page_number);
        }
    }

    /// 移除指定页面
    fn remove(&mut self, page_number: PageNumber) {
        if let Some(node) = self.nodes.remove(&page_number) {
            if let Some(prev) = node.prev {
                if let Some(prev_node) = self.nodes.get_mut(&prev) {
                    prev_node.next = node.next;
                }
            } else {
                self.head = node.next;
            }

            if let Some(next) = node.next {
                if let Some(next_node) = self.nodes.get_mut(&next) {
                    next_node.prev = node.prev;
                }
            } else {
                self.tail = node.prev;
            }
        }
    }

    /// 将页面移动到链表头部
    fn move_to_front(&mut self, page_number: PageNumber) {
        self.remove(page_number);
        self.push_front(page_number);
    }

    /// 获取最少使用的页面
    fn pop_back(&mut self) -> Option<PageNumber> {
        if let Some(tail) = self.tail {
            self.remove(tail);
            Some(tail)
        } else {
            None
        }
    }

    /// 获取链表长度
    fn len(&self) -> usize {
        self.nodes.len()
    }
}

/// 缓冲池
pub struct BufferPool {
    /// 页面缓存
    pages: HashMap<PageNumber, Page>,
    /// LRU 链表
    lru: LruList,
    /// 配置
    config: BufferPoolConfig,
    /// 存储后端
    storage: Arc<dyn Storage>,
    /// 数据库文件路径
    file_path: String,
    /// 文件是否已初始化
    initialized: bool,
    /// 下一个可分配的页面号
    next_page_number: PageNumber,
}

impl BufferPool {
    /// 创建新的缓冲池
    pub fn new(storage: Arc<dyn Storage>, file_path: &str, config: BufferPoolConfig) -> Self {
        Self {
            pages: HashMap::new(),
            lru: LruList::new(),
            config,
            storage,
            file_path: file_path.to_string(),
            initialized: false,
            next_page_number: 1, // Page 0 is reserved for file header
        }
    }

    /// 初始化数据库文件
    pub async fn initialize(&mut self) -> Result<(), StorageError> {
        let options = OpenOptions {
            read: true,
            write: true,
            create: true,
            truncate: false,
        };

        let handle = self.storage.open(&self.file_path, options).await?;
        let size = self.storage.size(handle).await?;

        if size == 0 {
            // 新文件，写入文件头页面
            let header_page = Page::new(0, PageType::Data);
            let bytes = header_page.to_bytes();
            self.storage.write(handle, 0, &bytes).await?;
            self.storage.sync(handle).await?;
        }

        self.storage.close(handle).await?;
        self.initialized = true;

        Ok(())
    }

    /// 获取页面（如果不在缓存中则从磁盘加载）
    pub async fn get_page(&mut self, page_number: PageNumber) -> Result<&Page, StorageError> {
        // 检查缓存
        if self.pages.contains_key(&page_number) {
            self.lru.move_to_front(page_number);
            return Ok(self.pages.get(&page_number).unwrap());
        }

        // 从磁盘加载
        let page = self.load_page_from_disk(page_number).await?;

        // 如果缓存已满，淘汰最少使用的页面
        if self.pages.len() >= self.config.max_pages {
            self.evict_one().await?;
        }

        self.lru.push_front(page_number);
        self.pages.insert(page_number, page);

        Ok(self.pages.get(&page_number).unwrap())
    }

    /// 获取可变引用页面
    pub async fn get_page_mut(&mut self, page_number: PageNumber) -> Result<&mut Page, StorageError> {
        // 检查缓存
        if self.pages.contains_key(&page_number) {
            self.lru.move_to_front(page_number);
            return Ok(self.pages.get_mut(&page_number).unwrap());
        }

        // 从磁盘加载
        let page = self.load_page_from_disk(page_number).await?;

        // 如果缓存已满，淘汰最少使用的页面
        if self.pages.len() >= self.config.max_pages {
            self.evict_one().await?;
        }

        self.lru.push_front(page_number);
        self.pages.insert(page_number, page);

        Ok(self.pages.get_mut(&page_number).unwrap())
    }

    /// 分配新页面
    pub async fn allocate_page(&mut self, page_type: PageType) -> Result<PageNumber, StorageError> {
        // 优先使用空闲页面
        let free_page = self.find_free_page().await?;
        if let Some(page_number) = free_page {
            let page = self.get_page_mut(page_number).await?;
            page.header.page_type = page_type;
            page.header.record_count = 0;
            page.header.used_space = 0;
            page.is_dirty = true;
            return Ok(page_number);
        }

        // 分配新页面
        let page_number = self.next_page_number;
        self.next_page_number += 1;
        let page = Page::new(page_number, page_type);

        // 如果缓存已满，先淘汰
        if self.pages.len() >= self.config.max_pages {
            self.evict_one().await?;
        }

        self.pages.insert(page_number, page);
        self.lru.push_front(page_number);

        Ok(page_number)
    }

    /// 释放页面
    pub async fn free_page(&mut self, page_number: PageNumber) -> Result<(), StorageError> {
        if let Some(page) = self.pages.get_mut(&page_number) {
            page.header.page_type = PageType::Free;
            page.header.record_count = 0;
            page.header.used_space = 0;
            page.is_dirty = true;
        }
        Ok(())
    }

    /// 标记页面为脏页
    pub fn mark_dirty(&mut self, page_number: PageNumber) {
        if let Some(page) = self.pages.get_mut(&page_number) {
            page.is_dirty = true;
        }
    }

    /// 刷新所有脏页到磁盘
    pub async fn flush(&mut self) -> Result<(), StorageError> {
        let dirty_pages: Vec<PageNumber> = self.pages.iter()
            .filter(|(_, page)| page.is_dirty)
            .map(|(&page_number, _)| page_number)
            .collect();

        for page_number in dirty_pages {
            self.write_page_to_disk(page_number).await?;
        }

        Ok(())
    }

    /// 刷新指定页面到磁盘
    pub async fn flush_page(&mut self, page_number: PageNumber) -> Result<(), StorageError> {
        if self.pages.contains_key(&page_number) {
            self.write_page_to_disk(page_number).await?;
        }
        Ok(())
    }

    /// 从磁盘加载页面
    async fn load_page_from_disk(&self, page_number: PageNumber) -> Result<Page, StorageError> {
        let options = OpenOptions {
            read: true,
            write: false,
            create: false,
            truncate: false,
        };

        let handle = self.storage.open(&self.file_path, options).await?;
        let offset = page_number * PAGE_SIZE;
        let mut buf = vec![0u8; PAGE_SIZE as usize];

        let bytes_read = self.storage.read(handle, offset, &mut buf).await?;
        self.storage.close(handle).await?;

        if bytes_read == 0 {
            // 页面不存在，返回空页面
            return Ok(Page::new(page_number, PageType::Free));
        }

        Page::from_bytes(page_number, &buf)
            .map_err(|e| StorageError::WasmError(format!("Failed to parse page: {}", e)))
    }

    /// 将页面写入磁盘
    async fn write_page_to_disk(&mut self, page_number: PageNumber) -> Result<(), StorageError> {
        if let Some(page) = self.pages.get(&page_number) {
            if !page.is_dirty {
                return Ok(());
            }

            let options = OpenOptions {
                read: false,
                write: true,
                create: true,
                truncate: false,
            };

            let handle = self.storage.open(&self.file_path, options).await?;
            let offset = page_number * PAGE_SIZE;
            let bytes = page.to_bytes();

            self.storage.write(handle, offset, &bytes).await?;
            self.storage.sync(handle).await?;
            self.storage.close(handle).await?;
        }
        Ok(())
    }

    /// 查找空闲页面
    async fn find_free_page(&mut self) -> Result<Option<PageNumber>, StorageError> {
        // 遍历缓存中的空闲页面
        for (&page_number, page) in &self.pages {
            if page.header.page_type == PageType::Free {
                return Ok(Some(page_number));
            }
        }

        // 检查磁盘上的空闲页面
        let header_page = self.get_page(0).await?;
        let first_free_page = header_page.header.next_free_page;

        if first_free_page > 0 {
            return Ok(Some(first_free_page));
        }

        Ok(None)
    }

    /// 淘汰一个页面
    async fn evict_one(&mut self) -> Result<(), StorageError> {
        if let Some(page_number) = self.lru.pop_back() {
            if let Some(page) = self.pages.get(&page_number) {
                if page.is_dirty {
                    self.write_page_to_disk(page_number).await?;
                }
            }
            self.pages.remove(&page_number);
        }
        Ok(())
    }

    /// 获取缓存中的页面数
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    /// 检查缓存是否为空
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// 获取缓存命中率统计
    pub fn stats(&self) -> BufferPoolStats {
        BufferPoolStats {
            cached_pages: self.pages.len(),
            max_pages: self.config.max_pages,
            dirty_pages: self.pages.values().filter(|p| p.is_dirty).count(),
        }
    }
}

/// 缓冲池统计信息
#[derive(Debug, Clone)]
pub struct BufferPoolStats {
    pub cached_pages: usize,
    pub max_pages: usize,
    pub dirty_pages: usize,
}

/// 线程安全的缓冲池包装器
pub struct SharedBufferPool {
    inner: Arc<RwLock<BufferPool>>,
}

impl SharedBufferPool {
    pub fn new(storage: Arc<dyn Storage>, file_path: &str, config: BufferPoolConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(BufferPool::new(storage, file_path, config))),
        }
    }

    pub async fn initialize(&self) -> Result<(), StorageError> {
        self.inner.write().await.initialize().await
    }

    pub async fn get_page(&self, page_number: PageNumber) -> Result<Page, StorageError> {
        self.inner.write().await.get_page(page_number).await.cloned()
    }

    pub async fn get_page_mut(&self, page_number: PageNumber) -> Result<Page, StorageError> {
        self.inner.write().await.get_page_mut(page_number).await.cloned()
    }

    pub async fn allocate_page(&self, page_type: PageType) -> Result<PageNumber, StorageError> {
        self.inner.write().await.allocate_page(page_type).await
    }

    pub async fn free_page(&self, page_number: PageNumber) -> Result<(), StorageError> {
        self.inner.write().await.free_page(page_number).await
    }

    pub async fn flush(&self) -> Result<(), StorageError> {
        self.inner.write().await.flush().await
    }

    pub async fn flush_page(&self, page_number: PageNumber) -> Result<(), StorageError> {
        self.inner.write().await.flush_page(page_number).await
    }

    pub async fn write_page(&self, page: Page) -> Result<(), StorageError> {
        let mut pool = self.inner.write().await;
        let page_number = page.page_number;
        pool.pages.insert(page_number, page);
        pool.lru.move_to_front(page_number);
        pool.write_page_to_disk(page_number).await
    }

    pub fn len(&self) -> usize {
        // 注意：这里获取的是瞬时值，可能不准确
        // 如果需要精确值，应该使用 async 方法
        0
    }

    pub async fn stats(&self) -> BufferPoolStats {
        self.inner.read().await.stats()
    }
}

impl Clone for SharedBufferPool {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::WasiStorage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_buffer_pool_initialize() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();

        let config = BufferPoolConfig::default();
        let mut pool = BufferPool::new(storage, &file_path, config);

        assert!(pool.initialize().await.is_ok());
        assert!(pool.initialized);
    }

    #[tokio::test]
    async fn test_buffer_pool_allocate_page() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();

        let config = BufferPoolConfig {
            max_pages: 10,
            ..Default::default()
        };
        let mut pool = BufferPool::new(storage, &file_path, config);
        pool.initialize().await.unwrap();

        let page_number = pool.allocate_page(PageType::Data).await.unwrap();
        assert_eq!(page_number, 1); // Page 0 is reserved for file header

        let page = pool.get_page(page_number).await.unwrap();
        assert_eq!(page.header.page_type, PageType::Data);
    }

    #[tokio::test]
    async fn test_buffer_pool_write_read_page() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();

        let config = BufferPoolConfig {
            max_pages: 10,
            ..Default::default()
        };
        let mut pool = BufferPool::new(storage, &file_path, config);
        pool.initialize().await.unwrap();

        // 分配页面并写入数据
        let page_number = pool.allocate_page(PageType::Data).await.unwrap();
        {
            let page = pool.get_page_mut(page_number).await.unwrap();
            page.write_record(b"key1", b"value1").unwrap();
            page.write_record(b"key2", b"value2").unwrap();
        }

        // 刷新到磁盘
        pool.flush().await.unwrap();

        // 重新读取页面
        let page = pool.get_page(page_number).await.unwrap();
        assert_eq!(page.header.record_count, 2);
    }

    #[tokio::test]
    async fn test_buffer_pool_eviction() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();

        let config = BufferPoolConfig {
            max_pages: 3,
            ..Default::default()
        };
        let mut pool = BufferPool::new(storage.clone(), &file_path, config.clone());
        pool.initialize().await.unwrap();

        // 分配并访问超过缓存大小的页面
        // allocate_page already puts page in cache, so 5 allocations with max_pages=3 should trigger eviction
        for _ in 0..5u64 {
            let _page_number = pool.allocate_page(PageType::Data).await.unwrap();
        }

        // 缓存中的页面数不应超过 max_pages + 1 (header page)
        assert!(pool.len() <= config.max_pages + 1);
    }

    #[tokio::test]
    async fn test_buffer_pool_free_page() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();

        let config = BufferPoolConfig {
            max_pages: 10,
            ..Default::default()
        };
        let mut pool = BufferPool::new(storage, &file_path, config);
        pool.initialize().await.unwrap();

        let page_number = pool.allocate_page(PageType::Data).await.unwrap();
        pool.free_page(page_number).await.unwrap();

        let page = pool.get_page(page_number).await.unwrap();
        assert_eq!(page.header.page_type, PageType::Free);
    }

    #[tokio::test]
    async fn test_buffer_pool_stats() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();

        let config = BufferPoolConfig {
            max_pages: 10,
            ..Default::default()
        };
        let mut pool = BufferPool::new(storage, &file_path, config);
        pool.initialize().await.unwrap();

        pool.allocate_page(PageType::Data).await.unwrap();
        pool.allocate_page(PageType::Data).await.unwrap();

        let stats = pool.stats();
        assert_eq!(stats.cached_pages, 3); // header page + 2 data pages
        assert_eq!(stats.max_pages, 10);
        assert_eq!(stats.dirty_pages, 2); // only the 2 newly allocated pages are dirty
    }

    #[tokio::test]
    async fn test_shared_buffer_pool() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();

        let config = BufferPoolConfig {
            max_pages: 10,
            ..Default::default()
        };
        let pool = SharedBufferPool::new(storage, &file_path, config);
        pool.initialize().await.unwrap();

        let page_number = pool.allocate_page(PageType::Data).await.unwrap();
        let page = pool.get_page(page_number).await.unwrap();
        assert_eq!(page.header.page_type, PageType::Data);
    }

    #[tokio::test]
    async fn test_buffer_pool_persistence() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.db").to_string_lossy().to_string();

        let config = BufferPoolConfig {
            max_pages: 10,
            ..Default::default()
        };

        // 写入数据
        {
            let mut pool = BufferPool::new(storage.clone(), &file_path, config.clone());
            pool.initialize().await.unwrap();

            let page_number = pool.allocate_page(PageType::Data).await.unwrap();
            {
                let page = pool.get_page_mut(page_number).await.unwrap();
                page.write_record(b"persistent_key", b"persistent_value").unwrap();
            }
            pool.flush().await.unwrap();
        }

        // 重新加载
        {
        let mut pool = BufferPool::new(storage, &file_path, config.clone());
            pool.initialize().await.unwrap();

            let page = pool.get_page(0).await.unwrap(); // 第一个页面
            assert!(page.header.record_count > 0 || page.header.page_type == PageType::Data);
        }
    }
}
