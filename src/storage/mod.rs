pub mod error;
pub mod r#trait;
pub mod wasi;
pub mod index;
pub mod lru;
pub mod tiered;
pub mod page;
pub mod buffer_pool;
pub mod page_manager;
pub mod btree;
#[cfg(target_arch = "wasm32")]
pub mod opfs;
pub use error::StorageError;
pub use r#trait::{Storage, FileHandle, OpenOptions, Result};
pub use wasi::WasiStorage;
pub use index::{DiskIndex, IndexEntry};
pub use lru::LruCache;
pub use tiered::TieredMap;
pub use page::{Page, PageHeader, RecordHeader, FileHeader, PageType, PageNumber, PAGE_SIZE};
pub use buffer_pool::{BufferPool, BufferPoolConfig, BufferPoolStats, SharedBufferPool};
pub use page_manager::{PageManager, SharedPageManager, PageStorageEngine, Record, RecordLocation};
pub use btree::{BTreeIndex, SharedBTreeIndex, IndexEntry as BTreeIndexEntry, BTREE_ORDER};
#[cfg(target_arch = "wasm32")]
pub use opfs::OpfsStorage;