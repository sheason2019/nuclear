pub mod error;
pub mod r#trait;
pub mod wasi;
pub mod index;
pub mod lru;
pub mod tiered;
#[cfg(target_arch = "wasm32")]
pub mod opfs;
pub use error::StorageError;
pub use r#trait::{Storage, FileHandle, OpenOptions, Result};
pub use wasi::WasiStorage;
pub use index::{DiskIndex, IndexEntry};
pub use lru::LruCache;
pub use tiered::TieredMap;
#[cfg(target_arch = "wasm32")]
pub use opfs::OpfsStorage;