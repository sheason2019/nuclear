pub mod error;
pub mod r#trait;
pub mod wasi;
#[cfg(target_arch = "wasm32")]
pub mod opfs;
pub use error::StorageError;
pub use r#trait::{Storage, FileHandle, OpenOptions, Result};
pub use wasi::WasiStorage;
#[cfg(target_arch = "wasm32")]
pub use opfs::OpfsStorage;