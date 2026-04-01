pub mod error;
pub mod r#trait;
pub mod wasi;
pub use error::StorageError;
pub use r#trait::{Storage, FileHandle, OpenOptions, Result};
pub use wasi::WasiStorage;