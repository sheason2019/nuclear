pub mod error;
pub mod r#trait;
pub use error::StorageError;
pub use r#trait::{Storage, FileHandle, OpenOptions, Result};