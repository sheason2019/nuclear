use async_trait::async_trait;
use super::error::StorageError;

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug, Clone, Copy)]
pub struct FileHandle(pub u64);

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