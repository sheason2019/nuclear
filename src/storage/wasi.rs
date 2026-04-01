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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wasi_storage_new() {
        let dir = tempdir().unwrap();
        let _storage = WasiStorage::new(dir.path());
    }

    #[tokio::test]
    async fn test_wasi_storage_open_create() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let handle = storage.open("test.bin", OpenOptions::default()).await.unwrap();
        storage.close(handle).await.unwrap();
    }

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
    async fn test_wasi_storage_write_read_large() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let handle = storage.open("test.bin", OpenOptions::default()).await.unwrap();
        let data = vec![0x42u8; 1024 * 1024];
        storage.write(handle, 0, &data).await.unwrap();
        
        let mut buf = vec![0u8; 1024 * 1024];
        let bytes = storage.read(handle, 0, &mut buf).await.unwrap();
        assert_eq!(bytes, 1024 * 1024);
        assert_eq!(buf, data);
        
        storage.close(handle).await.unwrap();
    }

    #[tokio::test]
    async fn test_wasi_storage_write_at_offset() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let handle = storage.open("test.bin", OpenOptions::default()).await.unwrap();
        storage.write(handle, 0, b"hello").await.unwrap();
        storage.write(handle, 5, b"world").await.unwrap();
        
        let mut buf = [0u8; 10];
        let bytes = storage.read(handle, 0, &mut buf).await.unwrap();
        assert_eq!(bytes, 10);
        assert_eq!(&buf, b"helloworld");
        
        storage.close(handle).await.unwrap();
    }

    #[tokio::test]
    async fn test_wasi_storage_read_at_offset() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let handle = storage.open("test.bin", OpenOptions::default()).await.unwrap();
        storage.write(handle, 0, b"helloworld").await.unwrap();
        
        let mut buf = [0u8; 5];
        let bytes = storage.read(handle, 5, &mut buf).await.unwrap();
        assert_eq!(bytes, 5);
        assert_eq!(&buf, b"world");
        
        storage.close(handle).await.unwrap();
    }

    #[tokio::test]
    async fn test_wasi_storage_size() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let handle = storage.open("test.bin", OpenOptions::default()).await.unwrap();
        storage.write(handle, 0, b"hello world").await.unwrap();
        storage.sync(handle).await.unwrap();
        
        let size = storage.size(handle).await.unwrap();
        assert_eq!(size, 11);
        
        storage.close(handle).await.unwrap();
    }

    #[tokio::test]
    async fn test_wasi_storage_size_empty() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let handle = storage.open("test.bin", OpenOptions::default()).await.unwrap();
        let size = storage.size(handle).await.unwrap();
        assert_eq!(size, 0);
        
        storage.close(handle).await.unwrap();
    }

    #[tokio::test]
    async fn test_wasi_storage_sync() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let handle = storage.open("test.bin", OpenOptions::default()).await.unwrap();
        storage.write(handle, 0, b"hello").await.unwrap();
        storage.sync(handle).await.unwrap();
        
        let mut buf = [0u8; 5];
        storage.read(handle, 0, &mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
        
        storage.close(handle).await.unwrap();
    }

    #[tokio::test]
    async fn test_wasi_storage_close() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let handle = storage.open("test.bin", OpenOptions::default()).await.unwrap();
        storage.close(handle).await.unwrap();
    }

    #[tokio::test]
    async fn test_wasi_storage_invalid_handle() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let invalid_handle = FileHandle(999);
        let result = storage.read(invalid_handle, 0, &mut [0u8; 5]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_wasi_storage_multiple_files() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let handle1 = storage.open("file1.bin", OpenOptions::default()).await.unwrap();
        let handle2 = storage.open("file2.bin", OpenOptions::default()).await.unwrap();
        
        storage.write(handle1, 0, b"file1").await.unwrap();
        storage.write(handle2, 0, b"file2").await.unwrap();
        
        let mut buf1 = [0u8; 5];
        let mut buf2 = [0u8; 5];
        storage.read(handle1, 0, &mut buf1).await.unwrap();
        storage.read(handle2, 0, &mut buf2).await.unwrap();
        
        assert_eq!(&buf1, b"file1");
        assert_eq!(&buf2, b"file2");
        
        storage.close(handle1).await.unwrap();
        storage.close(handle2).await.unwrap();
    }

    #[tokio::test]
    async fn test_wasi_storage_overwrite() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let handle = storage.open("test.bin", OpenOptions::default()).await.unwrap();
        storage.write(handle, 0, b"hello").await.unwrap();
        storage.write(handle, 0, b"world").await.unwrap();
        
        let mut buf = [0u8; 5];
        storage.read(handle, 0, &mut buf).await.unwrap();
        assert_eq!(&buf, b"world");
        
        storage.close(handle).await.unwrap();
    }

    #[tokio::test]
    async fn test_wasi_storage_truncate() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let handle = storage.open("test.bin", OpenOptions::default()).await.unwrap();
        storage.write(handle, 0, b"hello world").await.unwrap();
        storage.close(handle).await.unwrap();
        
        let handle = storage.open("test.bin", OpenOptions {
            truncate: true,
            ..Default::default()
        }).await.unwrap();
        
        let size = storage.size(handle).await.unwrap();
        assert_eq!(size, 0);
        
        storage.close(handle).await.unwrap();
    }

    #[tokio::test]
    async fn test_wasi_storage_read_partial() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let handle = storage.open("test.bin", OpenOptions::default()).await.unwrap();
        storage.write(handle, 0, b"hello world").await.unwrap();
        
        let mut buf = [0u8; 3];
        let bytes = storage.read(handle, 0, &mut buf).await.unwrap();
        assert_eq!(bytes, 3);
        assert_eq!(&buf, b"hel");
        
        storage.close(handle).await.unwrap();
    }

    #[tokio::test]
    async fn test_wasi_storage_read_beyond_eof() {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        
        let handle = storage.open("test.bin", OpenOptions::default()).await.unwrap();
        storage.write(handle, 0, b"hello").await.unwrap();
        
        let mut buf = [0u8; 10];
        let bytes = storage.read(handle, 0, &mut buf).await.unwrap();
        assert_eq!(bytes, 5);
        assert_eq!(&buf[..5], b"hello");
        
        storage.close(handle).await.unwrap();
    }
}