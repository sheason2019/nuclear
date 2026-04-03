use serde::{Serialize, Deserialize};
use crate::storage::{Storage, OpenOptions, error::StorageError};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    Begin { txn_id: u64, timestamp: u64 },
    Insert {
        txn_id: u64,
        collection: String,
        key: String,
        data: Vec<u8>,
    },
    Update {
        txn_id: u64,
        collection: String,
        key: String,
        data: Vec<u8>,
    },
    Delete {
        txn_id: u64,
        collection: String,
        key: String,
    },
    Commit { txn_id: u64 },
    Rollback { txn_id: u64 },
}

#[derive(Clone)]
pub struct WriteAheadLog {
    path: PathBuf,
    storage: Arc<dyn Storage>,
    current_offset: Arc<RwLock<u64>>,
}

impl WriteAheadLog {
    pub async fn new(storage: Arc<dyn Storage>, base_path: &str) -> Result<Self, StorageError> {
        let path = PathBuf::from(format!("{}/wal.log", base_path));
        
        let offset = if let Ok(handle) = storage.open(path.to_str().unwrap(), OpenOptions {
            read: true,
            write: false,
            create: false,
            truncate: false,
        }).await {
            let size = storage.size(handle).await?;
            storage.close(handle).await?;
            size
        } else {
            let _ = storage.open(path.to_str().unwrap(), OpenOptions {
                read: false,
                write: true,
                create: true,
                truncate: false,
            }).await;
            0
        };
        
        Ok(Self {
            path,
            storage,
            current_offset: Arc::new(RwLock::new(offset)),
        })
    }

    pub async fn append(&self, entry: &WalEntry) -> Result<(), StorageError> {
        let data = bincode::serialize(entry)
            .map_err(|e| StorageError::WasmError(e.to_string()))?;
        
        let len_bytes = (data.len() as u64).to_le_bytes();
        
        let mut offset = self.current_offset.write().await;
        
        let options = OpenOptions {
            read: false,
            write: true,
            create: true,
            truncate: false,
        };
        
        let handle = self.storage.open(self.path.to_str().unwrap(), options).await?;
        self.storage.write(handle, *offset, &len_bytes).await?;
        self.storage.write(handle, *offset + 8, &data).await?;
        self.storage.sync(handle).await?;
        self.storage.close(handle).await?;
        
        *offset += 8 + data.len() as u64;
        
        Ok(())
    }

    pub async fn read_all(&self) -> Result<Vec<WalEntry>, StorageError> {
        let options = OpenOptions {
            read: true,
            write: false,
            create: false,
            truncate: false,
        };
        
        let handle = match self.storage.open(self.path.to_str().unwrap(), options).await {
            Ok(h) => h,
            Err(_) => return Ok(Vec::new()),
        };
        
        let total_size = self.storage.size(handle).await?;
        self.storage.close(handle).await?;
        
        let mut entries = Vec::new();
        let mut offset = 0u64;
        
        while offset < total_size {
            let handle = self.storage.open(self.path.to_str().unwrap(), OpenOptions {
                read: true,
                write: false,
                create: false,
                truncate: false,
            }).await?;
            
            let mut len_bytes = [0u8; 8];
            self.storage.read(handle, offset, &mut len_bytes).await?;
            let len = u64::from_le_bytes(len_bytes);
            
            let mut data = vec![0u8; len as usize];
            self.storage.read(handle, offset + 8, &mut data).await?;
            self.storage.close(handle).await?;
            
            let entry: WalEntry = bincode::deserialize(&data)
                .map_err(|e| StorageError::WasmError(e.to_string()))?;
            
            entries.push(entry);
            offset += 8 + len;
        }
        
        Ok(entries)
    }

    pub async fn truncate(&self) -> Result<(), StorageError> {
        let options = OpenOptions {
            read: false,
            write: true,
            create: true,
            truncate: true,
        };
        
        let handle = self.storage.open(self.path.to_str().unwrap(), options).await?;
        self.storage.close(handle).await?;
        
        let mut offset = self.current_offset.write().await;
        *offset = 0;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::WasiStorage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wal_append_and_read() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let wal = WriteAheadLog::new(storage, dir.path().to_str().unwrap()).await.unwrap();
        
        wal.append(&WalEntry::Begin { txn_id: 1, timestamp: 1000 }).await.unwrap();
        wal.append(&WalEntry::Commit { txn_id: 1 }).await.unwrap();
        
        let entries = wal.read_all().await.unwrap();
        assert_eq!(entries.len(), 2);
        
        match &entries[0] {
            WalEntry::Begin { txn_id, .. } => assert_eq!(*txn_id, 1),
            _ => panic!("Expected Begin"),
        }
    }

    #[tokio::test]
    async fn test_wal_truncate() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let wal = WriteAheadLog::new(storage, dir.path().to_str().unwrap()).await.unwrap();
        
        wal.append(&WalEntry::Begin { txn_id: 1, timestamp: 1000 }).await.unwrap();
        wal.truncate().await.unwrap();
        
        let entries = wal.read_all().await.unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[tokio::test]
    async fn test_wal_multiple_entries() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let wal = WriteAheadLog::new(storage, dir.path().to_str().unwrap()).await.unwrap();
        
        wal.append(&WalEntry::Begin { txn_id: 1, timestamp: 1000 }).await.unwrap();
        wal.append(&WalEntry::Insert {
            txn_id: 1,
            collection: "users".to_string(),
            key: "user1".to_string(),
            data: vec![1, 2, 3],
        }).await.unwrap();
        wal.append(&WalEntry::Commit { txn_id: 1 }).await.unwrap();
        
        let entries = wal.read_all().await.unwrap();
        assert_eq!(entries.len(), 3);
    }
}
