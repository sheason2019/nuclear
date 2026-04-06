pub mod wal;

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::Arc;
use crate::storage::error::StorageError;
use self::wal::{WriteAheadLog, WalEntry};
use std::sync::atomic::{AtomicU64, Ordering};

static TXN_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: u64,
    pub state: TransactionState,
    pub operations: Vec<Operation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    Insert {
        collection: String,
        key: String,
        data: Vec<u8>,
    },
    Update {
        collection: String,
        key: String,
        data: Vec<u8>,
    },
    Delete {
        collection: String,
        key: String,
    },
}

pub struct TransactionManager {
    wal: WriteAheadLog,
    active_transactions: HashMap<u64, Transaction>,
}

impl TransactionManager {
    pub async fn new(wal: WriteAheadLog) -> Result<Self, StorageError> {
        let mut tm = Self {
            wal,
            active_transactions: HashMap::new(),
        };
        
        tm.recover().await?;
        
        Ok(tm)
    }

    pub fn begin(&mut self) -> Transaction {
        let id = TXN_COUNTER.fetch_add(1, Ordering::SeqCst);
        let txn = Transaction {
            id,
            state: TransactionState::Active,
            operations: Vec::new(),
        };
        
        self.active_transactions.insert(id, txn.clone());
        txn
    }

    pub async fn commit(&mut self, txn: &mut Transaction) -> Result<(), StorageError> {
        if txn.state != TransactionState::Active {
            return Err(StorageError::WasmError("Transaction is not active".to_string()));
        }
        
        self.wal.append(&WalEntry::Begin {
            txn_id: txn.id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }).await?;
        
        for op in &txn.operations {
            match op {
                Operation::Insert { collection, key, data } => {
                    self.wal.append(&WalEntry::Insert {
                        txn_id: txn.id,
                        collection: collection.clone(),
                        key: key.clone(),
                        data: data.clone(),
                    }).await?;
                }
                Operation::Update { collection, key, data } => {
                    self.wal.append(&WalEntry::Update {
                        txn_id: txn.id,
                        collection: collection.clone(),
                        key: key.clone(),
                        data: data.clone(),
                    }).await?;
                }
                Operation::Delete { collection, key } => {
                    self.wal.append(&WalEntry::Delete {
                        txn_id: txn.id,
                        collection: collection.clone(),
                        key: key.clone(),
                    }).await?;
                }
            }
        }
        
        self.wal.append(&WalEntry::Commit { txn_id: txn.id }).await?;
        
        txn.state = TransactionState::Committed;
        self.active_transactions.remove(&txn.id);
        
        Ok(())
    }

    pub async fn rollback(&mut self, txn: &mut Transaction) -> Result<(), StorageError> {
        if txn.state != TransactionState::Active {
            return Err(StorageError::WasmError("Transaction is not active".to_string()));
        }
        
        self.wal.append(&WalEntry::Rollback { txn_id: txn.id }).await?;
        
        txn.state = TransactionState::RolledBack;
        txn.operations.clear();
        self.active_transactions.remove(&txn.id);
        
        Ok(())
    }

    pub fn get_transaction(&self, txn_id: u64) -> Option<&Transaction> {
        self.active_transactions.get(&txn_id)
    }

    pub fn get_transaction_mut(&mut self, txn_id: u64) -> Option<&mut Transaction> {
        self.active_transactions.get_mut(&txn_id)
    }

    pub fn remove_transaction(&mut self, txn_id: u64) -> Option<Transaction> {
        self.active_transactions.remove(&txn_id)
    }

    pub async fn commit_by_id(&mut self, txn_id: u64) -> Result<(), StorageError> {
        let txn = self.active_transactions.remove(&txn_id)
            .ok_or_else(|| StorageError::WasmError("Transaction not found".to_string()))?;
        
        let mut txn = txn;
        self.commit(&mut txn).await
    }

    pub async fn rollback_by_id(&mut self, txn_id: u64) -> Result<(), StorageError> {
        let txn = self.active_transactions.remove(&txn_id)
            .ok_or_else(|| StorageError::WasmError("Transaction not found".to_string()))?;
        
        let mut txn = txn;
        self.rollback(&mut txn).await
    }

    async fn recover(&mut self) -> Result<(), StorageError> {
        let entries = self.wal.read_all().await?;
        
        let mut committed_txns: HashMap<u64, Vec<WalEntry>> = HashMap::new();
        let mut uncommitted_txns: HashMap<u64, Vec<WalEntry>> = HashMap::new();
        
        for entry in entries {
            let txn_id = match &entry {
                WalEntry::Begin { txn_id, .. } => *txn_id,
                WalEntry::Insert { txn_id, .. } => *txn_id,
                WalEntry::Update { txn_id, .. } => *txn_id,
                WalEntry::Delete { txn_id, .. } => *txn_id,
                WalEntry::Commit { txn_id } => *txn_id,
                WalEntry::Rollback { txn_id } => *txn_id,
            };
            
            if matches!(&entry, WalEntry::Commit { .. }) {
                if let Some(ops) = uncommitted_txns.remove(&txn_id) {
                    committed_txns.insert(txn_id, ops);
                }
            } else if matches!(&entry, WalEntry::Rollback { .. }) {
                uncommitted_txns.remove(&txn_id);
            } else {
                uncommitted_txns.entry(txn_id).or_default().push(entry);
            }
        }
        
        if !uncommitted_txns.is_empty() {
            self.wal.truncate().await?;
            
            for (txn_id, entries) in &committed_txns {
                for entry in entries {
                    self.wal.append(entry).await?;
                }
                self.wal.append(&WalEntry::Commit { txn_id: *txn_id }).await?;
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Storage, WasiStorage};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_transaction_begin_commit() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let wal = WriteAheadLog::new(storage, dir.path().to_str().unwrap()).await.unwrap();
        let mut tm = TransactionManager::new(wal).await.unwrap();
        
        let mut txn = tm.begin();
        assert!(matches!(txn.state, TransactionState::Active));
        
        txn.operations.push(Operation::Insert {
            collection: "users".to_string(),
            key: "user1".to_string(),
            data: vec![1, 2, 3],
        });
        
        tm.commit(&mut txn).await.unwrap();
        assert!(matches!(txn.state, TransactionState::Committed));
    }

    #[tokio::test]
    async fn test_transaction_rollback() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let wal = WriteAheadLog::new(storage, dir.path().to_str().unwrap()).await.unwrap();
        let mut tm = TransactionManager::new(wal).await.unwrap();
        
        let mut txn = tm.begin();
        txn.operations.push(Operation::Insert {
            collection: "users".to_string(),
            key: "user1".to_string(),
            data: vec![1, 2, 3],
        });
        
        tm.rollback(&mut txn).await.unwrap();
        assert!(matches!(txn.state, TransactionState::RolledBack));
    }

    #[tokio::test]
    async fn test_transaction_recover_committed() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let wal = WriteAheadLog::new(storage.clone(), dir.path().to_str().unwrap()).await.unwrap();
        let mut tm = TransactionManager::new(wal).await.unwrap();
        
        let mut txn = tm.begin();
        txn.operations.push(Operation::Insert {
            collection: "users".to_string(),
            key: "user1".to_string(),
            data: vec![1, 2, 3],
        });
        
        tm.commit(&mut txn).await.unwrap();
        
        let new_wal = WriteAheadLog::new(storage, dir.path().to_str().unwrap()).await.unwrap();
        let _tm2 = TransactionManager::new(new_wal).await.unwrap();
    }
}
