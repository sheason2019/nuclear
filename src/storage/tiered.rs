use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use crate::storage::{Storage, OpenOptions, error::StorageError};
use super::index::{DiskIndex, IndexEntry};
use super::lru::LruCache;

pub struct TieredMap<K, V, S: Storage> {
    cache: Arc<RwLock<LruCache<K, V>>>,
    index: Arc<RwLock<DiskIndex>>,
    storage: Arc<S>,
    data_path: String,
    index_path: String,
    max_cache_size: usize,
}

impl<K, V, S> TieredMap<K, V, S>
where
    K: Clone + std::hash::Hash + Eq + Serialize + for<'de> Deserialize<'de> + Send + Sync + std::fmt::Display,
    V: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync,
    S: Storage + 'static,
{
    pub async fn new(
        storage: Arc<S>,
        collection: &str,
        base_path: &str,
        max_cache_size: usize,
    ) -> Result<Self, StorageError> {
        let data_path = format!("{}/{}.data.bin", base_path, collection);
        let index_path = format!("{}/{}.index.json", base_path, collection);

        let index = DiskIndex::load(&*storage, &index_path).await.unwrap_or_default();

        let cache = LruCache::new(max_cache_size);

        Ok(Self {
            cache: Arc::new(RwLock::new(cache)),
            index: Arc::new(RwLock::new(index)),
            storage,
            data_path,
            index_path,
            max_cache_size,
        })
    }

    pub async fn get(&self, key: &K) -> Result<Option<V>, StorageError> {
        let mut cache = self.cache.write().await;
        
        if let Some(value) = cache.get(key) {
            return Ok(Some(value.clone()));
        }
        
        drop(cache);
        
        let index = self.index.read().await;
        let key_str = key.to_string();
        let entry = index.get(&key_str).cloned();
        drop(index);
        
        if let Some(entry) = entry {
            let value = self.load_from_disk(&entry).await?;
            
            let mut cache = self.cache.write().await;
            if cache.len() >= self.max_cache_size {
                if let Some((evicted_key, evicted_value)) = cache.evict_one() {
                    drop(cache);
                    self.save_to_disk(&evicted_key, &evicted_value).await?;
                } else {
                    drop(cache);
                }
            } else {
                drop(cache);
            }
            
            let mut cache = self.cache.write().await;
            cache.insert(key.clone(), value.clone());
            drop(cache);
            
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    pub async fn insert(&self, key: K, value: V) -> Result<(), StorageError> {
        let mut cache = self.cache.write().await;
        
        if cache.len() >= self.max_cache_size && !cache.contains_key(&key) {
            if let Some((evicted_key, evicted_value)) = cache.evict_one() {
                drop(cache);
                self.save_to_disk(&evicted_key, &evicted_value).await?;
            } else {
                drop(cache);
            }
        } else {
            drop(cache);
        }
        
        let mut cache = self.cache.write().await;
        cache.insert(key, value);
        
        Ok(())
    }

    pub async fn remove(&self, key: &K) -> Result<Option<V>, StorageError> {
        let mut cache = self.cache.write().await;
        let value = cache.remove(key);
        drop(cache);
        
        let mut index = self.index.write().await;
        index.remove(&key.to_string());
        drop(index);
        
        self.save_index().await?;
        
        Ok(value)
    }

    pub async fn contains_key(&self, key: &K) -> Result<bool, StorageError> {
        let cache = self.cache.read().await;
        if cache.contains_key(key) {
            return Ok(true);
        }
        drop(cache);
        
        let index = self.index.read().await;
        Ok(index.get(&key.to_string()).is_some())
    }

    pub async fn keys(&self) -> Result<Vec<K>, StorageError> {
        let index = self.index.read().await;
        let keys: Vec<K> = index.keys()
            .filter_map(|k| serde_json::from_value(serde_json::Value::String(k.clone())).ok())
            .collect();
        Ok(keys)
    }

    pub async fn len(&self) -> Result<usize, StorageError> {
        let cache = self.cache.read().await;
        let cache_count = cache.len();
        drop(cache);
        
        let index = self.index.read().await;
        let index_count = index.len();
        drop(index);
        
        Ok(cache_count.max(index_count))
    }

    pub async fn is_empty(&self) -> Result<bool, StorageError> {
        Ok(self.len().await? == 0)
    }

    pub async fn flush(&self) -> Result<(), StorageError> {
        let cache = self.cache.read().await;
        let keys: Vec<K> = cache.keys().cloned().collect();
        drop(cache);
        
        for key in keys {
            let mut cache = self.cache.write().await;
            let value = cache.get(&key).cloned();
            drop(cache);
            
            if let Some(value) = value {
                self.save_to_disk(&key, &value).await?;
            }
        }
        
        self.save_index().await?;
        
        Ok(())
    }

    async fn save_to_disk(&self, key: &K, value: &V) -> Result<(), StorageError> {
        let data = bincode::serialize(value)
            .map_err(|e| StorageError::WasmError(e.to_string()))?;
        
        let options = OpenOptions {
            read: false,
            write: true,
            create: true,
            truncate: false,
        };
        
        let handle = self.storage.open(&self.data_path, options).await?;
        let size = self.storage.size(handle).await?;
        
        self.storage.write(handle, size, &data).await?;
        self.storage.sync(handle).await?;
        self.storage.close(handle).await?;
        
        let mut index = self.index.write().await;
        index.insert(key.to_string(), IndexEntry {
            offset: size,
            size: data.len() as u64,
            last_accessed: current_timestamp(),
        });
        drop(index);
        
        self.save_index().await?;
        
        Ok(())
    }

    async fn load_from_disk(&self, entry: &IndexEntry) -> Result<V, StorageError> {
        let options = OpenOptions {
            read: true,
            write: false,
            create: false,
            truncate: false,
        };
        
        let handle = self.storage.open(&self.data_path, options).await?;
        let mut buf = vec![0u8; entry.size as usize];
        self.storage.read(handle, entry.offset, &mut buf).await?;
        self.storage.close(handle).await?;
        
        let value = bincode::deserialize(&buf)
            .map_err(|e| StorageError::WasmError(e.to_string()))?;
        
        Ok(value)
    }

    async fn save_index(&self) -> Result<(), StorageError> {
        let index = self.index.read().await;
        index.save(&*self.storage, &self.index_path).await?;
        Ok(())
    }
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::WasiStorage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_tiered_map_new() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(WasiStorage::new(dir.path()));
        let map: TieredMap<String, String, WasiStorage> = TieredMap::new(
            storage,
            "test",
            dir.path().to_str().unwrap(),
            10,
        ).await.unwrap();

        assert!(map.is_empty().await.unwrap());
    }

    #[tokio::test]
    async fn test_tiered_map_insert_get() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(WasiStorage::new(dir.path()));
        let map: TieredMap<String, String, WasiStorage> = TieredMap::new(
            storage,
            "test",
            dir.path().to_str().unwrap(),
            10,
        ).await.unwrap();

        map.insert("key1".to_string(), "value1".to_string()).await.unwrap();
        let value = map.get(&"key1".to_string()).await.unwrap();
        assert_eq!(value, Some("value1".to_string()));
    }

    #[tokio::test]
    async fn test_tiered_map_eviction() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(WasiStorage::new(dir.path()));
        let map: TieredMap<String, String, WasiStorage> = TieredMap::new(
            storage,
            "test",
            dir.path().to_str().unwrap(),
            2,
        ).await.unwrap();

        map.insert("key1".to_string(), "value1".to_string()).await.unwrap();
        map.insert("key2".to_string(), "value2".to_string()).await.unwrap();
        
        assert_eq!(map.len().await.unwrap(), 2);
        
        map.insert("key3".to_string(), "value3".to_string()).await.unwrap();
        
        assert!(map.len().await.unwrap() >= 2);
        
        map.flush().await.unwrap();
        assert!(map.len().await.unwrap() >= 2);
    }

    #[tokio::test]
    async fn test_tiered_map_remove() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(WasiStorage::new(dir.path()));
        let map: TieredMap<String, String, WasiStorage> = TieredMap::new(
            storage,
            "test",
            dir.path().to_str().unwrap(),
            10,
        ).await.unwrap();

        map.insert("key1".to_string(), "value1".to_string()).await.unwrap();
        let removed = map.remove(&"key1".to_string()).await.unwrap();
        assert_eq!(removed, Some("value1".to_string()));
        assert!(map.is_empty().await.unwrap());
    }

    #[tokio::test]
    async fn test_tiered_map_contains_key() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(WasiStorage::new(dir.path()));
        let map: TieredMap<String, String, WasiStorage> = TieredMap::new(
            storage,
            "test",
            dir.path().to_str().unwrap(),
            10,
        ).await.unwrap();

        map.insert("key1".to_string(), "value1".to_string()).await.unwrap();
        assert!(map.contains_key(&"key1".to_string()).await.unwrap());
        assert!(!map.contains_key(&"key2".to_string()).await.unwrap());
    }

    #[tokio::test]
    async fn test_tiered_map_flush() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(WasiStorage::new(dir.path()));
        let map: TieredMap<String, String, WasiStorage> = TieredMap::new(
            storage,
            "test",
            dir.path().to_str().unwrap(),
            100,
        ).await.unwrap();

        map.insert("key1".to_string(), "value1".to_string()).await.unwrap();
        map.insert("key2".to_string(), "value2".to_string()).await.unwrap();
        
        let value = map.get(&"key1".to_string()).await.unwrap();
        assert_eq!(value, Some("value1".to_string()));
    }
}
