use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use crate::storage::{Storage, OpenOptions, error::StorageError};

#[derive(Default)]
pub struct DiskIndex {
    entries: HashMap<String, IndexEntry>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    pub offset: u64,
    pub size: u64,
    pub last_accessed: u64,
}

impl DiskIndex {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&IndexEntry> {
        self.entries.get(key)
    }

    pub fn insert(&mut self, key: String, entry: IndexEntry) {
        self.entries.insert(key, entry);
    }

    pub fn remove(&mut self, key: &str) -> Option<IndexEntry> {
        self.entries.remove(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.entries.keys()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub async fn save<S: Storage>(&self, storage: &S, path: &str) -> Result<(), StorageError> {
        let json = serde_json::to_vec(self)
            .map_err(|e| StorageError::WasmError(e.to_string()))?;

        let options = OpenOptions {
            read: false,
            write: true,
            create: true,
            truncate: true,
        };

        let handle = storage.open(path, options).await?;
        storage.write(handle, 0, &json).await?;
        storage.sync(handle).await?;
        storage.close(handle).await?;

        Ok(())
    }

    pub async fn load<S: Storage>(storage: &S, path: &str) -> Result<Self, StorageError> {
        let options = OpenOptions {
            read: true,
            write: false,
            create: false,
            truncate: false,
        };

        let handle = match storage.open(path, options).await {
            Ok(h) => h,
            Err(_) => return Ok(Self::new()),
        };

        let size = storage.size(handle).await? as usize;
        let mut buf = vec![0u8; size];
        storage.read(handle, 0, &mut buf).await?;
        storage.close(handle).await?;

        let index: DiskIndex = serde_json::from_slice(&buf)
            .map_err(|e| StorageError::WasmError(e.to_string()))?;

        Ok(index)
    }
}

impl Serialize for DiskIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.entries.len()))?;
        for (k, v) in &self.entries {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for DiskIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let entries = HashMap::<String, IndexEntry>::deserialize(deserializer)?;
        Ok(Self { entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disk_index_new() {
        let index = DiskIndex::new();
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_disk_index_insert_get() {
        let mut index = DiskIndex::new();
        index.insert("key1".to_string(), IndexEntry {
            offset: 0,
            size: 100,
            last_accessed: 1000,
        });

        let entry = index.get("key1").unwrap();
        assert_eq!(entry.offset, 0);
        assert_eq!(entry.size, 100);
    }

    #[test]
    fn test_disk_index_remove() {
        let mut index = DiskIndex::new();
        index.insert("key1".to_string(), IndexEntry {
            offset: 0,
            size: 100,
            last_accessed: 1000,
        });

        let removed = index.remove("key1").unwrap();
        assert_eq!(removed.offset, 0);
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_disk_index_keys() {
        let mut index = DiskIndex::new();
        index.insert("key1".to_string(), IndexEntry {
            offset: 0,
            size: 100,
            last_accessed: 1000,
        });
        index.insert("key2".to_string(), IndexEntry {
            offset: 100,
            size: 200,
            last_accessed: 2000,
        });

        let keys: Vec<&String> = index.keys().collect();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_disk_index_serialization() {
        let mut index = DiskIndex::new();
        index.insert("key1".to_string(), IndexEntry {
            offset: 0,
            size: 100,
            last_accessed: 1000,
        });

        let json = serde_json::to_string(&index).unwrap();
        let deserialized: DiskIndex = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.len(), 1);
        assert_eq!(deserialized.get("key1").unwrap().offset, 0);
    }
}
