use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

use super::{Storage, OpenOptions, StorageError};

/// B-tree order (max keys per node)
pub const BTREE_ORDER: usize = 32;

/// Index entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    pub offset: u64,
    pub size: u64,
    pub last_accessed: u64,
}

impl IndexEntry {
    pub fn new(offset: u64, size: u64) -> Self {
        Self {
            offset,
            size,
            last_accessed: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

/// B-tree node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeNode {
    pub keys: Vec<String>,
    pub values: Vec<IndexEntry>,
    pub children: Vec<u64>,
    pub is_leaf: bool,
    pub next_leaf: u64,
}

impl BTreeNode {
    pub fn new_leaf() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            is_leaf: true,
            next_leaf: 0,
        }
    }

    pub fn new_internal() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            is_leaf: false,
            next_leaf: 0,
        }
    }

    pub fn is_full(&self) -> bool {
        self.keys.len() >= BTREE_ORDER
    }

    pub fn find_key_position(&self, key: &str) -> usize {
        self.keys.partition_point(|k| k.as_str() < key)
    }
}

/// B-tree index
pub struct BTreeIndex {
    root: u64,
    nodes: HashMap<u64, BTreeNode>,
    next_page: u64,
    storage: Arc<dyn Storage>,
    file_path: String,
}

impl BTreeIndex {
    pub fn new(storage: Arc<dyn Storage>, file_path: &str) -> Self {
        let mut nodes = HashMap::new();
        let root_page = 1u64;
        nodes.insert(root_page, BTreeNode::new_leaf());

        Self {
            root: root_page,
            nodes,
            next_page: 2,
            storage,
            file_path: file_path.to_string(),
        }
    }

    pub async fn load(storage: Arc<dyn Storage>, file_path: &str) -> Result<Self, StorageError> {
        let options = OpenOptions {
            read: true,
            write: false,
            create: false,
            truncate: false,
        };

        let handle = match storage.open(file_path, options).await {
            Ok(h) => h,
            Err(_) => return Ok(Self::new(storage, file_path)),
        };

        let size = storage.size(handle).await? as usize;
        if size == 0 {
            storage.close(handle).await?;
            return Ok(Self::new(storage, file_path));
        }

        let mut buf = vec![0u8; size];
        storage.read(handle, 0, &mut buf).await?;
        storage.close(handle).await?;

        let index_data: BTreeIndexData = bincode::deserialize(&buf)
            .map_err(|e| StorageError::WasmError(format!("Deserialization error: {}", e)))?;

        Ok(Self {
            root: index_data.root,
            nodes: index_data.nodes,
            next_page: index_data.next_page,
            storage,
            file_path: file_path.to_string(),
        })
    }

    pub async fn save(&self) -> Result<(), StorageError> {
        let index_data = BTreeIndexData {
            root: self.root,
            nodes: self.nodes.clone(),
            next_page: self.next_page,
        };

        let data = bincode::serialize(&index_data)
            .map_err(|e| StorageError::WasmError(format!("Serialization error: {}", e)))?;

        let options = OpenOptions {
            read: false,
            write: true,
            create: true,
            truncate: true,
        };

        let handle = self.storage.open(&self.file_path, options).await?;
        self.storage.write(handle, 0, &data).await?;
        self.storage.sync(handle).await?;
        self.storage.close(handle).await?;

        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<&IndexEntry> {
        let mut page = self.root;
        loop {
            let node = self.nodes.get(&page)?;
            let pos = node.find_key_position(key);

            if pos < node.keys.len() && node.keys[pos] == key {
                return node.values.get(pos);
            }

            if node.is_leaf {
                return None;
            }

            page = node.children[pos];
        }
    }

    pub fn insert(&mut self, key: String, entry: IndexEntry) {
        let root_page = self.root;
        let root_is_full = self.nodes.get(&root_page).map(|n| n.is_full()).unwrap_or(false);

        if root_is_full {
            let new_root_page = self.next_page;
            self.next_page += 1;

            let mut new_root = BTreeNode::new_internal();
            new_root.children.push(root_page);
            self.nodes.insert(new_root_page, new_root);
            self.root = new_root_page;
        }

        let root_page = self.root;
        self.insert_non_full(root_page, key, entry);
    }

    fn insert_non_full(&mut self, page: u64, key: String, entry: IndexEntry) {
        let is_leaf = self.nodes.get(&page).map(|n| n.is_leaf).unwrap_or(true);

        if is_leaf {
            if let Some(node) = self.nodes.get_mut(&page) {
                let pos = node.find_key_position(&key);
                if pos < node.keys.len() && node.keys[pos] == key {
                    node.values[pos] = entry;
                } else {
                    node.keys.insert(pos, key);
                    node.values.insert(pos, entry);
                }
            }
        } else {
            let child_pos = self.nodes.get(&page).map(|n| n.find_key_position(&key)).unwrap_or(0);
            let child_page = self.nodes.get(&page).and_then(|n| n.children.get(child_pos).copied()).unwrap();
            let child_is_full = self.nodes.get(&child_page).map(|n| n.is_full()).unwrap_or(false);

            if child_is_full {
                let (new_page, mid_key, mid_value) = self.split_child(page, child_pos);

                let node = self.nodes.get_mut(&page).unwrap();
                node.keys.insert(child_pos, mid_key);
                node.values.insert(child_pos, mid_value);
                node.children.insert(child_pos + 1, new_page);

                let new_child_pos = node.find_key_position(&key);
                let new_child_page = node.children[new_child_pos];
                self.insert_non_full(new_child_page, key, entry);
            } else {
                self.insert_non_full(child_page, key, entry);
            }
        }
    }

    fn split_child(&mut self, parent_page: u64, child_pos: usize) -> (u64, String, IndexEntry) {
        let child_page = self.nodes.get(&parent_page).and_then(|n| n.children.get(child_pos).copied()).unwrap();
        let child = self.nodes.get(&child_page).unwrap().clone();

        let mid = child.keys.len() / 2;
        let mid_key = child.keys[mid].clone();
        let mid_value = child.values[mid].clone();

        let new_page = self.next_page;
        self.next_page += 1;

        let mut new_node = if child.is_leaf {
            BTreeNode::new_leaf()
        } else {
            BTreeNode::new_internal()
        };

        new_node.keys = child.keys[mid + 1..].to_vec();
        new_node.values = child.values[mid + 1..].to_vec();
        if !child.is_leaf {
            new_node.children = child.children[mid + 1..].to_vec();
        }

        if child.is_leaf {
            new_node.next_leaf = child.next_leaf;
            if let Some(parent) = self.nodes.get(&parent_page) {
                if child_pos + 1 < parent.children.len() {
                    let next_sibling = parent.children[child_pos + 1];
                    if let Some(next_node) = self.nodes.get(&next_sibling) {
                        new_node.next_leaf = next_node.next_leaf;
                    }
                }
            }
            if let Some(next_node) = self.nodes.get_mut(&new_page) {
                if let Some(parent) = self.nodes.get_mut(&parent_page) {
                    if child_pos + 1 < parent.children.len() {
                        let next_sibling = parent.children[child_pos + 1];
                        if let Some(next_node) = self.nodes.get_mut(&next_sibling) {
                            next_node.next_leaf = new_page;
                        }
                    }
                }
            }
        }

        let child_node = self.nodes.get_mut(&child_page).unwrap();
        child_node.keys.truncate(mid);
        child_node.values.truncate(mid);
        child_node.children.truncate(mid + 1);

        self.nodes.insert(new_page, new_node);

        (new_page, mid_key, mid_value)
    }

    pub fn remove(&mut self, key: &str) -> Option<IndexEntry> {
        let result = self.get(key).cloned();
        if result.is_some() {
            let root = self.root;
            self.remove_from_root(&root, key);
        }
        result
    }

    fn remove_from_root(&mut self, page: &u64, key: &str) {
        if let Some(node) = self.nodes.get(page) {
            if node.is_leaf {
                if let Some(node) = self.nodes.get_mut(page) {
                    if let Some(pos) = node.keys.iter().position(|k| k == key) {
                        node.keys.remove(pos);
                        node.values.remove(pos);
                    }
                }
            } else {
                let pos = node.find_key_position(key);
                if pos < node.keys.len() && node.keys[pos] == key {
                    self.remove_from_internal_node(*page, pos);
                } else if pos < node.children.len() {
                    let child_page = node.children[pos];
                    self.remove_from_root(&child_page, key);
                }
            }
        }
    }

    fn remove_from_internal_node(&mut self, page: u64, pos: usize) {
        let left_child = self.nodes.get(&page).and_then(|n| n.children.get(pos).copied());
        if let Some(left) = left_child {
            let predecessor = self.get_max_key(&left);
            if let Some((key, value)) = predecessor {
                if let Some(node) = self.nodes.get_mut(&page) {
                    node.keys[pos] = key.clone();
                    node.values[pos] = value;
                }
                self.remove_from_root(&left, &key);
            }
        }
    }

    fn get_max_key(&self, page: &u64) -> Option<(String, IndexEntry)> {
        if let Some(node) = self.nodes.get(page) {
            if node.is_leaf {
                if !node.keys.is_empty() {
                    let last = node.keys.len() - 1;
                    return Some((node.keys[last].clone(), node.values[last].clone()));
                }
            } else {
                let last_child = node.children.last().copied();
                if let Some(child) = last_child {
                    return self.get_max_key(&child);
                }
            }
        }
        None
    }

    pub fn range(&self, start: &str, end: &str) -> Vec<(String, IndexEntry)> {
        let mut results = Vec::new();
        self.range_collect(&self.root, start, end, &mut results);
        results
    }

    fn range_collect(&self, page: &u64, start: &str, end: &str, results: &mut Vec<(String, IndexEntry)>) {
        if let Some(node) = self.nodes.get(page) {
            if node.is_leaf {
                for i in 0..node.keys.len() {
                    if node.keys[i].as_str() >= start && node.keys[i].as_str() < end {
                        results.push((node.keys[i].clone(), node.values[i].clone()));
                    }
                    if node.keys[i].as_str() >= end {
                        return;
                    }
                }
            } else {
                for child in &node.children {
                    self.range_collect(child, start, end, results);
                }
            }
        }
    }

    pub fn keys(&self) -> Vec<String> {
        let mut keys = Vec::new();
        self.collect_keys(&self.root, &mut keys);
        keys
    }

    fn collect_keys(&self, page: &u64, keys: &mut Vec<String>) {
        if let Some(node) = self.nodes.get(page) {
            if node.is_leaf {
                keys.extend(node.keys.clone());
            } else {
                for i in 0..node.children.len() {
                    self.collect_keys(&node.children[i], keys);
                    if i < node.keys.len() {
                        keys.push(node.keys[i].clone());
                    }
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.count_keys(&self.root)
    }

    fn count_keys(&self, page: &u64) -> usize {
        if let Some(node) = self.nodes.get(page) {
            let self_count = node.keys.len();
            if node.is_leaf {
                self_count
            } else {
                self_count + node.children.iter().map(|c| self.count_keys(c)).sum::<usize>()
            }
        } else {
            0
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BTreeIndexData {
    root: u64,
    nodes: HashMap<u64, BTreeNode>,
    next_page: u64,
}

/// Thread-safe B-tree index wrapper
pub struct SharedBTreeIndex {
    inner: Arc<RwLock<BTreeIndex>>,
}

impl SharedBTreeIndex {
    pub fn new(storage: Arc<dyn Storage>, file_path: &str) -> Self {
        Self {
            inner: Arc::new(RwLock::new(BTreeIndex::new(storage, file_path))),
        }
    }

    pub async fn load(storage: Arc<dyn Storage>, file_path: &str) -> Result<Self, StorageError> {
        let index = BTreeIndex::load(storage, file_path).await?;
        Ok(Self {
            inner: Arc::new(RwLock::new(index)),
        })
    }

    pub async fn get(&self, key: &str) -> Option<IndexEntry> {
        self.inner.read().await.get(key).cloned()
    }

    pub async fn insert(&self, key: String, entry: IndexEntry) {
        self.inner.write().await.insert(key, entry);
    }

    pub async fn remove(&self, key: &str) -> Option<IndexEntry> {
        self.inner.write().await.remove(key)
    }

    pub async fn range(&self, start: &str, end: &str) -> Vec<(String, IndexEntry)> {
        self.inner.read().await.range(start, end)
    }

    pub async fn keys(&self) -> Vec<String> {
        self.inner.read().await.keys()
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.is_empty()
    }

    pub async fn contains_key(&self, key: &str) -> bool {
        self.inner.read().await.contains_key(key)
    }

    pub async fn save(&self) -> Result<(), StorageError> {
        self.inner.read().await.save().await
    }
}

impl Clone for SharedBTreeIndex {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::WasiStorage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_btree_insert_get() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.idx").to_string_lossy().to_string();
        let mut index = BTreeIndex::new(storage, &file_path);

        index.insert("key1".to_string(), IndexEntry::new(100, 50));
        index.insert("key2".to_string(), IndexEntry::new(200, 60));
        index.insert("key3".to_string(), IndexEntry::new(300, 70));

        assert_eq!(index.get("key1").unwrap().offset, 100);
        assert_eq!(index.get("key2").unwrap().offset, 200);
        assert_eq!(index.get("key3").unwrap().offset, 300);
    }

    #[tokio::test]
    async fn test_btree_remove() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.idx").to_string_lossy().to_string();
        let mut index = BTreeIndex::new(storage, &file_path);

        index.insert("key1".to_string(), IndexEntry::new(100, 50));
        index.insert("key2".to_string(), IndexEntry::new(200, 60));
        index.insert("key3".to_string(), IndexEntry::new(300, 70));

        let removed = index.remove("key2").unwrap();
        assert_eq!(removed.offset, 200);

        assert!(index.get("key2").is_none());
        assert!(index.get("key1").is_some());
        assert!(index.get("key3").is_some());
    }

    #[tokio::test]
    async fn test_btree_range() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.idx").to_string_lossy().to_string();
        let mut index = BTreeIndex::new(storage, &file_path);

        for i in 0..10 {
            let key = format!("key{:02}", i);
            index.insert(key, IndexEntry::new(i * 100, 50));
        }

        let results = index.range("key03", "key07");
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].0, "key03");
        assert_eq!(results[3].0, "key06");
    }

    #[tokio::test]
    async fn test_btree_len() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.idx").to_string_lossy().to_string();
        let mut index = BTreeIndex::new(storage, &file_path);

        assert!(index.is_empty());

        for i in 0..5 {
            let key = format!("key{}", i);
            index.insert(key, IndexEntry::new(i * 100, 50));
        }

        assert_eq!(index.len(), 5);
    }

    #[tokio::test]
    async fn test_btree_update_existing() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.idx").to_string_lossy().to_string();
        let mut index = BTreeIndex::new(storage, &file_path);

        index.insert("key1".to_string(), IndexEntry::new(100, 50));
        index.insert("key1".to_string(), IndexEntry::new(200, 60));

        let entry = index.get("key1").unwrap();
        assert_eq!(entry.offset, 200);
        assert_eq!(entry.size, 60);
        assert_eq!(index.len(), 1);
    }

    #[tokio::test]
    async fn test_btree_contains_key() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.idx").to_string_lossy().to_string();
        let mut index = BTreeIndex::new(storage, &file_path);

        index.insert("key1".to_string(), IndexEntry::new(100, 50));

        assert!(index.contains_key("key1"));
        assert!(!index.contains_key("key2"));
    }

    #[tokio::test]
    async fn test_btree_persistence() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.idx").to_string_lossy().to_string();

        {
            let mut index = BTreeIndex::new(storage.clone(), &file_path);
            index.insert("key1".to_string(), IndexEntry::new(100, 50));
            index.insert("key2".to_string(), IndexEntry::new(200, 60));
            index.save().await.unwrap();
        }

        {
            let index = BTreeIndex::load(storage, &file_path).await.unwrap();
            assert_eq!(index.get("key1").unwrap().offset, 100);
            assert_eq!(index.get("key2").unwrap().offset, 200);
        }
    }

    #[tokio::test]
    async fn test_btree_many_keys() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.idx").to_string_lossy().to_string();
        let mut index = BTreeIndex::new(storage, &file_path);

        for i in 0..100 {
            let key = format!("key{:04}", i);
            index.insert(key, IndexEntry::new(i * 100, 50));
        }

        assert_eq!(index.len(), 100);

        for i in 0..100 {
            let key = format!("key{:04}", i);
            let entry = index.get(&key).unwrap();
            assert_eq!(entry.offset, i * 100);
        }
    }

    #[tokio::test]
    async fn test_btree_ordered_iteration() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.idx").to_string_lossy().to_string();
        let mut index = BTreeIndex::new(storage, &file_path);

        index.insert("charlie".to_string(), IndexEntry::new(300, 50));
        index.insert("alice".to_string(), IndexEntry::new(100, 50));
        index.insert("bob".to_string(), IndexEntry::new(200, 50));

        let keys = index.keys();
        assert_eq!(keys, vec!["alice", "bob", "charlie"]);
    }

    #[tokio::test]
    async fn test_shared_btree_index() {
        let dir = tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(WasiStorage::new(dir.path()));
        let file_path = dir.path().join("test.idx").to_string_lossy().to_string();
        let index = SharedBTreeIndex::new(storage, &file_path);

        index.insert("key1".to_string(), IndexEntry::new(100, 50)).await;

        let entry = index.get("key1").await.unwrap();
        assert_eq!(entry.offset, 100);

        let keys = index.keys().await;
        assert_eq!(keys, vec!["key1"]);
    }
}
