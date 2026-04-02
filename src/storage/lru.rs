use std::collections::HashMap;
use std::hash::Hash;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct LruCache<K, V> {
    entries: HashMap<K, CacheEntry<V>>,
    max_size: usize,
}

struct CacheEntry<V> {
    value: V,
    last_accessed: u64,
}

impl<K, V> LruCache<K, V>
where
    K: Clone + Hash + Eq,
{
    pub fn new(max_size: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_size,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.last_accessed = current_timestamp();
            Some(&entry.value)
        } else {
            None
        }
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.last_accessed = current_timestamp();
            Some(&mut entry.value)
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<K> {
        if self.entries.len() >= self.max_size && !self.entries.contains_key(&key) {
            let lru_key = self.find_lru_key();
            if let Some(k) = lru_key {
                self.entries.remove(&k);
                let entry = CacheEntry {
                    value,
                    last_accessed: current_timestamp(),
                };
                self.entries.insert(key, entry);
                return Some(k);
            }
        }

        let entry = CacheEntry {
            value,
            last_accessed: current_timestamp(),
        };
        self.entries.insert(key, entry);
        None
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.entries.remove(key).map(|entry| entry.value)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.entries.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.values().map(|entry| &entry.value)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.entries.iter().map(|(k, entry)| (k, &entry.value))
    }

    fn find_lru_key(&self) -> Option<K> {
        self.entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_accessed)
            .map(|(k, _)| k.clone())
    }

    pub fn evict_one(&mut self) -> Option<(K, V)> {
        let lru_key = self.find_lru_key()?;
        let entry = self.entries.remove(&lru_key)?;
        Some((lru_key, entry.value))
    }

    pub fn evict_until(&mut self, target_size: usize) -> Vec<(K, V)> {
        let mut evicted = Vec::new();
        while self.entries.len() > target_size {
            if let Some((key, value)) = self.evict_one() {
                evicted.push((key, value));
            } else {
                break;
            }
        }
        evicted
    }
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_new() {
        let cache: LruCache<String, String> = LruCache::new(10);
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_lru_cache_insert_get() {
        let mut cache = LruCache::new(10);
        cache.insert("key1".to_string(), "value1".to_string());

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(&"key1".to_string()), Some(&"value1".to_string()));
    }

    #[test]
    fn test_lru_cache_eviction() {
        let mut cache = LruCache::new(2);
        cache.insert("key1".to_string(), "value1".to_string());
        cache.insert("key2".to_string(), "value2".to_string());

        let evicted = cache.insert("key3".to_string(), "value3".to_string());
        assert!(evicted.is_some());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_lru_cache_access_updates_timestamp() {
        let mut cache = LruCache::new(2);
        cache.insert("key1".to_string(), "value1".to_string());
        cache.insert("key2".to_string(), "value2".to_string());

        cache.get(&"key1".to_string());

        let evicted = cache.insert("key3".to_string(), "value3".to_string());
        assert_eq!(evicted, Some("key2".to_string()));
    }

    #[test]
    fn test_lru_cache_remove() {
        let mut cache = LruCache::new(10);
        cache.insert("key1".to_string(), "value1".to_string());

        let removed = cache.remove(&"key1".to_string());
        assert_eq!(removed, Some("value1".to_string()));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_lru_cache_contains_key() {
        let mut cache = LruCache::new(10);
        cache.insert("key1".to_string(), "value1".to_string());

        assert!(cache.contains_key(&"key1".to_string()));
        assert!(!cache.contains_key(&"key2".to_string()));
    }

    #[test]
    fn test_lru_cache_keys() {
        let mut cache = LruCache::new(10);
        cache.insert("key1".to_string(), "value1".to_string());
        cache.insert("key2".to_string(), "value2".to_string());

        let keys: Vec<&String> = cache.keys().collect();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_lru_cache_evict_one() {
        let mut cache = LruCache::new(10);
        cache.insert("key1".to_string(), "value1".to_string());
        cache.insert("key2".to_string(), "value2".to_string());

        let evicted = cache.evict_one();
        assert!(evicted.is_some());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_lru_cache_evict_until() {
        let mut cache = LruCache::new(10);
        for i in 0..5 {
            cache.insert(format!("key{}", i), format!("value{}", i));
        }

        let evicted = cache.evict_until(2);
        assert_eq!(evicted.len(), 3);
        assert_eq!(cache.len(), 2);
    }
}
