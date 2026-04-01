use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::core::{VectorClock, LWWMap};
use crate::storage::{Storage, error::StorageError};

pub struct Database<S: Storage> {
    storage: Arc<S>,
    collections: Arc<RwLock<HashMap<String, Collection>>>,
    node_id: String,
    clock: Arc<RwLock<VectorClock>>,
    relations: Arc<RwLock<HashMap<String, RelationConfig>>>,
}

struct Collection {
    data: LWWMap<String, RecordData>,
}

#[derive(Clone)]
struct RecordData {
    fields: serde_json::Value,
    meta: RecordMeta,
}

impl PartialEq for RecordData {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

#[derive(Clone)]
struct RecordMeta {
    id: String,
    created_at: u64,
    updated_at: u64,
    clock: VectorClock,
}

struct RelationConfig {
    target_collection: String,
    foreign_key: String,
    local_key: String,
}

impl<S: Storage> Database<S> {
    pub async fn open(storage: S, node_id: String) -> Result<Self, StorageError> {
        Ok(Self {
            storage: Arc::new(storage),
            collections: Arc::new(RwLock::new(HashMap::new())),
            node_id,
            clock: Arc::new(RwLock::new(VectorClock::new())),
            relations: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn query(&self, _query: &str) -> Result<serde_json::Value, StorageError> {
        todo!()
    }

    pub async fn mutation(&self, _mutation: &str) -> Result<serde_json::Value, StorageError> {
        todo!()
    }

    pub async fn subscribe(&self, _subscription: &str) -> Result<futures::stream::Empty<serde_json::Value>, StorageError> {
        Ok(futures::stream::empty())
    }

    pub async fn register_relation(
        &self,
        collection: &str,
        field: &str,
        target_collection: &str,
        foreign_key: &str,
        local_key: &str,
    ) -> Result<(), StorageError> {
        let mut relations = self.relations.write().await;
        let key = format!("{}.{}", collection, field);
        relations.insert(key, RelationConfig {
            target_collection: target_collection.to_string(),
            foreign_key: foreign_key.to_string(),
            local_key: local_key.to_string(),
        });
        Ok(())
    }

    async fn resolve_relation(
        &self,
        collection: &str,
        _record_id: &str,
        field: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        let relations = self.relations.read().await;
        let key = format!("{}.{}", collection, field);
        
        if let Some(_config) = relations.get(&key) {
            todo!()
        } else {
            Ok(None)
        }
    }
}
