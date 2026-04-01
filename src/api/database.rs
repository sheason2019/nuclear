use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use async_graphql::{Schema, EmptySubscription};
use crate::core::{VectorClock, LWWMap};
use crate::storage::{Storage, error::StorageError};
use crate::graphql::{QueryRoot, MutationRoot};

pub struct Database<S: Storage + 'static> {
    storage: Arc<S>,
    pub(crate) collections: Arc<RwLock<HashMap<String, Collection>>>,
    node_id: String,
    pub(crate) clock: Arc<RwLock<VectorClock>>,
    relations: Arc<RwLock<HashMap<String, RelationConfig>>>,
}

pub(crate) struct Collection {
    pub data: LWWMap<String, RecordData>,
}

#[derive(Clone)]
pub(crate) struct RecordData {
    pub fields: serde_json::Value,
    pub meta: RecordMeta,
}

impl PartialEq for RecordData {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

#[derive(Clone)]
pub(crate) struct RecordMeta {
    pub id: String,
    pub created_at: u64,
    pub updated_at: u64,
    pub clock: VectorClock,
}

struct RelationConfig {
    target_collection: String,
    foreign_key: String,
    local_key: String,
}

#[derive(Clone)]
pub(crate) struct GraphqlDatabase {
    pub collections: Arc<RwLock<HashMap<String, Collection>>>,
    pub clock: Arc<RwLock<VectorClock>>,
    pub node_id: String,
}

impl GraphqlDatabase {
    pub async fn get_records(&self, collection: &str) -> Result<Vec<(String, RecordData)>, StorageError> {
        let collections = self.collections.read().await;
        if let Some(col) = collections.get(collection) {
            let mut records = Vec::new();
            for key in col.data.keys() {
                if let Some(record) = col.data.get(key) {
                    records.push((key.clone(), record.clone()));
                }
            }
            Ok(records)
        } else {
            Ok(Vec::new())
        }
    }

    pub async fn get_record(&self, collection: &str, id: &str) -> Result<Option<RecordData>, StorageError> {
        let collections = self.collections.read().await;
        if let Some(col) = collections.get(collection) {
            let id_string = id.to_string();
            Ok(col.data.get(&id_string).cloned())
        } else {
            Ok(None)
        }
    }

    pub async fn create_record(&self, collection: &str, data: serde_json::Value) -> Result<RecordData, StorageError> {
        let mut collections = self.collections.write().await;
        let col = collections.entry(collection.to_string())
            .or_insert_with(|| Collection { data: LWWMap::new(&self.node_id) });
        
        let id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp_millis() as u64;
        
        let record = RecordData {
            fields: data,
            meta: RecordMeta {
                id: id.clone(),
                created_at: now,
                updated_at: now,
                clock: VectorClock::new(),
            },
        };
        
        col.data.insert(id, record.clone());
        Ok(record)
    }

    pub async fn update_record(&self, collection: &str, id: &str, data: serde_json::Value) -> Result<Option<RecordData>, StorageError> {
        let mut collections = self.collections.write().await;
        if let Some(col) = collections.get_mut(collection) {
            let id_string = id.to_string();
            if let Some(mut record) = col.data.get(&id_string).cloned() {
                let now = chrono::Utc::now().timestamp_millis() as u64;
                record.fields = data;
                record.meta.updated_at = now;
                col.data.insert(id_string, record.clone());
                Ok(Some(record))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub async fn delete_record(&self, collection: &str, id: &str) -> Result<bool, StorageError> {
        let mut collections = self.collections.write().await;
        if let Some(col) = collections.get_mut(collection) {
            col.data.remove(id.to_string());
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn count_records(&self, collection: &str) -> Result<i32, StorageError> {
        let collections = self.collections.read().await;
        if let Some(col) = collections.get(collection) {
            Ok(col.data.keys().count() as i32)
        } else {
            Ok(0)
        }
    }
}

impl<S: Storage + 'static> Database<S> {
    pub async fn open(storage: S, node_id: String) -> Result<Self, StorageError> {
        Ok(Self {
            storage: Arc::new(storage),
            collections: Arc::new(RwLock::new(HashMap::new())),
            node_id,
            clock: Arc::new(RwLock::new(VectorClock::new())),
            relations: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn query(&self, query: &str) -> Result<serde_json::Value, StorageError> {
        let db_clone = self.clone_for_graphql();
        let schema = Schema::build(QueryRoot, MutationRoot, EmptySubscription)
            .data(db_clone)
            .finish();
        
        let result = schema.execute(query).await;
        
        if result.is_err() {
            return Err(StorageError::WasmError(
                result.errors.iter()
                    .map(|e| e.message.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        
        serde_json::to_value(&result.data)
            .map_err(|e| StorageError::WasmError(e.to_string()))
    }

    pub async fn mutation(&self, mutation: &str) -> Result<serde_json::Value, StorageError> {
        let db_clone = self.clone_for_graphql();
        let schema = Schema::build(QueryRoot, MutationRoot, EmptySubscription)
            .data(db_clone)
            .finish();
        
        let result = schema.execute(mutation).await;
        
        if result.is_err() {
            return Err(StorageError::WasmError(
                result.errors.iter()
                    .map(|e| e.message.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        
        serde_json::to_value(&result.data)
            .map_err(|e| StorageError::WasmError(e.to_string()))
    }

    fn clone_for_graphql(&self) -> GraphqlDatabase {
        GraphqlDatabase {
            collections: self.collections.clone(),
            clock: self.clock.clone(),
            node_id: self.node_id.clone(),
        }
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

    #[allow(dead_code)]
    async fn resolve_relation(
        &self,
        collection: &str,
        record_id: &str,
        field: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        let relations = self.relations.read().await;
        let key = format!("{}.{}", collection, field);
        
        if let Some(config) = relations.get(&key) {
            let collections = self.collections.read().await;
            
            if let Some(source_col) = collections.get(collection) {
                let record_id_string = record_id.to_string();
                if let Some(source_record) = source_col.data.get(&record_id_string) {
                    if let Some(local_value) = source_record.fields.get(&config.local_key) {
                        let target_col = collections.get(&config.target_collection);
                        if let Some(target_col) = target_col {
                            let mut related_records = Vec::new();
                            
                            for target_key in target_col.data.keys() {
                                if let Some(target_record) = target_col.data.get(target_key) {
                                    if let Some(foreign_value) = target_record.fields.get(&config.foreign_key) {
                                        if local_value == foreign_value {
                                            related_records.push(target_record.fields.clone());
                                        }
                                    }
                                }
                            }
                            
                            return Ok(Some(serde_json::Value::Array(related_records)));
                        }
                    }
                }
            }
            
            Ok(Some(serde_json::Value::Array(vec![])))
        } else {
            Ok(None)
        }
    }
}
