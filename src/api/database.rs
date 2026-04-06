use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use async_graphql::{Schema, EmptySubscription};
use crate::core::{VectorClock, LWWMap};
use crate::storage::{Storage, error::StorageError, OpenOptions};
use crate::storage::{PageStorageEngine, BufferPoolConfig};
use crate::storage::btree::{SharedBTreeIndex, IndexEntry};
use crate::graphql::{QueryRoot, MutationRoot, EventBus, Event, EventType};
use crate::sync::{ChangeLog, ChangeEntry, Operation as SyncOperation, SyncMessage};
use crate::transaction::wal::WriteAheadLog;
use crate::transaction::{TransactionManager, Transaction, TransactionState, Operation as TxnOperation};
use crate::constraints::{ConstraintManager, CollectionConstraints, ConstraintValidator};
use serde::{Serialize, Deserialize};

pub struct Database<S: Storage + 'static> {
    storage: Arc<S>,
    page_engine: Option<PageStorageEngine>,
    pub(crate) collections: Arc<RwLock<HashMap<String, Collection>>>,
    node_id: String,
    pub(crate) clock: Arc<RwLock<VectorClock>>,
    relations: Arc<RwLock<HashMap<String, RelationConfig>>>,
    base_path: String,
    event_bus: EventBus,
    changelog: Arc<RwLock<ChangeLog>>,
    pub(crate) wal: Arc<WriteAheadLog>,
    txn_manager: Arc<RwLock<Option<TransactionManager>>>,
    constraint_manager: Arc<RwLock<ConstraintManager>>,
    cached_schema: Arc<RwLock<Option<Schema<crate::graphql::QueryRoot, crate::graphql::MutationRoot, EmptySubscription>>>>,
}

pub(crate) struct Collection {
    pub data: LWWMap<String, RecordData>,
    pub indexes: HashMap<String, SharedBTreeIndex>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct RecordData {
    pub fields: serde_json::Value,
    pub meta: RecordMeta,
}

impl PartialEq for RecordData {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

#[derive(Clone, Serialize, Deserialize)]
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
    pub storage: Arc<dyn Storage>,
    pub base_path: String,
    pub event_bus: EventBus,
    pub changelog: Arc<RwLock<ChangeLog>>,
    pub wal: Arc<WriteAheadLog>,
    pub txn_manager: Arc<RwLock<Option<TransactionManager>>>,
    pub constraint_manager: Arc<RwLock<ConstraintManager>>,
    page_engine: Option<PageStorageEngine>,
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

    pub async fn begin_transaction(&self) -> Result<u64, StorageError> {
        let mut tm = self.txn_manager.write().await;
        if let Some(tm) = tm.as_mut() {
            let txn = tm.begin();
            Ok(txn.id)
        } else {
            Err(StorageError::WasmError("Transaction manager not initialized".to_string()))
        }
    }

    pub async fn commit_transaction(&self, txn_id: u64) -> Result<(), StorageError> {
        let mut tm = self.txn_manager.write().await;
        if let Some(tm) = tm.as_mut() {
            tm.commit_by_id(txn_id).await?;
        }
        Ok(())
    }

    pub async fn rollback_transaction(&self, txn_id: u64) -> Result<(), StorageError> {
        let mut tm = self.txn_manager.write().await;
        if let Some(tm) = tm.as_mut() {
            tm.rollback_by_id(txn_id).await?;
        }
        Ok(())
    }

    pub async fn create_record(&self, collection: &str, data: serde_json::Value) -> Result<RecordData, StorageError> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp_millis() as u64;
        
        let cm = self.constraint_manager.read().await;
        let mut data = data;
        cm.apply_defaults(collection, &mut data);
        cm.validate(collection, &data)?;
        drop(cm);
        
        let data_bytes = bincode::serialize(&data)
            .map_err(|e| StorageError::WasmError(e.to_string()))?;
        
        let mut tm = self.txn_manager.write().await;
        if let Some(tm) = tm.as_mut() {
            let mut txn = tm.begin();
            txn.operations.push(TxnOperation::Insert {
                collection: collection.to_string(),
                key: id.clone(),
                data: data_bytes.clone(),
            });
            
            let mut clock = self.clock.write().await;
            clock.increment(&self.node_id);
            let current_clock = clock.clone();
            drop(clock);
            
            let record = RecordData {
                fields: data.clone(),
                meta: RecordMeta {
                    id: id.clone(),
                    created_at: now,
                    updated_at: now,
                    clock: current_clock.clone(),
                },
            };
            
            {
                let mut collections = self.collections.write().await;
                let col = collections.entry(collection.to_string())
                    .or_insert_with(|| Collection { data: LWWMap::new(&self.node_id), indexes: HashMap::new() });
                col.data.insert(id.clone(), record.clone());
            }
            
            if let Err(e) = tm.commit(&mut txn).await {
                let mut collections = self.collections.write().await;
                if let Some(col) = collections.get_mut(collection) {
                    col.data.remove(id);
                }
                return Err(e);
            }
            
            let mut changelog = self.changelog.write().await;
            changelog.add_entry(ChangeEntry {
                id: uuid::Uuid::new_v4().to_string(),
                timestamp: now,
                collection: collection.to_string(),
                record_id: id.clone(),
                operation: SyncOperation::Create,
                data: Some(data.clone()),
                vector_clock: current_clock,
            });
            drop(changelog);

            self.save_collection(collection).await?;

            self.update_indexes_on_insert(collection, &id, &data).await;

            let _ = self.event_bus.publish(Event {
                event_type: EventType::Created,
                collection: collection.to_string(),
                record_id: id.clone(),
                data: Some(record.fields.clone()),
            });
            
            Ok(record)
        } else {
            Err(StorageError::WasmError("Transaction manager not initialized".to_string()))
        }
    }

    pub async fn update_record(&self, collection: &str, id: &str, data: serde_json::Value) -> Result<Option<RecordData>, StorageError> {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        
        let cm = self.constraint_manager.read().await;
        let mut data = data;
        cm.apply_defaults(collection, &mut data);
        cm.validate(collection, &data)?;
        drop(cm);
        
        let data_bytes = bincode::serialize(&data)
            .map_err(|e| StorageError::WasmError(e.to_string()))?;
        
        let mut tm = self.txn_manager.write().await;
        if let Some(tm) = tm.as_mut() {
            let mut txn = tm.begin();
            txn.operations.push(TxnOperation::Update {
                collection: collection.to_string(),
                key: id.to_string(),
                data: data_bytes.clone(),
            });
            
            let mut clock = self.clock.write().await;
            clock.increment(&self.node_id);
            let current_clock = clock.clone();
            drop(clock);
            
            let mut collections = self.collections.write().await;
            if let Some(col) = collections.get_mut(collection) {
                let id_string = id.to_string();
                if let Some(mut record) = col.data.get(&id_string).cloned() {
                    let old_record = record.clone();
                    let old_fields = record.fields.clone();
                    record.fields = data.clone();
                    record.meta.updated_at = now;
                    record.meta.clock = current_clock.clone();
                    col.data.insert(id_string.clone(), record.clone());
                    drop(collections);
                    
                    if let Err(e) = tm.commit(&mut txn).await {
                        let mut collections = self.collections.write().await;
                        if let Some(col) = collections.get_mut(collection) {
                            col.data.insert(id_string, old_record);
                        }
                        return Err(e);
                    }
                    
                    let mut changelog = self.changelog.write().await;
                    changelog.add_entry(ChangeEntry {
                        id: uuid::Uuid::new_v4().to_string(),
                        timestamp: now,
                        collection: collection.to_string(),
                        record_id: id.to_string(),
                        operation: SyncOperation::Update,
                        data: Some(data.clone()),
                        vector_clock: current_clock,
                    });
                    drop(changelog);
                    
                    self.save_collection(collection).await?;

                    self.update_indexes_on_update(collection, id, &old_fields, &data).await;

                    let _ = self.event_bus.publish(Event {
                        event_type: EventType::Updated,
                        collection: collection.to_string(),
                        record_id: id.to_string(),
                        data: Some(record.fields.clone()),
                    });
                    
                    Ok(Some(record))
                } else {
                    drop(collections);
                    let _ = tm.rollback(&mut txn).await;
                    Ok(None)
                }
            } else {
                drop(collections);
                let _ = tm.rollback(&mut txn).await;
                Ok(None)
            }
        } else {
            Err(StorageError::WasmError("Transaction manager not initialized".to_string()))
        }
    }

    pub async fn delete_record(&self, collection: &str, id: &str) -> Result<bool, StorageError> {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        
        let mut tm = self.txn_manager.write().await;
        if let Some(tm) = tm.as_mut() {
            let mut txn = tm.begin();
            txn.operations.push(TxnOperation::Delete {
                collection: collection.to_string(),
                key: id.to_string(),
            });
            
            let mut collections = self.collections.write().await;
            if let Some(col) = collections.get_mut(collection) {
                let old_record = col.data.get(&id.to_string()).cloned();
                let old_fields = old_record.as_ref().map(|r| r.fields.clone());
                col.data.remove(id.to_string());
                drop(collections);
                
                if let Err(e) = tm.commit(&mut txn).await {
                    if let Some(record) = old_record {
                        let mut collections = self.collections.write().await;
                        if let Some(col) = collections.get_mut(collection) {
                            col.data.insert(id.to_string(), record);
                        }
                    }
                    return Err(e);
                }
                
                let mut clock = self.clock.write().await;
                clock.increment(&self.node_id);
                let current_clock = clock.clone();
                drop(clock);
                
                let mut changelog = self.changelog.write().await;
                changelog.add_entry(ChangeEntry {
                    id: uuid::Uuid::new_v4().to_string(),
                    timestamp: now,
                    collection: collection.to_string(),
                    record_id: id.to_string(),
                    operation: SyncOperation::Delete,
                    data: None,
                    vector_clock: current_clock,
                });
                drop(changelog);
                
                self.save_collection(collection).await?;

                if let Some(fields) = &old_fields {
                    self.update_indexes_on_delete(collection, id, fields).await;
                }

                let _ = self.event_bus.publish(Event {
                    event_type: EventType::Deleted,
                    collection: collection.to_string(),
                    record_id: id.to_string(),
                    data: None,
                });
                
                Ok(true)
            } else {
                drop(collections);
                let _ = tm.rollback(&mut txn).await;
                Ok(false)
            }
        } else {
            Err(StorageError::WasmError("Transaction manager not initialized".to_string()))
        }
    }

    pub async fn count_records(&self, collection: &str) -> Result<i32, StorageError> {
        let collections = self.collections.read().await;
        if let Some(col) = collections.get(collection) {
            Ok(col.data.values().count() as i32)
        } else {
            Ok(0)
        }
    }

    pub async fn create_index(&self, collection: &str, field: &str) -> Result<(), StorageError> {
        let mut collections = self.collections.write().await;
        let col = collections.entry(collection.to_string())
            .or_insert_with(|| Collection { data: LWWMap::new(&self.node_id), indexes: HashMap::new() });

        if col.indexes.contains_key(field) {
            return Ok(()); // Already indexed
        }

        let index = SharedBTreeIndex::new(
            self.storage.clone(),
            &format!("{}/idx_{}.btree", self.base_path, collection),
        );
        // Build index from existing records
        for (key, record) in col.data.iter() {
            if let Some(value) = record.fields.get(field) {
                let index_key = format!("{}\x00{}", value, key);
                index.insert(index_key, IndexEntry::new(0, 0)).await;
            }
        }
        col.indexes.insert(field.to_string(), index);
        Ok(())
    }

    pub async fn drop_index(&self, collection: &str, field: &str) -> Result<bool, StorageError> {
        let mut collections = self.collections.write().await;
        if let Some(col) = collections.get_mut(collection) {
            Ok(col.indexes.remove(field).is_some())
        } else {
            Ok(false)
        }
    }

    pub async fn create_records(&self, collection: &str, items: Vec<serde_json::Value>) -> Result<Vec<RecordData>, StorageError> {
        let mut tm = self.txn_manager.write().await;
        if let Some(tm) = tm.as_mut() {
            let mut txn = tm.begin();
            let mut results = Vec::new();

            for data in items {
                let id = uuid::Uuid::new_v4().to_string();
                let now = chrono::Utc::now().timestamp_millis() as u64;

                let cm = self.constraint_manager.read().await;
                cm.apply_defaults(collection, &mut data.clone());
                cm.validate(collection, &data)?;
                drop(cm);

                let data_bytes = bincode::serialize(&data)
                    .map_err(|e| StorageError::WasmError(e.to_string()))?;

                txn.operations.push(TxnOperation::Insert {
                    collection: collection.to_string(),
                    key: id.clone(),
                    data: data_bytes,
                });

                let mut clock = self.clock.write().await;
                clock.increment(&self.node_id);
                let current_clock = clock.clone();
                drop(clock);

                let record = RecordData {
                    fields: data.clone(),
                    meta: RecordMeta {
                        id: id.clone(),
                        created_at: now,
                        updated_at: now,
                        clock: current_clock.clone(),
                    },
                };

                {
                    let mut collections = self.collections.write().await;
                    let col = collections.entry(collection.to_string())
                        .or_insert_with(|| Collection { data: LWWMap::new(&self.node_id), indexes: HashMap::new() });
                    col.data.insert(id.clone(), record.clone());
                }

                self.update_indexes_on_insert(collection, &id, &data).await;
                results.push(record);
            }

            if let Err(e) = tm.commit(&mut txn).await {
                // Rollback all inserts
                let mut collections = self.collections.write().await;
                if let Some(col) = collections.get_mut(collection) {
                    for record in &results {
                        col.data.remove(record.meta.id.clone());
                    }
                }
                return Err(e);
            }

            self.save_collection(collection).await?;
            Ok(results)
        } else {
            Err(StorageError::WasmError("Transaction manager not initialized".to_string()))
        }
    }

    pub async fn delete_records(&self, collection: &str, ids: Vec<String>) -> Result<i32, StorageError> {
        let mut tm = self.txn_manager.write().await;
        if let Some(tm) = tm.as_mut() {
            let mut txn = tm.begin();
            let mut deleted_count = 0i32;
            let mut deleted_records = Vec::new();

            for id in &ids {
                txn.operations.push(TxnOperation::Delete {
                    collection: collection.to_string(),
                    key: id.clone(),
                });
            }

            let mut collections = self.collections.write().await;
            if let Some(col) = collections.get_mut(collection) {
                for id in &ids {
                    if let Some(old_record) = col.data.get(id).cloned() {
                        col.data.remove(id.clone());
                        deleted_records.push((id.clone(), old_record));
                        deleted_count += 1;
                    }
                }
            }
            drop(collections);

            if let Err(e) = tm.commit(&mut txn).await {
                let mut collections = self.collections.write().await;
                if let Some(col) = collections.get_mut(collection) {
                    for (id, record) in deleted_records {
                        col.data.insert(id, record);
                    }
                }
                return Err(e);
            }

            let mut clock = self.clock.write().await;
            clock.increment(&self.node_id);
            drop(clock);

            for (id, record) in &deleted_records {
                self.update_indexes_on_delete(collection, id, &record.fields).await;
            }

            self.save_collection(collection).await?;
            Ok(deleted_count)
        } else {
            Err(StorageError::WasmError("Transaction manager not initialized".to_string()))
        }
    }

    /// Check if a field has an index and return candidate record IDs
    async fn query_index(&self, collection: &str, field: &str) -> Option<Vec<String>> {
        let collections = self.collections.read().await;
        let col = collections.get(collection)?;
        let _index = col.indexes.get(field)?;
        // For now, return None to indicate index exists but we fall through to full scan.
        // Full index-based filtering would need operator-aware range scans.
        None
    }

    async fn update_indexes_on_insert(&self, collection: &str, id: &str, fields: &serde_json::Value) {
        let collections = self.collections.read().await;
        if let Some(col) = collections.get(collection) {
            for (field, index) in &col.indexes {
                if let Some(value) = fields.get(field) {
                    let index_key = format!("{}\x00{}", value, id);
                    index.insert(index_key, IndexEntry::new(0, 0)).await;
                }
            }
        }
    }

    async fn update_indexes_on_delete(&self, collection: &str, id: &str, fields: &serde_json::Value) {
        let collections = self.collections.read().await;
        if let Some(col) = collections.get(collection) {
            for (field, index) in &col.indexes {
                if let Some(value) = fields.get(field) {
                    let index_key = format!("{}\x00{}", value, id);
                    index.remove(&index_key).await;
                }
            }
        }
    }

    async fn update_indexes_on_update(&self, collection: &str, id: &str, old_fields: &serde_json::Value, new_fields: &serde_json::Value) {
        let collections = self.collections.read().await;
        if let Some(col) = collections.get(collection) {
            for (field, index) in &col.indexes {
                let old_val = old_fields.get(field);
                let new_val = new_fields.get(field);
                if old_val != new_val {
                    if let Some(v) = old_val {
                        let index_key = format!("{}\x00{}", v, id);
                        index.remove(&index_key).await;
                    }
                    if let Some(v) = new_val {
                        let index_key = format!("{}\x00{}", v, id);
                        index.insert(index_key, IndexEntry::new(0, 0)).await;
                    }
                }
            }
        }
    }
    
    async fn save_collection(&self, name: &str) -> Result<(), StorageError> {
        if let Some(engine) = &self.page_engine {
            let collections = self.collections.read().await;
            if let Some(collection) = collections.get(name) {
                engine.page_manager.initialize_collection(name).await?;
                for key in collection.data.keys() {
                    if let Some(record) = collection.data.get(key) {
                        let value = bincode::serialize(record)
                            .map_err(|e| StorageError::WasmError(format!("Serialization error: {}", e)))?;
                        engine.page_manager.insert(name, key.as_bytes(), &value).await?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl<S: Storage + 'static> Database<S> {
    pub async fn open(storage: S, node_id: String, base_path: String) -> Result<Self, StorageError> {
        let storage_arc = Arc::new(storage);
        let wal = WriteAheadLog::new(storage_arc.clone(), &base_path).await?;
        let wal_arc = Arc::new(wal);
        
        let db_file = format!("{}/nuclear.db", base_path);
        let _ = tokio::fs::create_dir_all(&base_path).await;
        let page_engine = PageStorageEngine::new(
            storage_arc.clone(),
            &db_file,
            BufferPoolConfig::default(),
        ).await?;
        
        let mut txn_manager = TransactionManager::new((*wal_arc).clone()).await?;

        // Get committed WAL entries for replay (before wrapping in Arc)
        let wal_entries = txn_manager.get_committed_entries().await?;

        let mut db = Self {
            storage: storage_arc,
            page_engine: Some(page_engine),
            collections: Arc::new(RwLock::new(HashMap::new())),
            node_id,
            clock: Arc::new(RwLock::new(VectorClock::new())),
            relations: Arc::new(RwLock::new(HashMap::new())),
            base_path,
            event_bus: EventBus::new(),
            changelog: Arc::new(RwLock::new(ChangeLog::new())),
            wal: wal_arc,
            txn_manager: Arc::new(RwLock::new(Some(txn_manager))),
            constraint_manager: Arc::new(RwLock::new(ConstraintManager::new())),
            cached_schema: Arc::new(RwLock::new(None)),
        };

        db.load_all().await?;

        // Replay committed WAL entries on top of page data
        if !wal_entries.is_empty() {
            db.replay_wal_entries(&wal_entries).await?;
            // Checkpoint: flush page engine and truncate WAL
            db.checkpoint().await?;
        }

        Ok(db)
    }

    pub async fn query(&self, query: &str) -> Result<serde_json::Value, StorageError> {
        let schema = self.get_or_build_schema().await;
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
        let schema = self.get_or_build_schema().await;
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

    async fn get_or_build_schema(&self) -> Schema<crate::graphql::QueryRoot, crate::graphql::MutationRoot, EmptySubscription> {
        {
            let cached = self.cached_schema.read().await;
            if let Some(schema) = cached.as_ref() {
                return schema.clone();
            }
        }
        let db_clone = self.clone_for_graphql();
        let schema = Schema::build(crate::graphql::QueryRoot, crate::graphql::MutationRoot, EmptySubscription)
            .data(db_clone)
            .finish();
        let mut cached = self.cached_schema.write().await;
        *cached = Some(schema.clone());
        schema
    }

    fn clone_for_graphql(&self) -> GraphqlDatabase {
        GraphqlDatabase {
            collections: self.collections.clone(),
            clock: self.clock.clone(),
            node_id: self.node_id.clone(),
            storage: self.storage.clone(),
            base_path: self.base_path.clone(),
            event_bus: self.event_bus.clone(),
            changelog: self.changelog.clone(),
            wal: self.wal.clone(),
            txn_manager: self.txn_manager.clone(),
            constraint_manager: self.constraint_manager.clone(),
            page_engine: self.page_engine.clone(),
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

    pub async fn define_constraints(&self, collection: &str, constraints: CollectionConstraints) -> Result<(), StorageError> {
        let mut cm = self.constraint_manager.write().await;
        cm.define_constraints(collection, constraints);
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
    
    pub async fn save(&self) -> Result<(), StorageError> {
        if let Some(engine) = &self.page_engine {
            let collections = self.collections.read().await;
            for (name, collection) in collections.iter() {
                engine.page_manager.initialize_collection(name).await?;
                for key in collection.data.keys() {
                    if let Some(record) = collection.data.get(key) {
                        let value = bincode::serialize(record)
                            .map_err(|e| StorageError::WasmError(format!("Serialization error: {}", e)))?;
                        engine.page_manager.insert(name, key.as_bytes(), &value).await?;
                    }
                }
            }
            engine.flush().await?;
        }
        Ok(())
    }
    
    async fn load_all(&mut self) -> Result<(), StorageError> {
        let mut collections = self.collections.write().await;
        collections.clear();

        if let Some(engine) = &self.page_engine {
            let names = engine.page_manager.collection_names().await;
            for name in names {
                let records = engine.page_manager.scan_collection(&name).await?;
                let mut lww_map = LWWMap::new(&self.node_id);

                for record in records {
                    if record.deleted {
                        continue;
                    }
                    match bincode::deserialize::<RecordData>(&record.value) {
                        Ok(data) => {
                            let key = String::from_utf8_lossy(&record.key).to_string();
                            let ts = data.meta.updated_at;
                            lww_map.insert_with_timestamp(
                                key,
                                data,
                                ts,
                                &self.node_id,
                            );
                        }
                        Err(e) => {
                            eprintln!("Failed to deserialize record in collection '{}': {}", name, e);
                        }
                    }
                }

                collections.insert(name, Collection { data: lww_map, indexes: HashMap::new() });
            }
        }

        Ok(())
    }

    async fn replay_wal_entries(&mut self, entries: &[crate::transaction::wal::WalEntry]) -> Result<(), StorageError> {
        use crate::transaction::wal::WalEntry;
        let now = chrono::Utc::now().timestamp_millis() as u64;

        for entry in entries {
            match entry {
                WalEntry::Insert { collection, key, data, .. } => {
                    let fields: serde_json::Value = bincode::deserialize(data)
                        .map_err(|e| StorageError::WasmError(format!("WAL replay deserialize: {}", e)))?;
                    let record = RecordData {
                        fields,
                        meta: RecordMeta {
                            id: key.clone(),
                            created_at: now,
                            updated_at: now,
                            clock: self.clock.read().await.clone(),
                        },
                    };
                    let mut collections = self.collections.write().await;
                    let col = collections.entry(collection.clone())
                        .or_insert_with(|| Collection { data: LWWMap::new(&self.node_id), indexes: HashMap::new() });
                    col.data.insert(key.clone(), record);
                }
                WalEntry::Update { collection, key, data, .. } => {
                    let fields: serde_json::Value = bincode::deserialize(data)
                        .map_err(|e| StorageError::WasmError(format!("WAL replay deserialize: {}", e)))?;
                    let mut collections = self.collections.write().await;
                    if let Some(col) = collections.get_mut(collection) {
                        if let Some(mut record) = col.data.get(key).cloned() {
                            record.fields = fields;
                            record.meta.updated_at = now;
                            record.meta.clock = self.clock.read().await.clone();
                            col.data.insert(key.clone(), record);
                        }
                    }
                }
                WalEntry::Delete { collection, key, .. } => {
                    let mut collections = self.collections.write().await;
                    if let Some(col) = collections.get_mut(collection) {
                        col.data.remove(key.clone());
                    }
                }
                _ => {} // Begin, Commit, Rollback — no action
            }
        }

        Ok(())
    }

    async fn checkpoint(&self) -> Result<(), StorageError> {
        self.save().await?;
        self.wal.truncate().await?;
        Ok(())
    }

    pub async fn get_sync_request(&self) -> SyncMessage {
        let clock = self.clock.read().await;
        SyncMessage::SyncRequest {
            from: self.node_id.clone(),
            clock: clock.clone(),
        }
    }
    
    pub async fn get_changes_since(&self, since: &VectorClock) -> Result<Vec<ChangeEntry>, StorageError> {
        let changelog = self.changelog.read().await;
        let entries = changelog.get_entries_since(since);
        Ok(entries.into_iter().cloned().collect())
    }
    
    pub async fn apply_sync_response(&self, response: SyncMessage) -> Result<(), StorageError> {
        match response {
            SyncMessage::SyncResponse { from: _, clock, changes } => {
                for change in changes {
                    self.apply_change(change).await?;
                }
                
                let mut my_clock = self.clock.write().await;
                my_clock.merge(&clock);
                drop(my_clock);
                
                Ok(())
            }
            _ => Err(StorageError::WasmError("Invalid sync response".to_string())),
        }
    }
    
    async fn apply_change(&self, change: ChangeEntry) -> Result<(), StorageError> {
        let mut my_clock = self.clock.write().await;
        my_clock.merge(&change.vector_clock);
        drop(my_clock);
        
        let mut changelog = self.changelog.write().await;
        changelog.add_entry(change.clone());
        drop(changelog);
        
        match change.operation {
            SyncOperation::Create | SyncOperation::Update => {
                if let Some(data) = change.data {
                    let mut collections = self.collections.write().await;
                    let col = collections.entry(change.collection.clone())
                        .or_insert_with(|| Collection { data: LWWMap::new(&self.node_id), indexes: HashMap::new() });
                    
                    let now = chrono::Utc::now().timestamp_millis() as u64;
                    
                    if let Some(record) = col.data.get(&change.record_id).cloned() {
                        if change.vector_clock.happens_before(&record.meta.clock) {
                            drop(collections);
                            return Ok(());
                        }
                    }
                    
                    let record = RecordData {
                        fields: data,
                        meta: RecordMeta {
                            id: change.record_id.clone(),
                            created_at: now,
                            updated_at: now,
                            clock: change.vector_clock.clone(),
                        },
                    };
                    col.data.insert(change.record_id.clone(), record);
                    
                    drop(collections);
                    self.save_collection_by_name(&change.collection).await?;
                }
            }
            SyncOperation::Delete => {
                let mut collections = self.collections.write().await;
                if let Some(col) = collections.get_mut(&change.collection) {
                    col.data.remove(change.record_id);
                }
                drop(collections);
                self.save_collection_by_name(&change.collection).await?;
            }
        }
        
        Ok(())
    }
    
    async fn save_collection_by_name(&self, name: &str) -> Result<(), StorageError> {
        if let Some(engine) = &self.page_engine {
            let collections = self.collections.read().await;
            if let Some(collection) = collections.get(name) {
                engine.page_manager.initialize_collection(name).await?;
                for key in collection.data.keys() {
                    if let Some(record) = collection.data.get(key) {
                        let value = bincode::serialize(record)
                            .map_err(|e| StorageError::WasmError(format!("Serialization error: {}", e)))?;
                        engine.page_manager.insert(name, key.as_bytes(), &value).await?;
                    }
                }
            }
        }
        Ok(())
    }
    
    pub async fn get_clock(&self) -> VectorClock {
        self.clock.read().await.clone()
    }
}
