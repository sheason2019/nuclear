use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use async_graphql::{Schema, EmptySubscription};
use crate::core::{VectorClock, LWWMap};
use crate::storage::{Storage, error::StorageError, OpenOptions};
use crate::storage::{PageStorageEngine, BufferPoolConfig};
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
}

pub(crate) struct Collection {
    pub data: LWWMap<String, RecordData>,
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
                    .or_insert_with(|| Collection { data: LWWMap::new(&self.node_id) });
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
                data: Some(data),
                vector_clock: current_clock,
            });
            drop(changelog);
            
            self.save_collection(collection).await?;
            
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
                        data: Some(data),
                        vector_clock: current_clock,
                    });
                    drop(changelog);
                    
                    self.save_collection(collection).await?;
                    
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
            Ok(col.data.keys().count() as i32)
        } else {
            Ok(0)
        }
    }
    
    async fn save_collection(&self, name: &str) -> Result<(), StorageError> {
        let collections = self.collections.read().await;
        if let Some(collection) = collections.get(name) {
            let mut records = Vec::new();
            for key in collection.data.keys() {
                if let Some(record) = collection.data.get(key) {
                    records.push(record.clone());
                }
            }
            
            let json = serde_json::to_vec(&records)
                .map_err(|e| StorageError::WasmError(format!("Serialization error: {}", e)))?;
            
            let path = format!("{}.json", name);
            
            let _ = tokio::fs::create_dir_all(&self.base_path).await;
            
            let options = OpenOptions {
                read: false,
                write: true,
                create: true,
                truncate: true,
            };
            
            let handle = self.storage.open(&path, options).await?;
            self.storage.write(handle, 0, &json).await?;
            self.storage.sync(handle).await?;
            self.storage.close(handle).await?;
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
        
        let txn_manager = TransactionManager::new((*wal_arc).clone()).await?;
        
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
        };
        
        db.load_all().await?;
        
        Ok(db)
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
            storage: self.storage.clone(),
            base_path: self.base_path.clone(),
            event_bus: self.event_bus.clone(),
            changelog: self.changelog.clone(),
            wal: self.wal.clone(),
            txn_manager: self.txn_manager.clone(),
            constraint_manager: self.constraint_manager.clone(),
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
    
    async fn save_collection(&self, name: &str, collection: &Collection) -> Result<(), StorageError> {
        if let Some(engine) = &self.page_engine {
            engine.page_manager.initialize_collection(name).await?;
            for key in collection.data.keys() {
                if let Some(record) = collection.data.get(key) {
                    let value = bincode::serialize(record)
                        .map_err(|e| StorageError::WasmError(format!("Serialization error: {}", e)))?;
                    engine.page_manager.insert(name, key.as_bytes(), &value).await?;
                }
            }
        }
        Ok(())
    }
    
    async fn load_all(&mut self) -> Result<(), StorageError> {
        let mut collections = self.collections.write().await;
        collections.clear();
        Ok(())
    }
    
    pub async fn load_collection(&self, name: &str) -> Result<(), StorageError> {
        let path = format!("{}.bin", name);
        let options = OpenOptions {
            read: true,
            write: false,
            create: false,
            truncate: false,
        };
        
        let handle = match self.storage.open(&path, options).await {
            Ok(h) => h,
            Err(_) => return Ok(()),
        };
        
        let size = self.storage.size(handle).await? as usize;
        let mut buf = vec![0u8; size];
        self.storage.read(handle, 0, &mut buf).await?;
        self.storage.close(handle).await?;
        
        let records: Vec<RecordData> = bincode::deserialize(&buf)
            .map_err(|e| StorageError::WasmError(format!("Deserialization error: {}", e)))?;
        
        let mut collections = self.collections.write().await;
        let mut lww_map = LWWMap::new(&self.node_id);
        
        for record in records {
            lww_map.insert(record.meta.id.clone(), record);
        }
        
        collections.insert(name.to_string(), Collection { data: lww_map });
        
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
                        .or_insert_with(|| Collection { data: LWWMap::new(&self.node_id) });
                    
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
