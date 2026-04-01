use std::time::Duration;
use crate::storage::Storage;
use super::database::Database;

pub struct DatabaseBuilder<S: Storage> {
    storage: S,
    node_id: Option<String>,
    base_path: Option<String>,
    sync_interval: Duration,
    cache_size: usize,
    relations: Vec<RelationConfig>,
}

pub struct RelationConfig {
    pub collection: String,
    pub field: String,
    pub target_collection: String,
    pub foreign_key: String,
    pub local_key: String,
}

impl<S: Storage + 'static> DatabaseBuilder<S> {
    pub fn new(storage: S) -> Self {
        Self {
            storage,
            node_id: None,
            base_path: None,
            sync_interval: Duration::from_secs(1),
            cache_size: 1024 * 1024 * 100,
            relations: Vec::new(),
        }
    }

    pub fn node_id(mut self, node_id: String) -> Self {
        self.node_id = Some(node_id);
        self
    }
    
    pub fn base_path(mut self, base_path: String) -> Self {
        self.base_path = Some(base_path);
        self
    }

    pub fn sync_interval(mut self, interval: Duration) -> Self {
        self.sync_interval = interval;
        self
    }

    pub fn cache_size(mut self, size: usize) -> Self {
        self.cache_size = size;
        self
    }

    pub fn relation(mut self, config: RelationConfig) -> Self {
        self.relations.push(config);
        self
    }

    pub fn relations(mut self, configs: Vec<RelationConfig>) -> Self {
        self.relations.extend(configs);
        self
    }

    pub async fn build(self) -> Result<Database<S>, crate::storage::error::StorageError> {
        let node_id = self.node_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let base_path = self.base_path.unwrap_or_else(|| "./data".to_string());
        let db = Database::open(self.storage, node_id, base_path).await?;
        
        for config in self.relations {
            db.register_relation(
                &config.collection,
                &config.field,
                &config.target_collection,
                &config.foreign_key,
                &config.local_key,
            ).await?;
        }
        
        Ok(db)
    }
}
