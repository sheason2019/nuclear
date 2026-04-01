use nuclear::Database;
use nuclear::api::DatabaseBuilder;
use nuclear::storage::WasiStorage;
use tempfile::tempdir;
use std::collections::HashSet;

struct TestNode {
    db: Database<WasiStorage>,
    id: String,
}

impl TestNode {
    async fn new(id: &str, base_path: &str) -> Self {
        let storage = WasiStorage::new(base_path);
        let db = DatabaseBuilder::new(storage)
            .node_id(id.to_string())
            .base_path(base_path.to_string())
            .build()
            .await
            .unwrap();
        
        Self {
            db,
            id: id.to_string(),
        }
    }
    
    async fn create_record(&self, collection: &str, data: serde_json::Value) -> String {
        let mut fields = Vec::new();
        if let Some(obj) = data.as_object() {
            for (key, value) in obj {
                match value {
                    serde_json::Value::String(s) => fields.push(format!("{}: \"{}\"", key, s)),
                    serde_json::Value::Number(n) => fields.push(format!("{}: {}", key, n)),
                    serde_json::Value::Bool(b) => fields.push(format!("{}: {}", key, b)),
                    _ => fields.push(format!("{}: \"{}\"", key, value)),
                }
            }
        }
        let data_str = fields.join(", ");
        
        let result = self.db.mutation(&format!(r#"
            mutation {{
                createRecord(collection: "{}", data: {{{}}}) {{
                    meta {{ id }}
                }}
            }}
        "#, collection, data_str)).await.unwrap();
        
        result.get("createRecord").unwrap()
            .get("meta").unwrap()
            .get("id").unwrap()
            .as_str().unwrap()
            .to_string()
    }
    
    async fn update_record(&self, collection: &str, id: &str, data: serde_json::Value) {
        let mut fields = Vec::new();
        if let Some(obj) = data.as_object() {
            for (key, value) in obj {
                match value {
                    serde_json::Value::String(s) => fields.push(format!("{}: \"{}\"", key, s)),
                    serde_json::Value::Number(n) => fields.push(format!("{}: {}", key, n)),
                    serde_json::Value::Bool(b) => fields.push(format!("{}: {}", key, b)),
                    _ => fields.push(format!("{}: \"{}\"", key, value)),
                }
            }
        }
        let data_str = fields.join(", ");
        
        self.db.mutation(&format!(r#"
            mutation {{
                updateRecord(collection: "{}", id: "{}", data: {{{}}}) {{
                    meta {{ id }}
                }}
            }}
        "#, collection, id, data_str)).await.unwrap();
    }
    
    async fn delete_record(&self, collection: &str, id: &str) {
        self.db.mutation(&format!(r#"
            mutation {{
                deleteRecord(collection: "{}", id: "{}")
            }}
        "#, collection, id)).await.unwrap();
    }
    
    async fn get_all_records(&self, collection: &str) -> Vec<serde_json::Value> {
        let result = self.db.query(&format!(r#"
            query {{
                records(collection: "{}") {{
                    meta {{ id createdAt updatedAt }}
                    data
                }}
            }}
        "#, collection)).await.unwrap();
        
        result.get("records").unwrap()
            .as_array().unwrap()
            .clone()
    }
    
    async fn get_record_by_id(&self, collection: &str, id: &str) -> Option<serde_json::Value> {
        let result = self.db.query(&format!(r#"
            query {{
                record(collection: "{}", id: "{}") {{
                    meta {{ id createdAt updatedAt }}
                    data
                }}
            }}
        "#, collection, id)).await.unwrap();
        
        let record = result.get("record").unwrap();
        if record.is_null() {
            None
        } else {
            Some(record.clone())
        }
    }
    
    async fn count_records(&self, collection: &str) -> i32 {
        let result = self.db.query(&format!(r#"
            query {{
                recordsAggregate(collection: "{}") {{
                    count
                }}
            }}
        "#, collection)).await.unwrap();
        
        result.get("recordsAggregate").unwrap()
            .get("count").unwrap()
            .as_i64().unwrap() as i32
    }
}

#[tokio::test]
async fn test_consistency_single_node_crud() {
    let dir = tempdir().unwrap();
    let node = TestNode::new("node1", dir.path().to_str().unwrap()).await;
    
    let id = node.create_record("users", serde_json::json!({
        "name": "Alice",
        "age": 25
    })).await;
    
    let record = node.get_record_by_id("users", &id).await.unwrap();
    assert_eq!(record.get("data").unwrap().get("name").unwrap(), "Alice");
    assert_eq!(record.get("data").unwrap().get("age").unwrap(), 25);
    
    node.update_record("users", &id, serde_json::json!({
        "name": "Alice Updated",
        "age": 26
    })).await;
    
    let record = node.get_record_by_id("users", &id).await.unwrap();
    assert_eq!(record.get("data").unwrap().get("name").unwrap(), "Alice Updated");
    assert_eq!(record.get("data").unwrap().get("age").unwrap(), 26);
    
    let count = node.count_records("users").await;
    assert!(count >= 1);
}

#[tokio::test]
async fn test_consistency_metadata_integrity() {
    let dir = tempdir().unwrap();
    let node = TestNode::new("node1", dir.path().to_str().unwrap()).await;
    
    let id = node.create_record("users", serde_json::json!({
        "name": "Alice"
    })).await;
    
    let record = node.get_record_by_id("users", &id).await.unwrap();
    let meta = record.get("meta").unwrap();
    
    assert!(meta.get("id").is_some());
    assert!(meta.get("createdAt").is_some());
    assert!(meta.get("updatedAt").is_some());
    
    let created_at = meta.get("createdAt").unwrap().as_str().unwrap();
    let updated_at = meta.get("updatedAt").unwrap().as_str().unwrap();
    
    assert_eq!(created_at, updated_at);
}

#[tokio::test]
async fn test_consistency_update_changes_updated_at() {
    let dir = tempdir().unwrap();
    let node = TestNode::new("node1", dir.path().to_str().unwrap()).await;
    
    let id = node.create_record("users", serde_json::json!({
        "name": "Alice"
    })).await;
    
    let record1 = node.get_record_by_id("users", &id).await.unwrap();
    let updated_at1 = record1.get("meta").unwrap()
        .get("updatedAt").unwrap()
        .as_str().unwrap()
        .to_string();
    
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    
    node.update_record("users", &id, serde_json::json!({
        "name": "Alice Updated"
    })).await;
    
    let record2 = node.get_record_by_id("users", &id).await.unwrap();
    let updated_at2 = record2.get("meta").unwrap()
        .get("updatedAt").unwrap()
        .as_str().unwrap()
        .to_string();
    
    assert_ne!(updated_at1, updated_at2);
}

#[tokio::test]
async fn test_consistency_unique_ids() {
    let dir = tempdir().unwrap();
    let node = TestNode::new("node1", dir.path().to_str().unwrap()).await;
    
    let mut ids = HashSet::new();
    
    for i in 0..100 {
        let id = node.create_record("users", serde_json::json!({
            "name": format!("User{}", i)
        })).await;
        
        assert!(ids.insert(id), "Duplicate ID found");
    }
    
    assert_eq!(ids.len(), 100);
}

#[tokio::test]
async fn test_consistency_multiple_collections() {
    let dir = tempdir().unwrap();
    let node = TestNode::new("node1", dir.path().to_str().unwrap()).await;
    
    let user_id = node.create_record("users", serde_json::json!({
        "name": "Alice"
    })).await;
    
    let post_id = node.create_record("posts", serde_json::json!({
        "title": "Hello World",
        "author_id": user_id
    })).await;
    
    let user = node.get_record_by_id("users", &user_id).await.unwrap();
    let post = node.get_record_by_id("posts", &post_id).await.unwrap();
    
    assert_eq!(user.get("data").unwrap().get("name").unwrap(), "Alice");
    assert_eq!(post.get("data").unwrap().get("author_id").unwrap().as_str().unwrap(), user_id);
}

#[tokio::test]
async fn test_consistency_filter_and_sort() {
    let dir = tempdir().unwrap();
    let node = TestNode::new("node1", dir.path().to_str().unwrap()).await;
    
    node.create_record("users", serde_json::json!({
        "name": "Alice",
        "age": 25
    })).await;
    
    node.create_record("users", serde_json::json!({
        "name": "Bob",
        "age": 30
    })).await;
    
    node.create_record("users", serde_json::json!({
        "name": "Charlie",
        "age": 28
    })).await;
    
    let result = node.db.query(r#"
        query {
            records(collection: "users", filter: {age: {gt: 26}}, orderBy: {age: ASC}) {
                data
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].get("data").unwrap().get("name").unwrap(), "Charlie");
    assert_eq!(records[1].get("data").unwrap().get("name").unwrap(), "Bob");
}

#[tokio::test]
async fn test_consistency_pagination() {
    let dir = tempdir().unwrap();
    let node = TestNode::new("node1", dir.path().to_str().unwrap()).await;
    
    for i in 0..20 {
        node.create_record("users", serde_json::json!({
            "name": format!("User{}", i),
            "age": 20 + i
        })).await;
    }
    
    let result = node.db.query(r#"
        query {
            records(collection: "users", orderBy: {age: ASC}, first: 5, offset: 10) {
                data
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records.len(), 5);
    assert_eq!(records[0].get("data").unwrap().get("age").unwrap(), 30);
}

#[tokio::test]
async fn test_consistency_large_dataset() {
    let dir = tempdir().unwrap();
    let node = TestNode::new("node1", dir.path().to_str().unwrap()).await;
    
    for i in 0..1000 {
        node.create_record("users", serde_json::json!({
            "name": format!("User{}", i),
            "age": 20 + (i % 50),
            "email": format!("user{}@example.com", i)
        })).await;
    }
    
    assert_eq!(node.count_records("users").await, 1000);
    
    let result = node.db.query(r#"
        query {
            records(collection: "users") {
                data
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records.len(), 1000);
}

#[tokio::test]
async fn test_consistency_concurrent_operations() {
    let dir = tempdir().unwrap();
    let node = std::sync::Arc::new(TestNode::new("node1", dir.path().to_str().unwrap()).await);
    
    let mut handles = vec![];
    
    for i in 0..10 {
        let node_clone = node.clone();
        let handle = tokio::spawn(async move {
            for j in 0..10 {
                node_clone.create_record("users", serde_json::json!({
                    "name": format!("User_{}_{}", i, j)
                })).await;
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
    
    assert_eq!(node.count_records("users").await, 100);
}

#[tokio::test]
async fn test_consistency_error_handling() {
    let dir = tempdir().unwrap();
    let node = TestNode::new("node1", dir.path().to_str().unwrap()).await;
    
    let result = node.db.query(r#"
        query {
            record(collection: "users", id: "nonexistent") {
                data
            }
        }
    "#).await.unwrap();
    
    assert!(result.get("record").unwrap().is_null());
    
    let result = node.db.mutation(r#"
        mutation {
            updateRecord(collection: "users", id: "nonexistent", data: {name: "Alice"}) {
                meta { id }
            }
        }
    "#).await.unwrap();
    
    assert!(result.get("updateRecord").unwrap().is_null());
    
    let result = node.db.mutation(r#"
        mutation {
            deleteRecord(collection: "users", id: "nonexistent")
        }
    "#).await.unwrap();
    
    assert_eq!(result.get("deleteRecord").unwrap(), false);
}
