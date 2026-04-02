use nuclear::Database;
use nuclear::api::DatabaseBuilder;
use nuclear::storage::WasiStorage;
use nuclear::sync::SyncMessage;
use tempfile::tempdir;
use std::collections::HashMap;

struct Node {
    db: Database<WasiStorage>,
    id: String,
}

impl Node {
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
    
    async fn get_all_records(&self, collection: &str) -> Vec<serde_json::Value> {
        let result = self.db.query(&format!(r#"
            query {{
                records(collection: "{}") {{
                    meta {{ id }}
                    data
                }}
            }}
        "#, collection)).await.unwrap();
        
        result.get("records").unwrap()
            .as_array().unwrap()
            .clone()
    }
    
    async fn get_clock(&self) -> nuclear::core::VectorClock {
        self.db.get_clock().await
    }
    
    async fn get_sync_request(&self) -> SyncMessage {
        self.db.get_sync_request().await
    }
    
    async fn get_changes_since(&self, since: &nuclear::core::VectorClock) -> Vec<nuclear::sync::ChangeEntry> {
        self.db.get_changes_since(since).await.unwrap()
    }
    
    async fn apply_sync_response(&self, response: SyncMessage) {
        self.db.apply_sync_response(response).await.unwrap();
    }
}

async fn sync_nodes(source: &Node, target: &Node) {
    let request = source.get_sync_request().await;
    
    let source_clock = match &request {
        SyncMessage::SyncRequest { clock, .. } => clock.clone(),
        _ => panic!("Invalid sync request"),
    };
    
    println!("Source clock: node1={}, node2={}", source_clock.get("node1"), source_clock.get("node2"));
    
    let changes = target.get_changes_since(&source_clock).await;
    
    println!("Changes from target: {}", changes.len());
    for change in &changes {
        println!("  - {:?} {} on {}.{}", change.operation, change.record_id, change.collection, change.record_id);
    }
    
    let target_clock = target.get_clock().await;
    
    let response = SyncMessage::SyncResponse {
        from: target.id.clone(),
        clock: target_clock,
        changes,
    };
    
    source.apply_sync_response(response).await;
}

async fn get_records_as_map(node: &Node, collection: &str) -> HashMap<String, serde_json::Value> {
    let records = node.get_all_records(collection).await;
    let mut map = HashMap::new();
    
    for record in records {
        let id = record.get("meta").unwrap()
            .get("id").unwrap()
            .as_str().unwrap()
            .to_string();
        let data = record.get("data").unwrap().clone();
        map.insert(id, data);
    }
    
    map
}

#[tokio::test]
async fn test_eventual_consistency_basic_sync() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    let node2 = Node::new("node2", dir2.path().to_str().unwrap()).await;
    
    node1.create_record("users", serde_json::json!({
        "name": "Alice"
    })).await;
    
    node2.create_record("users", serde_json::json!({
        "name": "Bob"
    })).await;
    
    let count1_before = node1.count_records("users").await;
    let count2_before = node2.count_records("users").await;
    
    println!("Before sync: node1={}, node2={}", count1_before, count2_before);
    
    assert_eq!(count1_before, 1);
    assert_eq!(count2_before, 1);
    
    sync_nodes(&node1, &node2).await;
    
    let count1_after_sync1 = node1.count_records("users").await;
    let count2_after_sync1 = node2.count_records("users").await;
    
    println!("After sync node1->node2: node1={}, node2={}", count1_after_sync1, count2_after_sync1);
    
    sync_nodes(&node2, &node1).await;
    
    let count1_after = node1.count_records("users").await;
    let count2_after = node2.count_records("users").await;
    
    println!("After sync node2->node1: node1={}, node2={}", count1_after, count2_after);
    
    assert_eq!(count1_after, 2);
    assert_eq!(count2_after, 2);
    
    let records1 = get_records_as_map(&node1, "users").await;
    let records2 = get_records_as_map(&node2, "users").await;
    
    assert_eq!(records1.len(), records2.len());
    for (id, _data) in &records1 {
        assert!(records2.contains_key(id));
    }
}

#[tokio::test]
async fn test_eventual_consistency_multiple_updates() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    let node2 = Node::new("node2", dir2.path().to_str().unwrap()).await;
    
    let id = node1.create_record("users", serde_json::json!({
        "name": "Alice",
        "age": 25
    })).await;
    
    sync_nodes(&node1, &node2).await;
    
    node1.update_record("users", &id, serde_json::json!({
        "name": "Alice Updated",
        "age": 26
    })).await;
    
    node2.update_record("users", &id, serde_json::json!({
        "name": "Alice Updated Again",
        "age": 27
    })).await;
    
    sync_nodes(&node1, &node2).await;
    sync_nodes(&node2, &node1).await;
    
    assert_eq!(node1.count_records("users").await, 1);
    assert_eq!(node2.count_records("users").await, 1);
    
    let records1 = get_records_as_map(&node1, "users").await;
    let records2 = get_records_as_map(&node2, "users").await;
    
    assert_eq!(records1.len(), records2.len());
}

#[tokio::test]
async fn test_eventual_consistency_complex_scenario() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    let node2 = Node::new("node2", dir2.path().to_str().unwrap()).await;
    let node3 = Node::new("node3", dir3.path().to_str().unwrap()).await;
    
    node1.create_record("users", serde_json::json!({"name": "Alice"})).await;
    node1.create_record("users", serde_json::json!({"name": "Bob"})).await;
    
    node2.create_record("users", serde_json::json!({"name": "Charlie"})).await;
    node2.create_record("posts", serde_json::json!({"title": "Hello"})).await;
    
    node3.create_record("users", serde_json::json!({"name": "David"})).await;
    node3.create_record("posts", serde_json::json!({"title": "World"})).await;
    
    sync_nodes(&node1, &node2).await;
    sync_nodes(&node2, &node1).await;
    
    sync_nodes(&node2, &node3).await;
    sync_nodes(&node3, &node2).await;
    
    sync_nodes(&node1, &node3).await;
    sync_nodes(&node3, &node1).await;
    
    let users1 = node1.count_records("users").await;
    let users2 = node2.count_records("users").await;
    let users3 = node3.count_records("users").await;
    
    assert_eq!(users1, users2);
    assert_eq!(users2, users3);
    assert_eq!(users1, 4);
    
    let posts1 = node1.count_records("posts").await;
    let posts2 = node2.count_records("posts").await;
    let posts3 = node3.count_records("posts").await;
    
    assert_eq!(posts1, posts2);
    assert_eq!(posts2, posts3);
    assert_eq!(posts1, 2);
}

#[tokio::test]
async fn test_eventual_consistency_data_identity() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    let node2 = Node::new("node2", dir2.path().to_str().unwrap()).await;
    
    for i in 0..10 {
        node1.create_record("users", serde_json::json!({
            "name": format!("User{}", i),
            "age": 20 + i
        })).await;
    }
    
    for i in 10..20 {
        node2.create_record("users", serde_json::json!({
            "name": format!("User{}", i),
            "age": 20 + i
        })).await;
    }
    
    sync_nodes(&node1, &node2).await;
    sync_nodes(&node2, &node1).await;
    
    let records1 = get_records_as_map(&node1, "users").await;
    let records2 = get_records_as_map(&node2, "users").await;
    
    assert_eq!(records1.len(), records2.len());
    assert_eq!(records1.len(), 20);
    
    for (id, data) in &records1 {
        assert_eq!(records2.get(id), Some(data));
    }
}

#[tokio::test]
async fn test_eventual_consistency_vector_clock_merge() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    let node2 = Node::new("node2", dir2.path().to_str().unwrap()).await;
    
    node1.create_record("users", serde_json::json!({"name": "Alice"})).await;
    node2.create_record("users", serde_json::json!({"name": "Bob"})).await;
    
    let clock1_before = node1.get_clock().await;
    let clock2_before = node2.get_clock().await;
    
    assert_eq!(clock1_before.get("node1"), 1);
    assert_eq!(clock1_before.get("node2"), 0);
    assert_eq!(clock2_before.get("node1"), 0);
    assert_eq!(clock2_before.get("node2"), 1);
    
    sync_nodes(&node1, &node2).await;
    sync_nodes(&node2, &node1).await;
    
    let clock1_after = node1.get_clock().await;
    let clock2_after = node2.get_clock().await;
    
    assert_eq!(clock1_after.get("node1"), 1);
    assert_eq!(clock1_after.get("node2"), 1);
    assert_eq!(clock2_after.get("node1"), 1);
    assert_eq!(clock2_after.get("node2"), 1);
}

#[tokio::test]
async fn test_eventual_consistency_changelog_integrity() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    let node2 = Node::new("node2", dir2.path().to_str().unwrap()).await;
    
    node1.create_record("users", serde_json::json!({"name": "Alice"})).await;
    node1.create_record("users", serde_json::json!({"name": "Bob"})).await;
    
    let initial_clock = node2.get_clock().await;
    let changes = node1.get_changes_since(&initial_clock).await;
    
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].collection, "users");
    assert_eq!(changes[1].collection, "users");
    
    sync_nodes(&node2, &node1).await;
    
    let final_clock = node2.get_clock().await;
    let remaining_changes = node1.get_changes_since(&final_clock).await;
    
    assert_eq!(remaining_changes.len(), 0);
}

#[tokio::test]
async fn test_eventual_consistency_repeated_sync() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    let node2 = Node::new("node2", dir2.path().to_str().unwrap()).await;
    
    for round in 0..3 {
        for i in 0..5 {
            node1.create_record("users", serde_json::json!({
                "name": format!("Round{}_User{}", round, i)
            })).await;
        }
        
        for i in 0..5 {
            node2.create_record("users", serde_json::json!({
                "name": format!("Round{}_User{}", round, i + 5)
            })).await;
        }
        
        sync_nodes(&node1, &node2).await;
        sync_nodes(&node2, &node1).await;
    }
    
    assert_eq!(node1.count_records("users").await, 30);
    assert_eq!(node2.count_records("users").await, 30);
    
    let records1 = get_records_as_map(&node1, "users").await;
    let records2 = get_records_as_map(&node2, "users").await;
    
    assert_eq!(records1.len(), records2.len());
    for id in records1.keys() {
        assert!(records2.contains_key(id));
    }
}

#[tokio::test]
async fn test_eventual_consistency_large_dataset() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    let node2 = Node::new("node2", dir2.path().to_str().unwrap()).await;
    
    for i in 0..100 {
        node1.create_record("users", serde_json::json!({
            "name": format!("User{}", i),
            "email": format!("user{}@example.com", i)
        })).await;
    }
    
    for i in 100..200 {
        node2.create_record("users", serde_json::json!({
            "name": format!("User{}", i),
            "email": format!("user{}@example.com", i)
        })).await;
    }
    
    sync_nodes(&node1, &node2).await;
    sync_nodes(&node2, &node1).await;
    
    assert_eq!(node1.count_records("users").await, 200);
    assert_eq!(node2.count_records("users").await, 200);
    
    let records1 = get_records_as_map(&node1, "users").await;
    let records2 = get_records_as_map(&node2, "users").await;
    
    assert_eq!(records1.len(), records2.len());
    for (id, data) in &records1 {
        assert_eq!(records2.get(id), Some(data));
    }
}

#[tokio::test]
async fn test_eventual_consistency_idempotent_sync() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    let node2 = Node::new("node2", dir2.path().to_str().unwrap()).await;
    
    node1.create_record("users", serde_json::json!({"name": "Alice"})).await;
    node2.create_record("users", serde_json::json!({"name": "Bob"})).await;
    
    sync_nodes(&node1, &node2).await;
    sync_nodes(&node2, &node1).await;
    
    let count1_before = node1.count_records("users").await;
    let count2_before = node2.count_records("users").await;
    
    sync_nodes(&node1, &node2).await;
    sync_nodes(&node2, &node1).await;
    sync_nodes(&node1, &node2).await;
    sync_nodes(&node2, &node1).await;
    
    let count1_after = node1.count_records("users").await;
    let count2_after = node2.count_records("users").await;
    
    assert_eq!(count1_before, count1_after);
    assert_eq!(count2_before, count2_after);
}
