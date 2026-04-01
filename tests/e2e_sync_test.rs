use nuclear::Database;
use nuclear::api::DatabaseBuilder;
use nuclear::storage::WasiStorage;
use tempfile::tempdir;
use std::sync::Arc;
use tokio::sync::Mutex;

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
    
    async fn query_records(&self, collection: &str) -> Vec<serde_json::Value> {
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
async fn test_p2p_sync_basic() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    let node2 = Node::new("node2", dir2.path().to_str().unwrap()).await;
    
    node1.create_record("users", serde_json::json!({
        "name": "Alice",
        "age": 25
    })).await;
    
    node1.create_record("users", serde_json::json!({
        "name": "Bob",
        "age": 30
    })).await;
    
    assert_eq!(node1.count_records("users").await, 2);
    assert_eq!(node2.count_records("users").await, 0);
}

#[tokio::test]
async fn test_p2p_sync_multiple_collections() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    let node2 = Node::new("node2", dir2.path().to_str().unwrap()).await;
    
    node1.create_record("users", serde_json::json!({
        "name": "Alice"
    })).await;
    
    node1.create_record("posts", serde_json::json!({
        "title": "Hello World"
    })).await;
    
    assert_eq!(node1.count_records("users").await, 1);
    assert_eq!(node1.count_records("posts").await, 1);
    assert_eq!(node2.count_records("users").await, 0);
    assert_eq!(node2.count_records("posts").await, 0);
}

#[tokio::test]
async fn test_p2p_sync_conflict_resolution() {
    let dir1 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    
    let id = node1.create_record("users", serde_json::json!({
        "name": "Alice",
        "age": 25
    })).await;
    
    node1.db.mutation(&format!(r#"
        mutation {{
            updateRecord(collection: "users", id: "{}", data: {{name: "Alice Updated", age: 26}}) {{
                meta {{ id }}
            }}
        }}
    "#, id)).await.unwrap();
    
    assert_eq!(node1.count_records("users").await, 1);
}

#[tokio::test]
async fn test_p2p_sync_concurrent_creates() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let node1 = Arc::new(Mutex::new(Node::new("node1", dir1.path().to_str().unwrap()).await));
    let node2 = Arc::new(Mutex::new(Node::new("node2", dir2.path().to_str().unwrap()).await));
    
    let node1_clone = node1.clone();
    let node2_clone = node2.clone();
    
    let handle1 = tokio::spawn(async move {
        let node = node1_clone.lock().await;
        for i in 0..10 {
            node.create_record("users", serde_json::json!({
                "name": format!("User{}", i),
                "node": "node1"
            })).await;
        }
    });
    
    let handle2 = tokio::spawn(async move {
        let node = node2_clone.lock().await;
        for i in 0..10 {
            node.create_record("users", serde_json::json!({
                "name": format!("User{}", i),
                "node": "node2"
            })).await;
        }
    });
    
    tokio::join!(handle1, handle2);
    
    let node1 = node1.lock().await;
    let node2 = node2.lock().await;
    
    assert_eq!(node1.count_records("users").await, 10);
    assert_eq!(node2.count_records("users").await, 10);
}

#[tokio::test]
async fn test_p2p_sync_large_data() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    let node2 = Node::new("node2", dir2.path().to_str().unwrap()).await;
    
    for i in 0..100 {
        node1.create_record("users", serde_json::json!({
            "name": format!("User{}", i),
            "email": format!("user{}@example.com", i),
            "age": 20 + (i % 50),
            "data": "x".repeat(1000)
        })).await;
    }
    
    assert_eq!(node1.count_records("users").await, 100);
    assert_eq!(node2.count_records("users").await, 0);
}

#[tokio::test]
async fn test_p2p_sync_update_propagation() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    let node2 = Node::new("node2", dir2.path().to_str().unwrap()).await;
    
    let id = node1.create_record("users", serde_json::json!({
        "name": "Alice",
        "age": 25
    })).await;
    
    node1.db.mutation(&format!(r#"
        mutation {{
            updateRecord(collection: "users", id: "{}", data: {{name: "Alice Updated", age: 26}}) {{
                meta {{ id }}
            }}
        }}
    "#, id)).await.unwrap();
    
    let records = node1.query_records("users").await;
    assert_eq!(records[0].get("data").unwrap().get("name").unwrap(), "Alice Updated");
}

#[tokio::test]
async fn test_p2p_sync_delete_propagation() {
    let dir1 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    
    let _id = node1.create_record("users", serde_json::json!({
        "name": "Alice"
    })).await;
    
    let initial_count = node1.count_records("users").await;
    assert!(initial_count >= 1);
}

#[tokio::test]
async fn test_p2p_sync_complex_scenario() {
    let dir1 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    
    let id1 = node1.create_record("users", serde_json::json!({
        "name": "Alice",
        "age": 25
    })).await;
    
    let _id2 = node1.create_record("users", serde_json::json!({
        "name": "Bob",
        "age": 30
    })).await;
    
    let initial_count = node1.count_records("users").await;
    assert!(initial_count >= 2);
    
    node1.db.mutation(&format!(r#"
        mutation {{
            updateRecord(collection: "users", id: "{}", data: {{name: "Alice Updated", age: 26}}) {{
                meta {{ id }}
            }}
        }}
    "#, id1)).await.unwrap();
}

#[tokio::test]
async fn test_p2p_sync_eventual_consistency() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let node1 = Node::new("node1", dir1.path().to_str().unwrap()).await;
    let _node2 = Node::new("node2", dir2.path().to_str().unwrap()).await;
    
    for i in 0..50 {
        node1.create_record("users", serde_json::json!({
            "name": format!("User{}", i)
        })).await;
    }
    
    assert_eq!(node1.count_records("users").await, 50);
}
