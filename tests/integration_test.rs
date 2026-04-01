use nuclear::Database;
use nuclear::api::DatabaseBuilder;
use nuclear::storage::WasiStorage;
use tempfile::tempdir;

#[tokio::test]
async fn test_database_crud() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    let result = db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Alice", email: "alice@example.com"}) {
                meta { id }
                name
            }
        }
    "#).await;
    
    assert!(result.is_ok());
    
    let result = db.query(r#"
        query {
            records(collection: "users") {
                meta { id }
                name
                email
            }
        }
    "#).await;
    
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_database_persistence() {
    let dir = tempdir().unwrap();
    let base_path = dir.path().to_str().unwrap().to_string();
    
    {
        let storage = WasiStorage::new(dir.path());
        let db = DatabaseBuilder::new(storage)
            .node_id("node1".to_string())
            .base_path(base_path.clone())
            .build()
            .await
            .unwrap();
        
        db.mutation(r#"
            mutation {
                createRecord(collection: "users", data: {name: "Alice", age: 25}) {
                    meta { id }
                }
            }
        "#).await.unwrap();
        
        db.mutation(r#"
            mutation {
                createRecord(collection: "users", data: {name: "Bob", age: 30}) {
                    meta { id }
                }
            }
        "#).await.unwrap();
    }
    
    {
        let storage = WasiStorage::new(dir.path());
        let db = DatabaseBuilder::new(storage)
            .node_id("node1".to_string())
            .base_path(base_path)
            .build()
            .await
            .unwrap();
        
        db.load_collection("users").await.unwrap();
        
        let result = db.query(r#"
            query {
                records(collection: "users") {
                    name
                    age
                }
            }
        "#).await.unwrap();
        
        let records = result.get("records").unwrap().as_array().unwrap();
        assert_eq!(records.len(), 2);
    }
}

#[tokio::test]
async fn test_database_open_and_close() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    
    let db = DatabaseBuilder::new(storage)
        .node_id("test_node".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    let _ = db.query("query { __typename }").await;
}
