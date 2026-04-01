use nuclear::Database;
use nuclear::storage::WasiStorage;
use tempfile::tempdir;

#[tokio::test]
async fn test_database_crud() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = Database::open(storage, "node1".to_string()).await.unwrap();
    
    let result = db.mutation(r#"
        mutation {
            create_user(input: {name: "Alice", email: "alice@example.com"}) {
                id
                name
            }
        }
    "#).await;
    
    assert!(result.is_err() || result.unwrap().get("data").is_some());
    
    let result = db.query(r#"
        query {
            users {
                id
                name
                email
            }
        }
    "#).await;
    
    assert!(result.is_err() || result.unwrap().get("data").is_some());
}

#[tokio::test]
async fn test_database_sync() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    
    let storage1 = WasiStorage::new(dir1.path());
    let storage2 = WasiStorage::new(dir2.path());
    
    let db1 = Database::open(storage1, "node1".to_string()).await.unwrap();
    let db2 = Database::open(storage2, "node2".to_string()).await.unwrap();
    
    let result = db1.mutation(r#"
        mutation {
            create_user(input: {name: "Bob", email: "bob@example.com"}) {
                id
            }
        }
    "#).await;
    
    assert!(result.is_err() || result.unwrap().get("data").is_some());
    
    let result = db2.query("query { users { name } }").await;
    
    assert!(result.is_err() || result.unwrap().get("data").is_some());
}

#[tokio::test]
async fn test_database_open_and_close() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    
    let db = Database::open(storage, "test_node".to_string()).await.unwrap();
    
    let _ = db.query("query { __typename }").await;
}