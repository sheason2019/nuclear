use nuclear::Database;
use nuclear::api::DatabaseBuilder;
use nuclear::storage::WasiStorage;
use tempfile::tempdir;

#[tokio::test]
async fn test_database_open() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await;
    
    assert!(db.is_ok());
}

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
async fn test_database_create_and_query() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
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
    
    let result = db.query(r#"
        query {
            records(collection: "users") {
                name
                age
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].get("name").unwrap(), "Alice");
    assert_eq!(records[0].get("age").unwrap(), 25);
}

#[tokio::test]
async fn test_database_update() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    let create_result = db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Alice", age: 25}) {
                meta { id }
            }
        }
    "#).await.unwrap();
    
    let id = create_result.get("createRecord").unwrap()
        .get("meta").unwrap()
        .get("id").unwrap()
        .as_str().unwrap();
    
    let update_result = db.mutation(&format!(r#"
        mutation {{
            updateRecord(collection: "users", id: "{}", data: {{name: "Alice Updated", age: 26}}) {{
                meta {{ id }}
                name
                age
            }}
        }}
    "#, id)).await.unwrap();
    
    assert_eq!(update_result.get("updateRecord").unwrap()
        .get("name").unwrap(), "Alice Updated");
    
    let query_result = db.query(r#"
        query {
            records(collection: "users") {
                name
                age
            }
        }
    "#).await.unwrap();
    
    let records = query_result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records[0].get("name").unwrap(), "Alice Updated");
    assert_eq!(records[0].get("age").unwrap(), 26);
}

#[tokio::test]
async fn test_database_delete() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    let create_result = db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Alice"}) {
                meta { id }
            }
        }
    "#).await.unwrap();
    
    let id = create_result.get("createRecord").unwrap()
        .get("meta").unwrap()
        .get("id").unwrap()
        .as_str().unwrap();
    
    let delete_result = db.mutation(&format!(r#"
        mutation {{
            deleteRecord(collection: "users", id: "{}")
        }}
    "#, id)).await.unwrap();
    
    assert_eq!(delete_result.get("deleteRecord").unwrap(), true);
    
    let query_result = db.query(r#"
        query {
            records(collection: "users") {
                name
            }
        }
    "#).await.unwrap();
    
    let records = query_result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records.len(), 0);
}

#[tokio::test]
async fn test_database_filter_eq() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Alice", age: 25}) { meta { id } }
        }
    "#).await.unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Bob", age: 30}) { meta { id } }
        }
    "#).await.unwrap();
    
    let result = db.query(r#"
        query {
            records(collection: "users", filter: {name: {eq: "Alice"}}) {
                name
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].get("name").unwrap(), "Alice");
}

#[tokio::test]
async fn test_database_filter_gt() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Alice", age: 25}) { meta { id } }
        }
    "#).await.unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Bob", age: 30}) { meta { id } }
        }
    "#).await.unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Charlie", age: 28}) { meta { id } }
        }
    "#).await.unwrap();
    
    let result = db.query(r#"
        query {
            records(collection: "users", filter: {age: {gt: 26}}) {
                name
                age
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records.len(), 2);
}

#[tokio::test]
async fn test_database_filter_contains() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Alice"}) { meta { id } }
        }
    "#).await.unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Bob"}) { meta { id } }
        }
    "#).await.unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Charlie"}) { meta { id } }
        }
    "#).await.unwrap();
    
    let result = db.query(r#"
        query {
            records(collection: "users", filter: {name: {contains: "li"}}) {
                name
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records.len(), 2);
}

#[tokio::test]
async fn test_database_sort_asc() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Charlie", age: 28}) { meta { id } }
        }
    "#).await.unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Alice", age: 25}) { meta { id } }
        }
    "#).await.unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Bob", age: 30}) { meta { id } }
        }
    "#).await.unwrap();
    
    let result = db.query(r#"
        query {
            records(collection: "users", orderBy: {name: ASC}) {
                name
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records[0].get("name").unwrap(), "Alice");
    assert_eq!(records[1].get("name").unwrap(), "Bob");
    assert_eq!(records[2].get("name").unwrap(), "Charlie");
}

#[tokio::test]
async fn test_database_sort_desc() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Alice", age: 25}) { meta { id } }
        }
    "#).await.unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Bob", age: 30}) { meta { id } }
        }
    "#).await.unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Charlie", age: 28}) { meta { id } }
        }
    "#).await.unwrap();
    
    let result = db.query(r#"
        query {
            records(collection: "users", orderBy: {age: DESC}) {
                name
                age
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records[0].get("age").unwrap(), 30);
    assert_eq!(records[1].get("age").unwrap(), 28);
    assert_eq!(records[2].get("age").unwrap(), 25);
}

#[tokio::test]
async fn test_database_pagination() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    for i in 0..10 {
        db.mutation(&format!(r#"
            mutation {{
                createRecord(collection: "users", data: {{name: "User{}", age: {}}}) {{ meta {{ id }} }}
            }}
        "#, i, 20 + i)).await.unwrap();
    }
    
    let result = db.query(r#"
        query {
            records(collection: "users", orderBy: {age: ASC}, first: 3) {
                name
                age
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].get("age").unwrap(), 20);
    
    let result = db.query(r#"
        query {
            records(collection: "users", orderBy: {age: ASC}, offset: 5, first: 3) {
                name
                age
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].get("age").unwrap(), 25);
}

#[tokio::test]
async fn test_database_aggregate() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Alice"}) { meta { id } }
        }
    "#).await.unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Bob"}) { meta { id } }
        }
    "#).await.unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Charlie"}) { meta { id } }
        }
    "#).await.unwrap();
    
    let result = db.query(r#"
        query {
            recordsAggregate(collection: "users") {
                count
            }
        }
    "#).await.unwrap();
    
    assert_eq!(result.get("recordsAggregate").unwrap()
        .get("count").unwrap(), 3);
}

#[tokio::test]
async fn test_database_multiple_collections() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Alice"}) { meta { id } }
        }
    "#).await.unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "posts", data: {title: "Hello World"}) { meta { id } }
        }
    "#).await.unwrap();
    
    let users = db.query(r#"
        query {
            records(collection: "users") { data }
        }
    "#).await.unwrap();
    
    let posts = db.query(r#"
        query {
            records(collection: "posts") { data }
        }
    "#).await.unwrap();
    
    assert_eq!(users.get("records").unwrap().as_array().unwrap().len(), 1);
    assert_eq!(posts.get("records").unwrap().as_array().unwrap().len(), 1);
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
async fn test_database_persistence_update() {
    let dir = tempdir().unwrap();
    let base_path = dir.path().to_str().unwrap().to_string();
    
    let id;
    
    {
        let storage = WasiStorage::new(dir.path());
        let db = DatabaseBuilder::new(storage)
            .node_id("node1".to_string())
            .base_path(base_path.clone())
            .build()
            .await
            .unwrap();
        
        let result = db.mutation(r#"
            mutation {
                createRecord(collection: "users", data: {name: "Alice", age: 25}) {
                    meta { id }
                }
            }
        "#).await.unwrap();
        
        id = result.get("createRecord").unwrap()
            .get("meta").unwrap()
            .get("id").unwrap()
            .as_str().unwrap().to_string();
    }
    
    {
        let storage = WasiStorage::new(dir.path());
        let db = DatabaseBuilder::new(storage)
            .node_id("node1".to_string())
            .base_path(base_path.clone())
            .build()
            .await
            .unwrap();
        
        db.load_collection("users").await.unwrap();
        
        db.mutation(&format!(r#"
            mutation {{
                updateRecord(collection: "users", id: "{}", data: {{name: "Alice Updated", age: 26}}) {{
                    meta {{ id }}
                }}
            }}
        "#, id)).await.unwrap();
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
        assert_eq!(records[0].get("name").unwrap(), "Alice Updated");
        assert_eq!(records[0].get("age").unwrap(), 26);
    }
}

#[tokio::test]
async fn test_database_persistence_delete() {
    let dir = tempdir().unwrap();
    let base_path = dir.path().to_str().unwrap().to_string();
    
    let id;
    
    {
        let storage = WasiStorage::new(dir.path());
        let db = DatabaseBuilder::new(storage)
            .node_id("node1".to_string())
            .base_path(base_path.clone())
            .build()
            .await
            .unwrap();
        
        let result = db.mutation(r#"
            mutation {
                createRecord(collection: "users", data: {name: "Alice"}) {
                    meta { id }
                }
            }
        "#).await.unwrap();
        
        id = result.get("createRecord").unwrap()
            .get("meta").unwrap()
            .get("id").unwrap()
            .as_str().unwrap().to_string();
    }
    
    {
        let storage = WasiStorage::new(dir.path());
        let db = DatabaseBuilder::new(storage)
            .node_id("node1".to_string())
            .base_path(base_path.clone())
            .build()
            .await
            .unwrap();
        
        db.load_collection("users").await.unwrap();
        
        db.mutation(&format!(r#"
            mutation {{
                deleteRecord(collection: "users", id: "{}")
            }}
        "#, id)).await.unwrap();
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
                }
            }
        "#).await.unwrap();
        
        let records = result.get("records").unwrap().as_array().unwrap();
        assert_eq!(records.len(), 0);
    }
}

#[tokio::test]
async fn test_database_combined_filter_sort_pagination() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Alice", age: 25}) { meta { id } }
        }
    "#).await.unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Bob", age: 30}) { meta { id } }
        }
    "#).await.unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Charlie", age: 28}) { meta { id } }
        }
    "#).await.unwrap();
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "David", age: 35}) { meta { id } }
        }
    "#).await.unwrap();
    
    let result = db.query(r#"
        query {
            records(
                collection: "users", 
                filter: {age: {gte: 26}}, 
                orderBy: {age: ASC}, 
                first: 2
            ) {
                name
                age
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].get("name").unwrap(), "Charlie");
    assert_eq!(records[1].get("name").unwrap(), "Bob");
}

#[tokio::test]
async fn test_database_empty_collection() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    let result = db.query(r#"
        query {
            records(collection: "users") {
                name
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records.len(), 0);
}

#[tokio::test]
async fn test_database_nonexistent_collection() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    let result = db.query(r#"
        query {
            records(collection: "nonexistent") {
                name
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    assert_eq!(records.len(), 0);
}

#[tokio::test]
async fn test_database_update_nonexistent() {
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
            updateRecord(collection: "users", id: "nonexistent", data: {name: "Alice"}) {
                meta { id }
            }
        }
    "#).await.unwrap();
    
    assert!(result.get("updateRecord").unwrap().is_null());
}

#[tokio::test]
async fn test_database_delete_nonexistent() {
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
            deleteRecord(collection: "users", id: "nonexistent")
        }
    "#).await.unwrap();
    
    assert_eq!(result.get("deleteRecord").unwrap(), false);
}
