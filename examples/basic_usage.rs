use nuclear::Database;
use nuclear::storage::WasiStorage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = WasiStorage::new("./data");
    
    let db = Database::builder(storage)
        .node_id("node1".to_string())
        .build()
        .await?;
    
    println!("Database initialized with node_id: node1");
    
    let create_result = db.mutation(r#"
        mutation {
            create_user(input: {name: "Alice", email: "alice@example.com"}) {
                id
                name
            }
        }
    "#).await?;
    
    println!("Created user: {:?}", create_result);
    
    let query_result = db.query(r#"
        query {
            users {
                id
                name
                email
            }
        }
    "#).await?;
    
    println!("Query result: {:?}", query_result);
    
    db.mutation(r#"
        mutation {
            create_user(input: {name: "Bob", email: "bob@example.com"}) {
                id
                name
            }
        }
    "#).await?;
    
    let filtered_result = db.query(r#"
        query {
            users(where: { name: { eq: "Alice" } }) {
                id
                name
                email
            }
        }
    "#).await?;
    
    println!("Filtered result: {:?}", filtered_result);
    
    println!("\nSubscribing to user updates...");
    let _stream = db.subscribe(r#"
        subscription {
            user_updated {
                id
                name
            }
        }
    "#).await?;
    
    println!("Subscription active - waiting for changes...");
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let _ = db.mutation(r#"
        mutation {
            update_user(id: "1", input: {name: "Alice Updated"}) {
                id
                name
            }
        }
    "#).await;
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    println!("\nExample completed successfully!");
    
    Ok(())
}