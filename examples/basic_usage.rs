use nuclear::Database;
use nuclear::api::DatabaseBuilder;
use nuclear::storage::WasiStorage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = WasiStorage::new("./data");
    
    let db: Database<WasiStorage> = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .base_path("./data".to_string())
        .build()
        .await?;
    
    println!("=== Nuclear WASM CRDT Database Demo ===\n");
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Alice", age: 25, email: "alice@example.com"}) {
                meta { id }
                name
            }
        }
    "#).await?;
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Bob", age: 30, email: "bob@example.com"}) {
                meta { id }
                name
            }
        }
    "#).await?;
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Charlie", age: 28, email: "charlie@example.com"}) {
                meta { id }
                name
            }
        }
    "#).await?;
    
    println!("Created 3 users (Alice, Bob, Charlie)\n");
    
    let result = db.query(r#"
        query {
            records(collection: "users") {
                meta { id }
                name
                age
                email
            }
        }
    "#).await?;
    println!("All users:\n{:?}\n", result);
    
    println!("Data saved to ./data/users.json");
    println!("\nTo verify persistence, restart the program and query again.");
    println!("The data should still be available.\n");
    
    let result = db.query(r#"
        query {
            records(collection: "users", filter: {age: {gt: 26}}) {
                meta { id }
                name
                age
            }
        }
    "#).await?;
    println!("Users with age > 26:\n{:?}\n", result);
    
    let result = db.query(r#"
        query {
            records(collection: "users", filter: {name: {contains: "li"}}) {
                meta { id }
                name
            }
        }
    "#).await?;
    println!("Users with name containing 'li':\n{:?}\n", result);
    
    let result = db.query(r#"
        query {
            records(collection: "users", orderBy: {age: DESC}) {
                meta { id }
                name
                age
            }
        }
    "#).await?;
    println!("Users sorted by age (DESC):\n{:?}\n", result);
    
    let result = db.query(r#"
        query {
            records(collection: "users", orderBy: {name: ASC}) {
                meta { id }
                name
                age
            }
        }
    "#).await?;
    println!("Users sorted by name (ASC):\n{:?}\n", result);
    
    let result = db.query(r#"
        query {
            records(collection: "users", filter: {age: {gte: 26}}, orderBy: {age: ASC}, first: 2) {
                meta { id }
                name
                age
            }
        }
    "#).await?;
    println!("Users with age >= 26, sorted by age ASC, first 2:\n{:?}\n", result);
    
    let result = db.query(r#"
        query {
            recordsAggregate(collection: "users") {
                count
            }
        }
    "#).await?;
    println!("Aggregate result:\n{:?}\n", result);
    
    println!("\n=== Demo completed successfully! ===");
    
    Ok(())
}
