use nuclear::Database;
use nuclear::api::DatabaseBuilder;
use nuclear::storage::WasiStorage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = WasiStorage::new("./data");
    
    let db: Database<WasiStorage> = DatabaseBuilder::new(storage)
        .node_id("node1".to_string())
        .build()
        .await?;
    
    println!("Database initialized with node_id: node1");
    
    let create_result = db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Alice", email: "alice@example.com"}) {
                _meta { id }
                name
            }
        }
    "#).await?;
    
    println!("Created user: {:?}", create_result);
    
    let query_result = db.query(r#"
        query {
            records(collection: "users") {
                _meta { id }
                name
                email
            }
        }
    "#).await?;
    
    println!("Query result: {:?}", query_result);
    
    db.mutation(r#"
        mutation {
            createRecord(collection: "users", data: {name: "Bob", email: "bob@example.com"}) {
                _meta { id }
                name
            }
        }
    "#).await?;
    
    let aggregate_result = db.query(r#"
        query {
            recordsAggregate(collection: "users") {
                count
            }
        }
    "#).await?;
    
    println!("Aggregate result: {:?}", aggregate_result);
    
    println!("\nExample completed successfully!");
    
    Ok(())
}
