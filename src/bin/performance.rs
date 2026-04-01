use nuclear::Database;
use nuclear::api::DatabaseBuilder;
use nuclear::storage::WasiStorage;
use tempfile::tempdir;
use std::time::{Duration, Instant};

struct BenchResult {
    operation: String,
    count: usize,
    duration: Duration,
    ops_per_sec: f64,
}

impl BenchResult {
    fn new(operation: &str, count: usize, duration: Duration) -> Self {
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        Self {
            operation: operation.to_string(),
            count,
            duration,
            ops_per_sec,
        }
    }
    
    fn print(&self) {
        println!(
            "{:<30} | {:>12} ops | {:>10.2?} | {:>15.2} ops/s",
            self.operation,
            self.count,
            self.duration,
            self.ops_per_sec
        );
    }
}

async fn bench_insert(db: &Database<WasiStorage>, count: usize) -> BenchResult {
    let start = Instant::now();
    
    for i in 0..count {
        db.mutation(&format!(r#"
            mutation {{
                createRecord(collection: "bench", data: {{id: {}, name: "User{}", age: {}, email: "user{}@example.com"}}) {{
                    meta {{ id }}
                }}
            }}
        "#, i, i, 20 + (i % 50), i)).await.unwrap();
    }
    
    let duration = start.elapsed();
    BenchResult::new("Insert", count, duration)
}

async fn bench_query_all(db: &Database<WasiStorage>, count: usize) -> BenchResult {
    let start = Instant::now();
    
    let result = db.query(r#"
        query {
            records(collection: "bench") {
                meta { id }
                data
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    let duration = start.elapsed();
    BenchResult::new("Query All", records.len(), duration)
}

async fn bench_query_filter(db: &Database<WasiStorage>, count: usize) -> BenchResult {
    let start = Instant::now();
    
    let result = db.query(r#"
        query {
            records(collection: "bench", filter: {age: {gte: 40, lte: 45}}) {
                meta { id }
                data
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    let duration = start.elapsed();
    BenchResult::new("Query Filter", records.len(), duration)
}

async fn bench_query_sort(db: &Database<WasiStorage>, count: usize) -> BenchResult {
    let start = Instant::now();
    
    let result = db.query(r#"
        query {
            records(collection: "bench", orderBy: {age: ASC}) {
                meta { id }
                data
            }
        }
    "#).await.unwrap();
    
    let records = result.get("records").unwrap().as_array().unwrap();
    let duration = start.elapsed();
    BenchResult::new("Query Sort", records.len(), duration)
}

async fn bench_query_pagination(db: &Database<WasiStorage>, count: usize) -> BenchResult {
    let start = Instant::now();
    
    let page_size = 100;
    let pages = count / page_size;
    
    for page in 0..pages {
        let offset = page * page_size;
        db.query(&format!(r#"
            query {{
                records(collection: "bench", orderBy: {{age: ASC}}, first: {}, offset: {}) {{
                    meta {{ id }}
                    data
                }}
            }}
        "#, page_size, offset)).await.unwrap();
    }
    
    let duration = start.elapsed();
    BenchResult::new("Query Pagination", pages, duration)
}

async fn bench_update(db: &Database<WasiStorage>, count: usize) -> BenchResult {
    let start = Instant::now();
    
    for i in 0..count {
        db.mutation(&format!(r#"
            mutation {{
                updateRecord(collection: "bench", id: "{}", data: {{name: "Updated{}", age: {}}}) {{
                    meta {{ id }}
                }}
            }}
        "#, i, i, 25 + (i % 50))).await.unwrap();
    }
    
    let duration = start.elapsed();
    BenchResult::new("Update", count, duration)
}

async fn bench_delete(db: &Database<WasiStorage>, count: usize) -> BenchResult {
    let start = Instant::now();
    
    for i in 0..count {
        db.mutation(&format!(r#"
            mutation {{
                deleteRecord(collection: "bench", id: "{}")
            }}
        "#, i)).await.unwrap();
    }
    
    let duration = start.elapsed();
    BenchResult::new("Delete", count, duration)
}

fn print_header() {
    println!("\n{:=<80}", "");
    println!("{:<30} | {:>12} | {:>10} | {:>15}", "Operation", "Count", "Duration", "Throughput");
    println!("{:=<80}", "");
}

fn print_footer() {
    println!("{:=<80}\n", "");
}

async fn run_benchmark(count: usize) {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("bench_node".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    println!("\nBenchmark: {} records", count);
    print_header();
    
    let result = bench_insert(&db, count).await;
    result.print();
    
    let result = bench_query_all(&db, count).await;
    result.print();
    
    let result = bench_query_filter(&db, count).await;
    result.print();
    
    let result = bench_query_sort(&db, count).await;
    result.print();
    
    let result = bench_query_pagination(&db, count).await;
    result.print();
    
    let result = bench_update(&db, count).await;
    result.print();
    
    let result = bench_delete(&db, count).await;
    result.print();
    
    print_footer();
}

#[tokio::test]
async fn bench_10k() {
    run_benchmark(10_000).await;
}

#[tokio::test]
async fn bench_100k() {
    run_benchmark(100_000).await;
}

#[tokio::test]
#[ignore]
async fn bench_1m() {
    run_benchmark(1_000_000).await;
}

#[tokio::test]
#[ignore]
async fn bench_10m() {
    run_benchmark(10_000_000).await;
}

#[tokio::test]
#[ignore]
async fn bench_100m() {
    run_benchmark(100_000_000).await;
}

#[tokio::test]
#[ignore]
async fn bench_1b() {
    run_benchmark(1_000_000_000).await;
}

#[tokio::test]
async fn bench_memory_usage() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("memory_node".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    let counts = [1000, 10000, 100000];
    
    println!("\nMemory Usage Benchmark");
    println!("{:=<60}", "");
    println!("{:>12} | {:>15} | {:>15}", "Records", "Insert Time", "Query Time");
    println!("{:=<60}", "");
    
    for &count in &counts {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        let db = DatabaseBuilder::new(storage)
            .node_id("memory_node".to_string())
            .base_path(dir.path().to_str().unwrap().to_string())
            .build()
            .await
            .unwrap();
        
        let start = Instant::now();
        for i in 0..count {
            db.mutation(&format!(r#"
                mutation {{
                    createRecord(collection: "bench", data: {{id: {}, name: "User{}", data: "{}"}}) {{
                        meta {{ id }}
                    }}
                }}
            "#, i, i, "x".repeat(100))).await.unwrap();
        }
        let insert_time = start.elapsed();
        
        let start = Instant::now();
        db.query(r#"
            query {
                records(collection: "bench") {
                    meta { id }
                    data
                }
            }
        "#).await.unwrap();
        let query_time = start.elapsed();
        
        println!(
            "{:>12} | {:>15.2?} | {:>15.2?}",
            count,
            insert_time,
            query_time
        );
    }
    
    println!("{:=<60}\n", "");
}

#[tokio::test]
async fn bench_concurrent_operations() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = std::sync::Arc::new(DatabaseBuilder::new(storage)
        .node_id("concurrent_node".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap());
    
    let concurrent_tasks = [1, 2, 4, 8, 16];
    let ops_per_task = 1000;
    
    println!("\nConcurrent Operations Benchmark");
    println!("{:=<60}", "");
    println!("{:>12} | {:>15} | {:>15}", "Tasks", "Total Ops", "Duration");
    println!("{:=<60}", "");
    
    for &tasks in &concurrent_tasks {
        let dir = tempdir().unwrap();
        let storage = WasiStorage::new(dir.path());
        let db = std::sync::Arc::new(DatabaseBuilder::new(storage)
            .node_id("concurrent_node".to_string())
            .base_path(dir.path().to_str().unwrap().to_string())
            .build()
            .await
            .unwrap());
        
        let start = Instant::now();
        
        let mut handles = vec![];
        for task_id in 0..tasks {
            let db_clone = db.clone();
            let handle = tokio::spawn(async move {
                for i in 0..ops_per_task {
                    let id = task_id * ops_per_task + i;
                    db_clone.mutation(&format!(r#"
                        mutation {{
                            createRecord(collection: "bench", data: {{id: {}, name: "Task{}_{}"}}) {{
                                meta {{ id }}
                            }}
                        }}
                    "#, id, task_id, i)).await.unwrap();
                }
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.await.unwrap();
        }
        
        let duration = start.elapsed();
        let total_ops = tasks * ops_per_task;
        
        println!(
            "{:>12} | {:>15} | {:>15.2?}",
            tasks,
            total_ops,
            duration
        );
    }
    
    println!("{:=<60}\n", "");
}

#[tokio::test]
async fn bench_filter_performance() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("filter_node".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    let count = 10000;
    
    for i in 0..count {
        db.mutation(&format!(r#"
            mutation {{
                createRecord(collection: "bench", data: {{id: {}, name: "User{}", age: {}, city: "{}"}}) {{
                    meta {{ id }}
                }}
            }}
        "#, i, i, 20 + (i % 60), if i % 2 == 0 { "Beijing" } else { "Shanghai" })).await.unwrap();
    }
    
    println!("\nFilter Performance Benchmark ({} records)", count);
    println!("{:=<60}", "");
    println!("{:<30} | {:>15}", "Filter", "Duration");
    println!("{:=<60}", "");
    
    let filters = vec![
        ("No Filter", "query { records(collection: \"bench\") { data } }"),
        ("Age > 40", "query { records(collection: \"bench\", filter: {age: {gt: 40}}) { data } }"),
        ("Age >= 40, Age <= 50", "query { records(collection: \"bench\", filter: {age: {gte: 40, lte: 50}}) { data } }"),
        ("City = Beijing", "query { records(collection: \"bench\", filter: {city: {eq: \"Beijing\"}}) { data } }"),
        ("Name contains User1", "query { records(collection: \"bench\", filter: {name: {contains: \"User1\"}}) { data } }"),
    ];
    
    for (name, query) in filters {
        let start = Instant::now();
        db.query(query).await.unwrap();
        let duration = start.elapsed();
        
        println!("{:<30} | {:>15.2?}", name, duration);
    }
    
    println!("{:=<60}\n", "");
}

#[tokio::test]
async fn bench_sort_performance() {
    let dir = tempdir().unwrap();
    let storage = WasiStorage::new(dir.path());
    let db = DatabaseBuilder::new(storage)
        .node_id("sort_node".to_string())
        .base_path(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .unwrap();
    
    let count = 10000;
    
    for i in 0..count {
        db.mutation(&format!(r#"
            mutation {{
                createRecord(collection: "bench", data: {{id: {}, name: "User{}", age: {}}}) {{
                    meta {{ id }}
                }}
            }}
        "#, i, i, 20 + (i % 60))).await.unwrap();
    }
    
    println!("\nSort Performance Benchmark ({} records)", count);
    println!("{:=<60}", "");
    println!("{:<30} | {:>15}", "Sort", "Duration");
    println!("{:=<60}", "");
    
    let sorts = vec![
        ("No Sort", "query { records(collection: \"bench\") { data } }"),
        ("Age ASC", "query { records(collection: \"bench\", orderBy: {age: ASC}) { data } }"),
        ("Age DESC", "query { records(collection: \"bench\", orderBy: {age: DESC}) { data } }"),
        ("Name ASC", "query { records(collection: \"bench\", orderBy: {name: ASC}) { data } }"),
    ];
    
    for (name, query) in sorts {
        let start = Instant::now();
        db.query(query).await.unwrap();
        let duration = start.elapsed();
        
        println!("{:<30} | {:>15.2?}", name, duration);
    }
    
    println!("{:=<60}\n", "");
}

#[tokio::main]
async fn main() {
    println!("Running benchmarks...");
    
    run_benchmark(1000).await;
    run_benchmark(5000).await;
    
    println!("Benchmarks completed.");
}
