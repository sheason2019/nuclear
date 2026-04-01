# Nuclear - WASM CRDT Database

A high-performance, WASM-based embedded NoSQL database with GraphQL query language support and CRDT-based synchronization for distributed systems.

## Features

- **WASM Compatible**: Runs natively in browsers and WASI-compatible runtimes
- **GraphQL API**: Full support for queries, mutations, and subscriptions
- **CRDT Synchronization**: Conflict-free replicated data types with Last-Writer-Wins (LWW) strategy
- **Real-time Sync**: WebSocket-based real-time data synchronization
- **Offline-First**: Works offline with automatic conflict resolution when syncing
- **Flat Data Design**: Optimized for flattened data with efficient relation queries
- **Zero Configuration**: Simple builder pattern for quick setup

## Quick Start

### Prerequisites

- Rust 1.70+
- wasm-pack (for WASM targets)

### Add to Your Project

```toml
[dependencies]
nuclear = "0.1.0"
```

### Basic Usage

```rust
use nuclear::Database;
use nuclear::storage::WasiStorage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = WasiStorage::new("./data");
    let db = Database::builder(storage)
        .node_id("node1".to_string())
        .build()
        .await?;
    
    // Create data with GraphQL mutation
    db.mutation(r#"
        mutation {
            create_user(input: {name: "Alice", email: "alice@example.com"}) {
                id
                name
            }
        }
    "#).await?;
    
    // Query data with GraphQL
    let result = db.query("query { users { id name email } }").await?;
    println!("{:?}", result);
    
    // Subscribe to real-time updates
    let mut stream = db.subscribe("subscription { user_updated { id name } }").await?;
    
    Ok(())
}
```

## Architecture

### Core Layers

1. **CRDT Engine** (`src/core/`)
   - LWW Register: Last-Writer-Wins value containers
   - LWW Map: Key-value store with CRDT semantics
   - Vector Clock: Logical time tracking for causality

2. **Storage Layer** (`src/storage/`)
   - Trait-based abstraction for storage backends
   - WasiStorage: WASI file system implementation
   - Pluggable design for browser/localStorage

3. **GraphQL Layer** (`src/graphql/`)
   - Schema definition with queries, mutations, subscriptions
   - Scalars for custom types
   - Integration with CRDT data model

4. **Sync Engine** (`src/sync/`)
   - SyncMessage: Wire format for synchronization
   - ConflictResolver: LWW-based conflict resolution

5. **API Layer** (`src/api/`)
   - Database: Main interface for all operations
   - DatabaseBuilder: Fluent builder for configuration

### Data Flow

```
User Query → GraphQL Parser → CRDT Engine → Storage → Response
                    ↓
              Sync Engine → Network → Other Nodes
```

## Building

### Native Build

```bash
cargo build --release
```

### WASM Build

```bash
wasm-pack build --target web
```

### Run Examples

```bash
cargo run --example basic_usage
```

## Testing

```bash
cargo test
cargo test --test integration_test
```

## Project Structure

```
nuclear/
├── src/
│   ├── api/           # Public API (Database, Builder)
│   ├── core/          # CRDT implementations
│   ├── graphql/      # GraphQL schema and execution
│   ├── storage/      # Storage abstractions
│   └── sync/         # Synchronization protocol
├── examples/          # Usage examples
├── tests/            # Integration tests
└── Cargo.toml        # Project manifest
```

## License

MIT License - see LICENSE file for details.