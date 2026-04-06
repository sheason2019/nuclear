# Nuclear Database — Production Readiness Roadmap

> Last updated: 2026-04-06

---

## Current Status

Nuclear 是一个 Rust 编写的嵌入式 CRDT NoSQL 数据库，支持 WASM 和 GraphQL。当前已实现核心功能：页式存储引擎、WAL 事务、BTree 索引、CRDT 同步协议、约束系统、GraphQL API。199 个单元测试全部通过。

以下按优先级列出距离生产就绪的剩余工作项。

---

## P0 — Blocking Issues (Must Fix)

| # | Issue | Impact | Status |
|---|-------|--------|--------|
| 1 | **WASM build is broken** — `JsDatabase::create` hardcodes `WasiStorage`, should use `OpfsStorage` in browser | WASM target completely non-functional | [ ] |
| 2 | **No CI/CD pipeline** — No GitHub Actions, no automated testing/build/release | No quality gate on commits | [ ] |
| 3 | **No real transaction isolation** — Each CRUD operation is an independent pseudo-transaction; no MVCC, no snapshot isolation, no multi-statement transaction API | Concurrent writes may cause data inconsistency | [ ] |
| 4 | **Sync module is non-functional** — No network transport layer, ChangeLog not persisted (lost on restart), no compaction/pruning | Multi-node sync completely broken | [ ] |
| 5 | **OPFS storage integer overflow** — `offset as i32` limits file size to 2GB | Browser-side data corruption on large DBs | [ ] |
| 6 | **ChangeLog unbounded growth** — Pure in-memory Vec with no cap, no trimming, no persistence | OOM on long-running instances | [ ] |

---

## P1 — High Priority

| # | Issue | Impact | Status |
|---|-------|--------|--------|
| 7 | **BTree index not used for query acceleration** — `query_index()` always returns None, all queries are full table scans | Indexes built but zero performance benefit | [ ] |
| 8 | **Error handling is a single string variant** — ~80+ error conditions all map to `StorageError::WasmError(String)` | No programmatic error handling possible | [ ] |
| 9 | **WAL has no integrity checks** — No per-entry checksum, bincode deserialization failure aborts recovery | Crash recovery unreliable | [ ] |
| 10 | **No logging framework** — Only 4x `eprintln!`, no structured logging, no log levels | Cannot debug issues in production | [ ] |
| 11 | **Unique constraint is in-memory only** — Tracking lost on restart, duplicates can be inserted after restart | Constraint breaks after persistence | [ ] |
| 12 | **Drop impl uses anti-pattern** — `spawn_blocking` creates new tokio Runtime inside existing async context, may panic | Data loss on shutdown | [ ] |
| 13 | **`compact()` silently swallows errors** — `write_record().ok()` can silently drop records | Data loss during compaction | [ ] |
| 14 | **`sync_file_header` resets all header fields** — Only writes `next_page_number`, other fields (total_pages, first_free_page, etc.) revert to defaults | Metadata loss on every header sync | [ ] |

---

## P2 — Medium Priority

| # | Issue | Impact | Status |
|---|-------|--------|--------|
| 15 | **No authentication/authorization** — Any connection can execute any operation | Fully open access after deployment | [ ] |
| 16 | **WASI storage `read` takes write lock** — Read operations serialized with writes, eliminating read concurrency | Severe read performance degradation | [ ] |
| 17 | **`SharedBufferPool::get_page` clones entire Page** — Every read/write copies 4KB | BufferPool cache ineffective | [ ] |
| 18 | **Record has hardcoded field accessors** — `name`/`email`/`age` defined directly on Record type | Misleading for a schemaless database | [ ] |
| 19 | **No cursor-based pagination** — Only offset-based, inefficient for large datasets | Large collection pagination impractical | [ ] |
| 20 | **Vector Clock has no GC** — Node IDs only accumulate, ephemeral nodes never pruned | Memory grows indefinitely | [ ] |
| 21 | **LWW Map tombstone has no GC** — Deleted keys permanently retained in HashMap | Memory grows indefinitely | [ ] |
| 22 | **No database migration mechanism** — `FileHeader.version` exists but no migration code | Version upgrades break compatibility | [ ] |
| 23 | **`tokio = "full"` bloats WASM binary** — Many features (process, signal, io-std) unnecessary for WASM | Unnecessary binary size increase | [ ] |
| 24 | **Regex constraint is fake** — Only checks for `@` and `.` in string, not real regex | Pattern constraint effectively bypassed | [ ] |
| 25 | **No TypeScript type definitions** — `package.json` references `./dist/types/index.d.ts` but no generator exists | JS/TS developers cannot use safely | [ ] |

---

## P3 — Low Priority / Nice-to-Have

| # | Issue | Impact | Status |
|---|-------|--------|--------|
| 26 | No backup/restore functionality | No disaster recovery | [ ] |
| 27 | No monitoring metrics (query latency, operation counts, error rates) | No runtime observability | [ ] |
| 28 | No persisted queries / query whitelisting | Query injection risk on public endpoints | [ ] |
| 29 | GraphQL introspection not controlled | Information leakage | [ ] |
| 30 | `tempfile` listed as both regular and dev dependency | Dependency hygiene | [ ] |
| 31 | `anyhow` imported but never used | Dependency bloat | [ ] |
| 32 | All modules are `pub`, no internal encapsulation | API surface too large | [ ] |
| 33 | No `wasm-bindgen-test` | No test coverage for WASM path | [ ] |
| 34 | LWW uses wall clock instead of HLC (Hybrid Logical Clock) | Clock skew causes data inconsistency | [ ] |
| 35 | No overflow page support (records > ~4KB cannot be stored) | `MAX_RECORD_SIZE` 4MB conflicts with actual ~4KB page limit | [ ] |

---

## Recommended Next Steps

Top 5 highest-impact improvements to tackle next:

1. **Fix WASM build** (P0 #1) — Wire `OpfsStorage` into `JsDatabase::create` with `#[cfg(target_arch = "wasm32")]`
2. **Connect BTree index to query path** (P1 #7) — Implement index-aware filtering in `records()` query
3. **Structured error types** (P1 #8) — Add dedicated `StorageError` variants: `Serialization`, `Validation`, `Transaction`, `Corruption`, `Capacity`, `Timeout`, `Conflict`, `AlreadyExists`
4. **Add `tracing` logging framework** (P1 #10) — Replace all `eprintln!` with structured spans
5. **Add CI/CD** (P0 #2) — GitHub Actions with `cargo test`, `cargo clippy`, `wasm-pack build`
