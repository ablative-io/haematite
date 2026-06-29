# Haematite

Content-addressed, branchable storage on BLAKE3-hashed prolly trees. Every write is immutable, every fork is a single hash, and every merge walks only what changed.

## What it is

Haematite is an embeddable Rust storage engine where data is identified by cryptographic content hashes. You can branch your data like you branch code in Git — fork a database with a single hash, make changes on the branch, and merge back, walking only the nodes that actually differ. It runs single-node, active-active across a cluster with epoch-fenced ownership, and in the browser via WebAssembly.

## Status

**v0.3.0.** Core storage, branching (fork **and** merge), the write-ahead log, TTL, the event store, and active-active distributed sync with epoch fencing are implemented and tested. The WebAssembly target (IndexedDB + OPFS backends, browser WebSocket transport) is in place; further browser-I/O hardening is ongoing.

## Install

```toml
[dependencies]
haematite = "0.3.0"
```

## Quick start

```rust
use haematite::{Database, DatabaseConfig};

// Create (or `Database::open(path)` to reopen) a single-node database.
let db = Database::create(DatabaseConfig {
    data_dir: "my.db".into(),
    shard_count: 4,
    sweep_interval: None, // Some(ms) enables the TTL sweep
    distributed: None,    // Some(DistributedDatabaseConfig) for active-active sync
})?;

// Key/value writes are staged and sealed into a content-addressed root.
db.put(key, value)?;
let value = db.get(b"key")?;
let roots = db.commit()?; // ShardRoots: the BLAKE3 hash(es) identifying this state

// Branch from a committed root in O(1) — records a hash, copies no data.
let branch = haematite::fork(root_hash);
# Ok::<(), Box<dyn std::error::Error>>(())
```

For append-only event streams, wrap the database in an `EventStore` (`append`, `read`, `cas`, `scan`).

## Features

- **Content-addressed storage** — BLAKE3-hashed prolly trees. The same data always produces the same hash.
- **Branching** — `fork` a database in O(1); `merge` walks only the diff between roots.
- **Write-ahead log** — crash-safe durable writes with a configurable fsync policy.
- **Sharded concurrency** — configurable shard count for concurrent writes without lock contention.
- **Distributed sync** — active-active replication with CAS-aware quorum and epoch-fenced shard ownership (no split-brain).
- **Event store** — append-only event streams with sequence tracking and CAS.
- **TTL** — time-to-live with a background sweep.
- **Zstd compression** — transparent on-disk compression with an LRU node cache.
- **WebAssembly** — runs in the browser over IndexedDB / OPFS with a browser WebSocket sync transport.

## Architecture

```
crates/haematite/src/
├── api/         — public key/value + event-store interfaces
├── branch/      — fork, merge, commit log, snapshots, pruning
├── db/          — database open/create, config, distributed config
├── shard/       — shard actor + concurrent access
├── store/       — content-addressed node storage (disk, memory, IndexedDB)
├── tree/        — prolly tree + diff
├── sync/        — active-active distribution, quorum, epoch fencing
├── sync_codec/  — platform-neutral sync wire codec (native + wasm)
├── ttl/         — time-to-live expiry
├── wal/         — write-ahead log for crash safety
└── wasm/        — WebAssembly target + web-worker runtime
```

## Requirements

- Rust 1.85+
- No external dependencies — no database server, no runtime to install

## License

Apache-2.0
