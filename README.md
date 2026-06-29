# Haematite

Content-addressed, branchable storage on BLAKE3-hashed prolly trees. Every write is immutable. Every fork is a single hash. Every merge walks only what changed.

## What it is

Haematite is an embeddable key-value storage engine where data is identified by cryptographic content hashes. You can branch your data like branching code in Git — fork a database with a single hash, make changes on the branch, and merge back. Built in Rust, designed to be embedded inside other programs.

## Status

**v0.1.0** — Early release. Core storage, branching, and WAL are implemented and tested. Merge, sync/distribution, and WASM target are in active development.

## Quick start

Add haematite to your `Cargo.toml`:

```toml
[dependencies]
haematite = "0.1.0"
```

```rust
use haematite::{Database, DatabaseConfig};

// Open a database
let config = DatabaseConfig::default();
let db = Database::open("my.db", config)?;

// Put and get
db.put(b"key", b"value")?;
let value = db.get(b"key")?;

// Fork — records a hash, copies no data
let branch = haematite::fork(&db, "experiment")?;
```

## Features

- **Content-addressed storage** — BLAKE3-hashed prolly trees. Same data always produces the same hash.
- **Branching** — Fork a database in O(1). Branch, experiment, merge back or discard.
- **Write-Ahead Log** — Crash-safe durable writes.
- **Sharded concurrency** — Configurable shard count for concurrent writes without lock contention.
- **TTL support** — Time-to-live with background sweep.
- **Zstd compression** — Transparent on-disk compression with LRU cache.
- **Event store** — Append-only event streams with sequence tracking.

## Architecture

```
haematite/
├── api/        — Public key-value and event store interfaces
├── branch/     — Fork, merge, commit log, snapshots, pruning
├── db/         — Database open, config, distributed config
├── shard/      — Shard-based concurrent access
├── store/      — Content-addressed node storage
├── tree/       — Prolly tree implementation
├── ttl/        — Time-to-live expiry
├── wal/        — Write-ahead log for crash safety
├── sync/       — Distributed sync (in development)
└── wasm/       — WebAssembly target (in development)
```

## Requirements

- Rust 1.85+
- No external dependencies — no database server, no runtime to install

## License

Apache-2.0
