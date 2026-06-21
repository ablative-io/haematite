# Haematite

An embeddable, branchable, distributable key-value storage engine built on the beamr BEAM virtual machine.

## What it is

Haematite is a content-addressed prolly tree storage engine. Every write appends to an immutable graph of nodes hashed with BLAKE3. There is no in-place mutation anywhere in the system. This single property is what makes everything else work.

**Fork is free.** Copying a database state is recording a 32-byte hash. No data is duplicated, no locks are acquired, no coordination protocol runs.

**Merge is cheap.** Two trees that share structure share hashes. Walking a merge only visits the nodes that actually differ. A million shared keys cost nothing; three changed keys cost three node paths.

> ⚠️ **Status (2026-06):** not yet implemented — describes intended design. The merge module is a stub.

**Sync is negotiation over hashes.** Two nodes compare root hashes. If they match, the databases are identical. If they don't, they exchange only the subtree hashes that differ. Transfer cost is proportional to the difference, not the dataset.

> ⚠️ **Status (2026-06):** not yet implemented — describes intended design. The sync/distribution module is a stub.

**Compression is invisible.** Nodes are zstd-compressed on disk and decompressed into an LRU cache on read. Applications see structured data; the storage layer handles density.

## Why it exists

Three products in the Ablative stack need a durable, branchable, replicatable storage layer:

- **Aion** (workflow engine) needs transactional workflow state. Branch on activity start, commit on success, discard on failure. Replaces Postgres for workflow durability.
- **Liminal** (messaging bus) needs durable conversation state. Conversations resume from committed state after crash. Message replay from any offset without external log infrastructure.
- **Real-time channels** (future) need presence state and message history with sub-millisecond fork for connection isolation.

A single storage engine serving all three means one replication protocol, one compression strategy, one consistency model, and one set of operational tools.

## How it works

### The prolly tree

A prolly tree is a B-tree variant where node boundaries are determined by content, not by insertion order. Specifically, a boundary is placed when `BLAKE3(key) % target_size == 0`. This makes the tree structure deterministic: the same set of keys always produces the same tree, regardless of insertion order. History-independence is the foundation of efficient diff, merge, and sync.

### Actor-per-shard

> ⚠️ **Status (2026-06):** not yet implemented — describes intended design. The `beamr` dependency is declared but imported nowhere; the shard module is a stub.

Each shard is a beamr process. Writes are message sends, reads are lock-free against the immutable tree, and crash isolation is automatic. The shard count is fixed at database creation time and cannot be changed. Keys are mapped to shards via consistent hashing.

### The six clusters

| Cluster | What it builds | Key briefs |
|---|---|---|
| **core** | Node types, tree operations, cursors, diff, WAL, shard actors | CORE-001 through CORE-009 |
| **persistence** | Disk store, WAL writer, crash recovery | PERSIST-001 through PERSIST-003 |
| **branching** | Fork, merge, snapshots, pruning | BRANCH-001 through BRANCH-004 |
| **api** | EventStore, key-value API, TTL expiry | API-001 through API-003 |
| **distribution** | Hash-based sync, merge-on-sync, topology | DIST-001 through DIST-003 |
| **wasm** | Browser build target, IndexedDB/OPFS backends | WASM-001 through WASM-003 |

> ⚠️ **Status (2026-06):** partial — only part of this table is wired. Implemented: the **core** tree node layer, cursors, and structural diff; the disk/cache/memory stores and zstd compression (**persistence**); the WAL buffer and durable writer (**core** WAL); and the snapshots/checkout slice of **branching**. The shard actors (**core**), **distribution** (sync/merge-on-sync/topology), the **api** cluster (EventStore, key-value API, TTL expiry), and **wasm** backends (browser build target, IndexedDB/OPFS) are not yet implemented — those rows describe intended design. Within **branching**, merge and fork are also not yet implemented (only snapshots and checkout have landed).

> ⚠️ **Status (2026-06):** partial — crash recovery is not yet wired. The durable WAL writer exists (mutations are framed, CRC32-checksummed, and fsync'd to an append-only file), but there is no replay/recovery path that reads the WAL back after a crash — that is CORE-006/PERSIST-003, not yet landed.

### Architecture decisions

- **ADR-001:** Content-addressed prolly tree over B+ tree. O(differences) merge instead of O(total size).
- **ADR-002:** BLAKE3 for content addressing. 32-byte keys, SIMD-accelerated, no length-extension attacks.
- **ADR-003:** WAL buffer for write amortisation. Batch N writes, flush one tree commit. WAL is uncompressed for crash recovery speed.
- **ADR-004:** Actor-per-shard with fixed shard count at creation. Zero write contention.
- **ADR-005:** Unbounded value size via ProcBin off-heap storage. Large values don't fragment the tree.
- **ADR-006:** Zstd compression per tree node on disk. Decompressed in LRU cache. WAL entries are uncompressed.

## What makes it different

Most embedded databases (RocksDB, SQLite, sled) give you a mutable key-value store with optional replication bolted on. Haematite gives you an immutable content-addressed graph with replication as a natural consequence of its structure.

The closest analogue is Dolt (git for databases), but Haematite is:

- **Embeddable.** It's a Rust library, not a server. Link it into your process and call functions.
- **Actor-native.** Built on beamr's process model. Each shard is a supervised actor with crash isolation and message-passing concurrency.
- **WASM-portable.** The prolly tree doesn't need OS primitives beyond memory allocation. Run it in the browser with IndexedDB backing or on edge workers with OPFS.
- **Designed for workflows.** Branch-on-activity and commit-on-success are first-class operations, not afterthoughts. Aion's deterministic replay requires a storage layer that can fork and discard cheaply. That's what this is.

## Current status

7 of 25 briefs landed. ~94 tests pass. The core tree primitives, cursor operations, structural diff, disk persistence with zstd compression, and LRU caching are complete. WAL buffer and the critical path through shard actors are next.
