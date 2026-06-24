# WASM-001: WASM build target, runtime adapter, and IndexedDB node store

**Cluster:** wasm
**Repo:** `/Users/tom/Developer/ablative/haematite`
**Clone URL:** https://github.com/ablative-io/haematite.git
**Base ref:** `main`
**Reviewers:** Waffles the Terrible
**Depends on:** CORE-001, PERSIST-001

---

## Purpose

Deliver the WASM compilation target, the web worker runtime adapter, and the IndexedDB-backed NodeStore. This is the foundation for running haematite in the browser — the same API, the same content-addressed tree, backed by IndexedDB instead of the filesystem.

## Task

Add a wasm feature flag that cfg-gates filesystem-only code (DiskStore, native durable WAL). Implement the WASM runtime adapter that runs shard actors on web workers via beamr-wasm. Implement IndexedDB NodeStore with hash as key and zstd-compressed bytes as value. IndexedDB is async — the adapter runs on web workers where transactions can be awaited. Reuse the LRU cache for decompressed nodes in WASM linear memory. Ensure BLAKE3 portable implementation produces identical hashes to native SIMD implementation. Target binary size under 2MB gzipped. Does not implement OPFS WAL (WASM-002) or browser transport (WASM-003).

## Requirements

### R1: WASM feature flag and cfg gates

**Spec:** THE SYSTEM SHALL add a wasm feature flag to Cargo.toml. WHEN the wasm feature is enabled, THE SYSTEM SHALL cfg-gate out filesystem-only code (DiskStore, native durable WAL). THE SYSTEM SHALL ensure no native-only dependencies leak into the WASM build.

**Acceptance criteria:**
- Cargo.toml has a [features] wasm entry
- DiskStore and native WAL are behind #[cfg(not(feature = "wasm"))]
- cargo check -p haematite --target wasm32-unknown-unknown --features wasm succeeds
- No native-only dependencies appear in the WASM dependency tree

**Files:**
- `crates/haematite/src/wasm/mod.rs` (create)
- `crates/haematite/Cargo.toml` (modify)
- `crates/haematite/src/lib.rs` (modify)
- `crates/haematite/src/store/mod.rs` (modify)

*Checklist: C1, C2 | Stories: S8, S9*

### R2: BLAKE3 portable hash parity

**Spec:** THE SYSTEM SHALL verify that the BLAKE3 portable (non-SIMD) implementation used in WASM produces identical hashes to the SIMD-accelerated implementation used in native builds. THE SYSTEM SHALL NOT use any WASM-specific hash implementation.

**Acceptance criteria:**
- Hashing the same input on native (SIMD) and WASM (portable) produces identical 32-byte hashes
- A test hashes a known input and asserts the output matches across both implementations
- The blake3 crate's portable feature is used for the WASM target

**Files:**
- `crates/haematite/Cargo.toml` (modify)

*Checklist: C3 | Stories: S6, S10, S8*

### R3: WASM runtime adapter

**Spec:** THE SYSTEM SHALL implement a runtime adapter that runs shard actors on web workers using beamr-wasm's scheduler. THE SYSTEM SHALL integrate with the browser's event loop. THE SYSTEM SHALL NOT run shard actors on the main thread.

**Acceptance criteria:**
- Shard actors run on web worker threads
- The main thread is not blocked by shard actor operations
- The adapter uses beamr-wasm's existing scheduler
- Multiple shard actors can run concurrently on separate web workers

**Files:**
- `crates/haematite/src/wasm/runtime.rs` (create)
- `crates/haematite/src/wasm/mod.rs` (modify)

*Checklist: C5 | Stories: S3*

### R4: IndexedDB NodeStore

**Spec:** THE SYSTEM SHALL implement an IndexedDB-backed NodeStore where each content-addressed node is stored as an IndexedDB object with the BLAKE3 hash as key and zstd-compressed bytes as value. THE SYSTEM SHALL NOT block the browser's main thread with IndexedDB operations. THE SYSTEM SHALL run IndexedDB transactions on web workers.

**Acceptance criteria:**
- IndexedDbStore implements the NodeStore trait
- put(node) stores compressed bytes in IndexedDB with hash as key
- get(hash) retrieves and decompresses the node from IndexedDB
- get(unknown_hash) returns None
- IndexedDB operations run on web workers, not the main thread

**Files:**
- `crates/haematite/src/store/indexeddb.rs` (create)
- `crates/haematite/src/store/mod.rs` (modify)

*Checklist: C6, C7 | Stories: S1*

### R5: LRU cache reuse in WASM

**Spec:** THE SYSTEM SHALL reuse the LRU cache from the persistence cluster to cache decompressed nodes in WASM linear memory. THE SYSTEM SHALL NOT implement a separate cache for WASM.

**Acceptance criteria:**
- The same LruCache type is used in both native and WASM builds
- Cached nodes are stored decompressed in WASM linear memory
- A cache hit avoids IndexedDB access

**Files:**
- `crates/haematite/src/store/indexeddb.rs` (modify)

*Checklist: C8*

### R6: Idempotent writes and functional parity

**Spec:** THE SYSTEM SHALL ensure IndexedDB NodeStore passes the same functional tests as the native DiskStore: idempotent writes, round-trip serialisation, and hash-based lookup. THE SYSTEM SHALL make write of an existing hash a no-op.

**Acceptance criteria:**
- put(node) twice returns the same hash both times without error
- A node round-trips through put/get correctly
- The IndexedDB store passes the same test suite as DiskStore

**Files:**
- `crates/haematite/src/store/indexeddb.rs` (modify)

*Checklist: C9, C10 | Stories: S4*

### R7: Binary size optimisation

**Spec:** THE SYSTEM SHALL keep the WASM binary size under 2MB gzipped. THE SYSTEM SHALL use wasm-opt for size optimisation. THE SYSTEM SHALL cfg-gate any dependency that significantly inflates the WASM binary.

**Acceptance criteria:**
- wasm-pack build --release produces a binary under 2MB gzipped
- wasm-opt is applied in the release build
- No unnecessary dependencies are included in the WASM build

**Files:**
- `crates/haematite/Cargo.toml` (modify)

*Checklist: C4 | Stories: S2*

## Boundaries

- SHALL NOT implement OPFS WAL backend — that is WASM-002
- SHALL NOT implement browser transport for distribution — that is WASM-003
- SHALL NOT run shard actors on the main thread
- SHALL NOT implement a WASM-specific hash function — use BLAKE3 portable

## Verification

- cargo check -p haematite --target wasm32-unknown-unknown --features wasm succeeds
- cargo test -p haematite --lib passes all tests (native)
- cargo clippy -p haematite produces no errors
- grep -r 'unwrap()\|expect(' crates/haematite/src/wasm/ --include='*.rs' -l finds no non-test files
- No file in crates/haematite/src/ exceeds 500 lines

## Architecture Decision Records

### ADR-002: BLAKE3 for content addressing

**Decision:** BLAKE3 over SHA-256 and BLAKE2b, because BLAKE3 is the fastest cryptographic hash available with SIMD acceleration, its 32-byte output is compact enough for node keys, and its tree-hashing mode enables parallel hashing of large nodes. SHA-256 is 3-5x slower on modern hardware. BLAKE2b is fast but BLAKE3 matches or exceeds it while being newer and better optimized.
**Decided by:** Sir Patick Stewart

### ADR-006: zstd compression per content-addressed node, WAL uncompressed

**Decision:** zstd compression per content-addressed tree node, WAL entries uncompressed, over compressing both or neither. The rejected alternatives: compressing WAL entries (poor ratio, added write latency on the hot path); no compression (wasted disk for tree nodes that compress well at ~3:1 for structured data).
**Decided by:** Bono, Frodo Baggins

## Constraints

- **CN1:** WASM build must compile to wasm32-unknown-unknown without any native-only dependencies. All platform-specific code must be behind cfg gates.
- **CN2:** BLAKE3 hashes must be identical between native and WASM builds. The portable BLAKE3 implementation must produce the same output as the SIMD-accelerated one.
- **CN3:** WASM binary size (gzipped) must stay under 2MB. Dependencies that bloat the binary must be cfg-gated or replaced.
- **CN4:** IndexedDB operations are async. The NodeStore adapter must not block the browser's main thread.
- **CN5:** OPFS fallback to IndexedDB must be transparent to the shard actor. The WAL interface is the same regardless of backend.
- **CN6:** The WAL entry format in OPFS must be byte-identical to the native durable WAL format so that WAL files are portable.

## Checklist Items

- **C1:** haematite compiles to wasm32-unknown-unknown with a wasm feature flag.
- **C2:** Filesystem-only code (DiskStore, native durable WAL) is cfg-gated out of the WASM build (CN1).
- **C3:** BLAKE3 uses the portable (non-SIMD) implementation on WASM and produces identical hashes to native (CN2).
- **C4:** WASM binary size stays under 2MB gzipped with wasm-opt size optimisation (CN3).
- **C5:** Shard actors run on web workers using the beamr-wasm scheduler.
- **C6:** IndexedDB NodeStore implements the NodeStore trait with hash as key and zstd-compressed bytes as value.
- **C7:** IndexedDB operations do not block the browser's main thread (CN4).
- **C8:** The LRU cache from the persistence cluster caches decompressed nodes in WASM linear memory.
- **C9:** IndexedDB NodeStore passes the same functional tests as the native DiskStore.
- **C10:** Node writes are idempotent: writing a hash that already exists in IndexedDB is a no-op.

## User Stories

- **S1:** As a frontend developer, I want to use the same haematite API in the browser as on the server so that I do not need to learn a separate client-side storage library.
- **S2:** As a frontend developer, I want the WASM binary to be under 2MB gzipped so that my application's initial load time is not dominated by the database engine.
- **S3:** As a frontend developer, I want haematite to run in a web worker so that database operations do not block the UI thread.
- **S4:** As a frontend developer, I want to fork and merge databases in the browser so that I can implement offline editing with later sync.
- **S6:** As an application developer, I want content-addressed hashes to be identical between the server and browser so that sync works by hash comparison without translation.
- **S8:** As an edge developer, I want haematite to run in Cloudflare Workers or Deno Deploy so that I can embed a branchable database at the edge.
- **S9:** As an edge developer, I want the WASM build to have no native-only dependencies so that it runs in any WASM-capable runtime.
- **S10:** As an operations engineer, I want to verify that the WASM build produces identical hashes to the native build so that sync between native and WASM nodes is reliable.

## Design Intention

The WASM cluster extends haematite to the browser and edge. When this cluster is complete, the same haematite API — get, put, range, fork, merge, commit — runs in a browser tab or Cloudflare Worker, backed by IndexedDB or OPFS instead of the filesystem. Content-addressed nodes are portable across backends: a tree written on a native server can be synced to a browser tab and read there with identical hashes. The experience should feel like the same database everywhere — write on the server, read in the browser, fork on the edge — not a separate product with a compatibility layer.

## Workflow Config

- Isolation: worktree
- Verify-fix cap: 3
- Review cap: 1
