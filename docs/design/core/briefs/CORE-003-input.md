# CORE-003: Structural diff between two prolly trees by root hash comparison

**Cluster:** core
**Repo:** `/Users/tom/Developer/ablative/haematite`
**Clone URL:** https://github.com/ablative-io/haematite.git
**Base ref:** `main`
**Reviewers:** Waffles the Terrible
**Depends on:** CORE-002

---

## Purpose

Implement the structural diff operation that walks two prolly trees simultaneously, skipping subtrees whose root hash matches, and yields only the entries that differ. The cost of the diff is O(differences), not O(total tree size) — this is a direct consequence of P4 (unmodified subtrees are free). The diff operation is the foundation that the branching cluster's merge operation will build on; without it, comparing two database snapshots requires a full scan. A debugging developer must be able to diff two large database states that differ in a handful of keys and receive results in sub-millisecond time.

## Task

Create crates/haematite/src/tree/diff.rs implementing a diff function that takes a NodeStore reference and two root hashes, walks both trees in parallel by key order, and yields DiffEntry items for each key that differs. DiffEntry has three variants: Added(key, value) when a key exists only in tree B, Removed(key, value) when a key exists only in tree A, and Modified(key, old_value, new_value) when a key exists in both trees with different values. The critical invariant: when two nodes at the same tree position share a hash, the function MUST NOT descend into them — the entire subtree is identical and can be skipped. This brief covers the diff algorithm and its types only. Branching, merging, and three-way merge are out of scope — those belong to the branching cluster.

## Requirements

### R1: DiffEntry type with Added, Removed, and Modified variants

**Spec:** THE SYSTEM SHALL define a DiffEntry enum with three variants: Added { key: Vec<u8>, value: Vec<u8> }, Removed { key: Vec<u8>, value: Vec<u8> }, and Modified { key: Vec<u8>, old_value: Vec<u8>, new_value: Vec<u8> }. THE SYSTEM SHALL implement Debug and PartialEq on DiffEntry. THE SYSTEM SHALL NOT include hash values in DiffEntry — callers work with keys and values, not internal tree identifiers.

**Acceptance criteria:**
- DiffEntry::Added { key, value } is constructible
- DiffEntry::Removed { key, value } is constructible
- DiffEntry::Modified { key, old_value, new_value } is constructible
- DiffEntry implements Debug
- DiffEntry implements PartialEq so tests can use assert_eq!
- DiffEntry fields use Vec<u8> for keys and values — no hash fields present

**Files:**
- `crates/haematite/src/tree/diff.rs` (create)

*Checklist: C19 | Stories: S13*

### R2: diff function signature and NodeStore integration

**Spec:** THE SYSTEM SHALL expose a diff function with the signature diff<S: NodeStore>(store: &S, root_a: &Hash, root_b: &Hash) -> Result<Vec<DiffEntry>, DiffError>. WHEN root_a and root_b are equal, THE SYSTEM SHALL return an empty Vec without loading any nodes from the store. THE SYSTEM SHALL NOT require the caller to hold the trees in memory — all node access MUST go through the NodeStore trait defined in CORE-001. IF a required node hash is not found in the store, THEN THE SYSTEM SHALL return DiffError::MissingNode(hash).

**Acceptance criteria:**
- diff(store, hash, hash) where both hashes are identical returns Ok(vec![]) without any store.get calls
- diff(store, root_a, root_b) returns Ok(entries) when both roots are reachable
- diff(store, root_a, root_b) returns Err(DiffError::MissingNode(h)) when a node at hash h is absent from the store
- DiffError implements std::error::Error and Debug
- The function accepts any S: NodeStore — it is not coupled to MemoryStore

**Files:**
- `crates/haematite/src/tree/diff.rs` (modify)

*Checklist: C19, C20 | Stories: S12, S13*

### R3: Subtree skipping when hashes match (P4)

**Spec:** WHILE walking both trees simultaneously, WHEN the current node hash in tree A equals the current node hash in tree B at the same position, THE SYSTEM SHALL skip that subtree entirely and produce no DiffEntry items for any key beneath it. THE SYSTEM SHALL NOT call store.get on a node whose hash matches the opposing tree's node hash at the same position — descending is prohibited when hashes match. This is P4: unmodified subtrees are free.

**Acceptance criteria:**
- A store that panics on get for shared-hash nodes does not panic during diff of two trees that share a subtree
- diff of two trees that share all subtrees except one leaf returns only the entries from that leaf — not entries from shared subtrees
- store.get is not called for any node whose hash appears identically on both sides of the comparison at the same tree position
- A test with a 1000-entry tree where one key is modified records fewer than 20 store.get calls

**Files:**
- `crates/haematite/src/tree/diff.rs` (modify)

*Checklist: C20, C21 | Stories: S14*

### R4: Parallel in-order traversal of leaf entries

**Spec:** WHEN both trees descend to leaf nodes at the same position, THE SYSTEM SHALL merge their key-value entry lists with a two-pointer scan in sorted key order. WHEN a key exists in leaf A but not leaf B, THE SYSTEM SHALL yield DiffEntry::Removed. WHEN a key exists in leaf B but not leaf A, THE SYSTEM SHALL yield DiffEntry::Added. WHEN a key exists in both leaves with differing values, THE SYSTEM SHALL yield DiffEntry::Modified. WHEN a key exists in both leaves with identical values, THE SYSTEM SHALL NOT yield any DiffEntry for that key.

**Acceptance criteria:**
- diff of leaf A = [(b, 2), (c, 3)] and leaf B = [(a, 1), (c, 3)] yields [Added(a,1), Removed(b,2)] in key order
- diff of leaf A = [(k, old)] and leaf B = [(k, new)] yields [Modified(k, old, new)]
- diff of leaf A = [(k, v)] and leaf B = [(k, v)] yields [] — identical values produce no entry
- diff results are in ascending key order

**Files:**
- `crates/haematite/src/tree/diff.rs` (modify)

*Checklist: C19 | Stories: S13*

### R5: Asymmetric descent when one side is a leaf and the other is internal

**Spec:** WHEN one tree has a leaf node at a given position and the other has an internal node (due to structural divergence from unequal key sets), THE SYSTEM SHALL expand the internal node by descending its children while treating the leaf as a terminal on its side. THE SYSTEM SHALL yield Added or Removed entries for all keys reachable under the internal subtree that are absent from the leaf side. THE SYSTEM SHALL NOT assume both trees are structurally identical — prolly tree boundaries are content-determined and two trees with different key sets will have different node shapes.

**Acceptance criteria:**
- diff where tree A has a single leaf with keys [a, b] and tree B has an internal node whose children span [a, b, c, d] yields Added entries for c and d
- diff where tree A has keys [a, b, c] as one leaf and tree B has the same keys split across two leaves under an internal node yields []
- No panic or error occurs when node depth differs between the two trees at a given key range

**Files:**
- `crates/haematite/src/tree/diff.rs` (modify)

*Checklist: C19, C21 | Stories: S13, S14*

### R6: Wire diff module into tree module root and crate public API

**Spec:** THE SYSTEM SHALL add diff as a public module in crates/haematite/src/tree/mod.rs using a pub mod declaration. THE SYSTEM SHALL re-export diff, DiffEntry, and DiffError from the crate root lib.rs. tree/mod.rs SHALL NOT contain logic — only pub mod diff and the existing pub mod declarations from CORE-001 and CORE-002.

**Acceptance criteria:**
- crates/haematite/src/tree/mod.rs contains pub mod diff and no logic
- haematite::diff is callable from outside the crate
- haematite::DiffEntry is accessible from outside the crate
- haematite::DiffError is accessible from outside the crate
- cargo check -p haematite succeeds

**Files:**
- `crates/haematite/src/tree/mod.rs` (modify)
- `crates/haematite/src/lib.rs` (modify)

*Checklist: C19 | Stories: S12*

### R7: Cost proportionality test: diff is O(differences) not O(tree size)

**Spec:** WHEN two trees share all but a small number of differing keys, THE SYSTEM SHALL complete the diff in time proportional to the number of differing keys, not the total number of keys. The store.get call count provides the observable proxy for this: the number of nodes fetched SHALL NOT grow linearly with the total key count when the difference count is held constant. THE SYSTEM SHALL NOT traverse shared subtrees to verify this — hash equality at a node is sufficient proof of subtree identity (P1).

**Acceptance criteria:**
- A test builds two trees each containing 10 000 entries where exactly 3 keys differ, runs diff, and asserts the DiffEntry count is 3
- The same test asserts that store.get was called fewer than 100 times (substantially less than 10 000)
- A test builds two entirely disjoint trees of 100 entries each and asserts diff returns 200 entries (100 Added, 100 Removed)
- diff on two empty trees (empty root hashes represented as the hash of an empty node) returns []

**Files:**
- `crates/haematite/src/tree/diff.rs` (modify)

*Checklist: C21 | Stories: S14*

## Boundaries

- SHALL NOT implement three-way merge or conflict resolution — those belong to the branching cluster
- SHALL NOT implement fork or branch creation — those belong to the branching cluster
- SHALL NOT implement WAL buffer diffing — diff operates on committed tree roots only
- SHALL NOT implement streaming or async iteration — diff returns a collected Vec<DiffEntry>
- SHALL NOT implement cross-shard diff — each call to diff operates on a single shard's tree; multi-shard coordination is the caller's responsibility
- SHALL NOT introduce new serialisation formats — node loading uses the NodeStore trait from CORE-001 exclusively

## Verification

- cargo check -p haematite succeeds with no errors
- cargo test -p haematite --lib passes all tests
- cargo clippy -p haematite produces no errors
- grep -r 'unwrap()\|expect(' crates/haematite/src/tree/diff.rs finds no occurrences outside #[cfg(test)] blocks
- wc -l crates/haematite/src/tree/diff.rs reports fewer than 500 lines
- grep 'pub mod diff' crates/haematite/src/tree/mod.rs confirms the module is wired
- grep 'pub use' crates/haematite/src/lib.rs | grep -E 'diff|DiffEntry|DiffError' confirms all three are re-exported

## Architecture Decision Records

### ADR-001: Content-addressed prolly tree over B+ tree with WAL overlay

**Decision:** Content-addressed prolly tree over B+ tree with WAL overlay, because branching/merging/sync is the headline feature and must be structural (O(differences), history-independent) rather than overlaid (O(branch-writes), order-dependent). The rejected alternative (B+ tree + WAL overlay) makes branching an afterthought — functional but O(branch-writes) for merge and requiring WAL coordination for replication.
**Decided by:** Bono, Frodo Baggins, Sir Patick Stewart

## Constraints

- **CN1:** No file over 500 lines. If a module approaches this limit, extract into submodules.
- **CN2:** No panics in production code. .unwrap() and .expect() only in tests.
- **CN3:** mod.rs contains ONLY pub mod declarations and pub use re-exports. No logic.
- **CN4:** All public types implement Debug. All error types implement std::error::Error.
- **CN5:** The prolly tree root hash for a given key-value set must be deterministic and history-independent: the same keys and values always produce the same root hash regardless of insertion order.
- **CN6:** WAL buffer reads must shadow tree reads: a key written to the buffer but not yet committed must be returned by get, not the stale tree value.
- **CN7:** Shard actor crash must not affect other shards. Supervision restarts only the failed shard.
- **CN8:** Batch append (multiple key-value pairs in one call) must produce exactly one tree commit, not N individual commits.

## Checklist Items

- **C19:** diff(root_a, root_b) walks both trees and yields only entries that differ.
- **C20:** diff skips subtrees whose root hash matches without descending into them.
- **C21:** diff cost is proportional to the number of differing entries, not the total tree size.

## User Stories

- **S12:** As a debugging developer, I want to get the current root hash of a committed database state so that I can identify and compare snapshots.
- **S13:** As a debugging developer, I want to diff two database states by root hash so that I can see exactly which keys changed between them.
- **S14:** As a debugging developer, I want the diff to run in time proportional to the number of differences, not the total database size, so that diffing large databases with small changes is fast.

## Design Intention

Haematite's core is a storage engine where concurrency is free and branching is structural. When this cluster is complete, a developer can open a haematite database, write to it from hundreds of concurrent actors without any locking discipline, fork it instantly by sharing a hash, and merge two forks by walking only the nodes that differ. The experience should feel like working with an in-memory data structure that happens to be durable — no connection pools, no transaction coordinators, no lock managers. The content-addressed prolly tree is what makes branching zero-friction; the actor-per-shard model is what makes concurrency zero-contention; the WAL buffer is what makes writes fast despite the content-addressing overhead. All three are load-bearing.

## Workflow Config

- Isolation: worktree
- Verify-fix cap: 3
- Review cap: 1
