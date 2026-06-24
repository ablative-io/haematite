# BRANCH-003: Snapshot registry and time-travel checkout

**Cluster:** branching
**Repo:** `/Users/tom/Developer/ablative/haematite`
**Clone URL:** https://github.com/ablative-io/haematite.git
**Base ref:** `main`
**Reviewers:** Waffles the Terrible
**Depends on:** CORE-002

---

## Purpose

Deliver the snapshot registry that maps human-readable names to committed root hashes, and the read-only checkout operation that provides instant time-travel access to any historical state. Every commit is a permanent snapshot; this brief makes them nameable and navigable.

## Task

Implement the snapshot registry: a persistent mapping from names to root hashes, plus a commit log of all root hashes in commit order. Implement snapshot(name) to record the current committed root hash under a name. Implement checkout(root_hash) to open a read-only view at that root hash — direct read from the content-addressed node store, no WAL replay. The read-only view supports get and range but rejects put/delete/commit. Implement snapshot listing with names, hashes, and timestamps. This brief does not implement pruning — that is BRANCH-004.

## Requirements

### R1: Snapshot registry

**Spec:** THE SYSTEM SHALL maintain a snapshot registry mapping human-readable names (strings) to root hashes. THE SYSTEM SHALL persist the registry across database restarts. THE SYSTEM SHALL NOT allow two snapshots with the same name — naming a duplicate is an error.

**Acceptance criteria:**
- SnapshotRegistry::name(name, root_hash) stores the mapping
- SnapshotRegistry::get(name) returns the root hash for a named snapshot
- SnapshotRegistry::get(name) returns None for unknown names
- Naming a snapshot with a name that already exists returns an error
- SnapshotRegistry implements Debug

**Files:**
- `crates/haematite/src/branch/snapshot.rs` (create)
- `crates/haematite/src/branch/mod.rs` (modify)

*Checklist: C18, C20 | Stories: S8*

### R2: Commit log

**Spec:** THE SYSTEM SHALL maintain a commit log of all root hashes in commit order. WHEN a commit produces a new root hash, THE SYSTEM SHALL append it to the log with a timestamp. THE SYSTEM SHALL support listing the log in chronological order. The commit log is populated by Database::commit (CORE-009 R5) — after collecting shard commit hashes, it appends the composite root hash to this log.

**Acceptance criteria:**
- Each commit appends the new root hash and timestamp to the log
- CommitLog::list() returns entries in chronological order
- Each log entry contains root hash and timestamp
- The log survives database restart

**Files:**
- `crates/haematite/src/branch/snapshot.rs` (modify)

*Checklist: C19 | Stories: S10*

### R3: Read-only checkout

**Spec:** WHEN checkout(root_hash) is called, THE SYSTEM SHALL return a read-only view at the specified root hash. THE SYSTEM SHALL read directly from the content-addressed node store without WAL replay. THE SYSTEM SHALL NOT allow writes (put, delete, commit) on the read-only view.

**Acceptance criteria:**
- checkout(root_hash) returns a ReadOnlyView
- ReadOnlyView::get(key) traverses the tree at the specified root hash
- ReadOnlyView::get does not consult any WAL buffer
- ReadOnlyView::put returns an error
- ReadOnlyView::delete returns an error
- ReadOnlyView::commit returns an error

**Files:**
- `crates/haematite/src/branch/checkout.rs` (create)
- `crates/haematite/src/branch/mod.rs` (modify)

*Checklist: C21, C22 | Stories: S9*

### R4: Read-only range queries

**Spec:** WHILE a read-only checkout is active, THE SYSTEM SHALL support range(from, to) queries that return key-value pairs in sorted order from the historical tree state.

**Acceptance criteria:**
- ReadOnlyView::range(from, to) returns entries in sorted key order
- Range results reflect the exact state at the checkout root hash
- Range results do not include entries written after the checkout point

**Files:**
- `crates/haematite/src/branch/checkout.rs` (modify)

*Checklist: C22 | Stories: S9, S11*

### R5: Snapshot listing

**Spec:** THE SYSTEM SHALL provide a list_snapshots() operation that returns all named snapshots with their root hashes and commit timestamps, in chronological order.

**Acceptance criteria:**
- list_snapshots() returns a Vec of (name, root_hash, timestamp) tuples
- Results are ordered chronologically by the time the snapshot was named
- An empty registry returns an empty list

**Files:**
- `crates/haematite/src/branch/snapshot.rs` (modify)

*Checklist: C23 | Stories: S10*

## Boundaries

- SHALL NOT implement fork or branch handle — those are BRANCH-001
- SHALL NOT implement merge or conflict resolution — those are BRANCH-002
- SHALL NOT implement snapshot pruning or node reclamation — that is BRANCH-004
- SHALL NOT allow writes through a read-only checkout view

## Verification

- cargo check -p haematite succeeds with no errors
- cargo test -p haematite --lib passes all tests
- cargo clippy -p haematite produces no errors
- grep -r 'unwrap()\|expect(' crates/haematite/src/branch/ --include='*.rs' -l finds no non-test files
- No file in crates/haematite/src/branch/ exceeds 500 lines

## Architecture Decision Records

### ADR-001: Content-addressed prolly tree over B+ tree with WAL overlay

**Decision:** Content-addressed prolly tree over B+ tree with WAL overlay, because branching/merging/sync is the headline feature and must be structural (O(differences), history-independent) rather than overlaid (O(branch-writes), order-dependent). The rejected alternative (B+ tree + WAL overlay) makes branching an afterthought — functional but O(branch-writes) for merge and requiring WAL coordination for replication.
**Decided by:** Bono, Frodo Baggins, Sir Patick Stewart

## Constraints

- **CN1:** Fork must complete in O(1) time and O(1) space — no tree node copying, only root hash recording and empty buffer allocation.
- **CN2:** Three-way merge must visit only subtrees where at least one hash differs across the three trees. Full-tree scan on merge is a correctness bug, not a performance issue.
- **CN3:** Conflict resolution must never silently discard data. LWW is explicit lossy; vector-clock surfaces true conflicts; custom functions receive all three values.
- **CN4:** Snapshot pruning must not delete tree nodes referenced by any active branch or named snapshot. Pruning a live reference is data loss.
- **CN5:** Branch operations must work per-shard: fork = fork each shard's tree, merge = merge each shard's tree. No cross-shard locking or coordination.
- **CN6:** Read-only checkout must not require WAL replay — it reads directly from the content-addressed node store at the specified root hash.

## Checklist Items

- **C18:** Snapshot registry maintains a mapping from human-readable names to root hashes.
- **C19:** Snapshot registry maintains a commit log of all root hashes in commit order.
- **C20:** snapshot(name) records the current committed root hash under the given name.
- **C21:** checkout(root_hash) opens a read-only view at the specified root hash without WAL replay (CN6).
- **C22:** Read-only checkout view supports get and range operations but rejects put/delete/commit.
- **C23:** Snapshot listing returns all named snapshots with their root hashes and commit timestamps.

## User Stories

- **S8:** As a debugging developer, I want to name a committed root hash as a snapshot so that I can return to that exact state later for investigation.
- **S9:** As a debugging developer, I want to check out a historical snapshot for read-only access so that I can inspect past state without replaying the entire write history.
- **S10:** As a debugging developer, I want to list all named snapshots with their timestamps so that I can find the right snapshot to investigate.
- **S11:** As a debugging developer, I want to diff two snapshots so that I can see exactly what changed between two points in time.

## Design Intention

Branching is the headline feature — the reason haematite exists instead of wrapping SQLite. When this cluster is complete, a developer can fork a database in constant time by sharing a root hash, diverge both sides with independent writes, and merge them back by walking only the subtrees that differ. Every committed root hash is a permanent, verifiable snapshot that can be checked out for read-only time-travel at any point in the future. The experience should feel like working with git branches — cheap to create, cheap to compare, cheap to merge — but for a live key-value database with concurrent writers. The content-addressed prolly tree from the core cluster is what makes this structural rather than bolted-on: identical subtrees share nodes, fork costs nothing, and merge cost is proportional to differences, not total data.

## Workflow Config

- Isolation: worktree
- Verify-fix cap: 3
- Review cap: 1
