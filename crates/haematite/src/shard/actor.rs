// CORE-007: Shard actor — owns tree + WAL buffer, handles get/put/delete/commit messages

use std::time::Duration;

mod errors;
pub mod handle;
mod liveness;
pub mod native;
mod scan;
mod startup;
mod stream_index;

use errors::{AppendError, CasError, HashCasError};

pub use handle::{RangeItem, ShardError, ShardHandle};

use crate::branch::current_timestamp;
use crate::store::NodeStore;
use crate::sync::ballot::{Ballot, Stamp};
use crate::tree::{Cursor, Hash, LeafNode, Node, batch_mutate_owned};
use crate::ttl::entry::{
    StampedEntry, encode_optional_ttl, encode_stamped_optional_ttl, encode_stamped_tombstone,
};
use crate::ttl::filter::{Visibility, is_expired_at, visible_value};
use crate::wal::{
    DurableWal, LookupResult, Mutation, PromiseRecord, RecoveredWal, WalBuffer, WalError,
};

/// Minimal shard write boundary used by the durable WAL layer.
///
/// Full beamr process wiring and range messages are delivered by later shard
/// briefs. This type keeps the durable write invariant executable today: a
/// mutation is appended to the durable WAL before it enters the in-memory buffer
/// and before `put`/`delete` can return `Ok`. Crash recovery can also seed the
/// actor with the committed tree root plus replayed WAL buffer so the same actor
/// accepts normal writes immediately after replay.
#[derive(Debug)]
pub struct ShardActor {
    wal: DurableWal,
    buffer: WalBuffer,
    committed_root: Option<Hash>,
    live_streams: stream_index::LiveStreamIndex,
    stream_index_errors: stream_index::SequenceIndexErrors,
    /// AA-3-0 actor-local durable promise state (design §3, R8). These are owned
    /// by THIS shard actor — never a `DashMap` consulted outside the slice — so
    /// the (future) epoch fence reads `promised` in the same slice that a Prepare
    /// mutates it, with no TOCTOU. Seeded from the recovered WAL on boot.
    ///
    /// `promised`: highest ballot promised in a Prepare; monotonic, never regresses.
    /// `owner_epoch`: ballot under which this node was elected owner, if any.
    /// `persisted_max_minted`: highest ballot counter ever minted (R4).
    promised: Ballot,
    owner_epoch: Option<Ballot>,
    persisted_max_minted: u64,
}

/// What a durable apply writes: a stamped value (with TTL) or a stamped tombstone
/// (AA-3-4b). Both go through the SAME fence + CAS + commit core.
enum ApplyKind {
    Value {
        value: Vec<u8>,
        ttl: Option<Duration>,
    },
    Tombstone,
}

/// One groupable durable WRITE to coalesce under a single group commit (audit E).
///
/// These are exactly the consecutive single-key durable writes that today each
/// fsync on their own: a scalar CAS, a stamped value apply, and a stamped
/// tombstone apply. Each carries its OWN precondition and (for the apply kinds)
/// its OWN stamp, so each gets its own monotonic stamp in queue order — the group
/// commit changes only HOW MANY fsyncs back the batch (one, not N), never the
/// per-write semantics. Promise-state mutators / `merge_adopt` are deliberately
/// NOT representable here: they fsync and/or raise `promised` and must keep their
/// individual ordering, so the driver stops the group at the first such command.
pub(super) enum GroupWrite {
    /// A scalar compare-and-swap (`ShardCommandKind::Cas`).
    Cas {
        key: Vec<u8>,
        expected: Option<u64>,
        new: u64,
    },
    /// A stamped value apply (`ShardCommandKind::ApplyDurable`).
    ApplyValue {
        key: Vec<u8>,
        expected: Option<Hash>,
        value: Vec<u8>,
        ttl: Option<Duration>,
        stamp: Stamp,
    },
    /// A stamped tombstone apply (`ShardCommandKind::ApplyDurableTombstone`).
    ApplyTombstone {
        key: Vec<u8>,
        expected: Option<Hash>,
        stamp: Stamp,
    },
}

/// Per-write outcome of [`ShardActor::apply_group`], aligned 1:1 with the input
/// `writes` so the driver can fan each result to its own reply channel.
#[derive(Debug)]
pub(super) enum GroupOutcome {
    /// The write was staged AND the group commit succeeded: reply `Ok(())`.
    Committed,
    /// The write's own precondition (CAS/fence) failed: reply this error. Its key
    /// was never buffered, so the rest of the group was unaffected.
    Rejected(ShardError),
    /// The write staged cleanly but the SHARED group commit failed; its key was
    /// rolled back. Reply this commit error so the caller retries (the write is
    /// not durable).
    CommitFailed(ShardError),
}

/// Outcome of [`ShardActor::record_promise`]: a Prepare promise is durably
/// accepted only if it strictly exceeds the persisted `promised` ballot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordPromiseOutcome {
    /// `promised` was advanced to (and fsync'd as) the new ballot.
    Promised,
    /// The ballot did not exceed the persisted `promised`; nothing was written.
    /// Carries the current `promised` so the caller can Nack with it (§2.2).
    Rejected { promised: Ballot },
}

/// An in-slice snapshot of a shard's election-relevant state (AA-3-2).
///
/// Read through the actor in ONE slice so the candidate's mint floor and the
/// acceptor's Promise reply observe a consistent `(promised, owner_epoch,
/// persisted_max_minted, committed_root)` — never a torn read across two
/// commands. `promised`/`owner_epoch`/`persisted_max_minted` drive the election
/// (§2.2); `committed_root` is carried in a Promise for handoff state-sync (§2.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromiseState {
    /// Highest ballot promised in a Prepare (monotonic, never regresses).
    pub promised: Ballot,
    /// The ballot under which this node was elected owner, if any.
    pub owner_epoch: Option<Ballot>,
    /// Highest minted-ballot counter ever persisted (R4 mint-floor input).
    pub persisted_max_minted: u64,
    /// This shard's last committed root, if any (§2.4 handoff state-sync).
    pub committed_root: Option<Hash>,
}

impl ShardActor {
    /// Build a shard write boundary around an already-open durable WAL.
    #[cfg(test)]
    #[must_use]
    pub fn new(wal: DurableWal) -> Self {
        Self {
            wal,
            buffer: WalBuffer::new(),
            committed_root: None,
            live_streams: stream_index::LiveStreamIndex::new(),
            stream_index_errors: stream_index::SequenceIndexErrors::new(),
            promised: Ballot::bottom(),
            owner_epoch: None,
            persisted_max_minted: 0,
        }
    }

    /// Build a normal shard actor from crash-recovered WAL state.
    ///
    /// Promise state (AA-3-0) is seeded from the recovered WAL: the latest
    /// persisted [`PromiseRecord`] if one exists, else the bottom defaults
    /// `(0,"")` / `None` / `0`. The WAL is also seeded with the recovered promise
    /// snapshot so the next commit truncation re-emits it (design §3). The live
    /// stream index is rebuilt from the recovered committed root before the actor
    /// accepts commands, so recovery observes the same baseline as a full walk.
    pub fn from_recovered<S>(
        mut wal: DurableWal,
        recovered: RecoveredWal,
        store: &S,
    ) -> Result<Self, WalError>
    where
        S: NodeStore + ?Sized,
    {
        let committed_root = recovered.committed_root();
        let stream_index = stream_index::rebuild(store, committed_root).map_err(tree_error)?;
        let promise = recovered
            .promise()
            .cloned()
            .unwrap_or_else(PromiseRecord::initial);
        wal.seed_promise(promise.clone());
        let buffer = recovered.into_buffer();
        let PromiseRecord {
            promised,
            owner_epoch,
            persisted_max_minted,
        } = promise;
        Ok(Self {
            wal,
            buffer,
            committed_root,
            live_streams: stream_index.live,
            stream_index_errors: stream_index.errors,
            promised,
            owner_epoch,
            persisted_max_minted,
        })
    }

    /// Last committed root hash known to this shard, if any.
    #[must_use]
    pub const fn committed_root(&self) -> Option<Hash> {
        self.committed_root
    }

    /// Append a put to the durable WAL, then buffer it for a future tree commit.
    #[cfg(test)]
    pub fn put<K, V>(&mut self, key: K, value: V) -> Result<(), WalError>
    where
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        let key = key.into();
        let value = value.into();
        self.put_encoded(key, value)
    }

    /// Append a put with optional TTL metadata to the durable WAL and buffer.
    pub fn put_with_ttl<K, V>(
        &mut self,
        key: K,
        value: V,
        ttl: Option<Duration>,
    ) -> Result<(), WalError>
    where
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        let key = key.into();
        let value = encode_ttl_value(value.into(), ttl)?;
        self.put_encoded(key, value)
    }

    fn put_encoded(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), WalError> {
        let mutation = Mutation::Put {
            key: key.clone(),
            value: value.clone(),
        };
        self.wal.append_mutation(&mutation)?;
        self.buffer.put(key, value);
        Ok(())
    }

    /// Append a STAMPED TOMBSTONE delete to the durable WAL, then buffer it for a
    /// future tree commit (AA-3-4b).
    ///
    /// A delete is unified into the one stamped write path: instead of a bare
    /// key-removal (`Mutation::Delete`), it stores a tombstone-kind stamped entry
    /// (`Mutation::Put` of the tombstone envelope) carrying `stamp`. The tombstone
    /// reads as ABSENT (`get` → `None`), so single-node read-after-delete is
    /// unchanged, but the delete is now a comparable, mergeable, stamped entry
    /// that PERSISTS in the tree — never a removal that the §2.4 merge could
    /// resurrect from a lagging node.
    pub fn delete<K>(&mut self, key: K, stamp: Stamp) -> Result<(), WalError>
    where
        K: Into<Vec<u8>>,
    {
        let key = key.into();
        let encoded = encode_stamped_tombstone(stamp);
        self.put_encoded(key, encoded)
    }

    /// Physically reclaim `key` from this shard's TTL sweep ONLY if its current
    /// stored value is present and expired now, re-checking the live buffer+tree
    /// inside the actor's single-threaded slice.
    ///
    /// The sweep computes its candidate set from an independent store+WAL
    /// snapshot; re-checking here closes the window in which a concurrent refresh
    /// (a fresh `put` landing between the snapshot and this delete) would be
    /// clobbered by an unconditional removal. Expiry is evaluated against the raw
    /// stored bytes (not the read-filtered view) at the current clock. Returns
    /// whether a removal was issued.
    ///
    /// # R-TOMB — tombstones are immortal; the sweep MUST skip them.
    ///
    /// A stamped tombstone (AA-3-4b) is NOT "expired data" to reclaim: removing it
    /// would, after the §2.4 union merge, RESURRECT a committed delete by making
    /// the deleting node indistinguishable from one that never wrote the key. This
    /// guard returns `false` for any tombstone REGARDLESS of clock, so the sweep
    /// can never physically remove one. (`is_expired_at` already returns `false`
    /// for a tombstone — it has no TTL — but this explicit guard makes R-TOMB
    /// load-bearing rather than incidental, and is asserted by a test.) Bounded
    /// tombstone GC needs a future membership-wide low-water-mark (every node past
    /// the tombstone's stamp) and is out of scope here.
    ///
    /// An actually-expired VALUE is reclaimed by a BARE buffer removal — a local
    /// GC of deterministically-expired data, not a committed delete — so it does
    /// not itself become a stamped tombstone (TTL expiry needs no replicated op,
    /// §2.4).
    pub fn delete_if_expired<S>(&mut self, key: &[u8], store: &S) -> Result<bool, WalError>
    where
        S: NodeStore + ?Sized,
    {
        let current = match self.buffer.get(key) {
            LookupResult::BufferedValue(value) => Some(value),
            LookupResult::BufferedDelete => None,
            LookupResult::NotBuffered => match self.committed_root {
                Some(root) => Cursor::new(store, root).get(key).map_err(tree_error)?,
                None => None,
            },
        };
        let Some(value) = current else {
            return Ok(false);
        };
        // R-TOMB: a tombstone is never swept, at any clock. Decode the stamped
        // envelope; if it is a tombstone, leave it (immortal). A decode error here
        // is treated as "not a tombstone" and falls through to the expiry check.
        if StampedEntry::decode(&value)
            .map_err(tree_error)?
            .is_some_and(|entry| entry.is_tombstone())
        {
            return Ok(false);
        }
        if is_expired_at(&value, current_timestamp()).map_err(tree_error)? {
            // Local GC of an expired value: a bare removal, NOT a stamped delete.
            self.buffer_remove(key.to_vec())?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Append a bare key-removal to the WAL (first) and buffer (local TTL-sweep GC
    /// only). This is NOT a committed delete: it carries no stamp and is used
    /// solely to reclaim deterministically-expired values (§2.4 "physical sweep is
    /// local GC"). User/replicated deletes go through [`Self::delete`] as
    /// tombstones.
    fn buffer_remove(&mut self, key: Vec<u8>) -> Result<(), WalError> {
        let mutation = Mutation::Delete { key: key.clone() };
        self.wal.append_mutation(&mutation)?;
        self.buffer.delete(key);
        Ok(())
    }

    /// Read through the recovered/live buffer first, then the committed tree.
    pub fn get<K, S>(&self, key: K, store: &S) -> Result<Option<Vec<u8>>, WalError>
    where
        K: AsRef<[u8]>,
        S: NodeStore + ?Sized,
    {
        let key = key.as_ref();
        match self.buffer.get(key) {
            LookupResult::BufferedValue(value) => visible_ttl_value(&value),
            LookupResult::BufferedDelete => Ok(None),
            LookupResult::NotBuffered => self.committed_root.map_or_else(
                || Ok(None),
                |root| {
                    visible_optional_ttl_value(
                        Cursor::new(store, root).get(key).map_err(tree_error)?,
                    )
                },
            ),
        }
    }

    /// Read the RAW stored envelope bytes for `key` (stamp + TTL header NOT
    /// stripped), or `None` if absent. Test-support for AA-3-4a: lets a test
    /// decode the committed stamp a replica stored, to prove every replica stored
    /// the IDENTICAL owner-assigned stamp. Unlike [`Self::get`] this does NOT apply
    /// the visibility filter, so the stamped envelope is returned verbatim.
    #[doc(hidden)]
    pub fn get_raw<K, S>(&self, key: K, store: &S) -> Result<Option<Vec<u8>>, WalError>
    where
        K: AsRef<[u8]>,
        S: NodeStore + ?Sized,
    {
        let key = key.as_ref();
        match self.buffer.get(key) {
            LookupResult::BufferedValue(value) => Ok(Some(value)),
            LookupResult::BufferedDelete => Ok(None),
            LookupResult::NotBuffered => self.committed_root.map_or_else(
                || Ok(None),
                |root| Cursor::new(store, root).get(key).map_err(tree_error),
            ),
        }
    }

    /// Flush buffered mutations to the tree, then atomically truncate the WAL.
    ///
    /// The in-memory buffer is cleared only after the new committed-root marker
    /// is durable. If WAL truncation fails after tree mutation succeeds, the
    /// buffer remains available for retry and the old committed root remains the
    /// actor baseline.
    pub fn commit<S>(&mut self, store: &mut S) -> Result<Hash, WalError>
    where
        S: NodeStore + ?Sized,
    {
        let baseline_root = match self.committed_root {
            Some(root) => root,
            None => store_empty_root(store)?,
        };
        // One materialisation of the write set (PERF-003): the buffered mutations
        // are collected ONCE into `batch` and then MOVED through normalisation by
        // `batch_mutate_owned` instead of being re-cloned. The buffer itself is
        // left intact here because `apply_committed_buffer` below still needs it on
        // the success path.
        let batch = buffered_batch(&self.buffer);
        let new_root = batch_mutate_owned(store, baseline_root, batch).map_err(tree_error)?;

        // Tier-0 durability barrier (node-rename fsync fix): the nodes for this
        // commit are now persisted (each file's DATA was fsync'd), but their
        // parent-directory entries are not yet durable. fsync each distinct
        // subdirectory that received a node STRICTLY BEFORE the WAL marker, so a
        // power loss can never leave a committed-root marker referencing nodes
        // whose directory entries are gone (which recovery rejects as
        // `MissingCommittedRoot`). In-memory stores make this a no-op.
        store.sync_dirty_dirs().map_err(tree_error)?;
        self.wal.commit(new_root)?;
        stream_index::apply_committed_buffer(
            &mut self.live_streams,
            &mut self.stream_index_errors,
            &self.buffer,
        );
        self.buffer = WalBuffer::new();
        self.committed_root = Some(new_root);
        Ok(new_root)
    }

    /// Atomically append event entries for one logical key and commit once.
    fn append<S>(
        &mut self,
        key: &[u8],
        entries: Vec<Vec<u8>>,
        expected_seq: u64,
        ttl: Option<Duration>,
        store: &mut S,
    ) -> Result<u64, AppendError>
    where
        S: NodeStore + ?Sized,
    {
        if entries.is_empty() {
            return Ok(expected_seq);
        }
        let seq_key = sequence_key(key);
        let actual = self.read_sequence(&seq_key, store)?;
        if actual != expected_seq {
            return Err(AppendError::SequenceConflict {
                expected: expected_seq,
                actual,
            });
        }
        let entry_count = u64::try_from(entries.len())
            .map_err(|_| WalError::TreeError("too many append entries".to_owned()))?;
        let new_seq = actual
            .checked_add(entry_count)
            .ok_or_else(|| WalError::TreeError("append sequence overflow".to_owned()))?;
        let mut mutations = Vec::with_capacity(entries.len().saturating_add(1));
        for (offset, entry) in entries.into_iter().enumerate() {
            let offset = u64::try_from(offset)
                .map_err(|_| WalError::TreeError("too many append entries".to_owned()))?;
            let seq = actual
                .checked_add(offset.saturating_add(1))
                .ok_or_else(|| WalError::TreeError("append sequence overflow".to_owned()))?;
            let value = encode_ttl_value(entry, ttl)?;
            mutations.push(Mutation::Put {
                key: event_key(key, seq),
                value,
            });
        }
        mutations.push(Mutation::Put {
            key: seq_key,
            value: new_seq.to_be_bytes().to_vec(),
        });
        // MULTI-key apply (event entries + the seq counter): the full-buffer
        // snapshot is load-bearing all-or-nothing rollback (PERF-003 preserves it,
        // as for `apply_durable_batch`) — a mid-batch failure must leave the buffer
        // exactly as before. A single-key snapshot cannot express that.
        let previous_buffer = self.buffer.clone();
        for mutation in mutations {
            buffer_mutation(&mut self.buffer, mutation);
        }
        match self.commit(store) {
            Ok(_root) => Ok(new_seq),
            Err(error) => {
                self.buffer = previous_buffer;
                Err(AppendError::from(error))
            }
        }
    }

    /// Read a scalar `u64` value for `key`, or `None` if the key is unset.
    ///
    /// A stored value must be exactly eight big-endian bytes; anything else is a
    /// corrupt scalar and surfaces as a tree error.
    fn read_value<S>(&self, key: &[u8], store: &S) -> Result<Option<u64>, WalError>
    where
        S: NodeStore + ?Sized,
    {
        self.get(key, store)?.map_or(Ok(None), |bytes| {
            bytes
                .as_slice()
                .try_into()
                .map(|raw| Some(u64::from_be_bytes(raw)))
                .map_err(|_| WalError::TreeError("invalid scalar value".to_owned()))
        })
    }

    /// Atomically compare-and-swap the scalar `u64` value at `key`.
    ///
    /// The read of the current value, the comparison against `expected`, and
    /// the write of `new` all run inside this one call. Because every shard
    /// command is executed by the shard's single-threaded native process — one
    /// command per slice, popped under the queue lock — no other command can
    /// observe or mutate `key` between the read and the write. That is what
    /// makes the operation atomic: there is no interleaving point.
    ///
    /// On a value mismatch the actual current value is returned in
    /// [`CasError::Mismatch`] and nothing is written. On a match the new value
    /// is buffered and committed as a single tree commit.
    fn cas<S>(
        &mut self,
        key: &[u8],
        expected: Option<u64>,
        new: u64,
        store: &mut S,
    ) -> Result<(), CasError>
    where
        S: NodeStore + ?Sized,
    {
        // Targeted single-key rollback (PERF-003): `stage_cas` buffers exactly one
        // key and returns that key's prior entry, so restoring it is a complete
        // inverse on a commit failure.
        let prior = self.stage_cas(key, expected, new, store)?;
        match self.commit(store) {
            Ok(_root) => Ok(()),
            Err(error) => {
                self.buffer.restore_entry(key, prior);
                Err(CasError::from(error))
            }
        }
    }

    /// Stage a CAS into the buffer WITHOUT committing (group-commit, audit E).
    ///
    /// Runs the read-compare against `expected` through the live buffer+tree (so a
    /// CAS staged earlier in the same group is observed), and on a match buffers
    /// the new value. Returns that key's PRIOR buffered entry so the caller can
    /// roll back just this key. On a mismatch NOTHING is buffered and
    /// [`CasError::Mismatch`] is returned — the key is left exactly as it was, so a
    /// failed CAS in a group never disturbs the other writes (partial-failure =
    /// independent, NOT all-or-nothing).
    fn stage_cas<S>(
        &mut self,
        key: &[u8],
        expected: Option<u64>,
        new: u64,
        store: &S,
    ) -> Result<Option<Mutation>, CasError>
    where
        S: NodeStore + ?Sized,
    {
        let actual = self.read_value(key, store)?;
        if actual != expected {
            return Err(CasError::Mismatch { expected, actual });
        }
        let prior = self.buffer.snapshot_entry(key);
        self.buffer.put(key, new.to_be_bytes());
        Ok(prior)
    }

    /// Conditionally and DURABLY apply a replicated write (active-active 2a-4).
    ///
    /// This is the receiver side of quorum-on-write. It reads the current visible
    /// value for `key`, hashes it, and compares the hash to `expected` (the
    /// proposing writer's CAS precondition; `None` means expect-absent). On a
    /// mismatch nothing is written and [`HashCasError::HashMismatch`] is returned —
    /// the CAS vote-against that fences a stale heal-mid-write proposal at a replica
    /// that has already moved on. On a match the value (with `ttl`) is buffered and
    /// committed in this same slice.
    ///
    /// The [`Self::commit`] call is the durability boundary: it persists the tree
    /// nodes to the [`crate::store::DiskStore`] (each node file is fsynced) and
    /// writes a fsynced committed-root marker into the WAL. Under the production
    /// `CommitOnly` WAL a plain `put_with_ttl` would only reach the OS page cache;
    /// committing here is what makes an `Ok` attest stable storage, so the caller
    /// can acknowledge `Applied` only AFTER this returns.
    ///
    /// # Epoch fence (AA-3-3, §2.3 — THE rule)
    ///
    /// BEFORE the CAS read, and in this SAME actor slice, the write's epoch
    /// (`stamp.epoch`) is checked against the shard's actor-local `self.promised`
    /// ballot:
    ///
    /// * `stamp.epoch < self.promised` → reject [`HashCasError::Fenced`], apply
    ///   NOTHING (no put, no commit). A stale/deposed owner is fenced — the §4
    ///   majority-intersection safety property.
    /// * `stamp.epoch >= self.promised` → run the existing CAS-compare → put →
    ///   commit. The data write does **NOT** mutate `self.promised` (R2 — standard
    ///   Paxos acceptor semantics: only a Prepare / [`Self::record_promise`] raises
    ///   `promised`). Accepting `>=` without raising is what stops an un-elected
    ///   high-epoch writer from fencing the true owner.
    ///
    /// Reading `self.promised` in the same `&mut self` slice as the CAS means there
    /// is no TOCTOU between the fence and the write (R8).
    ///
    /// # Commit stamp (AA-3-4a, §2.4)
    ///
    /// On a MATCH the value is stored in the STAMPED envelope carrying `stamp` —
    /// the IDENTICAL stamp the owner assigned (receiver: `(proposal.epoch,
    /// proposal.seq)`; proposer: its own `(live_epoch, seq)`), never one this
    /// replica invents (R-SEQ). The stamp is stored ALONGSIDE the value+ttl; the
    /// CAS hash (`current_value_hash`) is taken over the logical value bytes
    /// (stamp- and TTL-stripped), so the stamp does NOT enter the CAS identity.
    fn apply_durable<S>(
        &mut self,
        key: &[u8],
        expected: Option<Hash>,
        value: Vec<u8>,
        ttl: Option<Duration>,
        stamp: Stamp,
        store: &mut S,
    ) -> Result<(), HashCasError>
    where
        S: NodeStore + ?Sized,
    {
        self.apply_durable_kind(key, expected, ApplyKind::Value { value, ttl }, stamp, store)
    }

    /// Conditionally + durably apply a stamped TOMBSTONE (AA-3-4b, §2.4).
    ///
    /// A delete travels through the SAME fence + CAS + stamp + commit machinery as
    /// a put: the epoch fence runs first (a stale owner's delete is `Fenced`); the
    /// CAS compares `expected` against the current LOGICAL value hash (so a delete
    /// expecting the live value MATCHES, and a delete on an already-tombstoned key
    /// — whose logical hash is `None` — matches only `expected = None`); on a match
    /// a stamped tombstone is stored (a `Put` of the tombstone envelope) and
    /// fsynced. The tombstone PERSISTS and reads as absent (R-TOMB).
    fn apply_durable_tombstone<S>(
        &mut self,
        key: &[u8],
        expected: Option<Hash>,
        stamp: Stamp,
        store: &mut S,
    ) -> Result<(), HashCasError>
    where
        S: NodeStore + ?Sized,
    {
        self.apply_durable_kind(key, expected, ApplyKind::Tombstone, stamp, store)
    }

    /// Shared fence + CAS + stamped-commit core for a value or tombstone apply.
    fn apply_durable_kind<S>(
        &mut self,
        key: &[u8],
        expected: Option<Hash>,
        kind: ApplyKind,
        stamp: Stamp,
        store: &mut S,
    ) -> Result<(), HashCasError>
    where
        S: NodeStore + ?Sized,
    {
        // Targeted single-key rollback (PERF-003): `stage_apply_kind` buffers
        // exactly one key and returns its prior entry, a complete inverse on a
        // commit failure — no whole-buffer clone needed.
        let prior = self.stage_apply_kind(key, expected, kind, stamp, store)?;
        match self.commit(store) {
            Ok(_root) => Ok(()),
            Err(error) => {
                self.buffer.restore_entry(key, prior);
                Err(HashCasError::from(error))
            }
        }
    }

    /// Stage a fence-checked, CAS-checked stamped value/tombstone apply into the
    /// buffer WITHOUT committing (group-commit, audit E).
    ///
    /// Runs the epoch fence (§2.3) and then the CAS over the LOGICAL value hash
    /// through the live buffer+tree (so a write staged earlier in the same group is
    /// observed), and on a match buffers the stamped envelope. Returns that key's
    /// PRIOR buffered entry for a single-key rollback. On a fence or CAS rejection
    /// NOTHING is buffered and the error is returned — the key is left untouched, so
    /// a failed apply in a group never disturbs the other writes (partial-failure =
    /// independent, NOT all-or-nothing). As on the single-key path an admitted `>=`
    /// write does NOT raise `self.promised` (R2).
    fn stage_apply_kind<S>(
        &mut self,
        key: &[u8],
        expected: Option<Hash>,
        kind: ApplyKind,
        stamp: Stamp,
        store: &S,
    ) -> Result<Option<Mutation>, HashCasError>
    where
        S: NodeStore + ?Sized,
    {
        // --- Epoch fence (§2.3), BEFORE the CAS read, same actor slice. ---------
        // Reject a stale owner whose epoch is below what we have promised. We do
        // NOT raise `self.promised` on the accept path (R2): only `record_promise`
        // (a Prepare) advances it. `>=` is accepted as a plain Paxos acceptor.
        if stamp.epoch < self.promised {
            return Err(HashCasError::Fenced {
                promised: self.promised.clone(),
                attempted: stamp.epoch,
            });
        }
        // CAS over the LOGICAL value hash (a tombstone reads as None, so this is
        // the same identity for a put or a delete — the stamp/kind are never part
        // of the CAS).
        let actual = self.current_value_hash(key, store)?;
        if actual != expected {
            return Err(HashCasError::HashMismatch { expected, actual });
        }
        let prior = self.buffer.snapshot_entry(key);
        let encoded = match kind {
            ApplyKind::Value { value, ttl } => {
                encode_stamped_optional_ttl(value, stamp, ttl).map_err(tree_error)?
            }
            ApplyKind::Tombstone => encode_stamped_tombstone(stamp),
        };
        self.buffer.put(key, encoded);
        Ok(prior)
    }

    /// Conditionally + durably apply a BATCH of value puts under ONE shared
    /// `stamp`, ALL-OR-NOTHING, in ONE WAL commit/fsync (A1a — the actor-level
    /// foundation for a future replicated multi-key append).
    ///
    /// This is the multi-key generalisation of [`Self::apply_durable`]: the same
    /// epoch fence + per-key CAS (over the stamp-excluded LOGICAL value hash) +
    /// stamped commit, but the WHOLE batch is validated BEFORE anything is
    /// mutated, and every key is stored with the IDENTICAL `stamp` in a SINGLE
    /// atomic tree commit. Each item is `(key, expected, value, ttl)`: `expected`
    /// is that key's own CAS precondition (`None` = expect-absent).
    ///
    /// # All-or-nothing ordering (CRITICAL)
    ///
    /// 1. **Epoch fence ONCE for the whole batch** (§2.3): if `stamp.epoch <
    ///    self.promised` the ENTIRE batch is rejected with [`HashCasError::Fenced`]
    ///    and NOTHING is written — checked before any CAS read, in this same actor
    ///    slice, so there is no TOCTOU (R8). As on the single-key path, an admitted
    ///    `>=` write does NOT raise `self.promised` (R2).
    /// 2. **Per-key CAS, all checked BEFORE any buffering**: for EACH item the
    ///    current logical value hash (`current_value_hash`, stamp- AND TTL-stripped)
    ///    is compared against that item's `expected`. The FIRST mismatch rejects the
    ///    ENTIRE batch with [`HashCasError::HashMismatch`]; no key is buffered, so
    ///    there is no partial application.
    /// 3. **Single commit**: only after EVERY fence + CAS check passes is each key's
    ///    stamped put buffered, then [`Self::commit`] is called ONCE — so the whole
    ///    batch lands in one `batch_mutate` + one fsync'd committed-root marker. On a
    ///    commit error the buffer is rolled back to its pre-batch snapshot.
    ///
    /// An empty batch is a no-op: the fence is still checked (a stale owner is
    /// fenced even with nothing to write), but no commit is issued.
    ///
    /// Value puts only (the append use is write-once event puts + a seq-counter
    /// put). Batch deletes/tombstones are out of scope for this increment.
    fn apply_durable_batch<S>(
        &mut self,
        items: Vec<handle::BatchItem>,
        stamp: Stamp,
        store: &mut S,
    ) -> Result<(), HashCasError>
    where
        S: NodeStore + ?Sized,
    {
        // --- (1) Epoch fence ONCE, BEFORE any CAS read, same actor slice. -------
        // A stale/deposed owner is fenced for the WHOLE batch; write NOTHING. We do
        // NOT raise `self.promised` on the accept path (R2): only `record_promise`
        // (a Prepare) advances it.
        if stamp.epoch < self.promised {
            return Err(HashCasError::Fenced {
                promised: self.promised.clone(),
                attempted: stamp.epoch,
            });
        }

        // --- (2) Per-key CAS, ALL checked before ANY mutation. ------------------
        // The CAS is over the LOGICAL value hash (stamp/TTL excluded). The FIRST
        // mismatch rejects the ENTIRE batch — nothing is buffered, so the apply is
        // strictly all-or-nothing (no partial writes).
        for (key, expected, _value, _ttl) in &items {
            let actual = self.current_value_hash(key, store)?;
            if actual != *expected {
                return Err(HashCasError::HashMismatch {
                    expected: *expected,
                    actual,
                });
            }
        }

        // --- (3) Only after ALL checks pass: buffer every key + commit ONCE. ----
        // Every key is stored with the IDENTICAL stamp, in a single atomic
        // (one `batch_mutate` + one fsync) tree commit via `commit`.
        if items.is_empty() {
            return Ok(());
        }
        let previous_buffer = self.buffer.clone();
        for (key, _expected, value, ttl) in items {
            let encoded = match encode_stamped_optional_ttl(value, stamp.clone(), ttl) {
                Ok(encoded) => encoded,
                Err(error) => {
                    self.buffer = previous_buffer;
                    return Err(tree_error(error).into());
                }
            };
            self.buffer.put(key, encoded);
        }
        match self.commit(store) {
            Ok(_root) => Ok(()),
            Err(error) => {
                self.buffer = previous_buffer;
                Err(HashCasError::from(error))
            }
        }
    }

    /// Coalesce a run of groupable durable WRITES into ONE group commit (audit E,
    /// the fsync-amplification collapse).
    ///
    /// Each write in `writes` is staged into the buffer IN ORDER — its own
    /// precondition (CAS/fence) is checked through the live buffer+tree, so a write
    /// to a key staged earlier in the same group observes that earlier write,
    /// exactly as if the writes had run sequentially (each with its own monotonic
    /// stamp). Then ONE [`Self::commit`] fsyncs the whole group's final root, and
    /// the result is fanned to every survivor.
    ///
    /// # Partial failure is INDEPENDENT, not all-or-nothing
    ///
    /// A write whose CAS precondition mismatches (or whose stamp is fenced) buffers
    /// NOTHING and is recorded as [`GroupOutcome::Rejected`]; it does NOT abort the
    /// rest of the group. The survivors are committed together. This is explicitly
    /// different from [`Self::apply_durable_batch`]'s all-or-nothing abort.
    ///
    /// # Linearization + recovery
    ///
    /// The single [`Self::commit`] is the ONE WAL marker for the group: a crash
    /// BEFORE it leaves none of the group durable (they retry); a crash AFTER it
    /// leaves every survivor durable, at the one committed root. If the commit
    /// itself fails, every staged survivor's key is rolled back to its pre-group
    /// state (in reverse stage order, so the earliest snapshot — the true pre-group
    /// value — wins) and each survivor is told [`GroupOutcome::CommitFailed`], so no
    /// caller is told `Ok` for a write that is not durable.
    ///
    /// Returns one [`GroupOutcome`] per input write, in the SAME order.
    pub(super) fn apply_group<S>(
        &mut self,
        writes: Vec<GroupWrite>,
        store: &mut S,
    ) -> Vec<GroupOutcome>
    where
        S: NodeStore + ?Sized,
    {
        // Stage each write; remember survivors as (input_index, key, prior) so a
        // commit failure can roll back exactly the keys that were buffered.
        let mut outcomes: Vec<Option<GroupOutcome>> = Vec::with_capacity(writes.len());
        let mut staged: Vec<(usize, Vec<u8>, Option<Mutation>)> = Vec::with_capacity(writes.len());
        for write in writes {
            let index = outcomes.len();
            match self.stage_group_write(write, store) {
                Ok((key, prior)) => {
                    staged.push((index, key, prior));
                    outcomes.push(None); // filled in after the group commit.
                }
                Err(error) => outcomes.push(Some(GroupOutcome::Rejected(error))),
            }
        }

        // No survivor staged anything ⇒ no commit (no fsync at all). Every slot is
        // already a Rejected; finalise and return.
        if staged.is_empty() {
            return finalise_group_outcomes(outcomes);
        }

        // ONE commit/fsync for the whole group's final root.
        match self.commit(store) {
            Ok(_root) => {
                for (index, _key, _prior) in staged {
                    outcomes[index] = Some(GroupOutcome::Committed);
                }
            }
            Err(error) => {
                // A shared group-commit fault must reach EVERY staged survivor, but
                // the underlying `WalError` is not `Clone` (it can wrap an
                // `io::Error`). Render it ONCE to a message and fan that message to
                // each survivor as a WAL error — every survivor learns the commit
                // failed and retries (none is told `Ok`). The original error's
                // display string is preserved; only its variant is generalised to
                // `TreeError`, which is acceptable for a retryable group-commit
                // fault that the caller surfaces through `ShardError::Wal` either way.
                let message = error.to_string();
                // Roll back every staged key in REVERSE stage order, so the earliest
                // snapshot (the true pre-group value for a key written more than once
                // in the group) is restored last and wins.
                for (index, key, prior) in staged.into_iter().rev() {
                    self.buffer.restore_entry(&key, prior);
                    outcomes[index] = Some(GroupOutcome::CommitFailed(ShardError::Wal(
                        WalError::TreeError(message.clone()),
                    )));
                }
            }
        }
        finalise_group_outcomes(outcomes)
    }

    /// Stage one [`GroupWrite`] into the buffer (no commit), returning the touched
    /// key and its prior entry for rollback, or the precondition error.
    fn stage_group_write<S>(
        &mut self,
        write: GroupWrite,
        store: &S,
    ) -> Result<(Vec<u8>, Option<Mutation>), ShardError>
    where
        S: NodeStore + ?Sized,
    {
        match write {
            GroupWrite::Cas { key, expected, new } => {
                let prior = self.stage_cas(&key, expected, new, store)?;
                Ok((key, prior))
            }
            GroupWrite::ApplyValue {
                key,
                expected,
                value,
                ttl,
                stamp,
            } => {
                let prior = self.stage_apply_kind(
                    &key,
                    expected,
                    ApplyKind::Value { value, ttl },
                    stamp,
                    store,
                )?;
                Ok((key, prior))
            }
            GroupWrite::ApplyTombstone {
                key,
                expected,
                stamp,
            } => {
                let prior =
                    self.stage_apply_kind(&key, expected, ApplyKind::Tombstone, stamp, store)?;
                Ok((key, prior))
            }
        }
    }

    /// Snapshot this shard's election-relevant state in ONE in-slice read
    /// (AA-3-2). Used by the candidate to compute the mint floor and by the
    /// acceptor to populate a Promise's `accepted_epoch`/`committed_root`. Reading
    /// all four through the same actor command guarantees they are mutually
    /// consistent (no torn read across a concurrent Prepare or commit).
    #[must_use]
    pub fn promise_state(&self) -> PromiseState {
        PromiseState {
            promised: self.promised().clone(),
            owner_epoch: self.owner_epoch().cloned(),
            persisted_max_minted: self.persisted_max_minted(),
            committed_root: self.committed_root(),
        }
    }

    /// Read this shard's current actor-local promise ballot (AA-3-0).
    #[must_use]
    pub const fn promised(&self) -> &Ballot {
        &self.promised
    }

    /// Read this shard's current owner epoch, if elected (AA-3-0).
    #[must_use]
    pub const fn owner_epoch(&self) -> Option<&Ballot> {
        self.owner_epoch.as_ref()
    }

    /// Read this shard's highest persisted minted-ballot counter (R4).
    #[must_use]
    pub const fn persisted_max_minted(&self) -> u64 {
        self.persisted_max_minted
    }

    /// Snapshot the three promise values into a [`PromiseRecord`] for fsync.
    fn promise_snapshot(&self) -> PromiseRecord {
        PromiseRecord {
            promised: self.promised.clone(),
            owner_epoch: self.owner_epoch.clone(),
            persisted_max_minted: self.persisted_max_minted,
        }
    }

    /// Durably record a Prepare promise (design §2.2 / §3, used by 3-2's Promise
    /// reply). Monotonic: the promise is accepted ONLY if `b > promised`; an
    /// equal-or-lower ballot is a no-op that returns the current `promised` so the
    /// caller can Nack with it. `promised` is NEVER regressed — that invariant is
    /// what the §4 majority-intersection fence rests on, and it survives restart
    /// because the persisted value is reloaded into `promised` on boot.
    ///
    /// On accept the new snapshot is FSYNC'd (forced, via `append_promise`)
    /// BEFORE the in-memory `promised` is advanced and BEFORE return, so a crash
    /// can never leave a node having replied Promise without the ballot durable.
    pub fn record_promise(&mut self, ballot: Ballot) -> Result<RecordPromiseOutcome, WalError> {
        if ballot <= self.promised {
            return Ok(RecordPromiseOutcome::Rejected {
                promised: self.promised.clone(),
            });
        }
        let snapshot = PromiseRecord {
            promised: ballot.clone(),
            owner_epoch: self.owner_epoch.clone(),
            persisted_max_minted: self.persisted_max_minted,
        };
        self.wal.append_promise(&snapshot)?;
        self.promised = ballot;
        Ok(RecordPromiseOutcome::Promised)
    }

    /// Durably record the ballot under which this node was elected owner (design
    /// §2.2 / §3). FSYNC'd before the owner's first served write and before
    /// return, so a crash between election win and persist leaves the node NOT
    /// owning, never silently double-owning.
    pub fn record_owner_epoch(&mut self, ballot: Ballot) -> Result<(), WalError> {
        let mut snapshot = self.promise_snapshot();
        snapshot.owner_epoch = Some(ballot.clone());
        self.wal.append_promise(&snapshot)?;
        self.owner_epoch = Some(ballot);
        Ok(())
    }

    /// Durably reserve a minted ballot counter (R4, design §2.2 / §3, used by 3-2
    /// before sending Prepare). Persists `persisted_max_minted = max(self,
    /// counter)`, FSYNC before return, and returns the reserved (post-max) value.
    /// Guarantees a restarted candidate's next ballot strictly exceeds every
    /// ballot it ever minted — the persisted value is reloaded on boot, so the
    /// counter never regresses or is reused across a crash.
    pub fn reserve_minted(&mut self, counter: u64) -> Result<u64, WalError> {
        let reserved = self.persisted_max_minted.max(counter);
        if reserved == self.persisted_max_minted {
            // No advance needed; the persisted floor already dominates. Still a
            // durable value (it was fsync'd when first reserved), so just report it.
            return Ok(reserved);
        }
        let mut snapshot = self.promise_snapshot();
        snapshot.persisted_max_minted = reserved;
        self.wal.append_promise(&snapshot)?;
        self.persisted_max_minted = reserved;
        Ok(reserved)
    }

    /// Hash of the current visible value for `key`, or `None` if it is absent or
    /// expired. The hash is `blake3` of the read-visible value bytes (TTL stripped),
    /// matching what a proposing writer hashes for its CAS precondition.
    fn current_value_hash<S>(&self, key: &[u8], store: &S) -> Result<Option<Hash>, WalError>
    where
        S: NodeStore + ?Sized,
    {
        Ok(self.get(key, store)?.map(|value| Hash::of(&value)))
    }

    /// Inspect buffered mutations; exposed for tests and future shard wiring.
    #[must_use]
    pub const fn buffer(&self) -> &WalBuffer {
        &self.buffer
    }

    /// Return this actor's index-backed stream enumeration.
    pub(super) fn scan_sequences(&self) -> Result<Vec<handle::StreamSeq>, ShardError> {
        scan::scan_sequences(&self.live_streams, &self.stream_index_errors)
    }

    /// Return the pre-PERF-002 full-walk scan view for regression tests.
    #[cfg(test)]
    fn scan_sequences_full_walk<S>(&self, store: &S) -> Result<Vec<handle::StreamSeq>, ShardError>
    where
        S: NodeStore + ?Sized,
    {
        stream_index::full_walk_with_buffer(store, self.committed_root, &self.buffer)
    }

    fn read_sequence<S>(&self, seq_key: &[u8], store: &S) -> Result<u64, WalError>
    where
        S: NodeStore + ?Sized,
    {
        self.get(seq_key, store)?.map_or(Ok(0), |bytes| {
            bytes
                .as_slice()
                .try_into()
                .map(u64::from_be_bytes)
                .map_err(|_| WalError::TreeError("invalid sequence metadata".to_owned()))
        })
    }

    /// Export every content-addressed node reachable from this shard's committed
    /// root (AA-3-4 handoff merge, SOURCE side).
    ///
    /// Reuses the existing pull primitive
    /// [`find_missing_nodes`](crate::sync::find_missing_nodes) against an EMPTY
    /// target (`target_root = None`), so it returns the FULL reachable node set,
    /// ordered children-before-parents (a crash during the remote apply never makes
    /// a parent visible without its descendants). The requester then idempotently
    /// `put`s only the nodes it lacks and folds this shard's committed root into its
    /// handoff merge (§2.4). Returns `(source_root, transfers)`.
    pub fn export_reachable<S>(
        &self,
        shard_id: crate::branch::ShardId,
        store: &S,
    ) -> Result<(Option<Hash>, Vec<crate::sync::NodeTransfer>), WalError>
    where
        S: NodeStore + ?Sized,
    {
        let source_root = self.committed_root;
        let missing =
            crate::sync::find_missing_nodes(store, &EmptyTarget, shard_id, source_root, None)
                .map_err(|error| WalError::TreeError(error.to_string()))?;
        Ok((source_root, missing.transfers))
    }

    /// Merge a promise majority's committed states into the local committed
    /// baseline and durably adopt the merged root (AA-3-4d handoff merge, §2.4,
    /// TARGET side). This is the LOSSLESS reconstruction step a freshly-elected
    /// owner runs BEFORE serving.
    ///
    /// For each promiser contribution `(promiser_root, transfers)`:
    /// 1. Hash-verify every transfer node and idempotently `put` it into the local
    ///    store (content-addressed; a node already held is a no-op). A mismatch
    ///    fails the whole call — the node stays elected-but-not-live (fail-closed).
    /// 2. Fold `acc = merge_committed_union(acc, promiser_root, store)` — the
    ///    ancestor-free union + per-key max-stamp merge — starting from `acc =`
    ///    this shard's LOCAL committed root. `merge_committed_union` is commutative,
    ///    associative, and idempotent over the total order `(epoch, seq)`, so the
    ///    fold over all promisers (in any order) is order-independent and keeps
    ///    every committed write/delete from every promiser (the chain tip per key).
    ///
    /// The merged root is then committed DURABLY: the live buffer is discarded (a
    /// freshly-elected owner has no client-acked local buffer to keep — any prior
    /// buffer was either committed, hence reachable from a root, or in-doubt and not
    /// acked, §2.0) and the committed-root marker is fsync'd via
    /// [`DurableWal::commit`] so the adopted baseline survives a crash. Returns the
    /// adopted root.
    ///
    /// `seq` needs NO recovery from the merge: the owner's `live_epoch` (from
    /// `acquire_shard`) strictly exceeds every merged write's epoch, so the owner's
    /// first write `(live_epoch, 0)` dominates every merged entry (R-LE, §2.4).
    pub fn merge_adopt<S>(
        &mut self,
        promisers: &[(Option<Hash>, Vec<crate::sync::NodeTransfer>)],
        store: &mut S,
    ) -> Result<Option<Hash>, WalError>
    where
        S: NodeStore + ?Sized,
    {
        // acc starts at the LOCAL committed root: the local node is itself part of
        // the promise majority, so its own committed writes must never be dropped.
        let mut acc = self.committed_root;

        for (promiser_root, transfers) in promisers {
            // Step 1: pull this promiser's committed tree nodes into the local store.
            for transfer in transfers {
                let actual = transfer.node.hash();
                if actual != transfer.hash {
                    return Err(WalError::TreeError(format!(
                        "handoff node hash mismatch: expected {:?}, actual {actual:?}",
                        transfer.hash
                    )));
                }
                let stored = store.put(&transfer.node).map_err(tree_error)?;
                if stored != transfer.hash {
                    return Err(WalError::TreeError(format!(
                        "handoff store wrote {stored:?}, expected {:?}",
                        transfer.hash
                    )));
                }
            }

            // Step 2: fold this promiser's committed root into the running union.
            acc = crate::sync::merge_committed_union(acc, *promiser_root, store)
                .map_err(|error| WalError::TreeError(error.to_string()))?;
        }

        // Adopt the merged union as the durable committed baseline. Discard the
        // buffer so commit re-roots cleanly; a None merge (no committed data
        // anywhere) leaves the committed root untouched.
        let Some(root) = acc else {
            return Ok(self.committed_root);
        };
        self.buffer = WalBuffer::new();
        let stream_index = stream_index::rebuild(store, Some(root)).map_err(tree_error)?;
        // Tier-0 durability barrier (node-rename fsync fix): the pulled handoff
        // nodes and any union-merge nodes were just persisted; fsync their
        // distinct parent directories STRICTLY BEFORE the WAL marker so a crash
        // cannot leave the adopted committed root referencing nodes whose
        // directory entries are not durable. In-memory stores make this a no-op.
        store.sync_dirty_dirs().map_err(tree_error)?;
        self.wal.commit(root)?;
        self.committed_root = Some(root);
        self.live_streams = stream_index.live;
        self.stream_index_errors = stream_index.errors;
        Ok(Some(root))
    }
}

/// Resolve the per-write outcome slots into a dense result vector.
///
/// Every slot is `Some` by construction (each write is either rejected at staging
/// or finalised by the group commit), but the type is `Option` so the slots can be
/// filled out of order. A `None` slot would be an internal logic error rather than
/// a real durability outcome; rather than `unwrap`/`panic` (forbidden), it is
/// mapped to a retryable WAL error so the caller never reads it as `Ok`.
fn finalise_group_outcomes(outcomes: Vec<Option<GroupOutcome>>) -> Vec<GroupOutcome> {
    outcomes
        .into_iter()
        .map(|slot| {
            slot.unwrap_or_else(|| {
                GroupOutcome::CommitFailed(ShardError::Wal(WalError::TreeError(
                    "group-commit outcome was not resolved".to_owned(),
                )))
            })
        })
        .collect()
}

fn buffer_mutation(buffer: &mut WalBuffer, mutation: Mutation) {
    match mutation {
        Mutation::Put { key, value } => buffer.put(key, value),
        Mutation::Delete { key } => buffer.delete(key),
    }
}

fn event_key(key: &[u8], seq: u64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len().saturating_add(9));
    encoded.extend_from_slice(key);
    encoded.push(0);
    encoded.extend_from_slice(&seq.to_be_bytes());
    encoded
}

/// Suffix appended to a stream key to form its sequence-metadata key.
pub(super) const SEQ_SUFFIX: &[u8] = &[0xff, b's', b'e', b'q'];

fn sequence_key(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len().saturating_add(SEQ_SUFFIX.len()));
    encoded.extend_from_slice(key);
    encoded.extend_from_slice(SEQ_SUFFIX);
    encoded
}

/// Recover a stream key from an encoded sequence-metadata key, if the encoded
/// key carries the [`SEQ_SUFFIX`].
pub(super) fn decode_sequence_key(encoded: &[u8]) -> Option<&[u8]> {
    encoded
        .len()
        .checked_sub(SEQ_SUFFIX.len())
        .and_then(|split| {
            let (stream_key, suffix) = encoded.split_at(split);
            (suffix == SEQ_SUFFIX).then_some(stream_key)
        })
}

fn buffered_batch(buffer: &WalBuffer) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    buffer
        .iter()
        .map(|mutation| match mutation {
            Mutation::Put { key, value } => (key.clone(), Some(value.clone())),
            Mutation::Delete { key } => (key.clone(), None),
        })
        .collect()
}

fn store_empty_root<S>(store: &mut S) -> Result<Hash, WalError>
where
    S: NodeStore + ?Sized,
{
    let node = Node::Leaf(LeafNode::new(Vec::new()).map_err(tree_error)?);
    store.put(&node).map_err(tree_error)
}

fn encode_ttl_value(value: Vec<u8>, ttl: Option<Duration>) -> Result<Vec<u8>, WalError> {
    encode_optional_ttl(value, ttl).map_err(tree_error)
}

fn visible_optional_ttl_value(value: Option<Vec<u8>>) -> Result<Option<Vec<u8>>, WalError> {
    value.map_or(Ok(None), |value| visible_ttl_value(&value))
}

fn visible_ttl_value(value: &[u8]) -> Result<Option<Vec<u8>>, WalError> {
    match visible_value(value).map_err(tree_error)? {
        Visibility::Live(value) => Ok(Some(value)),
        Visibility::Expired => Ok(None),
    }
}

fn tree_error(error: impl std::fmt::Display) -> WalError {
    WalError::TreeError(error.to_string())
}

/// A target-node reader that reports EVERY node as absent (AA-3-4).
///
/// Driving [`find_missing_nodes`](crate::sync::find_missing_nodes) against this
/// makes the source export the FULL reachable set from its committed root, which
/// is exactly what handoff merge wants (§2.4): the new owner pulls every
/// promiser's complete committed tree and folds them, rather than computing a
/// divergence-aware diff.
struct EmptyTarget;

impl crate::sync::TargetNodeReader for EmptyTarget {
    fn read_target_node(
        &self,
        _hash: Hash,
    ) -> Result<Option<crate::sync::TargetNodeSummary>, crate::sync::SyncError> {
        Ok(None)
    }
}

#[cfg(test)]
#[path = "actor/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "actor/stream_index_tests.rs"]
mod stream_index_tests;

#[cfg(test)]
mod storage_tests {
    use super::ShardActor;
    use crate::store::MemoryStore;
    use crate::tree::{Hash, LeafNode, Node, batch_mutate};
    use crate::wal::{DurableWal, FsyncPolicy, LookupResult, WalEntry, WalError, WalRecovery};
    use std::path::{Path, PathBuf};

    #[derive(Debug)]
    struct TempWal {
        dir: tempfile::TempDir,
        path: PathBuf,
    }

    impl TempWal {
        fn path(&self) -> &Path {
            debug_assert!(self.path.starts_with(self.dir.path()));
            &self.path
        }
    }

    fn temp_path(name: &str) -> Result<TempWal, WalError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join(name);
        Ok(TempWal { dir, path })
    }

    fn empty_root(store: &mut MemoryStore) -> Result<Hash, WalError> {
        let leaf =
            LeafNode::new(Vec::new()).map_err(|error| WalError::TreeError(error.to_string()))?;
        Ok(store.put(&Node::Leaf(leaf)))
    }

    fn test_stamp(counter: u64, node: &str, seq: u64) -> crate::sync::ballot::Stamp {
        use crate::sync::ballot::{Ballot, Stamp};
        use crate::sync::topology::SyncNodeId;
        Stamp::new(Ballot::new(counter, SyncNodeId::new(node)), seq)
    }

    #[test]
    fn put_returns_ok_only_after_entry_is_written_to_wal() -> Result<(), WalError> {
        let temp = temp_path("actor-put.wal")?;
        let path = temp.path();
        let wal = DurableWal::new(path, FsyncPolicy::CommitOnly)?;
        let mut actor = ShardActor::new(wal);

        actor.put(b"event".to_vec(), b"payload".to_vec())?;

        assert_eq!(
            actor.buffer().get(b"event"),
            LookupResult::BufferedValue(b"payload".to_vec())
        );
        assert_eq!(
            DurableWal::read_file(path)?.entries(),
            &[WalEntry::put(b"event".to_vec(), b"payload".to_vec())]
        );
        Ok(())
    }

    /// AA-3-4b: a delete is now a STAMPED TOMBSTONE, not a bare key-removal. The
    /// WAL entry is a `Put` of the tombstone envelope (ASSERTION CHANGED from the
    /// pre-3-4b `WalEntry::delete`), the buffer holds a buffered VALUE (the
    /// tombstone bytes), and yet the key reads as absent (`get` → `None`). The
    /// tombstone persists in the tree — that is the whole point: a delete is a
    /// comparable, mergeable entry the §2.4 merge cannot resurrect.
    #[test]
    fn delete_writes_a_stamped_tombstone_to_wal_and_reads_as_absent() -> Result<(), WalError> {
        let temp = temp_path("actor-delete.wal")?;
        let path = temp.path();
        let wal = DurableWal::new(path, FsyncPolicy::CommitOnly)?;
        let mut actor = ShardActor::new(wal);
        let store = MemoryStore::new();

        let stamp = test_stamp(2, "owner", 0);
        actor.delete(b"event".to_vec(), stamp.clone())?;

        // The buffered entry is the tombstone envelope (a Put), and the WAL holds
        // that same Put — NOT a bare delete.
        let tombstone = crate::ttl::entry::encode_stamped_tombstone(stamp);
        assert_eq!(
            actor.buffer().get(b"event"),
            LookupResult::BufferedValue(tombstone.clone())
        );
        assert_eq!(
            DurableWal::read_file(path)?.entries(),
            &[WalEntry::put(b"event".to_vec(), tombstone)]
        );
        // It reads as absent (read-after-delete unchanged).
        assert_eq!(actor.get(b"event", &store)?, None);
        Ok(())
    }

    #[test]
    fn delete_if_expired_removes_only_expired_values() -> Result<(), WalError> {
        // The sweep's atomic re-check: delete_if_expired must remove an expired
        // entry but leave a live (or refreshed) one untouched. Falsifiable — an
        // unconditional delete would also drop the live "keep" value.
        let temp = temp_path("actor-delete-if-expired.wal")?;
        let wal = DurableWal::new(temp.path(), FsyncPolicy::CommitOnly)?;
        let mut actor = ShardActor::new(wal);
        let store = MemoryStore::new();

        // A live (never-expiring) value is NOT removed.
        actor.put(b"live".to_vec(), b"keep".to_vec())?;
        assert!(!actor.delete_if_expired(b"live", &store)?);
        assert_eq!(actor.get(b"live", &store)?, Some(b"keep".to_vec()));

        // An expired value IS removed.
        actor.put_with_ttl(
            b"gone".to_vec(),
            b"stale".to_vec(),
            Some(std::time::Duration::ZERO),
        )?;
        assert!(actor.delete_if_expired(b"gone", &store)?);
        assert_eq!(actor.get(b"gone", &store)?, None);

        // An absent key is a no-op.
        assert!(!actor.delete_if_expired(b"missing", &store)?);
        Ok(())
    }

    /// AA-3-4b R-TOMB GATE: the TTL sweep MUST NEVER physically remove a tombstone,
    /// at ANY clock — a swept tombstone is indistinguishable from never-written and
    /// would resurrect a committed delete on the next §2.4 merge. Drive the sweep
    /// against a tombstone and assert it stays put (still a tombstone in storage,
    /// stamp intact), while an actually-expired VALUE alongside it IS swept.
    #[test]
    fn r_tomb_sweep_never_removes_a_tombstone() -> Result<(), WalError> {
        let temp = temp_path("actor-rtomb.wal")?;
        let wal = DurableWal::new(temp.path(), FsyncPolicy::CommitOnly)?;
        let mut actor = ShardActor::new(wal);
        let store = MemoryStore::new();

        // A committed delete: a stamped tombstone under some epoch.
        let stamp = test_stamp(5, "owner", 9);
        actor.delete(b"tomb".to_vec(), stamp.clone())?;
        assert_eq!(actor.get(b"tomb", &store)?, None, "tombstone reads as None");

        // Drive the sweep over the tombstone: it must NOT be removed (R-TOMB),
        // regardless of the clock — a tombstone has no TTL and is immortal.
        assert!(
            !actor.delete_if_expired(b"tomb", &store)?,
            "R-TOMB: the sweep must NEVER remove a tombstone"
        );

        // The tombstone entry is STILL PRESENT in storage with its stamp intact.
        let raw = actor
            .get_raw(b"tomb", &store)?
            .ok_or_else(|| WalError::TreeError("tombstone vanished from storage".to_owned()))?;
        let decoded = crate::ttl::entry::StampedEntry::decode(&raw)
            .map_err(|error| WalError::TreeError(error.to_string()))?
            .ok_or_else(|| WalError::TreeError("tombstone is not a stamped entry".to_owned()))?;
        assert!(
            decoded.is_tombstone(),
            "the swept-over entry is still a tombstone"
        );
        assert_eq!(
            decoded.stamp(),
            &stamp,
            "the tombstone's stamp is intact after the sweep"
        );
        assert_eq!(actor.get(b"tomb", &store)?, None, "still reads as None");

        // CONTRAST — an actually-expired VALUE alongside it IS swept (local GC).
        actor.put_with_ttl(
            b"expired".to_vec(),
            b"stale".to_vec(),
            Some(std::time::Duration::ZERO),
        )?;
        assert!(
            actor.delete_if_expired(b"expired", &store)?,
            "an actually-expired value is still swept"
        );
        assert_eq!(actor.get(b"expired", &store)?, None);
        Ok(())
    }

    #[test]
    fn from_recovered_accepts_put_get_delete_and_appends_after_replayed_entries()
    -> Result<(), WalError> {
        let temp = temp_path("actor-resume.wal")?;
        let mut store = MemoryStore::new();
        let committed_root = empty_root(&mut store)?;
        let mut wal = DurableWal::new(temp.path(), FsyncPolicy::CommitOnly)?;
        wal.commit(committed_root)?;
        wal.append(&WalEntry::put(b"replayed".to_vec(), b"before".to_vec()))?;
        drop(wal);

        let recovered = WalRecovery::recover_path(temp.path(), &store)?;
        let wal = DurableWal::new(temp.path(), FsyncPolicy::CommitOnly)?;
        let mut actor = ShardActor::from_recovered(wal, recovered, &store)?;

        assert_eq!(actor.committed_root(), Some(committed_root));
        assert_eq!(actor.get(b"replayed", &store)?, Some(b"before".to_vec()));
        actor.put(b"new".to_vec(), b"after".to_vec())?;
        // AA-3-4b: a delete is a stamped tombstone (a Put of the tombstone
        // envelope), so the recovered WAL shows that Put, not a bare delete. The
        // key still reads as absent.
        let stamp = test_stamp(1, "owner", 0);
        actor.delete(b"replayed".to_vec(), stamp.clone())?;

        assert_eq!(actor.get(b"new", &store)?, Some(b"after".to_vec()));
        assert_eq!(actor.get(b"replayed", &store)?, None);
        let tombstone = crate::ttl::entry::encode_stamped_tombstone(stamp);
        assert_eq!(
            DurableWal::read_file(temp.path())?.entries(),
            &[
                WalEntry::put(b"replayed".to_vec(), b"before".to_vec()),
                WalEntry::put(b"new".to_vec(), b"after".to_vec()),
                WalEntry::put(b"replayed".to_vec(), tombstone),
            ]
        );
        Ok(())
    }

    #[test]
    fn commit_after_recovery_truncates_wal_updates_root_and_tree_reads() -> Result<(), WalError> {
        let temp = temp_path("actor-commit-after-recovery.wal")?;
        let mut store = MemoryStore::new();
        let committed_root = empty_root(&mut store)?;
        let mut wal = DurableWal::new(temp.path(), FsyncPolicy::CommitOnly)?;
        wal.commit(committed_root)?;
        wal.append(&WalEntry::put(b"event".to_vec(), b"payload".to_vec()))?;
        drop(wal);

        let recovered = WalRecovery::recover_path(temp.path(), &store)?;
        let wal = DurableWal::new(temp.path(), FsyncPolicy::CommitOnly)?;
        let mut actor = ShardActor::from_recovered(wal, recovered, &store)?;

        let new_root = actor.commit(&mut store)?;

        let contents = DurableWal::read_file(temp.path())?;
        assert_eq!(contents.committed_root(), Some(new_root));
        assert_eq!(contents.entries(), &[]);
        assert_eq!(actor.committed_root(), Some(new_root));
        assert!(actor.buffer().is_empty());
        assert_eq!(actor.get(b"event", &store)?, Some(b"payload".to_vec()));
        assert_ne!(new_root, committed_root);
        Ok(())
    }

    #[test]
    fn recovered_actor_matches_uncrashed_actor_after_same_commit() -> Result<(), WalError> {
        let crashed = temp_path("actor-crashed.wal")?;
        let uncrashed = temp_path("actor-uncrashed.wal")?;
        let mut crashed_store = MemoryStore::new();
        let mut uncrashed_store = MemoryStore::new();
        let crashed_root = empty_root(&mut crashed_store)?;
        let uncrashed_root = empty_root(&mut uncrashed_store)?;

        let mut crashed_wal = DurableWal::new(crashed.path(), FsyncPolicy::CommitOnly)?;
        crashed_wal.commit(crashed_root)?;
        crashed_wal.append(&WalEntry::put(b"k".to_vec(), b"v1".to_vec()))?;
        drop(crashed_wal);

        let recovered = WalRecovery::recover_path(crashed.path(), &crashed_store)?;
        let crashed_wal = DurableWal::new(crashed.path(), FsyncPolicy::CommitOnly)?;
        let mut recovered_actor =
            ShardActor::from_recovered(crashed_wal, recovered, &crashed_store)?;
        recovered_actor.put(b"k".to_vec(), b"v2".to_vec())?;
        let recovered_root = recovered_actor.commit(&mut crashed_store)?;

        let uncrashed_wal = DurableWal::new(uncrashed.path(), FsyncPolicy::CommitOnly)?;
        let mut uncrashed_actor = ShardActor::new(uncrashed_wal);
        let uncrashed_root = batch_mutate(
            &mut uncrashed_store,
            uncrashed_root,
            &[(b"k".to_vec(), Some(b"v2".to_vec()))],
        )
        .map_err(|error| WalError::TreeError(error.to_string()))?;
        uncrashed_actor.put(b"k".to_vec(), b"v2".to_vec())?;
        let committed_uncrashed_root = uncrashed_actor.commit(&mut uncrashed_store)?;

        assert_eq!(
            recovered_actor.get(b"k", &crashed_store)?,
            Some(b"v2".to_vec())
        );
        assert_eq!(committed_uncrashed_root, uncrashed_root);
        assert_eq!(recovered_root, committed_uncrashed_root);
        Ok(())
    }
}

/// Node-rename fsync durability fix: crash-injection tests for the parent-dir
/// barrier (Tier-0 correctness, found in the perf/durability audit).
///
/// The bug: `DiskStore` syncs each node file's DATA before the atomic rename that
/// publishes it, but never fsync'd the parent DIRECTORY. On power loss the
/// rename's directory entry can be lost even though the WAL marker (written
/// after) says the commit is durable, so recovery walks the committed root and
/// hits a node whose file is unreachable.
///
/// The fix: `ShardActor::commit` invokes `store.sync_dirty_dirs()` — fsyncing
/// each DISTINCT subdir that received a node this commit — STRICTLY BEFORE
/// `wal.commit` writes the marker.
///
/// HONEST scope: a true kernel power-loss can't be reproduced in a unit test, so
/// these tests simulate the precise failure window with a wrapper store. The
/// barrier seam is `sync_dirty_dirs`; the wrapper makes that seam either LOSSY
/// (drops the just-written node files, modelling lost directory entries) or
/// DURABLE (delegates to the real `DiskStore`). The pair proves the causal chain
/// the fix closes: a marker published over non-durable node dir entries makes
/// recovery fail, and the real barrier prevents exactly that.
#[cfg(test)]
mod node_dir_fsync_tests {
    use super::ShardActor;
    use crate::store::{DiskStore, MemoryStore, NodeStore};
    use crate::tree::{Hash, Node};
    use crate::wal::{DurableWal, FsyncPolicy, WalError, WalRecovery};
    use std::cell::RefCell;
    use std::path::PathBuf;
    use std::sync::Arc;

    /// A `DiskStore` wrapper whose `sync_dirty_dirs` barrier is configurable, used
    /// to model the power-loss window where node FILES are persisted but their
    /// parent DIRECTORY ENTRIES are not yet durable.
    ///
    /// Every `put` is forwarded to the real inner `DiskStore` (so the node bytes
    /// are genuinely written and content-addressed), and the node's path is
    /// recorded as "pending" since the last barrier. The barrier then either:
    ///
    /// * `lossy = true`: DELETES every pending node file — exactly the on-disk
    ///   state after a crash that lost the unsynced rename directory entries —
    ///   and does NOT call the real barrier. The marker the actor writes next
    ///   therefore references nodes that are not durably linked.
    /// * `lossy = false`: delegates to the real `DiskStore::sync_dirty_dirs`,
    ///   fsyncing the directory entries (the fix), so nothing is lost.
    #[derive(Debug)]
    struct CrashWindowStore {
        inner: DiskStore,
        lossy: bool,
        pending: RefCell<Vec<PathBuf>>,
        dir: PathBuf,
    }

    impl CrashWindowStore {
        fn new(dir: PathBuf, lossy: bool) -> Result<Self, WalError> {
            let inner =
                DiskStore::new(&dir).map_err(|error| WalError::TreeError(error.to_string()))?;
            Ok(Self {
                inner,
                lossy,
                pending: RefCell::new(Vec::new()),
                dir,
            })
        }

        fn node_path(&self, hash: &Hash) -> PathBuf {
            let hex = hash.to_string();
            let (prefix, file_name) = hex.split_at(2);
            self.dir.join(prefix).join(file_name)
        }
    }

    impl NodeStore for CrashWindowStore {
        type Error = crate::store::StoreError;

        fn get(&self, hash: &Hash) -> Result<Option<Arc<Node>>, Self::Error> {
            self.inner.get(hash)
        }

        fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
            let hash = self.inner.put(node)?;
            self.pending.borrow_mut().push(self.node_path(&hash));
            Ok(hash)
        }

        fn sync_dirty_dirs(&self) -> Result<(), Self::Error> {
            let pending = std::mem::take(&mut *self.pending.borrow_mut());
            if self.lossy {
                // Model the lost directory entries: the files written this commit
                // become unreachable, as if their renames never reached disk.
                for path in pending {
                    match std::fs::remove_file(&path) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(crate::store::StoreError::Io(error)),
                    }
                }
                Ok(())
            } else {
                // The fix: fsync the real directory entries.
                self.inner.sync_dirty_dirs()
            }
        }
    }

    /// Build an actor over a fresh on-disk WAL and the given store, commit one
    /// write, then re-open from the SAME WAL path with a FRESH COLD-CACHE
    /// `DiskStore` over the SAME node directory — the faithful crash-recovery
    /// shape: a new process reads only what is on disk, never a warm in-memory
    /// cache. Returns the committed root plus the recovery result (and the cold
    /// store, so the caller can read committed values back).
    fn commit_then_recover(
        store: &mut CrashWindowStore,
        wal_path: &std::path::Path,
        nodes_dir: &std::path::Path,
    ) -> Result<(Hash, DiskStore, Result<ShardActor, WalError>), WalError> {
        let wal = DurableWal::new(wal_path, FsyncPolicy::CommitOnly)?;
        let mut actor = ShardActor::new(wal);
        actor.put(b"durable-key".to_vec(), b"durable-value".to_vec())?;
        let committed_root = actor.commit(store)?;
        drop(actor); // close the WAL file: the "crash" boundary.

        // Recover through a COLD store over the same node dir: a warm cache would
        // mask a lost file, so this is what a real restarted process observes.
        // `recover_path` VERIFIES the committed root is present in the store — the
        // bug surfaces here as `MissingCommittedRoot`. We return THIS result (not
        // an unwrapped one) so the caller can assert the rejection.
        let cold = DiskStore::new(nodes_dir).map_err(|e| WalError::TreeError(e.to_string()))?;
        let actor = match WalRecovery::recover_path(wal_path, &cold) {
            Ok(recovered) => {
                let wal = DurableWal::new(wal_path, FsyncPolicy::CommitOnly)?;
                ShardActor::from_recovered(wal, recovered, &cold)
            }
            Err(error) => Err(error),
        };
        Ok((committed_root, cold, actor))
    }

    /// FALSIFIER: with a LOSSY barrier (directory entries lost on the simulated
    /// crash) the committed-root marker references nodes whose files are gone, so
    /// recovery MUST reject with `MissingCommittedRoot`. This is the bug the fix
    /// closes; if `commit` did not fsync the directory entries, production would
    /// behave exactly like this.
    #[test]
    fn lossy_dir_barrier_makes_recovery_reject_missing_committed_root() -> Result<(), WalError> {
        let dir = tempfile::tempdir()?;
        let wal = tempfile::tempdir()?;
        let wal_path = wal.path().join("shard.wal");
        let nodes_dir = dir.path().join("nodes");
        let mut store = CrashWindowStore::new(nodes_dir.clone(), true)?;

        let (committed_root, _cold, recovered) =
            commit_then_recover(&mut store, &wal_path, &nodes_dir)?;

        match recovered {
            Err(WalError::MissingCommittedRoot { root }) => {
                assert_eq!(
                    root, committed_root,
                    "recovery must name the marker's now-unreachable root"
                );
                Ok(())
            }
            Err(other) => Err(other),
            Ok(_actor) => Err(WalError::TreeError(
                "expected MissingCommittedRoot when the dir barrier loses node files, \
                 but recovery succeeded"
                    .to_owned(),
            )),
        }
    }

    /// THE FIX: with the DURABLE barrier (`sync_dirty_dirs` fsyncs the real
    /// directory entries) the node files survive the simulated crash, so recovery
    /// from the SAME WAL succeeds and reads the committed value back FROM DISK.
    #[test]
    fn durable_dir_barrier_lets_recovery_read_committed_value() -> Result<(), WalError> {
        let dir = tempfile::tempdir()?;
        let wal = tempfile::tempdir()?;
        let wal_path = wal.path().join("shard.wal");
        let nodes_dir = dir.path().join("nodes");
        let mut store = CrashWindowStore::new(nodes_dir.clone(), false)?;

        let (committed_root, cold, recovered) =
            commit_then_recover(&mut store, &wal_path, &nodes_dir)?;
        let actor = recovered?;

        assert_eq!(actor.committed_root(), Some(committed_root));
        assert_eq!(
            actor.get(b"durable-key", &cold)?,
            Some(b"durable-value".to_vec()),
            "the committed value must be readable from disk after recovery"
        );
        Ok(())
    }

    /// ORDERING: the barrier is invoked BEFORE the WAL marker. A store whose
    /// `sync_dirty_dirs` records the marker state at barrier time proves the
    /// directory fsync happens while the marker is still ABSENT, so a crash can
    /// never publish a marker over un-fenced directory entries.
    #[test]
    fn barrier_runs_strictly_before_the_wal_marker() -> Result<(), WalError> {
        /// Records whether the on-disk WAL already carried a committed-root marker
        /// at the instant the barrier ran. Correct ordering ⇒ no marker yet.
        #[derive(Debug)]
        struct OrderingStore {
            inner: MemoryStore,
            wal_path: PathBuf,
            marker_present_at_barrier: RefCell<Option<bool>>,
        }

        impl NodeStore for OrderingStore {
            type Error = std::convert::Infallible;

            fn get(&self, hash: &Hash) -> Result<Option<Arc<Node>>, Self::Error> {
                Ok(self.inner.get(hash))
            }

            fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
                Ok(self.inner.put(node))
            }

            fn sync_dirty_dirs(&self) -> Result<(), Self::Error> {
                // Read the on-disk WAL as it stands at barrier time. If ordering
                // is correct the marker has NOT been written yet.
                let present = DurableWal::read_file(&self.wal_path)
                    .ok()
                    .and_then(|contents| contents.committed_root())
                    .is_some();
                *self.marker_present_at_barrier.borrow_mut() = Some(present);
                Ok(())
            }
        }

        let wal_dir = tempfile::tempdir()?;
        let wal_path = wal_dir.path().join("shard.wal");
        let mut store = OrderingStore {
            inner: MemoryStore::new(),
            wal_path: wal_path.clone(),
            marker_present_at_barrier: RefCell::new(None),
        };
        let wal = DurableWal::new(&wal_path, FsyncPolicy::CommitOnly)?;
        let mut actor = ShardActor::new(wal);
        actor.put(b"k".to_vec(), b"v".to_vec())?;
        actor.commit(&mut store)?;

        let observed = *store.marker_present_at_barrier.borrow();
        assert_eq!(
            observed,
            Some(false),
            "the dir-sync barrier must run while the WAL marker is still absent \
             (strictly before wal.commit)"
        );
        // And after commit the marker IS present — proving the barrier preceded it.
        assert!(
            DurableWal::read_file(&wal_path)?.committed_root().is_some(),
            "the marker must be written by commit (after the barrier)"
        );
        Ok(())
    }
}

/// AA-3-0 GATE: durable promise-state crash-injection / recovery tests.
///
/// These are the falsifiable proof for increment 3-0. Each test persists promise
/// state through a real on-disk WAL, then SIMULATES A CRASH by dropping the WAL
/// and actor WITHOUT a clean shutdown and re-opening from the SAME on-disk path
/// via `WalRecovery::recover_path` + `ShardActor::from_recovered`. Recovery reads
/// the value back FROM DISK (not from retained in-memory state), so a regression
/// that only updated memory — never fsync'd the frame — fails here.
///
/// What is and is NOT exercised: this drives the lowest-level reopen-from-disk
/// path (WAL file + recovery + actor seed). It does NOT go through the beamr
/// process / `ShardHandle` queue, because the `shard` module is `pub(crate)` and
/// the actor is the durability boundary anyway — the mutators fsync inside the
/// actor slice, which the `ShardHandle`/native dispatch only forwards to. The
/// crash is a process-less `drop` of the WAL handle (closing the OS file) plus a
/// fresh `recover_path`, which is exactly the durability question: did the bytes
/// reach stable storage before the mutator returned?
#[cfg(test)]
mod promise_recovery_tests {
    use super::{Ballot, RecordPromiseOutcome, ShardActor};
    use crate::store::MemoryStore;
    use crate::sync::topology::SyncNodeId;
    use crate::wal::{DurableWal, FsyncPolicy, WalError, WalRecovery};
    use std::path::{Path, PathBuf};

    struct TempWal {
        _dir: tempfile::TempDir,
        path: PathBuf,
    }

    fn temp_wal() -> Result<TempWal, WalError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("shard.wal");
        Ok(TempWal { _dir: dir, path })
    }

    fn ballot(counter: u64, node: &str) -> Ballot {
        Ballot::new(counter, SyncNodeId::from(node))
    }

    /// Re-open the actor from the SAME on-disk WAL, simulating a crash recovery.
    /// A FRESH `MemoryStore` is used so no committed-tree state can leak across
    /// the "crash"; these tests never commit data, so the store is only consulted
    /// (and finds nothing) for the absent committed-root verification.
    fn reopen(path: &Path) -> Result<ShardActor, WalError> {
        let store = MemoryStore::new();
        let recovered = WalRecovery::recover_path(path, &store)?;
        let wal = DurableWal::new(path, FsyncPolicy::CommitOnly)?;
        ShardActor::from_recovered(wal, recovered, &store)
    }

    /// (a) A persisted promise survives a crash, and (b) a lower ballot is
    /// rejected after restart — monotonicity survives the crash.
    #[test]
    fn promise_is_durable_and_monotonic_across_crash() -> Result<(), WalError> {
        let temp = temp_wal()?;

        // record_promise((5,X)) returns -> the ballot must be on stable storage.
        {
            let wal = DurableWal::new(&temp.path, FsyncPolicy::CommitOnly)?;
            let mut actor = ShardActor::new(wal);
            assert_eq!(
                actor.record_promise(ballot(5, "X"))?,
                RecordPromiseOutcome::Promised
            );
            // SIMULATE CRASH: drop the WAL handle + actor with no clean shutdown.
            drop(actor);
        }

        // (a) Recovery yields promised == (5,X) — read back FROM DISK.
        let mut recovered = reopen(&temp.path)?;
        assert_eq!(
            recovered.promised(),
            &ballot(5, "X"),
            "a returned record_promise must survive a crash"
        );

        // (b) A lower ballot (3,Y) is rejected/no-op and never regresses promised.
        let outcome = recovered.record_promise(ballot(3, "Y"))?;
        assert_eq!(
            outcome,
            RecordPromiseOutcome::Rejected {
                promised: ballot(5, "X")
            },
            "promised must never regress below a persisted ballot after restart"
        );
        assert_eq!(recovered.promised(), &ballot(5, "X"), "promised unchanged");

        // And a STRICTLY higher ballot is still accepted post-restart.
        assert_eq!(
            recovered.record_promise(ballot(6, "A"))?,
            RecordPromiseOutcome::Promised
        );
        drop(recovered);
        let again = reopen(&temp.path)?;
        assert_eq!(again.promised(), &ballot(6, "A"), "higher ballot persisted");
        Ok(())
    }

    /// (c) `reserve_minted(7)` survives a crash and the next reserved counter
    /// strictly exceeds it — no ballot regress across restart (R4).
    #[test]
    fn reserved_minted_counter_never_regresses_across_crash() -> Result<(), WalError> {
        let temp = temp_wal()?;

        {
            let wal = DurableWal::new(&temp.path, FsyncPolicy::CommitOnly)?;
            let mut actor = ShardActor::new(wal);
            assert_eq!(actor.reserve_minted(7)?, 7);
            // SIMULATE CRASH.
            drop(actor);
        }

        // Reopen FROM DISK: persisted_max_minted >= 7.
        let mut recovered = reopen(&temp.path)?;
        assert!(
            recovered.persisted_max_minted() >= 7,
            "reserved minted counter must survive a crash"
        );

        // A lower request never lowers the floor (idempotent max).
        assert_eq!(
            recovered.reserve_minted(4)?,
            7,
            "lower request keeps the floor"
        );
        assert_eq!(recovered.persisted_max_minted(), 7);

        // The NEXT mint floor strictly exceeds the persisted value: minting
        // (persisted+1) reserves 8 > 7, so no ballot it minted can be reused.
        let next = recovered.persisted_max_minted() + 1;
        assert_eq!(recovered.reserve_minted(next)?, 8);
        assert!(
            recovered.persisted_max_minted() >= next,
            "next reserved counter must strictly exceed the prior persisted floor"
        );
        drop(recovered);
        let again = reopen(&temp.path)?;
        assert_eq!(
            again.persisted_max_minted(),
            8,
            "advance persisted across crash"
        );
        Ok(())
    }

    /// `owner_epoch` is durable across a crash too (design §3 value 2).
    #[test]
    fn owner_epoch_is_durable_across_crash() -> Result<(), WalError> {
        let temp = temp_wal()?;
        {
            let wal = DurableWal::new(&temp.path, FsyncPolicy::CommitOnly)?;
            let mut actor = ShardActor::new(wal);
            actor.record_owner_epoch(ballot(4, "owner"))?;
            drop(actor);
        }
        let recovered = reopen(&temp.path)?;
        assert_eq!(recovered.owner_epoch(), Some(&ballot(4, "owner")));
        Ok(())
    }

    /// All three values co-persist in one snapshot and survive together: a
    /// promise, then an owner epoch, then a mint reservation, then crash → all
    /// three recover. Proves the full-snapshot frame reconstructs the latest of
    /// each field (not just the last-written one).
    #[test]
    fn all_three_values_co_persist_across_crash() -> Result<(), WalError> {
        let temp = temp_wal()?;
        {
            let wal = DurableWal::new(&temp.path, FsyncPolicy::CommitOnly)?;
            let mut actor = ShardActor::new(wal);
            assert_eq!(
                actor.record_promise(ballot(2, "P"))?,
                RecordPromiseOutcome::Promised
            );
            actor.record_owner_epoch(ballot(2, "P"))?;
            assert_eq!(actor.reserve_minted(5)?, 5);
            drop(actor);
        }
        let recovered = reopen(&temp.path)?;
        assert_eq!(recovered.promised(), &ballot(2, "P"));
        assert_eq!(recovered.owner_epoch(), Some(&ballot(2, "P")));
        assert_eq!(recovered.persisted_max_minted(), 5);
        Ok(())
    }

    /// A promise frame survives a commit truncation (the §3 fsync-domain hazard):
    /// after promise state is persisted, a data commit rewrites the WAL to just
    /// the marker — the writer must re-emit the promise snapshot so a later crash
    /// still recovers it. The same `store` carries the committed tree across the
    /// "crash" (in production this is the on-disk `DiskStore`), so committed-root
    /// verification passes and the only question is whether the promise re-emit
    /// survived.
    #[test]
    fn promise_survives_commit_truncation_and_crash() -> Result<(), WalError> {
        let temp = temp_wal()?;
        let mut store = MemoryStore::new();
        {
            let wal = DurableWal::new(&temp.path, FsyncPolicy::CommitOnly)?;
            let mut actor = ShardActor::new(wal);
            assert_eq!(
                actor.record_promise(ballot(9, "Z"))?,
                RecordPromiseOutcome::Promised
            );
            // A data write + commit truncates the WAL to the committed-root marker.
            actor.put(b"k".to_vec(), b"v".to_vec())?;
            let _root = actor.commit(&mut store)?;
            // SIMULATE CRASH: drop the WAL + actor with no clean shutdown.
            drop(actor);
        }
        // Reopen FROM DISK against the store holding the committed tree.
        let recovered = WalRecovery::recover_path(&temp.path, &store)?;
        let wal = DurableWal::new(&temp.path, FsyncPolicy::CommitOnly)?;
        let actor = ShardActor::from_recovered(wal, recovered, &store)?;
        assert_eq!(
            actor.promised(),
            &ballot(9, "Z"),
            "promise must survive a commit truncation + crash (re-emit after marker)"
        );
        Ok(())
    }
}

/// Group commit (haematite hot-path audit item E): coalescing, INDEPENDENT
/// partial-failure, and crash-injection atomicity tests for [`ShardActor::apply_group`].
///
/// These drive the actor directly (not the beamr process), because the group
/// commit's correctness lives in the actor: `apply_group` stages each write into
/// the buffer in order (CAS reading through the buffer), does ONE `commit`/fsync
/// for the survivors, and fans the outcome to each write. The native handler only
/// pops the consecutive run and forwards each outcome to its reply channel.
#[cfg(test)]
mod group_commit_tests {
    use super::{GroupOutcome, GroupWrite, ShardActor};
    use crate::shard::actor::handle::ShardError;
    use crate::store::{DiskStore, MemoryStore, NodeStore};
    use crate::sync::ballot::{Ballot, Stamp};
    use crate::sync::topology::SyncNodeId;
    use crate::tree::{Hash, LeafNode, Node};
    use crate::wal::{DurableWal, FsyncPolicy, WalError, WalRecovery};
    use std::cell::Cell;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    /// A `NodeStore` wrapper that counts how many times the commit barrier
    /// (`sync_dirty_dirs`) runs — exactly ONCE per [`ShardActor::commit`], strictly
    /// before the WAL marker — so a test can assert N writes coalesced into ONE
    /// commit. The inner store is a real [`MemoryStore`] so reads/writes behave
    /// normally.
    #[derive(Debug)]
    struct CommitCountingStore {
        inner: MemoryStore,
        commits: Cell<usize>,
    }

    impl CommitCountingStore {
        fn new() -> Self {
            Self {
                inner: MemoryStore::new(),
                commits: Cell::new(0),
            }
        }

        fn commit_count(&self) -> usize {
            self.commits.get()
        }
    }

    impl NodeStore for CommitCountingStore {
        type Error = std::convert::Infallible;

        fn get(&self, hash: &Hash) -> Result<Option<Arc<Node>>, Self::Error> {
            Ok(self.inner.get(hash))
        }

        fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
            Ok(self.inner.put(node))
        }

        fn sync_dirty_dirs(&self) -> Result<(), Self::Error> {
            self.commits.set(self.commits.get().saturating_add(1));
            Ok(())
        }
    }

    fn stamp(counter: u64, node: &str, seq: u64) -> Stamp {
        Stamp::new(Ballot::new(counter, SyncNodeId::new(node)), seq)
    }

    fn apply_value(key: &[u8], expected: Option<Hash>, value: &[u8], stamp: Stamp) -> GroupWrite {
        GroupWrite::ApplyValue {
            key: key.to_vec(),
            expected,
            value: value.to_vec(),
            ttl: None,
            stamp,
        }
    }

    /// COALESCING: N consecutive same-shard durable writes go through ONE commit
    /// (one barrier/fsync), every write replies `Committed`, and every value is
    /// readable at the single committed root.
    ///
    /// Non-vacuous: it asserts the commit count is exactly 1 for a 3-write group —
    /// the pre-group behaviour (commit-per-write) would record 3. It also reads all
    /// three values back, and proves the LAST-staged write to a repeated key wins
    /// (CAS-through-buffer ordering), which a regression that committed each write
    /// independently or reordered the group would break.
    #[test]
    fn group_of_writes_coalesces_into_one_commit_all_readable() -> Result<(), WalError> {
        let dir = tempfile::tempdir()?;
        let wal = DurableWal::new(dir.path().join("group.wal"), FsyncPolicy::CommitOnly)?;
        let mut actor = ShardActor::new(wal);
        let mut store = CommitCountingStore::new();

        // Four writes, two of them to the SAME key (k1) so the second must observe
        // the first through the buffer and win.
        let writes = vec![
            apply_value(b"k1", None, b"first", stamp(1, "owner", 0)),
            apply_value(b"k2", None, b"v2", stamp(1, "owner", 1)),
            GroupWrite::Cas {
                key: b"counter".to_vec(),
                expected: None,
                new: 7,
            },
            // k1 again: expects the in-group value's hash, then overwrites it.
            apply_value(
                b"k1",
                Some(Hash::of(b"first")),
                b"second",
                stamp(1, "owner", 2),
            ),
        ];
        let outcomes = actor.apply_group(writes, &mut store);

        assert_eq!(outcomes.len(), 4);
        for outcome in &outcomes {
            assert!(
                matches!(outcome, GroupOutcome::Committed),
                "every write in a clean group must commit, got {outcome:?}"
            );
        }
        // THE coalescing assertion: ONE commit for the whole group.
        assert_eq!(
            store.commit_count(),
            1,
            "N grouped writes must produce exactly ONE commit/fsync"
        );

        // All values are readable at the single committed root; the repeated-key
        // write that ran last (CAS-through-buffer) won.
        assert_eq!(actor.get(b"k1", &store)?, Some(b"second".to_vec()));
        assert_eq!(actor.get(b"k2", &store)?, Some(b"v2".to_vec()));
        assert_eq!(actor.read_value(b"counter", &store)?, Some(7));
        assert!(
            actor.committed_root().is_some(),
            "the group committed a root"
        );
        assert!(actor.buffer().is_empty(), "commit cleared the buffer");
        Ok(())
    }

    /// PARTIAL FAILURE is INDEPENDENT, not all-or-nothing: a group where ONE write
    /// has a failing CAS precondition replies `Err` to ONLY that write; the others
    /// commit and are readable, and the failed key is unchanged.
    ///
    /// Non-vacuous: it pre-seeds `mid` so the middle write's expect-absent CAS
    /// mismatches, then asserts the FIRST and LAST writes (whose CAS passes) ARE
    /// committed and readable while `mid` keeps its seeded value. A regression that
    /// reused the all-or-nothing batch abort would leave the survivors absent.
    #[test]
    fn partial_failure_commits_survivors_and_leaves_failed_key_unchanged() -> Result<(), WalError> {
        let dir = tempfile::tempdir()?;
        let wal = DurableWal::new(dir.path().join("partial.wal"), FsyncPolicy::CommitOnly)?;
        let mut actor = ShardActor::new(wal);
        let mut store = CommitCountingStore::new();

        // Pre-seed `mid` with a committed value (its own commit) so an expect-absent
        // CAS on it inside the group mismatches.
        let seeded = actor.apply_group(
            vec![apply_value(b"mid", None, b"seeded", stamp(1, "owner", 0))],
            &mut store,
        );
        assert!(matches!(seeded.as_slice(), [GroupOutcome::Committed]));
        let commits_after_seed = store.commit_count();

        // Group: ok1 (expect-absent: passes), mid (expect-absent: MISMATCHES, it is
        // present), ok2 (expect-absent: passes). Only `mid` must fail.
        let writes = vec![
            apply_value(b"ok1", None, b"a", stamp(2, "owner", 0)),
            apply_value(b"mid", None, b"should-not-apply", stamp(2, "owner", 1)),
            apply_value(b"ok2", None, b"b", stamp(2, "owner", 2)),
        ];
        let outcomes = actor.apply_group(writes, &mut store);

        assert!(
            matches!(outcomes[0], GroupOutcome::Committed),
            "survivor ok1 must commit, got {:?}",
            outcomes[0]
        );
        assert!(
            matches!(
                &outcomes[1],
                GroupOutcome::Rejected(ShardError::CasHashMismatch { expected, actual })
                    if expected.is_none() && *actual == Some(Hash::of(b"seeded"))
            ),
            "only the failed CAS write is Rejected, got {:?}",
            outcomes[1]
        );
        assert!(
            matches!(outcomes[2], GroupOutcome::Committed),
            "survivor ok2 must commit, got {:?}",
            outcomes[2]
        );

        // The survivors committed together in ONE commit (not one each, not zero).
        assert_eq!(
            store.commit_count(),
            commits_after_seed.saturating_add(1),
            "the survivors share exactly one group commit"
        );

        // Survivors readable; the failed key UNCHANGED (still the seeded value).
        assert_eq!(actor.get(b"ok1", &store)?, Some(b"a".to_vec()));
        assert_eq!(actor.get(b"ok2", &store)?, Some(b"b".to_vec()));
        assert_eq!(
            actor.get(b"mid", &store)?,
            Some(b"seeded".to_vec()),
            "a failed CAS must leave the buffer as if that write never happened"
        );
        Ok(())
    }

    /// PARTIAL FAILURE where EVERY write fails: no survivor, so NO commit (no fsync
    /// at all), and every write is Rejected. Proves the group never commits an empty
    /// survivor set.
    #[test]
    fn all_writes_fail_means_no_commit_at_all() -> Result<(), WalError> {
        let dir = tempfile::tempdir()?;
        let wal = DurableWal::new(dir.path().join("allfail.wal"), FsyncPolicy::CommitOnly)?;
        let mut actor = ShardActor::new(wal);
        let mut store = CommitCountingStore::new();

        // Seed both keys so two expect-absent CAS writes both mismatch.
        actor.apply_group(
            vec![
                apply_value(b"a", None, b"x", stamp(1, "owner", 0)),
                apply_value(b"b", None, b"y", stamp(1, "owner", 1)),
            ],
            &mut store,
        );
        let commits_after_seed = store.commit_count();

        let outcomes = actor.apply_group(
            vec![
                apply_value(b"a", None, b"nope", stamp(2, "owner", 0)),
                apply_value(b"b", None, b"nope", stamp(2, "owner", 1)),
            ],
            &mut store,
        );
        assert!(outcomes.iter().all(|o| matches!(
            o,
            GroupOutcome::Rejected(ShardError::CasHashMismatch { .. })
        )));
        assert_eq!(
            store.commit_count(),
            commits_after_seed,
            "an all-rejected group must not commit (no fsync)"
        );
        Ok(())
    }

    // --- Crash injection: a group commit is ONE atomic WAL marker. ---------------
    //
    // Reuses the `CrashWindowStore` design from `node_dir_fsync_tests`: every node
    // `put` goes to a real inner `DiskStore`, and the `sync_dirty_dirs` barrier is
    // either LOSSY (deletes the just-written node files, modelling lost directory
    // entries — the on-disk state after a crash that lost the unsynced renames) or
    // DURABLE (delegates to the real fsync). The group commit calls the barrier
    // ONCE, strictly before the single WAL marker, so this models a crash in the
    // one-marker window for the WHOLE group.

    #[derive(Debug)]
    struct CrashWindowStore {
        inner: DiskStore,
        lossy: bool,
        pending: std::cell::RefCell<Vec<PathBuf>>,
        dir: PathBuf,
    }

    impl CrashWindowStore {
        fn new(dir: PathBuf, lossy: bool) -> Result<Self, WalError> {
            let inner =
                DiskStore::new(&dir).map_err(|error| WalError::TreeError(error.to_string()))?;
            Ok(Self {
                inner,
                lossy,
                pending: std::cell::RefCell::new(Vec::new()),
                dir,
            })
        }

        fn node_path(&self, hash: &Hash) -> PathBuf {
            let hex = hash.to_string();
            let (prefix, file_name) = hex.split_at(2);
            self.dir.join(prefix).join(file_name)
        }
    }

    impl NodeStore for CrashWindowStore {
        type Error = crate::store::StoreError;

        fn get(&self, hash: &Hash) -> Result<Option<Arc<Node>>, Self::Error> {
            self.inner.get(hash)
        }

        fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
            let hash = self.inner.put(node)?;
            self.pending.borrow_mut().push(self.node_path(&hash));
            Ok(hash)
        }

        fn sync_dirty_dirs(&self) -> Result<(), Self::Error> {
            let pending = std::mem::take(&mut *self.pending.borrow_mut());
            if self.lossy {
                for path in pending {
                    match std::fs::remove_file(&path) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(crate::store::StoreError::Io(error)),
                    }
                }
                Ok(())
            } else {
                self.inner.sync_dirty_dirs()
            }
        }
    }

    fn empty_root(store: &mut CrashWindowStore) -> Result<Hash, WalError> {
        let leaf =
            LeafNode::new(Vec::new()).map_err(|error| WalError::TreeError(error.to_string()))?;
        store
            .put(&Node::Leaf(leaf))
            .map_err(|error| WalError::TreeError(error.to_string()))
    }

    /// Apply a 3-write group, then re-open from the SAME WAL path with a FRESH
    /// cold-cache `DiskStore` over the SAME node dir — the faithful crash-recovery
    /// shape. Returns the cold store and the recovery result.
    fn group_then_recover(
        store: &mut CrashWindowStore,
        wal_path: &Path,
        nodes_dir: &Path,
    ) -> Result<(DiskStore, Result<ShardActor, WalError>), WalError> {
        let wal = DurableWal::new(wal_path, FsyncPolicy::CommitOnly)?;
        let mut actor = ShardActor::new(wal);
        let outcomes = actor.apply_group(
            vec![
                apply_value(b"g1", None, b"v1", stamp(1, "owner", 0)),
                apply_value(b"g2", None, b"v2", stamp(1, "owner", 1)),
                apply_value(b"g3", None, b"v3", stamp(1, "owner", 2)),
            ],
            store,
        );
        assert!(
            outcomes
                .iter()
                .all(|o| matches!(o, GroupOutcome::Committed)),
            "the group must have committed before the crash"
        );
        drop(actor); // close the WAL file: the "crash" boundary.

        let cold = DiskStore::new(nodes_dir).map_err(|e| WalError::TreeError(e.to_string()))?;
        let actor = match WalRecovery::recover_path(wal_path, &cold) {
            Ok(recovered) => {
                let wal = DurableWal::new(wal_path, FsyncPolicy::CommitOnly)?;
                ShardActor::from_recovered(wal, recovered, &cold)
            }
            Err(error) => Err(error),
        };
        Ok((cold, actor))
    }

    /// FALSIFIER (crash BEFORE the durable marker window closes): a LOSSY barrier
    /// loses the group's node directory entries, so the single committed-root marker
    /// references nodes whose files are gone and recovery MUST reject with
    /// `MissingCommittedRoot` — recovery never sees a torn/partial group root.
    #[test]
    fn group_commit_with_lossy_barrier_is_rejected_never_partial() -> Result<(), WalError> {
        let dir = tempfile::tempdir()?;
        let wal = tempfile::tempdir()?;
        let wal_path = wal.path().join("group.wal");
        let nodes_dir = dir.path().join("nodes");
        let mut store = CrashWindowStore::new(nodes_dir.clone(), true)?;
        empty_root(&mut store)?;

        let (_cold, recovered) = group_then_recover(&mut store, &wal_path, &nodes_dir)?;
        match recovered {
            Err(WalError::MissingCommittedRoot { .. }) => Ok(()),
            Err(other) => Err(other),
            Ok(_actor) => Err(WalError::TreeError(
                "expected MissingCommittedRoot when the group's node dir entries are lost, \
                 but recovery succeeded"
                    .to_owned(),
            )),
        }
    }

    /// THE FIX (crash AFTER the marker is durable): a DURABLE barrier keeps every
    /// group node, so recovery from the SAME WAL succeeds and reads back ALL the
    /// group's survivors FROM DISK — all-or-none, never a subset.
    #[test]
    fn group_commit_with_durable_barrier_recovers_all_survivors() -> Result<(), WalError> {
        let dir = tempfile::tempdir()?;
        let wal = tempfile::tempdir()?;
        let wal_path = wal.path().join("group.wal");
        let nodes_dir = dir.path().join("nodes");
        let mut store = CrashWindowStore::new(nodes_dir.clone(), false)?;
        empty_root(&mut store)?;

        let (cold, recovered) = group_then_recover(&mut store, &wal_path, &nodes_dir)?;
        let actor = recovered?;
        assert_eq!(actor.get(b"g1", &cold)?, Some(b"v1".to_vec()));
        assert_eq!(actor.get(b"g2", &cold)?, Some(b"v2".to_vec()));
        assert_eq!(actor.get(b"g3", &cold)?, Some(b"v3".to_vec()));
        Ok(())
    }

    /// COMMIT-FAILURE rollback: when the shared group commit fails, every staged
    /// survivor's key is rolled back (buffer left as pre-group) and each survivor is
    /// told the commit failed — none is told `Ok`. Modelled with a store whose
    /// `put` fails on the Nth node so `commit`'s `batch_mutate` errors.
    #[test]
    fn group_commit_failure_rolls_back_all_survivors() -> Result<(), WalError> {
        /// A store that returns an error from `put` once it has been called
        /// `fail_after` times, to force a `commit` failure mid-batch.
        #[derive(Debug)]
        struct FailingStore {
            inner: MemoryStore,
            puts: Cell<usize>,
            fail_after: usize,
        }

        impl NodeStore for FailingStore {
            type Error = crate::store::StoreError;

            fn get(&self, hash: &Hash) -> Result<Option<Arc<Node>>, Self::Error> {
                Ok(self.inner.get(hash))
            }

            fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
                let count = self.puts.get().saturating_add(1);
                self.puts.set(count);
                if count > self.fail_after {
                    return Err(crate::store::StoreError::Io(std::io::Error::other(
                        "injected commit failure",
                    )));
                }
                Ok(self.inner.put(node))
            }
        }

        let dir = tempfile::tempdir()?;
        let wal = DurableWal::new(dir.path().join("fail.wal"), FsyncPolicy::CommitOnly)?;
        let mut actor = ShardActor::new(wal);
        // Allow the empty-root put; fail the first tree-building put in commit.
        let mut store = FailingStore {
            inner: MemoryStore::new(),
            puts: Cell::new(0),
            fail_after: 0,
        };

        let outcomes = actor.apply_group(
            vec![
                apply_value(b"x", None, b"vx", stamp(1, "owner", 0)),
                apply_value(b"y", None, b"vy", stamp(1, "owner", 1)),
            ],
            &mut store,
        );
        // Both staged cleanly (their CAS passed) but the SHARED commit failed, so
        // both are CommitFailed — never Committed.
        assert_eq!(outcomes.len(), 2);
        for outcome in &outcomes {
            assert!(
                matches!(outcome, GroupOutcome::CommitFailed(_)),
                "a failed group commit must tell every survivor CommitFailed, got {outcome:?}"
            );
        }
        // The buffer was rolled back to its pre-group (empty) state, so a later read
        // through a healthy store sees nothing committed.
        assert!(
            actor.buffer().is_empty(),
            "a failed group commit rolls back every staged survivor's key"
        );
        assert_eq!(actor.committed_root(), None, "nothing was committed");
        Ok(())
    }
}
