// CORE-007: Shard actor — owns tree + WAL buffer, handles get/put/delete/commit messages

use std::time::Duration;

mod errors;
pub mod handle;
pub mod native;
mod scan;
mod startup;

use errors::{AppendError, CasError, HashCasError};

pub use handle::{RangeItem, ShardError, ShardHandle};

use crate::branch::current_timestamp;
use crate::store::NodeStore;
use crate::tree::{Cursor, Hash, LeafNode, Node, batch_mutate};
use crate::ttl::entry::encode_optional_ttl;
use crate::ttl::filter::{Visibility, is_expired_at, visible_value};
use crate::wal::{DurableWal, LookupResult, Mutation, RecoveredWal, WalBuffer, WalError};

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
        }
    }

    /// Build a normal shard actor from crash-recovered WAL state.
    #[must_use]
    pub fn from_recovered(wal: DurableWal, recovered: RecoveredWal) -> Self {
        let committed_root = recovered.committed_root();
        Self {
            wal,
            buffer: recovered.into_buffer(),
            committed_root,
        }
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

    /// Append a delete to the durable WAL, then buffer it for a future tree commit.
    pub fn delete<K>(&mut self, key: K) -> Result<(), WalError>
    where
        K: Into<Vec<u8>>,
    {
        let key = key.into();
        let mutation = Mutation::Delete { key: key.clone() };
        self.wal.append_mutation(&mutation)?;
        self.buffer.delete(key);
        Ok(())
    }

    /// Delete `key` only if its current stored value is present and expired now,
    /// re-checking the live buffer+tree inside the actor's single-threaded slice.
    ///
    /// The sweep computes its candidate set from an independent store+WAL
    /// snapshot; re-checking here closes the window in which a concurrent refresh
    /// (a fresh `put` landing between the snapshot and this delete) would be
    /// clobbered by an unconditional delete. Expiry is evaluated against the raw
    /// stored bytes (not the read-filtered view) at the current clock. Returns
    /// whether a delete was issued.
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
        if is_expired_at(&value, current_timestamp()).map_err(tree_error)? {
            self.delete(key.to_vec())?;
            Ok(true)
        } else {
            Ok(false)
        }
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
        let batch = buffered_batch(&self.buffer);
        let new_root = batch_mutate(store, baseline_root, batch.as_slice()).map_err(tree_error)?;

        self.wal.commit(new_root)?;
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
        let actual = self.read_value(key, store)?;
        if actual != expected {
            return Err(CasError::Mismatch { expected, actual });
        }
        let previous_buffer = self.buffer.clone();
        self.buffer.put(key, new.to_be_bytes());
        match self.commit(store) {
            Ok(_root) => Ok(()),
            Err(error) => {
                self.buffer = previous_buffer;
                Err(CasError::from(error))
            }
        }
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
    fn apply_durable<S>(
        &mut self,
        key: &[u8],
        expected: Option<Hash>,
        value: Vec<u8>,
        ttl: Option<Duration>,
        store: &mut S,
    ) -> Result<(), HashCasError>
    where
        S: NodeStore + ?Sized,
    {
        let actual = self.current_value_hash(key, store)?;
        if actual != expected {
            return Err(HashCasError::HashMismatch { expected, actual });
        }
        let previous_buffer = self.buffer.clone();
        let encoded = encode_ttl_value(value, ttl)?;
        self.buffer.put(key, encoded);
        match self.commit(store) {
            Ok(_root) => Ok(()),
            Err(error) => {
                self.buffer = previous_buffer;
                Err(HashCasError::from(error))
            }
        }
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

#[cfg(test)]
#[path = "actor/tests.rs"]
mod tests;

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

    #[test]
    fn delete_returns_ok_only_after_entry_is_written_to_wal() -> Result<(), WalError> {
        let temp = temp_path("actor-delete.wal")?;
        let path = temp.path();
        let wal = DurableWal::new(path, FsyncPolicy::CommitOnly)?;
        let mut actor = ShardActor::new(wal);

        actor.delete(b"event".to_vec())?;

        assert_eq!(actor.buffer().get(b"event"), LookupResult::BufferedDelete);
        assert_eq!(
            DurableWal::read_file(path)?.entries(),
            &[WalEntry::delete(b"event".to_vec())]
        );
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
        let mut actor = ShardActor::from_recovered(wal, recovered);

        assert_eq!(actor.committed_root(), Some(committed_root));
        assert_eq!(actor.get(b"replayed", &store)?, Some(b"before".to_vec()));
        actor.put(b"new".to_vec(), b"after".to_vec())?;
        actor.delete(b"replayed".to_vec())?;

        assert_eq!(actor.get(b"new", &store)?, Some(b"after".to_vec()));
        assert_eq!(actor.get(b"replayed", &store)?, None);
        assert_eq!(
            DurableWal::read_file(temp.path())?.entries(),
            &[
                WalEntry::put(b"replayed".to_vec(), b"before".to_vec()),
                WalEntry::put(b"new".to_vec(), b"after".to_vec()),
                WalEntry::delete(b"replayed".to_vec()),
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
        let mut actor = ShardActor::from_recovered(wal, recovered);

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
        let mut recovered_actor = ShardActor::from_recovered(crashed_wal, recovered);
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
