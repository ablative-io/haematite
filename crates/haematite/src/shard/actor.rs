// CORE-007: Shard actor — owns tree + WAL buffer, handles get/put/delete/commit messages

use crate::store::NodeStore;
use crate::tree::{Cursor, Hash, LeafNode, Node, batch_mutate};
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
    pub fn put<K, V>(&mut self, key: K, value: V) -> Result<(), WalError>
    where
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        let key = key.into();
        let value = value.into();
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

    /// Read through the recovered/live buffer first, then the committed tree.
    pub fn get<K, S>(&self, key: K, store: &S) -> Result<Option<Vec<u8>>, WalError>
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
        let batch = buffered_batch(&self.buffer);
        let new_root = batch_mutate(store, baseline_root, batch.as_slice()).map_err(tree_error)?;

        self.wal.commit(new_root)?;
        self.buffer = WalBuffer::new();
        self.committed_root = Some(new_root);
        Ok(new_root)
    }

    /// Inspect buffered mutations; exposed for tests and future shard wiring.
    #[must_use]
    pub const fn buffer(&self) -> &WalBuffer {
        &self.buffer
    }
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

fn tree_error(error: impl std::fmt::Display) -> WalError {
    WalError::TreeError(error.to_string())
}

#[cfg(test)]
mod tests {
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
