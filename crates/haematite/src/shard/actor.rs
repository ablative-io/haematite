// CORE-007: Shard actor — owns tree + WAL buffer, handles get/put/delete/commit messages

use crate::wal::{DurableWal, Mutation, WalBuffer, WalError};

/// Minimal shard write boundary used by the durable WAL layer.
///
/// Full beamr process wiring, tree reads, commits, range messages, and crash
/// recovery replay are delivered by later shard/recovery briefs. This type keeps
/// the PERSIST-002 acknowledgement invariant executable today: a mutation is
/// appended to the durable WAL before it enters the in-memory buffer and before
/// `put`/`delete` can return `Ok`.
#[derive(Debug)]
pub struct ShardActor {
    wal: DurableWal,
    buffer: WalBuffer,
}

impl ShardActor {
    /// Build a shard write boundary around an already-open durable WAL.
    #[must_use]
    pub fn new(wal: DurableWal) -> Self {
        Self {
            wal,
            buffer: WalBuffer::new(),
        }
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

    /// Inspect buffered mutations; exposed for tests and future shard wiring.
    #[must_use]
    pub const fn buffer(&self) -> &WalBuffer {
        &self.buffer
    }
}

#[cfg(test)]
mod tests {
    use super::ShardActor;
    use crate::wal::{DurableWal, FsyncPolicy, LookupResult, WalEntry, WalError};
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
}
