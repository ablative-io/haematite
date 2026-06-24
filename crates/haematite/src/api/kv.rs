//! API-002: general key-value operations on [`Database`].
//!
//! This module is intentionally a thin facade over the database handle and shard
//! actors. Single-key operations route to exactly one owning shard with the
//! database router's stable `BLAKE3(key) % shard_count` convention; `commit`
//! fans out once to every shard and returns each shard's current root hash by
//! shard id. The WAL buffer/tree semantics live in [`crate::shard::actor`], not
//! in this API layer.
//!
//! `range` remains a shard-local `[from, to)` query routed from the lower bound.
//! It does not hide a cross-shard fan-out or global merge; callers that choose a
//! sharded keyspace should keep range prefixes shard-local when they require a
//! sorted result.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::db::helpers::{map_shard_error, ordered_hashes, range_on_handle};
use crate::db::{Database, DatabaseError, run_indexed_parallel};
use crate::shard::actor::ShardHandle;
use crate::sync::{Ack, ConsistencyError, ConsistencyMode, wait_for_quorum_from_receiver};
use crate::tree::Hash;

/// Key bytes used by the general KV API.
pub type KvKey = Vec<u8>;

/// Value bytes used by the general KV API.
pub type KvValue = Vec<u8>;

/// One key-value pair returned by [`Database::range`].
pub type KvEntry = (KvKey, KvValue);

/// Sorted shard-local range result returned by [`Database::range`].
pub type KvRange = Vec<KvEntry>;

/// Root hash returned for each shard id after [`Database::commit`].
pub type ShardRoots = BTreeMap<usize, Hash>;

impl Database {
    /// Read one key through its owning shard actor.
    ///
    /// The shard actor checks its WAL buffer first and only falls through to the
    /// committed tree when the key is not buffered, so an uncommitted put shadows
    /// a stale committed value and an uncommitted delete shadows any tree value.
    pub fn get(&self, key: &[u8]) -> Result<Option<KvValue>, DatabaseError> {
        self.handle_for(key)?
            .get(key.to_vec(), self.timeout())
            .map_err(map_shard_error)
    }

    /// Append a single-key put mutation to the owning shard's durable WAL and
    /// live WAL buffer.
    ///
    /// This does not flush to the tree; [`Self::commit`] owns that boundary.
    pub fn put(&self, key: KvKey, value: KvValue) -> Result<(), DatabaseError> {
        self.put_with_consistency(key, value, ConsistencyMode::default())
    }

    /// Append a single-key put using the requested per-operation consistency
    /// policy. Eventual mode preserves the existing local WAL acknowledgment
    /// boundary. Strong mode performs the local write first, then waits for
    /// quorum acknowledgments supplied by the distribution sync path.
    pub fn put_with_consistency(
        &self,
        key: KvKey,
        value: KvValue,
        consistency: ConsistencyMode,
    ) -> Result<(), DatabaseError> {
        self.put_with_ttl_and_consistency(key, value, None, consistency)
    }

    /// Append a single-key put with optional TTL metadata.
    ///
    /// This does not flush to the tree; [`Self::commit`] owns that boundary.
    /// A `Some(ttl)` write requires `DatabaseConfig::sweep_interval` to be set.
    pub fn put_with_ttl(
        &self,
        key: KvKey,
        value: KvValue,
        ttl: Option<Duration>,
    ) -> Result<(), DatabaseError> {
        self.put_with_ttl_and_consistency(key, value, ttl, ConsistencyMode::default())
    }

    /// Append a single-key put with optional TTL metadata and per-operation
    /// consistency policy.
    pub fn put_with_ttl_and_consistency(
        &self,
        key: KvKey,
        value: KvValue,
        ttl: Option<Duration>,
        consistency: ConsistencyMode,
    ) -> Result<(), DatabaseError> {
        self.validate_ttl_write(ttl)?;
        self.handle_for(&key)?
            .put_with_ttl(key, value, ttl, self.timeout())
            .map_err(map_shard_error)?;
        wait_for_consistency(consistency)
    }

    /// Append a single-key tombstone to the owning shard's durable WAL and live
    /// WAL buffer.
    ///
    /// This does not flush to the tree; [`Self::commit`] owns that boundary.
    pub fn delete(&self, key: KvKey) -> Result<(), DatabaseError> {
        self.handle_for(&key)?
            .delete(key, self.timeout())
            .map_err(map_shard_error)
    }

    /// Read a shard-local `[from, to)` key range in ascending key order.
    ///
    /// The request is routed to the shard owning `from`. Inside that shard, the
    /// actor merges committed tree entries with sorted WAL-buffer entries:
    /// buffered puts shadow tree entries for the same key, and buffered deletes
    /// suppress tree entries. `from >= to` returns an empty result without
    /// routing.
    pub fn range(&self, from: &[u8], to: &[u8]) -> Result<KvRange, DatabaseError> {
        if from >= to {
            return Ok(Vec::new());
        }
        range_on_handle(self.handle_for(from)?, from, to, self.timeout())
    }

    /// Flush every shard's WAL buffer to its prolly tree and return roots by
    /// shard id.
    ///
    /// Exactly one `Commit` command is sent to each shard actor. The shard actor
    /// applies its entire buffer as one `batch_mutate` call, persists the new
    /// root marker, clears the buffer, and replies with the current root. A call
    /// with no buffered writes still returns the current root for every shard.
    pub fn commit(&self) -> Result<ShardRoots, DatabaseError> {
        let handles = self.shard_handles_in_order().to_vec();
        let timeout = self.timeout();
        let results = run_indexed_parallel(handles, |handle: ShardHandle| handle.commit(timeout))?;
        let hashes = ordered_hashes(results, self.shard_count())?;
        Ok(hashes.into_iter().enumerate().collect())
    }
}

fn wait_for_consistency(consistency: ConsistencyMode) -> Result<(), DatabaseError> {
    let ConsistencyMode::Strong(strong) = consistency else {
        return Ok(());
    };

    // DIST-002 defines the per-operation API and quorum wait semantics, but
    // DIST-001/DIST-003 own node transfer and topology scheduling. Until that
    // sync path feeds remote acknowledgments here, a single-node operation can
    // complete with the local durable WAL ack; multi-node strong writes fail
    // honestly rather than pretending replication happened.
    let (sender, receiver) = std::sync::mpsc::channel::<Ack<usize>>();
    let result = wait_for_quorum_from_receiver(strong, &receiver)
        .map(drop)
        .map_err(|error| map_consistency_error(&error));
    drop(sender);
    result
}

fn map_consistency_error(error: &ConsistencyError) -> DatabaseError {
    DatabaseError::ConsistencyError(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::path::Path;
    use std::time::Duration;

    use crate::DatabaseError;
    use crate::db::{Database, DatabaseConfig};
    use crate::sync::{ConsistencyMode, EventualConsistency, StrongConsistency};
    use crate::wal::{DurableWal, OperationType};

    fn config_for(path: &Path, shard_count: usize) -> DatabaseConfig {
        DatabaseConfig {
            data_dir: path.to_path_buf(),
            shard_count,
            sweep_interval: None,
            distributed: None,
        }
    }

    fn shard_local_keys(db: &Database, count: usize, from: &[u8]) -> Vec<Vec<u8>> {
        let target_shard = db.shard_for(from);
        let mut keys = Vec::with_capacity(count);
        let mut candidate = 0_u64;
        while keys.len() < count {
            let key = format!("r:{candidate:010}").into_bytes();
            if db.shard_for(&key) == target_shard {
                keys.push(key);
            }
            candidate = candidate.saturating_add(1);
            assert!(candidate < 10_000, "failed to find enough shard-local keys");
        }
        keys
    }

    fn wal_path(data_dir: &Path, shard_id: usize) -> std::path::PathBuf {
        data_dir.join(format!("shard-{shard_id}")).join("shard.wal")
    }

    #[test]
    fn get_routes_to_owner_and_checks_buffer_before_tree() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let data_dir = dir.path().join("db");
        let db = Database::create(config_for(&data_dir, 4))?;
        let key = b"conversation-state".to_vec();

        assert_eq!(db.get(b"missing")?, None);
        db.put(key.clone(), b"old".to_vec())?;
        db.commit()?;
        db.put(key.clone(), b"new".to_vec())?;

        assert_eq!(db.get(&key)?, Some(b"new".to_vec()));
        Ok(())
    }

    #[test]
    fn get_after_committed_delete_returns_none_even_with_stale_tree_value()
    -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let data_dir = dir.path().join("db");
        let db = Database::create(config_for(&data_dir, 4))?;
        let key = b"conversation-state".to_vec();

        db.put(key.clone(), b"tree-value".to_vec())?;
        db.commit()?;
        // commit() clears the buffer, so this value can only come from the tree.
        assert_eq!(db.get(&key)?, Some(b"tree-value".to_vec()));

        // A buffered tombstone must suppress the still-present committed tree value.
        db.delete(key.clone())?;
        assert_eq!(db.get(&key)?, None);
        Ok(())
    }

    #[test]
    fn put_and_delete_buffer_wal_mutations_without_tree_flush() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let data_dir = dir.path().join("db");
        let db = Database::create(config_for(&data_dir, 4))?;
        let key = b"durable-channel".to_vec();
        let shard_id = db.shard_for(&key);

        db.put(key.clone(), b"live".to_vec())?;
        assert_eq!(db.get(&key)?, Some(b"live".to_vec()));
        db.delete(key.clone())?;
        assert_eq!(db.get(&key)?, None);
        drop(db);

        let contents = DurableWal::read_file(wal_path(&data_dir, shard_id))?;
        assert_eq!(contents.committed_root(), None);
        assert_eq!(contents.entries().len(), 2);
        assert_eq!(contents.entries()[0].operation_type(), OperationType::Put);
        assert_eq!(contents.entries()[0].key(), key.as_slice());
        assert_eq!(contents.entries()[0].value(), Some(b"live".as_slice()));
        assert_eq!(
            contents.entries()[1].operation_type(),
            OperationType::Delete
        );
        assert_eq!(contents.entries()[1].key(), key.as_slice());
        assert_eq!(contents.entries()[1].value(), None);

        let reopened = Database::open(&data_dir)?;
        assert_eq!(reopened.get(&key)?, None);
        Ok(())
    }

    #[test]
    fn range_merges_tree_and_buffer_entries_in_sorted_order() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let data_dir = dir.path().join("db");
        let db = Database::create(config_for(&data_dir, 4))?;
        let range_from = b"r:";
        let range_to = b"r;";
        let mut keys = shard_local_keys(&db, 4, range_from);
        keys.sort();

        db.put(keys[0].clone(), b"tree-a".to_vec())?;
        db.put(keys[1].clone(), b"tree-b".to_vec())?;
        db.put(keys[2].clone(), b"tree-c".to_vec())?;
        db.commit()?;

        db.put(keys[1].clone(), b"buffer-b".to_vec())?;
        db.delete(keys[2].clone())?;
        db.put(keys[3].clone(), b"buffer-d".to_vec())?;

        assert_eq!(
            db.range(range_from, range_to)?,
            vec![
                (keys[0].clone(), b"tree-a".to_vec()),
                (keys[1].clone(), b"buffer-b".to_vec()),
                (keys[3].clone(), b"buffer-d".to_vec()),
            ]
        );
        Ok(())
    }

    #[test]
    fn commit_returns_current_roots_by_shard_and_clears_buffers() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let data_dir = dir.path().join("db");
        let db = Database::create(config_for(&data_dir, 4))?;

        let empty_roots = db.commit()?;
        assert_eq!(empty_roots.len(), 4);
        assert_eq!(
            empty_roots.keys().copied().collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );

        db.put(b"after-empty-commit".to_vec(), b"value".to_vec())?;
        let committed_roots = db.commit()?;
        assert_eq!(committed_roots.len(), 4);
        assert_eq!(db.get(b"after-empty-commit")?, Some(b"value".to_vec()));

        let repeated_roots = db.commit()?;
        assert_eq!(repeated_roots, committed_roots);
        drop(db);

        let reopened = Database::open(&data_dir)?;
        assert_eq!(
            reopened.get(b"after-empty-commit")?,
            Some(b"value".to_vec())
        );
        assert_eq!(reopened.commit()?, committed_roots);
        Ok(())
    }

    #[test]
    fn put_with_eventual_consistency_returns_after_local_write() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let data_dir = dir.path().join("db");
        let db = Database::create(config_for(&data_dir, 4))?;
        let mode = ConsistencyMode::Eventual(EventualConsistency::new(Duration::from_secs(30)));

        db.put_with_consistency(b"eventual".to_vec(), b"value".to_vec(), mode)?;

        assert_eq!(db.get(b"eventual")?, Some(b"value".to_vec()));
        Ok(())
    }

    #[test]
    fn put_with_single_node_strong_consistency_counts_local_ack() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let data_dir = dir.path().join("db");
        let db = Database::create(config_for(&data_dir, 4))?;
        let mode = ConsistencyMode::Strong(StrongConsistency::new(1, Duration::from_secs(1)));

        db.put_with_consistency(b"strong-local".to_vec(), b"value".to_vec(), mode)?;

        assert_eq!(db.get(b"strong-local")?, Some(b"value".to_vec()));
        Ok(())
    }

    #[test]
    fn different_operations_can_choose_different_consistency_modes() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let data_dir = dir.path().join("db");
        let db = Database::create(config_for(&data_dir, 4))?;

        db.put_with_consistency(
            b"eventual".to_vec(),
            b"value".to_vec(),
            ConsistencyMode::eventual(Duration::from_secs(30)),
        )?;
        let strong_result = db.put_with_consistency(
            b"strong".to_vec(),
            b"value".to_vec(),
            ConsistencyMode::strong(3, Duration::from_millis(1)),
        );

        assert_eq!(db.get(b"eventual")?, Some(b"value".to_vec()));
        assert!(matches!(
            strong_result,
            Err(DatabaseError::ConsistencyError(_))
        ));
        assert_eq!(db.get(b"strong")?, Some(b"value".to_vec()));
        Ok(())
    }

    #[test]
    fn put_with_ttl_requires_configured_sweep_interval() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let data_dir = dir.path().join("db");
        let db = Database::create(config_for(&data_dir, 4))?;

        assert!(matches!(
            db.put_with_ttl(
                b"temporary".to_vec(),
                b"value".to_vec(),
                Some(Duration::from_secs(1)),
            ),
            Err(crate::DatabaseError::MissingSweepInterval)
        ));
        Ok(())
    }

    #[test]
    fn get_and_range_filter_expired_ttl_entries() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let data_dir = dir.path().join("db");
        let mut config = config_for(&data_dir, 4);
        // This test exercises read-time TTL filtering, not the physical sweep, so
        // use a long interval: the sweep's first tick fires after `interval`, well
        // past this sub-second test, keeping the scheduler free of sweep load.
        // (Physical sweeping is covered by ttl::sweep's dedicated test.)
        config.sweep_interval = Some(60_000);
        let db = Database::create(config)?;
        let range_from = b"ttl:";
        let range_to = b"ttl;";
        let mut keys = Vec::new();
        let mut candidate = 0_u64;
        while keys.len() < 2 {
            let key = format!("ttl:{candidate:04}").into_bytes();
            if db.shard_for(&key) == db.shard_for(range_from) {
                keys.push(key);
            }
            candidate = candidate.saturating_add(1);
        }
        keys.sort();

        db.put_with_ttl(keys[0].clone(), b"expired".to_vec(), Some(Duration::ZERO))?;
        db.put_with_ttl(
            keys[1].clone(),
            b"live".to_vec(),
            Some(Duration::from_secs(60)),
        )?;

        assert_eq!(db.get(&keys[0])?, None);
        assert_eq!(db.get(&keys[1])?, Some(b"live".to_vec()));
        assert_eq!(
            db.range(range_from, range_to)?,
            vec![(keys[1].clone(), b"live".to_vec())]
        );
        Ok(())
    }
}
