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
use crate::sync::{
    Ack, ConsistencyError, ConsistencyMode, SyncNodeId, wait_for_quorum_from_receiver,
};
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

    /// Append a single-key STAMPED TOMBSTONE to the owning shard's durable WAL and
    /// live WAL buffer (AA-3-4b).
    ///
    /// A delete is unified into the one stamped write path: it stores a tombstone
    /// (a comparable, mergeable, stamped entry that reads as absent), NOT a bare
    /// key-removal. Single-node read-after-delete is unchanged (the key reads as
    /// `None`), but the delete now persists in the tree so the §2.4 union merge can
    /// never resurrect it from a lagging node. The stamp is drawn from this shard's
    /// in-memory serve-authority exactly like a put (R-LE / R-SEQ); with no live
    /// election it is `(bottom, seq)` (single-node / 2a-compat). For a quorum-
    /// replicated, fenced delete use [`Self::replicate_delete`].
    ///
    /// This does not flush to the tree; [`Self::commit`] owns that boundary.
    pub fn delete(&self, key: KvKey) -> Result<(), DatabaseError> {
        let stamp = self.next_stamp_for_key(&key);
        self.handle_for(&key)?
            .delete(key, stamp, self.timeout())
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

    /// Read a `[from, to)` range from the shard at `shard_id` (by index), in
    /// ascending key order. Unlike [`Self::range`] (which routes by the `from`
    /// key's hash), this names the shard directly — the primitive a cross-shard
    /// enumeration fans out over every shard with. `from >= to` returns empty.
    pub fn range_per_shard(
        &self,
        shard_id: usize,
        from: &[u8],
        to: &[u8],
    ) -> Result<KvRange, DatabaseError> {
        if from >= to {
            return Ok(Vec::new());
        }
        range_on_handle(self.handle_for_shard(shard_id)?, from, to, self.timeout())
    }

    /// Append a routed single-key put, co-located by `route_key` (AA-4-1).
    ///
    /// Routes to a shard by hashing `route_key` (via [`Self::handle_for`]) but
    /// reads/writes the physical `key` bytes. The owning shard stores the physical
    /// key inside `route_key`'s shard, so every routed operation that supplies the
    /// SAME `route_key` lands on the SAME shard regardless of the physical key —
    /// this is the general-KV generalization of how [`crate::EventStore::append`]
    /// co-locates a stream's events by routing on `stream_key`.
    ///
    /// Callers MUST use a stable `route_key` for every record in a co-located
    /// family; mixing route keys for the same physical key splits it across shards
    /// and breaks read-after-write. Like plain [`Self::put`] this is a local WAL
    /// append with no consistency/quorum wait; [`Self::commit`] owns the tree
    /// flush.
    pub fn put_routed(
        &self,
        route_key: &[u8],
        key: KvKey,
        value: KvValue,
    ) -> Result<(), DatabaseError> {
        self.handle_for(route_key)?
            .put_with_ttl(key, value, None, self.timeout())
            .map_err(map_shard_error)
    }

    /// Read a routed single key, co-located by `route_key` (AA-4-1).
    ///
    /// Routes to a shard by hashing `route_key` (via [`Self::handle_for`]) but
    /// reads the physical `key` bytes from that shard. Mirrors [`Self::get`]'s
    /// buffer-before-tree read semantics inside `route_key`'s shard. The caller
    /// MUST pass the SAME `route_key` used for the matching [`Self::put_routed`];
    /// a different route key routes to a different shard and will not see the
    /// value (the co-location guarantee is keyed on `route_key`, exactly as
    /// [`crate::EventStore`] co-locates a stream by `stream_key`).
    pub fn get_routed(
        &self,
        route_key: &[u8],
        key: &[u8],
    ) -> Result<Option<KvValue>, DatabaseError> {
        self.handle_for(route_key)?
            .get(key.to_vec(), self.timeout())
            .map_err(map_shard_error)
    }

    /// Append a routed single-key STAMPED TOMBSTONE, co-located by `route_key`
    /// (AA-4-1).
    ///
    /// Routes to a shard by hashing `route_key` (via [`Self::handle_for`]) but
    /// deletes the physical `key` inside that shard. Mirrors [`Self::delete`]'s
    /// stamped-tombstone path: the stamp is still drawn per physical `key` (R-LE /
    /// R-SEQ); only the routing target changes to `route_key`. The caller MUST use
    /// the SAME `route_key` as the matching [`Self::put_routed`] so the tombstone
    /// lands on the shard that holds the record — the same route-by-key
    /// co-location [`crate::EventStore`] relies on for `stream_key`.
    pub fn delete_routed(&self, route_key: &[u8], key: KvKey) -> Result<(), DatabaseError> {
        let stamp = self.next_stamp_for_key(&key);
        self.handle_for(route_key)?
            .delete(key, stamp, self.timeout())
            .map_err(map_shard_error)
    }

    /// Read a routed `[from, to)` range, co-located by `route_key` (AA-4-1).
    ///
    /// Routes to a shard by hashing `route_key` (via [`Self::handle_for`]) but
    /// scans the physical `[from, to)` range inside that one shard. Mirrors
    /// [`Self::range`]'s shard-local merge of committed tree and WAL-buffer
    /// entries; `from >= to` returns empty without routing. The scan is
    /// deliberately shard-LOCAL within `route_key`'s shard: a co-located record
    /// family (all sharing one stable `route_key`) lives entirely on that shard,
    /// so this returns the whole family in sorted order. Callers MUST use the SAME
    /// `route_key` used to write the family — the same route-by-key co-location
    /// [`crate::EventStore`] relies on for `stream_key`.
    pub fn range_routed(
        &self,
        route_key: &[u8],
        from: &[u8],
        to: &[u8],
    ) -> Result<KvRange, DatabaseError> {
        if from >= to {
            return Ok(Vec::new());
        }
        range_on_handle(self.handle_for(route_key)?, from, to, self.timeout())
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
    //
    // The ack channel keys on the real node identity `SyncNodeId` (2a-2): the
    // quorum primitive is generic over the node id and the live producer is wired
    // in 2a-3, so here the sender is still dropped immediately.
    let (sender, receiver) = std::sync::mpsc::channel::<Ack<SyncNodeId>>();
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
        // ASSERTION CHANGED (AA-3-4b): a delete is now a STAMPED TOMBSTONE — a
        // `Put` of the tombstone envelope, NOT a bare `OperationType::Delete`. The
        // tombstone is a stamped entry (magic `HMSTMP01`), reads as absent, and
        // survives reopen as a committed delete (mergeable, never resurrected).
        assert_eq!(contents.entries()[1].operation_type(), OperationType::Put);
        assert_eq!(contents.entries()[1].key(), key.as_slice());
        let tombstone = contents.entries()[1].value().ok_or("tombstone is a Put")?;
        assert!(tombstone.starts_with(b"HMSTMP01"), "delete stores a stamped tombstone");

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

    #[test]
    fn range_per_shard_returns_only_that_shards_keys_and_union_is_complete()
    -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let data_dir = dir.path().join("db");
        let db = Database::create(config_for(&data_dir, 3))?;
        assert_eq!(db.shard_count(), 3);

        // A range covering every test key. All keys share the "k:" prefix and are
        // spread across shards by whole-key BLAKE3 routing.
        let low = b"k:";
        let high = b"k;";

        // Put a spread of keys (and one delete) so multiple shards are populated.
        let mut put_keys: Vec<Vec<u8>> = Vec::new();
        for i in 0..30_u64 {
            let key = format!("k:{i:04}").into_bytes();
            db.put(key.clone(), format!("v{i}").into_bytes())?;
            put_keys.push(key);
        }
        // A committed-then-deleted key must NOT appear in any shard's range.
        let deleted = b"k:deleted".to_vec();
        db.put(deleted.clone(), b"gone".to_vec())?;
        db.commit()?;
        db.delete(deleted.clone())?;

        // Confirm the keys really do spread across more than one shard, else this
        // test would not exercise per-shard routing.
        let distinct_shards: std::collections::BTreeSet<usize> =
            put_keys.iter().map(|k| db.shard_for(k)).collect();
        assert!(
            distinct_shards.len() > 1,
            "test keys must span multiple shards (got {distinct_shards:?})"
        );

        // Per-shard: every returned key belongs to that shard, and the union over
        // all shards equals exactly the live key set.
        let mut union: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        for s in 0..db.shard_count() {
            let entries = db.range_per_shard(s, low, high)?;
            for (key, _value) in &entries {
                assert_eq!(
                    db.shard_for(key),
                    s,
                    "key {key:?} surfaced on shard {s} but routes to {}",
                    db.shard_for(key)
                );
                assert!(
                    union.insert(key.clone()),
                    "key {key:?} appeared in more than one shard's result"
                );
            }
        }
        let expected: std::collections::BTreeSet<Vec<u8>> = put_keys.iter().cloned().collect();
        assert_eq!(union, expected, "union of shard ranges must equal live keys");
        assert!(!union.contains(&deleted), "deleted key must not appear");

        // Empty range [k, k) returns empty on every shard.
        for s in 0..db.shard_count() {
            assert_eq!(db.range_per_shard(s, low, low)?, Vec::new());
        }

        // Out-of-range shard id errors cleanly (no panic).
        let err = db.range_per_shard(db.shard_count(), low, high);
        assert!(err.is_err(), "out-of-range shard_id must error, not panic");

        Ok(())
    }

    #[test]
    fn routed_ops_colocate_by_route_key_not_physical_key() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let data_dir = dir.path().join("db");
        let db = Database::create(config_for(&data_dir, 3))?;
        assert_eq!(db.shard_count(), 3);

        // Physical key and a bracketing range for the routed scan.
        let physical_key = b"physical:key".to_vec();
        let lo = b"physical:";
        let hi = b"physical;";

        // Find a route_key whose shard differs from the physical key's shard, so
        // the test proves routing follows route_key, not the physical key. If they
        // matched, a non-routed get could "accidentally" find the value.
        let physical_shard = db.shard_for(&physical_key);
        let mut route_key: Vec<u8> = Vec::new();
        let mut found = false;
        for candidate in 0_u64..10_000 {
            let candidate_key = format!("route:{candidate:06}").into_bytes();
            if db.shard_for(&candidate_key) != physical_shard {
                route_key = candidate_key;
                found = true;
                break;
            }
        }
        assert!(
            found,
            "must find a route_key on a different shard (test is non-vacuous)"
        );
        let route_shard = db.shard_for(&route_key);
        assert_ne!(
            route_shard, physical_shard,
            "route_key and physical key must hash to different shards"
        );

        let value = b"routed-value".to_vec();
        db.put_routed(&route_key, physical_key.clone(), value.clone())?;
        db.commit()?;

        // Routed read finds it (routes to route_key's shard).
        assert_eq!(db.get_routed(&route_key, &physical_key)?, Some(value.clone()));

        // A NON-routed get(physical_key) routes to shard_for(physical_key) — a
        // DIFFERENT shard — so it must NOT see the value. This is the proof that
        // co-location is keyed on route_key, not the physical key.
        assert_eq!(
            db.get(&physical_key)?,
            None,
            "non-routed get must not find a value co-located by route_key"
        );

        // The value physically lives on route_key's shard, not the physical key's.
        let in_route_shard = db.range_per_shard(route_shard, lo, hi)?;
        assert!(
            in_route_shard.iter().any(|(k, _)| k == &physical_key),
            "value must physically live on route_key's shard"
        );
        let in_physical_shard = db.range_per_shard(physical_shard, lo, hi)?;
        assert!(
            !in_physical_shard.iter().any(|(k, _)| k == &physical_key),
            "value must NOT live on the physical key's shard"
        );

        // Routed range finds it and is shard-local to route_key's shard.
        let routed_range = db.range_routed(&route_key, lo, hi)?;
        assert_eq!(routed_range, vec![(physical_key.clone(), value)]);

        // Routed delete removes it.
        db.delete_routed(&route_key, physical_key.clone())?;
        db.commit()?;
        assert_eq!(db.get_routed(&route_key, &physical_key)?, None);

        Ok(())
    }
}
