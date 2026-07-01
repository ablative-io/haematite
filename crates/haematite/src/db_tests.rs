use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use beamr::process::ExitReason;
use serde_json::Value;

use super::{Database, DatabaseConfig, DatabaseError};
use crate::db::DistributedDatabaseConfig;
use crate::sync::{SyncNodeId, SyncPair, SyncTopology};

fn config_for(path: &Path, shard_count: usize) -> DatabaseConfig {
    DatabaseConfig {
        data_dir: path.to_path_buf(),
        shard_count,
        sweep_interval: None,
        distributed: None,
    }
}

fn check_database_error(error: &DatabaseError) {
    let error_ref: &dyn Error = error;
    let display = error_ref.to_string();
    assert!(!display.is_empty());
    let debug = format!("{error:?}");
    assert!(!debug.is_empty());
}

fn assert_config_json(path: &Path, shard_count: usize) -> Result<(), Box<dyn Error>> {
    let bytes = fs::read(path.join("config.json"))?;
    let parsed: Value = serde_json::from_slice(&bytes)?;
    assert!(parsed.get("data_dir").is_some());
    assert_eq!(
        parsed.get("shard_count"),
        Some(&Value::from(u64::try_from(shard_count)?))
    );
    assert_eq!(parsed.get("sweep_interval"), Some(&Value::Null));
    assert_eq!(parsed.get("distributed"), Some(&Value::Null));
    Ok(())
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

#[test]
fn database_error_implements_error_debug_and_display() {
    check_database_error(&DatabaseError::DirectoryCreate(io::Error::other(
        "directory",
    )));
    check_database_error(&DatabaseError::ConfigWrite(io::Error::other("write")));
    check_database_error(&DatabaseError::ConfigRead(io::Error::other("read")));
    check_database_error(&DatabaseError::ConfigParse("parse".to_owned()));
    check_database_error(&DatabaseError::InvalidShardCount);
    check_database_error(&DatabaseError::ShardSpawn("spawn".to_owned()));
    check_database_error(&DatabaseError::ShardError("shard".to_owned()));
    check_database_error(&DatabaseError::IoError(io::Error::other("io")));
    check_database_error(&DatabaseError::SequenceConflict {
        expected: 1,
        actual: 2,
    });
    check_database_error(&DatabaseError::MissingSweepInterval);
    check_database_error(&DatabaseError::InvalidSweepInterval);
}

#[test]
fn create_rejects_zero_shards() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let result = Database::create(config_for(dir.path(), 0));
    assert!(matches!(result, Err(DatabaseError::InvalidShardCount)));
    Ok(())
}

#[test]
fn create_writes_config_and_materialises_shards_lazily() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 4))?;

    // The database root + config are written eagerly...
    assert!(data_dir.is_dir());
    assert_config_json(&data_dir, 4)?;

    // ...but LAZY materialisation means NO per-shard directory exists until a
    // shard is first touched (the O(used)-not-O(count) boot property).
    for index in 0..4 {
        assert!(
            !data_dir.join(format!("shard-{index}")).exists(),
            "shard-{index} must not be pre-created under lazy materialisation"
        );
    }

    // Touching a key materialises exactly its shard's directory, and no other.
    let touched = db.shard_for(b"key");
    db.put(b"key".to_vec(), b"value".to_vec())?;
    for index in 0..4 {
        let exists = data_dir.join(format!("shard-{index}")).is_dir();
        assert_eq!(
            exists,
            index == touched,
            "only the touched shard-{touched} directory should be materialised"
        );
    }

    drop(db);
    Ok(())
}

#[test]
fn create_failure_when_parent_is_file_leaves_no_accessible_partial_database()
-> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let file_parent = dir.path().join("not-a-directory");
    fs::write(&file_parent, b"blocking file")?;
    let data_dir = file_parent.join("db");

    assert!(matches!(
        Database::create(config_for(&data_dir, 2)),
        Err(DatabaseError::DirectoryCreate(_))
    ));
    assert!(!data_dir.exists());
    Ok(())
}

#[test]
fn open_reports_missing_and_malformed_config() -> Result<(), Box<dyn Error>> {
    let missing = tempfile::tempdir()?;
    assert!(matches!(
        Database::open(missing.path()),
        Err(DatabaseError::ConfigRead(_))
    ));

    let malformed = tempfile::tempdir()?;
    fs::write(malformed.path().join("config.json"), b"not json")?;
    assert!(matches!(
        Database::open(malformed.path()),
        Err(DatabaseError::ConfigParse(_))
    ));
    Ok(())
}

#[test]
fn open_rejects_zero_shards_from_config() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    fs::write(
        dir.path().join("config.json"),
        serde_json::json!({
            "data_dir": dir.path(),
            "shard_count": 0,
            "sweep_interval": null,
            "distributed": null,
        })
        .to_string(),
    )?;

    assert!(matches!(
        Database::open(dir.path()),
        Err(DatabaseError::InvalidShardCount)
    ));
    Ok(())
}

#[test]
fn get_put_delete_and_uncommitted_wal_recovery_work() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 4))?;
    assert_eq!(db.shard_for(b"key"), db.shard_for(b"key"));

    assert_eq!(db.get(b"missing")?, None);
    db.put(b"key".to_vec(), b"value".to_vec())?;
    assert_eq!(db.get(b"key")?, Some(b"value".to_vec()));
    db.delete(b"key".to_vec())?;
    assert_eq!(db.get(b"key")?, None);

    db.put(b"buffered".to_vec(), b"wal-value".to_vec())?;
    drop(db);

    let reopened = Database::open(&data_dir)?;
    assert_eq!(reopened.get(b"buffered")?, Some(b"wal-value".to_vec()));
    drop(reopened);
    Ok(())
}

#[test]
fn range_empty_and_buffer_shadowing_cases_work() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 4))?;
    let range_from = b"r:";
    let range_to = b"r;";
    let keys = shard_local_keys(&db, 2, range_from);

    assert!(db.range(range_to, range_from)?.is_empty());
    assert!(db.range(b"empty:", b"empty;")?.is_empty());

    db.put(keys[1].clone(), b"kept".to_vec())?;
    db.put(keys[0].clone(), b"deleted".to_vec())?;
    db.delete(keys[0].clone())?;

    assert_eq!(
        db.range(range_from, range_to)?,
        vec![(keys[1].clone(), b"kept".to_vec())]
    );
    drop(db);
    Ok(())
}

#[test]
fn end_to_end_create_commit_range_drop_open_read_cycle() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 4))?;
    let range_from = b"r:";
    let range_to = b"r;";
    let keys = shard_local_keys(&db, 100, range_from);
    let mut expected = Vec::with_capacity(keys.len());

    for (index, key) in keys.iter().enumerate() {
        let value = format!("value-{index:03}").into_bytes();
        db.put(key.clone(), value.clone())?;
        expected.push((key.clone(), value));
    }
    expected.sort_by(|left, right| left.0.cmp(&right.0));

    let committed = db.commit()?;
    assert_eq!(committed.len(), 4);
    for (key, value) in &expected {
        assert_eq!(db.get(key)?, Some(value.clone()));
    }

    let ranged = db.range(range_from, range_to)?;
    assert_eq!(ranged, expected);
    drop(db);

    let reopened = Database::open(&data_dir)?;
    // LAZY: reopening materialises NO shard up front — the modulus base is still
    // 4, but nothing is spawned until a key touches a shard.
    assert_eq!(reopened.shard_count(), 4);
    assert_eq!(reopened.router.materialised_shard_ids().len(), 0);
    for (key, value) in &expected {
        assert_eq!(reopened.get(key)?, Some(value.clone()));
    }
    // After reading every key back, exactly the shards those keys hash to are
    // materialised (and every key round-tripped, proving WAL recovery on first
    // touch), never more than the 4-shard base.
    let touched: std::collections::BTreeSet<usize> =
        keys.iter().map(|key| reopened.shard_for(key)).collect();
    assert_eq!(
        reopened.router.materialised_shard_ids(),
        touched.into_iter().collect::<Vec<_>>()
    );
    drop(reopened);
    Ok(())
}

#[test]
fn commit_returns_one_hash_per_shard_and_database_remains_writable() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 4))?;

    assert_eq!(db.commit()?.len(), 4);
    db.put(b"after-empty-commit".to_vec(), b"value".to_vec())?;
    assert_eq!(db.get(b"after-empty-commit")?, Some(b"value".to_vec()));
    assert_eq!(db.commit()?.len(), 4);
    drop(db);
    Ok(())
}

#[test]
fn indexed_parallel_helper_invokes_multiple_workers_concurrently() -> Result<(), Box<dyn Error>> {
    let entered = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(AtomicBool::new(false));
    let verifier_entered = Arc::clone(&entered);
    let verifier_release = Arc::clone(&release);
    let verifier = std::thread::spawn(move || -> Result<(), String> {
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while verifier_entered.load(Ordering::SeqCst) < 2 {
            if std::time::Instant::now() >= deadline {
                verifier_release.store(true, Ordering::SeqCst);
                return Err("parallel workers did not overlap".to_owned());
            }
            std::thread::yield_now();
        }
        verifier_release.store(true, Ordering::SeqCst);
        Ok(())
    });
    let work_entered = Arc::clone(&entered);
    let work_release = Arc::clone(&release);

    let results = super::run_indexed_parallel(vec![10_u8, 20], move |value| {
        work_entered.fetch_add(1, Ordering::SeqCst);
        while !work_release.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        value + 1
    })?;

    verifier
        .join()
        .map_err(|_| io::Error::other("parallel verifier thread panicked"))?
        .map_err(io::Error::other)?;
    assert_eq!(results, vec![(0, 11), (1, 21)]);
    Ok(())
}

#[test]
fn append_read_events_and_sequence_conflicts_work() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 4))?;
    let key = b"workflow-1".to_vec();

    assert_eq!(db.append(key.clone(), Vec::new(), 0)?, 0);
    assert_eq!(
        db.append(
            key.clone(),
            vec![b"e1".to_vec(), b"e2".to_vec(), b"e3".to_vec()],
            0,
        )?,
        3
    );
    assert_eq!(
        db.read_events(&key)?,
        vec![b"e1".to_vec(), b"e2".to_vec(), b"e3".to_vec()]
    );
    assert_eq!(
        db.read_events_from(&key, 2)?,
        vec![b"e2".to_vec(), b"e3".to_vec()]
    );
    assert!(matches!(
        db.append(key.clone(), vec![b"late".to_vec()], 0),
        Err(DatabaseError::SequenceConflict {
            expected: 0,
            actual: 3
        })
    ));
    assert_eq!(db.read_events(&key)?.len(), 3);
    drop(db);
    Ok(())
}

// --- CSOT-1: durable cluster/members genesis round-trip (task #146) ---------

#[test]
fn read_cluster_members_is_none_before_any_genesis_write() -> Result<(), Box<dyn Error>> {
    // GATE (b) storage half: a fresh cluster has NO durable record, so
    // read_cluster_members returns None — the signal that resolve_membership must
    // fall back to static config (byte-identical to pre-CSOT-1).
    let dir = tempfile::tempdir()?;
    let db = Database::create(config_for(&dir.path().join("db"), 4))?;
    assert!(
        db.read_cluster_members()?.is_none(),
        "no record exists on a never-formed cluster"
    );
    drop(db);
    Ok(())
}

#[test]
fn single_node_genesis_cluster_members_round_trips() -> Result<(), Box<dyn Error>> {
    use crate::sync::ClusterMembers;

    // GATE (c) storage half: a lone node writes its own denominator-1 record and
    // reads back the IDENTICAL record (self-quorum, safe today).
    let dir = tempfile::tempdir()?;
    let db = Database::create(config_for(&dir.path().join("db"), 4))?;

    let genesis = ClusterMembers::genesis("cluster-solo", SyncNodeId::from("solo"))?;
    db.write_genesis_cluster_members(&genesis)?;

    let read_back = db.read_cluster_members()?;
    assert_eq!(
        read_back.as_ref(),
        Some(&genesis),
        "genesis record round-trips byte-for-byte"
    );
    let read_back = read_back.ok_or("record present after genesis write")?;
    assert_eq!(
        read_back.denominator(),
        1,
        "lone node self-quorum denominator"
    );
    assert_eq!(read_back.config_epoch, 0, "genesis is config epoch 0");

    drop(db);
    Ok(())
}

#[test]
fn genesis_write_is_exactly_once_and_survives_reopen() -> Result<(), Box<dyn Error>> {
    use crate::sync::ClusterMembers;

    // A second genesis attempt must NOT silently clobber the durable record: it
    // conflicts on the sequence-0 append. And the record must survive a DB reopen
    // (real durability, not just in-memory).
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let genesis = ClusterMembers::genesis("cluster-solo", SyncNodeId::from("solo"))?;

    {
        let db = Database::create(config_for(&data_dir, 4))?;
        db.write_genesis_cluster_members(&genesis)?;
        assert!(
            matches!(
                db.write_genesis_cluster_members(&genesis),
                Err(DatabaseError::SequenceConflict { .. })
            ),
            "a second genesis write must conflict, never silently overwrite"
        );
    }

    let reopened = Database::open(&data_dir)?;
    assert_eq!(
        reopened.read_cluster_members()?.as_ref(),
        Some(&genesis),
        "durable record survives reopen"
    );
    drop(reopened);
    Ok(())
}

#[test]
fn scan_sequence_keys_for_shards_scopes_to_named_shards() -> Result<(), Box<dyn Error>> {
    use std::collections::{BTreeMap, BTreeSet};

    let dir = tempfile::tempdir()?;
    let db = Database::create(config_for(&dir.path().join("db"), 3))?;

    // Append one event to many streams and group each by its owning shard.
    let mut by_shard: BTreeMap<usize, BTreeSet<Vec<u8>>> = BTreeMap::new();
    for i in 0..30_u64 {
        let key = format!("stream-{i:04}").into_bytes();
        db.append(key.clone(), vec![b"e".to_vec()], 0)?;
        by_shard.entry(db.shard_for(&key)).or_default().insert(key);
    }
    db.commit()?;
    assert!(by_shard.len() >= 2, "streams must span multiple shards");

    // The full scan returns every stream.
    let all: BTreeSet<Vec<u8>> = db
        .scan_sequence_keys()?
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    let expected: BTreeSet<Vec<u8>> = by_shard.values().flatten().cloned().collect();
    assert_eq!(all, expected, "full scan must return all streams");

    // A scoped scan returns ONLY the named shard's streams.
    for (&shard, keys) in &by_shard {
        let scoped: BTreeSet<Vec<u8>> = db
            .scan_sequence_keys_for_shards(&[shard])?
            .into_iter()
            .map(|(k, seq)| {
                assert_eq!(
                    db.shard_for(&k),
                    shard,
                    "scoped scan surfaced a foreign-shard stream"
                );
                assert_eq!(seq, 1, "each stream has exactly one event");
                k
            })
            .collect();
        assert_eq!(
            &scoped, keys,
            "shard {shard} scan must return exactly its own streams"
        );
    }

    // An out-of-range shard id errors cleanly rather than panicking.
    assert!(
        db.scan_sequence_keys_for_shards(&[db.shard_count()])
            .is_err()
    );
    Ok(())
}

#[test]
fn concurrent_appends_with_same_expected_seq_conflict_without_partial_write()
-> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Arc::new(Database::create(config_for(&data_dir, 4))?);
    let key = b"workflow-concurrent".to_vec();
    let barrier = Arc::new(Barrier::new(3));
    let mut joins = Vec::new();

    for entry in [b"left".to_vec(), b"right".to_vec()] {
        let db = Arc::clone(&db);
        let key = key.clone();
        let barrier = Arc::clone(&barrier);
        joins.push(std::thread::spawn(move || {
            barrier.wait();
            db.append(key, vec![entry], 0)
        }));
    }

    barrier.wait();
    let mut ok = 0;
    let mut conflicts = 0;
    for join in joins {
        match join
            .join()
            .map_err(|_| io::Error::other("append thread panicked"))?
        {
            Ok(1) => ok += 1,
            Err(DatabaseError::SequenceConflict {
                expected: 0,
                actual: 1,
            }) => conflicts += 1,
            other => return Err(format!("unexpected append result: {other:?}").into()),
        }
    }

    assert_eq!(ok, 1);
    assert_eq!(conflicts, 1);
    assert_eq!(db.read_events(&key)?.len(), 1);
    drop(db);
    Ok(())
}

#[test]
fn drop_shutdowns_shards_without_panicking_after_a_crash() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 2))?;
    db.put(b"key".to_vec(), b"value".to_vec())?;
    // LAZY: the single put materialised exactly the one shard `key` hashes to.
    let handles = db.router.materialised_handles();
    if let Some(first) = handles.first() {
        db.scheduler.exit_signal(0, first.pid(), ExitReason::Kill)?;
    }
    drop(db);

    for handle in handles {
        let result = handle.get(b"key".to_vec(), Duration::from_millis(25));
        assert!(result.is_err());
    }
    Ok(())
}

#[test]
fn crate_root_reexports_database_types() {
    fn accepts_root_types(
        config: crate::DatabaseConfig,
        maybe_database: Option<&crate::Database>,
        maybe_error: Option<&crate::DatabaseError>,
    ) -> (PathBuf, usize, bool, bool) {
        (
            config.data_dir,
            config.shard_count,
            maybe_database.is_none(),
            maybe_error.is_none(),
        )
    }

    let result = accepts_root_types(
        crate::DatabaseConfig {
            data_dir: PathBuf::from("root"),
            shard_count: 1,
            sweep_interval: None,
            distributed: None,
        },
        None,
        None,
    );
    assert_eq!(result, (PathBuf::from("root"), 1, true, true));
}

#[test]
fn distributed_creation_without_topology_is_an_error() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let mut config = config_for(&dir.path().join("db"), 2);
    config.distributed = Some(DistributedDatabaseConfig {
        local_node: SyncNodeId::from("a"),
        nodes: vec!["a".into(), "b".into()],
        topology: None,
        sync_interval: 1_000,
    });

    assert!(matches!(
        Database::create(config),
        Err(DatabaseError::MissingSyncTopology)
    ));
    Ok(())
}

#[test]
fn distributed_creation_writes_explicit_topology_config() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let mut config = config_for(&data_dir, 2);
    config.distributed = Some(DistributedDatabaseConfig {
        local_node: SyncNodeId::from("a"),
        nodes: vec!["a".into(), "b".into(), "c".into()],
        topology: Some(SyncTopology::Custom(vec![
            SyncPair::new("a", "b"),
            SyncPair::new("b", "c"),
        ])),
        sync_interval: 60_000,
    });

    let db = Database::create(config)?;
    let bytes = fs::read(data_dir.join("config.json"))?;
    let parsed: Value = serde_json::from_slice(&bytes)?;
    let distributed = parsed
        .get("distributed")
        .and_then(Value::as_object)
        .ok_or("distributed config missing")?;

    assert_eq!(distributed.get("local_node"), Some(&Value::from("a")));
    assert_eq!(distributed.get("sync_interval"), Some(&Value::from(60_000)));
    assert_eq!(
        distributed.get("topology"),
        Some(&serde_json::json!({
            "Custom": [
                { "source": "a", "target": "b" },
                { "source": "b", "target": "c" }
            ]
        }))
    );

    drop(db);
    Ok(())
}
