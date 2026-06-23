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

fn config_for(path: &Path, shard_count: usize) -> DatabaseConfig {
    DatabaseConfig {
        data_dir: path.to_path_buf(),
        shard_count,
        sweep_interval: None,
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
fn create_writes_config_and_shard_directories() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 4))?;

    assert!(data_dir.is_dir());
    assert_config_json(&data_dir, 4)?;
    for index in 0..4 {
        assert!(data_dir.join(format!("shard-{index}")).is_dir());
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
    assert_eq!(reopened.router.handles_in_order().len(), 4);
    for (key, value) in &expected {
        assert_eq!(reopened.get(key)?, Some(value.clone()));
    }
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
    let handles = db.router.handles_in_order().to_vec();
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
        },
        None,
        None,
    );
    assert_eq!(result, (PathBuf::from("root"), 1, true, true));
}
