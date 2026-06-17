//! Unit tests for the snapshot registry and commit log. Split out of
//! `snapshot.rs` via `#[path]` so that file stays within the branch module's
//! 500-line cap as test coverage grows.

use super::{CommitLog, SnapshotError, SnapshotRegistry, current_timestamp};
use crate::tree::Hash;

fn hash(byte: u8) -> Hash {
    Hash::from_bytes([byte; 32])
}

#[test]
fn name_stores_and_get_returns_mapping() -> Result<(), SnapshotError> {
    let mut registry = SnapshotRegistry::new();
    registry.name("nightly", hash(1))?;
    assert_eq!(registry.get("nightly"), Some(hash(1)));
    Ok(())
}

#[test]
fn get_returns_none_for_unknown_name() {
    let registry = SnapshotRegistry::new();
    assert_eq!(registry.get("missing"), None);
}

#[test]
fn naming_duplicate_is_an_error_and_keeps_original() -> Result<(), SnapshotError> {
    let mut registry = SnapshotRegistry::new();
    registry.name("release", hash(1))?;
    let result = registry.name("release", hash(2));
    assert!(matches!(result, Err(SnapshotError::DuplicateName(name)) if name == "release"));
    assert_eq!(registry.get("release"), Some(hash(1)));
    assert_eq!(registry.len(), 1);
    Ok(())
}

#[test]
fn list_snapshots_is_empty_for_empty_registry() {
    let registry = SnapshotRegistry::new();
    assert!(registry.is_empty());
    assert!(registry.list_snapshots().is_empty());
}

#[test]
fn list_snapshots_reports_names_hashes_and_timestamps_in_order() -> Result<(), SnapshotError> {
    let mut registry = SnapshotRegistry::new();
    registry.name_at("first", hash(1), 100)?;
    registry.name_at("second", hash(2), 200)?;
    registry.name_at("third", hash(3), 300)?;
    let listed = registry.list_snapshots();
    assert_eq!(
        listed,
        vec![
            ("first".to_owned(), hash(1), 100),
            ("second".to_owned(), hash(2), 200),
            ("third".to_owned(), hash(3), 300),
        ]
    );
    Ok(())
}

#[test]
fn snapshot_timestamp_is_naming_time_not_commit_time() -> Result<(), SnapshotError> {
    // A long-committed root named now lists with the naming timestamp.
    let mut registry = SnapshotRegistry::new();
    registry.name_at("tagged", hash(1), 999)?;
    assert_eq!(
        registry.list_snapshots(),
        vec![("tagged".to_owned(), hash(1), 999)]
    );
    Ok(())
}

#[test]
fn registry_survives_restart() -> Result<(), SnapshotError> {
    let dir = tempfile::tempdir().map_err(SnapshotError::Io)?;
    let path = dir.path().join("registry.bin");
    {
        let mut registry = SnapshotRegistry::open(&path)?;
        registry.name_at("alpha", hash(7), 10)?;
        registry.name_at("beta", hash(8), 20)?;
    }
    let reopened = SnapshotRegistry::open(&path)?;
    assert_eq!(reopened.get("alpha"), Some(hash(7)));
    assert_eq!(reopened.get("beta"), Some(hash(8)));
    assert_eq!(
        reopened.list_snapshots(),
        vec![
            ("alpha".to_owned(), hash(7), 10),
            ("beta".to_owned(), hash(8), 20),
        ]
    );
    Ok(())
}

#[test]
fn duplicate_name_does_not_corrupt_persisted_registry() -> Result<(), SnapshotError> {
    let dir = tempfile::tempdir().map_err(SnapshotError::Io)?;
    let path = dir.path().join("registry.bin");
    let mut registry = SnapshotRegistry::open(&path)?;
    registry.name_at("only", hash(1), 5)?;
    let _ = registry.name("only", hash(2));
    let reopened = SnapshotRegistry::open(&path)?;
    assert_eq!(reopened.list_snapshots().len(), 1);
    assert_eq!(reopened.get("only"), Some(hash(1)));
    Ok(())
}

#[test]
fn commit_log_appends_in_chronological_order() -> Result<(), SnapshotError> {
    let mut log = CommitLog::new();
    log.append(hash(1), 100)?;
    log.append(hash(2), 200)?;
    let entries = log.list();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].root_hash, hash(1));
    assert_eq!(entries[0].timestamp, 100);
    assert_eq!(entries[1].root_hash, hash(2));
    assert_eq!(entries[1].timestamp, 200);
    Ok(())
}

#[test]
fn commit_log_survives_restart() -> Result<(), SnapshotError> {
    let dir = tempfile::tempdir().map_err(SnapshotError::Io)?;
    let path = dir.path().join("commit.log");
    {
        let mut log = CommitLog::open(&path)?;
        log.append(hash(3), 30)?;
        log.append(hash(4), 40)?;
    }
    let reopened = CommitLog::open(&path)?;
    let entries = reopened.list();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].root_hash, hash(3));
    assert_eq!(entries[1].root_hash, hash(4));
    assert_eq!(entries[1].timestamp, 40);
    Ok(())
}

#[test]
fn current_timestamp_is_after_epoch() {
    assert!(current_timestamp() > 0);
}

#[test]
fn open_rejects_corrupt_file() -> Result<(), SnapshotError> {
    let dir = tempfile::tempdir().map_err(SnapshotError::Io)?;
    let path = dir.path().join("registry.bin");
    std::fs::write(&path, b"not a valid registry").map_err(SnapshotError::Io)?;
    assert!(matches!(
        SnapshotRegistry::open(&path),
        Err(SnapshotError::Corrupt(_))
    ));
    Ok(())
}
