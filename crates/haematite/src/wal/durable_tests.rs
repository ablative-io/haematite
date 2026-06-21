use super::{DurableWal, FsyncPolicy, WalFileContents, truncation_marker_frame};
use crate::tree::Hash;
use crate::wal::buffer::{Mutation, WalError};
use crate::wal::entry::WalEntry;
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

fn frame(entry: &WalEntry) -> Vec<u8> {
    let bytes = entry.serialise();
    let mut frame = Vec::new();
    frame.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    frame.extend_from_slice(&bytes);
    frame
}

fn assert_file_contents(contents: &WalFileContents, entries: &[WalEntry], root: Option<Hash>) {
    assert_eq!(contents.entries(), entries);
    assert_eq!(contents.committed_root(), root);
}

#[test]
fn new_creates_file_when_missing() -> Result<(), WalError> {
    let temp = temp_path("create.wal")?;
    let path = temp.path();
    assert!(!path.exists());
    let wal = DurableWal::new(path, FsyncPolicy::PerWrite)?;
    assert!(path.exists());
    assert!(format!("{wal:?}").contains("DurableWal"));
    Ok(())
}

#[test]
fn opening_existing_file_appends_without_truncating() -> Result<(), WalError> {
    let temp = temp_path("append.wal")?;
    let path = temp.path();
    let first = WalEntry::put(b"k".to_vec(), b"v".to_vec());
    {
        let mut wal = DurableWal::new(path, FsyncPolicy::PerWrite)?;
        wal.append(&first)?;
    }
    let first_len = std::fs::read(path)?.len();

    let second = WalEntry::delete(b"k".to_vec());
    {
        let mut wal = DurableWal::new(path, FsyncPolicy::PerWrite)?;
        wal.append(&second)?;
    }
    let total_len = std::fs::read(path)?.len();
    assert!(total_len > first_len);
    assert_file_contents(&DurableWal::read_file(path)?, &[first, second], None);
    Ok(())
}

#[test]
fn append_writes_entry_to_end_of_file() -> Result<(), WalError> {
    let temp = temp_path("put.wal")?;
    let path = temp.path();
    let entry = WalEntry::put(b"k".to_vec(), b"v".to_vec());
    {
        let mut wal = DurableWal::new(path, FsyncPolicy::PerWrite)?;
        wal.append(&entry)?;
    }
    let bytes = std::fs::read(path)?;
    assert_eq!(bytes, frame(&entry));
    assert_file_contents(&DurableWal::read_file(path)?, &[entry], None);
    Ok(())
}

#[test]
fn append_mutation_writes_corresponding_wal_entry() -> Result<(), WalError> {
    let temp = temp_path("mutation.wal")?;
    let path = temp.path();
    let mutation = Mutation::Put {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    };
    let expected = WalEntry::from(&mutation);
    {
        let mut wal = DurableWal::new(path, FsyncPolicy::PerWrite)?;
        wal.append_mutation(&mutation)?;
    }
    assert_file_contents(&DurableWal::read_file(path)?, &[expected], None);
    Ok(())
}

#[test]
fn multiple_appends_are_consecutive_frames_with_no_gaps() -> Result<(), WalError> {
    let temp = temp_path("two.wal")?;
    let path = temp.path();
    let first = WalEntry::put(b"k1".to_vec(), b"v1".to_vec());
    let second = WalEntry::delete(b"k2".to_vec());
    {
        let mut wal = DurableWal::new(path, FsyncPolicy::PerWrite)?;
        wal.append(&first)?;
        wal.append(&second)?;
    }
    let bytes = std::fs::read(path)?;

    let mut expected = frame(&first);
    expected.extend_from_slice(&frame(&second));
    assert_eq!(bytes, expected);
    assert_file_contents(&DurableWal::read_file(path)?, &[first, second], None);
    Ok(())
}

#[test]
fn file_grows_monotonically_across_appends() -> Result<(), WalError> {
    let temp = temp_path("growth.wal")?;
    let path = temp.path();
    let mut wal = DurableWal::new(path, FsyncPolicy::CommitOnly)?;
    let first = WalEntry::put(b"a".to_vec(), b"1".to_vec());
    let second = WalEntry::put(b"b".to_vec(), b"2".to_vec());

    let initial = std::fs::metadata(path)?.len();
    wal.append(&first)?;
    let after_first = std::fs::metadata(path)?.len();
    wal.append(&second)?;
    let after_second = std::fs::metadata(path)?.len();

    assert!(initial < after_first);
    assert!(after_first < after_second);
    Ok(())
}

#[test]
fn append_returns_after_entry_bytes_are_visible_for_acknowledgement() -> Result<(), WalError> {
    let temp = temp_path("ack.wal")?;
    let path = temp.path();
    let entry = WalEntry::put(b"event".to_vec(), b"payload".to_vec());
    let mut wal = DurableWal::new(path, FsyncPolicy::CommitOnly)?;

    wal.append(&entry)?;

    assert_file_contents(&DurableWal::read_file(path)?, &[entry], None);
    Ok(())
}

#[test]
fn per_write_policy_syncs_after_every_append_by_resetting_counter() -> Result<(), WalError> {
    let temp = temp_path("per-write.wal")?;
    let path = temp.path();
    let mut wal = DurableWal::new(path, FsyncPolicy::PerWrite)?;
    wal.append(&WalEntry::delete(b"one".to_vec()))?;
    assert_eq!(wal.writes_since_sync, 0);
    wal.append(&WalEntry::delete(b"two".to_vec()))?;
    assert_eq!(wal.writes_since_sync, 0);
    Ok(())
}

#[test]
fn batched_policy_syncs_after_every_nth_append() -> Result<(), WalError> {
    let temp = temp_path("batched.wal")?;
    let path = temp.path();
    let mut wal = DurableWal::new(path, FsyncPolicy::Batched(5))?;

    for index in 0..4 {
        wal.append(&WalEntry::delete(vec![index]))?;
        assert_eq!(wal.writes_since_sync, index as usize + 1);
    }
    wal.append(&WalEntry::delete(vec![4]))?;
    assert_eq!(wal.writes_since_sync, 0);
    wal.append(&WalEntry::delete(vec![5]))?;
    assert_eq!(wal.writes_since_sync, 1);
    Ok(())
}

#[test]
fn commit_only_policy_syncs_only_when_commit_replaces_file() -> Result<(), WalError> {
    let temp = temp_path("commit-only.wal")?;
    let path = temp.path();
    let root = Hash::from_bytes([9; 32]);
    let mut wal = DurableWal::new(path, FsyncPolicy::CommitOnly)?;
    wal.append(&WalEntry::delete(b"one".to_vec()))?;
    wal.append(&WalEntry::delete(b"two".to_vec()))?;
    assert_eq!(wal.writes_since_sync, 2);

    wal.commit(root)?;

    assert_eq!(wal.writes_since_sync, 0);
    assert_eq!(std::fs::read(path)?, truncation_marker_frame(root));
    assert_file_contents(&DurableWal::read_file(path)?, &[], Some(root));
    Ok(())
}

#[test]
fn batched_zero_policy_is_rejected() -> Result<(), WalError> {
    let temp = temp_path("invalid-policy.wal")?;
    let path = temp.path();
    assert!(matches!(
        DurableWal::new(path, FsyncPolicy::Batched(0)),
        Err(WalError::InvalidFsyncPolicy { interval: 0 })
    ));
    Ok(())
}

#[test]
fn commit_truncates_wal_to_single_marker_with_root_hash() -> Result<(), WalError> {
    let temp = temp_path("commit.wal")?;
    let path = temp.path();
    let root = Hash::from_bytes([7; 32]);
    {
        let mut wal = DurableWal::new(path, FsyncPolicy::PerWrite)?;
        wal.append(&WalEntry::put(b"k1".to_vec(), b"v1".to_vec()))?;
        wal.append(&WalEntry::delete(b"k2".to_vec()))?;
        wal.commit(root)?;
    }

    let bytes = std::fs::read(path)?;
    assert_eq!(bytes, truncation_marker_frame(root));
    let contents = DurableWal::read_file(path)?;
    assert_file_contents(&contents, &[], Some(root));
    Ok(())
}

#[test]
fn appends_after_commit_continue_after_marker() -> Result<(), WalError> {
    let temp = temp_path("post-commit.wal")?;
    let path = temp.path();
    let root = Hash::from_bytes([3; 32]);
    let entry = WalEntry::put(b"after".to_vec(), b"commit".to_vec());
    {
        let mut wal = DurableWal::new(path, FsyncPolicy::PerWrite)?;
        wal.append(&WalEntry::delete(b"before".to_vec()))?;
        wal.commit(root)?;
        wal.append(&entry)?;
    }

    let mut expected = truncation_marker_frame(root);
    expected.extend_from_slice(&frame(&entry));
    assert_eq!(std::fs::read(path)?, expected);

    let contents = DurableWal::read_file(path)?;
    assert_file_contents(&contents, &[entry], Some(root));
    Ok(())
}

#[test]
fn scanner_rejects_corrupt_truncation_marker_checksum() -> Result<(), WalError> {
    let temp = temp_path("corrupt-marker.wal")?;
    let path = temp.path();
    let root = Hash::from_bytes([4; 32]);
    let mut bytes = truncation_marker_frame(root);
    let last_index = bytes.len().saturating_sub(1);
    bytes[last_index] ^= 0xff;
    std::fs::write(path, bytes)?;

    assert!(matches!(
        DurableWal::read_file(path),
        Err(WalError::ChecksumMismatch { .. })
    ));
    Ok(())
}

#[test]
fn scanner_rejects_truncated_frame() -> Result<(), WalError> {
    let temp = temp_path("truncated.wal")?;
    let path = temp.path();
    std::fs::write(path, 10u32.to_le_bytes())?;
    assert!(matches!(
        DurableWal::read_file(path),
        Err(WalError::Truncated)
    ));
    Ok(())
}

#[test]
fn durable_wal_and_policy_are_debug() -> Result<(), WalError> {
    let temp = temp_path("debug.wal")?;
    let path = temp.path();
    let wal = DurableWal::new(path, FsyncPolicy::PerWrite)?;
    assert!(format!("{wal:?}").contains("DurableWal"));
    assert!(format!("{:?}", FsyncPolicy::Batched(5)).contains("Batched"));
    Ok(())
}
