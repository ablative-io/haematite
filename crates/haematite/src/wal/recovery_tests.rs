use super::{RecoveredEntry, WalRecovery, decode_payload};
use crate::tree::Hash;
use crate::wal::buffer::{LookupResult, Mutation, WalError};
use crate::wal::entry::{TAG_DELETE, TAG_PUT, WalEntry};
use crate::wal::{DurableWal, FsyncPolicy};
use std::path::{Path, PathBuf};

#[path = "recovery_persist_003_tests.rs"]
mod persist_003;

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

fn entry_frame(entry: &WalEntry) -> Vec<u8> {
    let bytes = entry.serialise();
    let mut frame = Vec::new();
    frame.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    frame.extend_from_slice(&bytes);
    frame
}

fn payload_frame(payload: &[u8]) -> Vec<u8> {
    let mut entry = payload.to_vec();
    entry.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    let mut frame = Vec::new();
    frame.extend_from_slice(&(entry.len() as u32).to_le_bytes());
    frame.extend_from_slice(&entry);
    frame
}

fn commit_frame(root: Hash) -> Vec<u8> {
    let mut payload = vec![0x03];
    payload.extend_from_slice(root.as_bytes());
    payload_frame(&payload)
}

fn put_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut payload = vec![TAG_PUT];
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
    payload.extend_from_slice(value);
    payload
}

fn delete_payload(key: &[u8]) -> Vec<u8> {
    let mut payload = vec![TAG_DELETE];
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);
    payload
}

fn recover_file(path: &Path) -> Result<crate::wal::WalBuffer, WalError> {
    let mut recovery = WalRecovery::open(path)?;
    recovery.recover(Hash::from_bytes([0; 32]))
}

#[test]
fn open_existing_file_returns_recovery() -> Result<(), WalError> {
    let temp = temp_path("existing.wal")?;
    std::fs::write(temp.path(), b"wal bytes")?;

    let recovery = WalRecovery::open(temp.path())?;

    assert!(format!("{recovery:?}").contains("WalRecovery"));
    Ok(())
}

#[test]
fn open_missing_file_returns_io_error() -> Result<(), WalError> {
    let temp = temp_path("missing.wal")?;

    assert!(matches!(
        WalRecovery::open(temp.path()),
        Err(WalError::Io(_))
    ));
    Ok(())
}

#[test]
fn open_does_not_truncate_existing_file() -> Result<(), WalError> {
    let temp = temp_path("non-truncate.wal")?;
    std::fs::write(temp.path(), b"durable")?;
    let before = std::fs::metadata(temp.path())?.len();

    let recovery = WalRecovery::open(temp.path())?;
    let after = std::fs::metadata(temp.path())?.len();

    assert_eq!(before, after);
    assert!(format!("{recovery:?}").contains("WalRecovery"));
    Ok(())
}

#[test]
fn read_frame_returns_payload_for_well_formed_frame() -> Result<(), WalError> {
    let temp = temp_path("frame.wal")?;
    let payload = put_payload(b"key", b"value");
    std::fs::write(temp.path(), payload_frame(&payload))?;
    let mut recovery = WalRecovery::open(temp.path())?;

    assert_eq!(recovery.read_frame()?, Some(payload));
    Ok(())
}

#[test]
fn read_frame_rejects_crc_mismatch_with_expected_and_actual() -> Result<(), WalError> {
    let temp = temp_path("bad-crc.wal")?;
    let payload = put_payload(b"k", b"v");
    let mut bytes = payload_frame(&payload);
    if let Some(byte) = bytes.last_mut() {
        *byte ^= 0xff;
    }
    std::fs::write(temp.path(), bytes)?;
    let mut recovery = WalRecovery::open(temp.path())?;

    assert!(matches!(
        recovery.read_frame(),
        Err(WalError::ChecksumMismatch { expected, actual })
            if expected != actual && actual == crc32fast::hash(&payload)
    ));
    Ok(())
}

#[test]
fn read_frame_returns_none_at_clean_eof() -> Result<(), WalError> {
    let temp = temp_path("empty.wal")?;
    std::fs::write(temp.path(), [])?;
    let mut recovery = WalRecovery::open(temp.path())?;

    assert_eq!(recovery.read_frame()?, None);
    Ok(())
}

#[test]
fn read_frame_returns_none_for_short_length_prefix() -> Result<(), WalError> {
    let temp = temp_path("short-prefix.wal")?;
    std::fs::write(temp.path(), [1, 0])?;
    let mut recovery = WalRecovery::open(temp.path())?;

    assert_eq!(recovery.read_frame()?, None);
    Ok(())
}

#[test]
fn read_frame_returns_none_for_short_payload() -> Result<(), WalError> {
    let temp = temp_path("short-payload.wal")?;
    let mut bytes = 10u32.to_le_bytes().to_vec();
    bytes.extend_from_slice(b"abc");
    std::fs::write(temp.path(), bytes)?;
    let mut recovery = WalRecovery::open(temp.path())?;

    assert_eq!(recovery.read_frame()?, None);
    Ok(())
}

#[test]
fn read_frame_returns_none_for_missing_checksum() -> Result<(), WalError> {
    let temp = temp_path("missing-checksum.wal")?;
    let payload = put_payload(b"k", b"v");
    let entry_len = u32::try_from(payload.len() + 4).map_err(|_| WalError::LengthOverflow)?;
    let mut bytes = entry_len.to_le_bytes().to_vec();
    bytes.extend_from_slice(&payload);
    std::fs::write(temp.path(), bytes)?;
    let mut recovery = WalRecovery::open(temp.path())?;

    assert_eq!(recovery.read_frame()?, None);
    Ok(())
}

#[test]
fn decode_put_payload_returns_mutation() -> Result<(), WalError> {
    let entry = decode_payload(&put_payload(b"k", b"v"))?;

    assert_eq!(
        entry,
        RecoveredEntry::Mutation(Mutation::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })
    );
    Ok(())
}

#[test]
fn decode_delete_payload_returns_mutation() -> Result<(), WalError> {
    let entry = decode_payload(&delete_payload(b"k"))?;

    assert_eq!(
        entry,
        RecoveredEntry::Mutation(Mutation::Delete { key: b"k".to_vec() })
    );
    Ok(())
}

#[test]
fn decode_commit_payload_returns_root_hash() -> Result<(), WalError> {
    let root = Hash::from_bytes([8; 32]);
    let mut payload = vec![0x03];
    payload.extend_from_slice(root.as_bytes());

    assert_eq!(decode_payload(&payload)?, RecoveredEntry::Commit(root));
    Ok(())
}

#[test]
fn decode_unknown_tag_returns_checksum_mismatch_message() {
    let error = decode_payload(&[0xff]).err();

    assert!(matches!(error, Some(WalError::ChecksumMismatch { .. })));
    assert!(matches!(
        error.as_ref().map(std::string::ToString::to_string),
        Some(message) if message.contains("unknown tag") && message.contains("0xff")
    ));
}

#[test]
fn decode_partial_put_field_is_corruption_error() {
    let mut payload = vec![TAG_PUT];
    payload.extend_from_slice(&4u32.to_le_bytes());
    payload.extend_from_slice(b"xy");

    assert!(matches!(
        decode_payload(&payload),
        Err(WalError::ChecksumMismatch { .. })
    ));
}

#[test]
fn recover_replays_only_mutations_after_last_commit() -> Result<(), WalError> {
    let temp = temp_path("after-commit.wal")?;
    let mut bytes = entry_frame(&WalEntry::put(b"a".to_vec(), b"1".to_vec()));
    bytes.extend_from_slice(&commit_frame(Hash::from_bytes([1; 32])));
    bytes.extend_from_slice(&entry_frame(&WalEntry::put(b"b".to_vec(), b"2".to_vec())));
    bytes.extend_from_slice(&entry_frame(&WalEntry::put(b"c".to_vec(), b"3".to_vec())));
    std::fs::write(temp.path(), bytes)?;

    let buffer = recover_file(temp.path())?;

    assert_eq!(buffer.len(), 2);
    assert_eq!(buffer.get(b"a"), LookupResult::NotBuffered);
    assert_eq!(buffer.get(b"b"), LookupResult::BufferedValue(b"2".to_vec()));
    assert_eq!(buffer.get(b"c"), LookupResult::BufferedValue(b"3".to_vec()));
    Ok(())
}

#[test]
fn recover_accepts_truncated_tail_after_commit() -> Result<(), WalError> {
    let temp = temp_path("truncated-tail.wal")?;
    let mut bytes = entry_frame(&WalEntry::put(b"a".to_vec(), b"1".to_vec()));
    bytes.extend_from_slice(&commit_frame(Hash::from_bytes([2; 32])));
    bytes.extend_from_slice(&entry_frame(&WalEntry::put(b"b".to_vec(), b"2".to_vec())));
    bytes.extend_from_slice(&100u32.to_le_bytes());
    bytes.extend_from_slice(b"short");
    std::fs::write(temp.path(), bytes)?;

    let buffer = recover_file(temp.path())?;

    assert_eq!(buffer.len(), 1);
    assert_eq!(buffer.get(b"b"), LookupResult::BufferedValue(b"2".to_vec()));
    Ok(())
}

#[test]
fn recover_replays_all_entries_when_no_commit_exists() -> Result<(), WalError> {
    let temp = temp_path("no-commit.wal")?;
    let mut bytes = entry_frame(&WalEntry::put(b"a".to_vec(), b"1".to_vec()));
    bytes.extend_from_slice(&entry_frame(&WalEntry::put(b"b".to_vec(), b"2".to_vec())));
    std::fs::write(temp.path(), bytes)?;

    let buffer = recover_file(temp.path())?;

    assert_eq!(buffer.len(), 2);
    assert_eq!(buffer.get(b"a"), LookupResult::BufferedValue(b"1".to_vec()));
    assert_eq!(buffer.get(b"b"), LookupResult::BufferedValue(b"2".to_vec()));
    Ok(())
}

#[test]
fn recover_resets_buffer_after_each_commit() -> Result<(), WalError> {
    let temp = temp_path("two-commits.wal")?;
    let mut bytes = entry_frame(&WalEntry::put(b"a".to_vec(), b"1".to_vec()));
    bytes.extend_from_slice(&commit_frame(Hash::from_bytes([4; 32])));
    bytes.extend_from_slice(&entry_frame(&WalEntry::put(b"b".to_vec(), b"2".to_vec())));
    bytes.extend_from_slice(&commit_frame(Hash::from_bytes([5; 32])));
    bytes.extend_from_slice(&entry_frame(&WalEntry::put(b"c".to_vec(), b"3".to_vec())));
    std::fs::write(temp.path(), bytes)?;

    let buffer = recover_file(temp.path())?;

    assert_eq!(buffer.len(), 1);
    assert_eq!(buffer.get(b"a"), LookupResult::NotBuffered);
    assert_eq!(buffer.get(b"b"), LookupResult::NotBuffered);
    assert_eq!(buffer.get(b"c"), LookupResult::BufferedValue(b"3".to_vec()));
    Ok(())
}

#[test]
fn durable_wal_round_trip_recovers_only_post_commit_mutations() -> Result<(), WalError> {
    let temp = temp_path("durable-round-trip.wal")?;
    let committed_root = Hash::from_bytes([7; 32]);
    let mut wal = DurableWal::new(temp.path(), FsyncPolicy::CommitOnly)?;
    wal.append(&WalEntry::put(b"a".to_vec(), b"1".to_vec()))?;
    wal.commit(committed_root)?;
    wal.append(&WalEntry::put(b"b".to_vec(), b"2".to_vec()))?;
    wal.append(&WalEntry::delete(b"gone".to_vec()))?;
    drop(wal);

    let mut recovery = WalRecovery::open(temp.path())?;
    let buffer = recovery.recover(committed_root)?;

    assert_eq!(buffer.len(), 2);
    assert_eq!(buffer.get(b"a"), LookupResult::NotBuffered);
    assert_eq!(buffer.get(b"b"), LookupResult::BufferedValue(b"2".to_vec()));
    assert_eq!(buffer.get(b"gone"), LookupResult::BufferedDelete);
    Ok(())
}

#[test]
fn recover_does_not_modify_wal_file() -> Result<(), WalError> {
    let temp = temp_path("unchanged.wal")?;
    let mut bytes = commit_frame(Hash::from_bytes([6; 32]));
    bytes.extend_from_slice(&entry_frame(&WalEntry::delete(b"gone".to_vec())));
    std::fs::write(temp.path(), bytes)?;
    let before = std::fs::metadata(temp.path())?.len();

    let buffer = recover_file(temp.path())?;
    let after = std::fs::metadata(temp.path())?.len();

    assert_eq!(before, after);
    assert_eq!(buffer.get(b"gone"), LookupResult::BufferedDelete);
    Ok(())
}
