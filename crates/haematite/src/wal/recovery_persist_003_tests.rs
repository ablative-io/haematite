use super::{commit_frame, entry_frame, temp_path};
use crate::shard::actor::ShardActor;
use crate::store::{MemoryStore, NodeStore};
use crate::tree::{Hash, LeafNode, Node};
use crate::wal::buffer::{LookupResult, WalError};
use crate::wal::entry::WalEntry;
use crate::wal::{DurableWal, FsyncPolicy, WalRecovery};
use std::fmt;
use std::sync::Once;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

fn stored_empty_root(store: &mut MemoryStore) -> Result<Hash, WalError> {
    let node = Node::Leaf(LeafNode::new(Vec::new()).map_err(|error| {
        WalError::TreeError(format!("failed to build empty root node: {error}"))
    })?);
    Ok(store.put(&node))
}

fn corrupt_last_byte(mut bytes: Vec<u8>) -> Vec<u8> {
    if let Some(byte) = bytes.last_mut() {
        *byte ^= 0xff;
    }
    bytes
}

struct BlockingStore {
    expected_root: Hash,
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

impl fmt::Debug for BlockingStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BlockingStore")
    }
}

impl NodeStore for BlockingStore {
    type Error = std::convert::Infallible;

    fn get(&self, hash: &Hash) -> Result<Option<std::sync::Arc<Node>>, Self::Error> {
        if *hash == self.expected_root && self.entered.send(()).is_ok() {
            assert!(self.release.recv().is_ok());
        }
        Ok(None)
    }

    fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
        Ok(node.hash())
    }
}

static TEST_LOGGER: RecoveryTestLogger = RecoveryTestLogger;
static WARNING_COUNT: AtomicUsize = AtomicUsize::new(0);
static LOGGER_INIT: Once = Once::new();

struct RecoveryTestLogger;

impl log::Log for RecoveryTestLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Warn
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata())
            && record.level() == log::Level::Warn
            && record.args().to_string().contains("wal recovery stopped")
        {
            WARNING_COUNT.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn flush(&self) {}
}

fn init_test_logger() {
    LOGGER_INIT.call_once(|| {
        if log::set_logger(&TEST_LOGGER).is_ok() {
            log::set_max_level(log::LevelFilter::Warn);
        }
    });
}

fn warning_count() -> usize {
    WARNING_COUNT.load(Ordering::Relaxed)
}

#[test]
fn recover_path_missing_wal_starts_with_empty_tree() -> Result<(), WalError> {
    let temp = temp_path("fresh-missing.wal")?;
    let store = MemoryStore::new();

    let recovered = WalRecovery::recover_path(temp.path(), &store)?;

    assert_eq!(recovered.committed_root(), None);
    assert!(recovered.buffer().is_empty());
    assert_eq!(recovered.replayed_mutations(), 0);
    assert!(!recovered.stopped_at_corruption());
    assert!(!temp.path().exists());
    Ok(())
}

#[test]
fn recover_path_marker_only_returns_committed_state() -> Result<(), WalError> {
    let temp = temp_path("marker-only.wal")?;
    let mut store = MemoryStore::new();
    let committed_root = stored_empty_root(&mut store)?;
    std::fs::write(temp.path(), commit_frame(committed_root))?;

    let recovered = WalRecovery::recover_path(temp.path(), &store)?;

    assert_eq!(recovered.committed_root(), Some(committed_root));
    assert!(recovered.buffer().is_empty());
    assert_eq!(recovered.replayed_mutations(), 0);
    assert!(!recovered.stopped_at_corruption());
    Ok(())
}

#[test]
fn recover_path_multiple_markers_return_last_root() -> Result<(), WalError> {
    let temp = temp_path("last-marker.wal")?;
    let mut store = MemoryStore::new();
    let stale_root = Hash::from_bytes([4; 32]);
    let committed_root = stored_empty_root(&mut store)?;
    let mut bytes = entry_frame(&WalEntry::put(b"a".to_vec(), b"1".to_vec()));
    bytes.extend_from_slice(&commit_frame(stale_root));
    bytes.extend_from_slice(&entry_frame(&WalEntry::put(b"b".to_vec(), b"2".to_vec())));
    bytes.extend_from_slice(&commit_frame(committed_root));
    bytes.extend_from_slice(&entry_frame(&WalEntry::put(b"c".to_vec(), b"3".to_vec())));
    std::fs::write(temp.path(), bytes)?;

    let recovered = WalRecovery::recover_path(temp.path(), &store)?;

    assert_eq!(recovered.committed_root(), Some(committed_root));
    assert_eq!(recovered.buffer().len(), 1);
    assert_eq!(recovered.buffer().get(b"a"), LookupResult::NotBuffered);
    assert_eq!(recovered.buffer().get(b"b"), LookupResult::NotBuffered);
    assert_eq!(
        recovered.buffer().get(b"c"),
        LookupResult::BufferedValue(b"3".to_vec())
    );
    assert_eq!(recovered.replayed_mutations(), 1);
    Ok(())
}

#[test]
fn recover_path_missing_committed_root_is_fatal() -> Result<(), WalError> {
    let temp = temp_path("missing-root.wal")?;
    let missing_root = Hash::from_bytes([9; 32]);
    std::fs::write(temp.path(), commit_frame(missing_root))?;
    let store = MemoryStore::new();

    let result = WalRecovery::recover_path(temp.path(), &store);

    assert!(matches!(
        &result,
        Err(WalError::MissingCommittedRoot { root }) if *root == missing_root
    ));
    let rendered = match result {
        Err(error) => error.to_string(),
        Ok(_) => String::new(),
    };
    assert!(rendered.contains(&missing_root.to_string()));
    Ok(())
}

#[test]
fn recover_stops_at_garbage_length_prefix_without_oom() -> Result<(), WalError> {
    // A crash (or corruption) can leave a frame whose length prefix claims far
    // more bytes than the file holds. Recovery must NOT allocate from that
    // untrusted length (~4 GiB here); it should treat the frame as a truncated
    // tail and return the prior recovered state. The test completing promptly is
    // itself the OOM-guard assertion.
    let temp = temp_path("garbage-length.wal")?;
    let mut store = MemoryStore::new();
    let committed_root = stored_empty_root(&mut store)?;
    let mut bytes = commit_frame(committed_root);
    bytes.extend_from_slice(&entry_frame(&WalEntry::put(b"k".to_vec(), b"v".to_vec())));
    bytes.extend_from_slice(&u32::MAX.to_le_bytes()); // garbage length prefix (~4 GiB)
    bytes.extend_from_slice(b"junk"); // far fewer than the claimed bytes
    std::fs::write(temp.path(), bytes)?;

    let recovered = WalRecovery::recover_path(temp.path(), &store)?;

    assert_eq!(recovered.committed_root(), Some(committed_root));
    assert_eq!(
        recovered.buffer().get(b"k"),
        LookupResult::BufferedValue(b"v".to_vec())
    );
    assert_eq!(recovered.replayed_mutations(), 1);
    assert!(!recovered.stopped_at_corruption());
    Ok(())
}

#[test]
fn recover_stops_at_corrupt_frame_after_commit_and_logs_warning() -> Result<(), WalError> {
    init_test_logger();
    let temp = temp_path("corrupt-after-commit.wal")?;
    let corrupt = corrupt_last_byte(entry_frame(&WalEntry::put(
        b"bad".to_vec(),
        b"crc".to_vec(),
    )));
    let mut bytes = entry_frame(&WalEntry::put(b"a".to_vec(), b"1".to_vec()));
    bytes.extend_from_slice(&commit_frame(Hash::from_bytes([3; 32])));
    bytes.extend_from_slice(&entry_frame(&WalEntry::put(
        b"good".to_vec(),
        b"before".to_vec(),
    )));
    bytes.extend_from_slice(&corrupt);
    bytes.extend_from_slice(&entry_frame(&WalEntry::put(
        b"ignored".to_vec(),
        b"after".to_vec(),
    )));
    std::fs::write(temp.path(), bytes)?;

    let before_warnings = warning_count();
    let mut recovery = WalRecovery::open(temp.path())?;
    let recovered = recovery.recover_unverified()?;

    assert!(recovered.stopped_at_corruption());
    assert!(warning_count() > before_warnings);
    assert_eq!(recovered.replayed_mutations(), 1);
    assert_eq!(
        recovered.buffer().get(b"good"),
        LookupResult::BufferedValue(b"before".to_vec())
    );
    assert_eq!(
        recovered.buffer().get(b"ignored"),
        LookupResult::NotBuffered
    );
    Ok(())
}

#[test]
fn shard_recovery_reads_only_the_requested_wal_file() -> Result<(), WalError> {
    let shard0 = temp_path("shard-0.wal")?;
    let shard5 = temp_path("shard-5.wal")?;
    let mut store = MemoryStore::new();
    let root = stored_empty_root(&mut store)?;
    let mut shard0_bytes = commit_frame(root);
    shard0_bytes.extend_from_slice(&entry_frame(&WalEntry::put(
        b"shard-0".to_vec(),
        b"ready".to_vec(),
    )));
    std::fs::write(shard0.path(), shard0_bytes)?;
    std::fs::write(
        shard5.path(),
        corrupt_last_byte(commit_frame(Hash::from_bytes([5; 32]))),
    )?;

    let recovered = WalRecovery::recover_path(shard0.path(), &store)?;

    assert_eq!(recovered.committed_root(), Some(root));
    assert_eq!(recovered.replayed_mutations(), 1);
    assert_eq!(
        recovered.buffer().get(b"shard-0"),
        LookupResult::BufferedValue(b"ready".to_vec())
    );
    Ok(())
}

#[test]
fn large_shard_wal_does_not_delay_other_shard_recovery() -> Result<(), WalError> {
    let small = temp_path("small-shard.wal")?;
    let large = temp_path("large-shard.wal")?;
    let mut store = MemoryStore::new();
    let root = stored_empty_root(&mut store)?;
    let mut small_bytes = commit_frame(root);
    small_bytes.extend_from_slice(&entry_frame(&WalEntry::put(
        b"fast".to_vec(),
        b"available".to_vec(),
    )));
    std::fs::write(small.path(), small_bytes)?;
    let mut large_bytes = commit_frame(root);
    for index in 0..256u32 {
        large_bytes.extend_from_slice(&entry_frame(&WalEntry::put(
            format!("large-{index:04}").into_bytes(),
            b"slow".to_vec(),
        )));
    }
    std::fs::write(large.path(), large_bytes)?;

    let recovered_small = WalRecovery::recover_path(small.path(), &store)?;

    assert_eq!(recovered_small.committed_root(), Some(root));
    assert_eq!(recovered_small.replayed_mutations(), 1);
    assert_eq!(
        recovered_small.buffer().get(b"fast"),
        LookupResult::BufferedValue(b"available".to_vec())
    );

    let recovered_large = WalRecovery::recover_path(large.path(), &store)?;
    assert_eq!(recovered_large.replayed_mutations(), 256);
    assert_eq!(
        recovered_large.buffer().get(b"large-0255"),
        LookupResult::BufferedValue(b"slow".to_vec())
    );
    Ok(())
}

#[test]
fn shard_zero_accepts_writes_while_shard_five_recovery_is_blocked() -> Result<(), WalError> {
    let shard0 = temp_path("concurrent-shard-0.wal")?;
    let shard5 = temp_path("concurrent-shard-5.wal")?;
    let mut shard0_store = MemoryStore::new();
    let shard0_root = stored_empty_root(&mut shard0_store)?;
    let mut shard0_bytes = commit_frame(shard0_root);
    shard0_bytes.extend_from_slice(&entry_frame(&WalEntry::put(
        b"ready".to_vec(),
        b"before".to_vec(),
    )));
    std::fs::write(shard0.path(), shard0_bytes)?;

    let shard5_root = Hash::from_bytes([5; 32]);
    std::fs::write(shard5.path(), commit_frame(shard5_root))?;
    let shard5_path = shard5.path().to_path_buf();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let recovery_thread = std::thread::spawn(move || {
        let store = BlockingStore {
            expected_root: shard5_root,
            entered: entered_tx,
            release: release_rx,
        };
        WalRecovery::recover_path(shard5_path, &store)
    });

    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| WalError::TreeError(format!("shard 5 recovery did not block: {error}")))?;

    let recovered = WalRecovery::recover_path(shard0.path(), &shard0_store)?;
    let wal = DurableWal::new(shard0.path(), FsyncPolicy::CommitOnly)?;
    let mut actor = ShardActor::from_recovered(wal, recovered, &shard0_store)?;
    actor.put(b"accepted".to_vec(), b"while-shard-5-blocked".to_vec())?;

    assert_eq!(
        actor.buffer().get(b"accepted"),
        LookupResult::BufferedValue(b"while-shard-5-blocked".to_vec())
    );

    release_tx
        .send(())
        .map_err(|error| WalError::TreeError(format!("failed to unblock shard 5: {error}")))?;
    let shard5_result = recovery_thread
        .join()
        .map_err(|_| WalError::TreeError("shard 5 recovery thread panicked".to_string()))?;
    assert!(matches!(
        shard5_result,
        Err(WalError::MissingCommittedRoot { root }) if root == shard5_root
    ));
    Ok(())
}

#[test]
fn recovered_wal_resumes_append_mode_after_replayed_entries() -> Result<(), WalError> {
    let temp = temp_path("resume-append.wal")?;
    let mut store = MemoryStore::new();
    let committed_root = stored_empty_root(&mut store)?;
    let mut wal = DurableWal::new(temp.path(), FsyncPolicy::CommitOnly)?;
    wal.commit(committed_root)?;
    wal.append(&WalEntry::put(b"replayed".to_vec(), b"before".to_vec()))?;
    drop(wal);

    let recovered = WalRecovery::recover_path(temp.path(), &store)?;
    assert_eq!(recovered.committed_root(), Some(committed_root));
    assert_eq!(
        recovered.buffer().get(b"replayed"),
        LookupResult::BufferedValue(b"before".to_vec())
    );

    let mut wal = DurableWal::new(temp.path(), FsyncPolicy::CommitOnly)?;
    wal.append(&WalEntry::put(b"new".to_vec(), b"after".to_vec()))?;
    drop(wal);

    assert_eq!(
        DurableWal::read_file(temp.path())?.entries(),
        &[
            WalEntry::put(b"replayed".to_vec(), b"before".to_vec()),
            WalEntry::put(b"new".to_vec(), b"after".to_vec()),
        ]
    );
    Ok(())
}

#[test]
fn commit_after_recovery_truncates_to_new_root() -> Result<(), WalError> {
    let temp = temp_path("commit-after-recovery.wal")?;
    let mut store = MemoryStore::new();
    let committed_root = stored_empty_root(&mut store)?;
    let mut wal = DurableWal::new(temp.path(), FsyncPolicy::CommitOnly)?;
    wal.commit(committed_root)?;
    wal.append(&WalEntry::put(b"event".to_vec(), b"payload".to_vec()))?;
    wal.append(&WalEntry::delete(b"missing".to_vec()))?;
    drop(wal);

    let recovered = WalRecovery::recover_path(temp.path(), &store)?;
    let mut buffer = recovered.into_buffer();
    assert_eq!(
        buffer.get(b"event"),
        LookupResult::BufferedValue(b"payload".to_vec())
    );
    assert_eq!(buffer.get(b"missing"), LookupResult::BufferedDelete);

    let new_root = buffer.commit(committed_root, &mut store)?;
    let mut wal = DurableWal::new(temp.path(), FsyncPolicy::CommitOnly)?;
    wal.commit(new_root)?;
    drop(wal);

    let contents = DurableWal::read_file(temp.path())?;
    assert_eq!(contents.committed_root(), Some(new_root));
    assert_eq!(contents.entries(), &[]);
    assert!(buffer.is_empty());
    assert_ne!(new_root, committed_root);
    Ok(())
}
