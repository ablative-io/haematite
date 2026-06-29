use std::fs;
use std::io;
use std::thread;
use std::time::Duration;

use haematite::tree::batch_mutate;
use haematite::wal::{DurableWal, FsyncPolicy, WalError, WalRecovery};
use haematite::{DeleteNode, DiskStore, Hash, LeafNode, Node, NodeError, NodeStore, StoreError};

const ZSTD_MAGIC_BYTES: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn leaf_node(key: &[u8], value: &[u8]) -> Result<Node, NodeError> {
    LeafNode::new(vec![(key.to_vec(), value.to_vec())]).map(Node::Leaf)
}

fn repetitive_leaf_node(entries: usize) -> Result<Node, NodeError> {
    let entries = (0..entries)
        .map(|index| (format!("key-{index:04}").into_bytes(), vec![b'x'; 256]))
        .collect();
    LeafNode::new(entries).map(Node::Leaf)
}

fn node_path(base_dir: &std::path::Path, hash: &Hash) -> std::path::PathBuf {
    let hex = hash.to_string();
    let (prefix, file_name) = hex.split_at(2);
    base_dir.join(prefix).join(file_name)
}

fn boxed_error(message: &'static str) -> Box<dyn std::error::Error> {
    Box::new(io::Error::other(message))
}

fn require_store_error(
    result: Result<DiskStore, StoreError>,
) -> Result<StoreError, Box<dyn std::error::Error>> {
    match result {
        Ok(_store) => Err(boxed_error("expected DiskStore construction to fail")),
        Err(error) => Ok(error),
    }
}

fn require_get_error(
    result: Result<Option<std::sync::Arc<Node>>, StoreError>,
) -> Result<StoreError, Box<dyn std::error::Error>> {
    match result {
        Ok(_node) => Err(boxed_error("expected DiskStore::get to fail")),
        Err(error) => Ok(error),
    }
}

#[test]
fn constructor_creates_base_directory_and_supports_configured_capacity() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let missing = temp_dir.path().join("missing");
    let store = DiskStore::new(&missing)?;
    assert!(missing.is_dir());
    assert_eq!(store.cache_capacity(), 1_024);
    assert!(format!("{store:?}").contains("DiskStore"));

    let configured = temp_dir.path().join("configured");
    let configured_store = DiskStore::with_cache_capacity(&configured, 2)?;
    assert!(configured.is_dir());
    assert_eq!(configured_store.cache_capacity(), 2);

    let file_path = temp_dir.path().join("not-a-directory");
    fs::write(&file_path, b"file")?;
    let file_error = require_store_error(DiskStore::new(&file_path))?;
    assert!(matches!(file_error, StoreError::NotADirectory { .. }));

    let zero_path = temp_dir.path().join("zero-missing");
    let zero_error = require_store_error(DiskStore::with_cache_capacity(&zero_path, 0))?;
    assert!(matches!(zero_error, StoreError::InvalidCapacity));
    assert!(!zero_path.exists());

    Ok(())
}

#[test]
fn store_error_traits_and_messages_are_available() {
    fn assert_error<T: std::error::Error>() {}

    assert_error::<StoreError>();
    let errors = [
        StoreError::DirectoryNotFound,
        StoreError::NotADirectory {
            path: std::path::PathBuf::from("file"),
        },
        StoreError::MissingParentDirectory {
            path: std::path::PathBuf::from("node"),
        },
        StoreError::InvalidCapacity,
        StoreError::Compression("zstd failed".to_owned()),
        StoreError::Deserialise("node failed".to_owned()),
        StoreError::Io(io::Error::other("io failed")),
    ];

    for error in errors {
        assert!(!error.to_string().is_empty());
    }
}

#[test]
fn put_writes_zstd_compressed_node_under_two_level_content_hash_path() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let mut store = DiskStore::with_cache_capacity(temp_dir.path(), 2)?;
    let node = repetitive_leaf_node(80)?;
    let serialised = node.serialise();
    let hash = store.put(&node)?;

    assert_eq!(hash, node.hash());
    let path = node_path(temp_dir.path(), &hash);
    assert!(path.is_file());
    assert!(!temp_dir.path().join(hash.to_string()).exists());
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("node path should have parent"))?;
    assert!(parent.is_dir());

    let compressed = fs::read(&path)?;
    assert!(compressed.len() < serialised.len());
    assert_eq!(
        compressed.get(..ZSTD_MAGIC_BYTES.len()),
        Some(ZSTD_MAGIC_BYTES.as_slice())
    );
    let decompressed = zstd::stream::decode_all(compressed.as_slice())?;
    assert_eq!(decompressed, serialised);
    assert_eq!(Node::deserialise(&decompressed)?, node);
    Ok(())
}

#[test]
fn duplicate_put_returns_hash_without_overwriting_existing_file() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let mut store = DiskStore::with_cache_capacity(temp_dir.path(), 2)?;
    let node = leaf_node(b"a", b"one")?;
    let hash = store.put(&node)?;
    let path = node_path(temp_dir.path(), &hash);
    let modified_before = fs::metadata(&path)?.modified()?;

    thread::sleep(Duration::from_millis(20));
    let second_hash = store.put(&node)?;
    let modified_after = fs::metadata(&path)?.modified()?;

    assert_eq!(second_hash, hash);
    assert_eq!(modified_after, modified_before);
    Ok(())
}

#[test]
fn get_reads_missing_disk_and_cached_nodes_correctly() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let mut writer = DiskStore::with_cache_capacity(temp_dir.path(), 2)?;
    let missing_hash = Hash::from_bytes([9; 32]);
    assert_eq!(writer.get(&missing_hash)?, None);
    assert!(!temp_dir.path().join("09").exists());

    let node = leaf_node(b"a", b"one")?;
    let hash = writer.put(&node)?;

    let reader = DiskStore::with_cache_capacity(temp_dir.path(), 2)?;
    assert_eq!(reader.get(&hash)?, Some(std::sync::Arc::new(node.clone())));

    fs::remove_file(node_path(temp_dir.path(), &hash))?;
    assert_eq!(reader.get(&hash)?, Some(std::sync::Arc::new(node)));
    Ok(())
}

#[test]
fn capacity_one_cache_evicts_the_least_recently_used_node_without_losing_disk_data() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let mut store = DiskStore::with_cache_capacity(temp_dir.path(), 1)?;
    let first = leaf_node(b"a", b"one")?;
    let second = leaf_node(b"b", b"two")?;
    let first_hash = store.put(&first)?;
    let second_hash = store.put(&second)?;

    assert_eq!(
        store.get(&first_hash)?,
        Some(std::sync::Arc::new(first.clone()))
    );

    fs::remove_file(node_path(temp_dir.path(), &first_hash))?;
    fs::remove_file(node_path(temp_dir.path(), &second_hash))?;
    assert_eq!(store.get(&second_hash)?, None);
    assert_eq!(store.get(&first_hash)?, Some(std::sync::Arc::new(first)));
    Ok(())
}

#[test]
fn disk_store_implements_fallible_node_store_trait() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let mut store = DiskStore::with_cache_capacity(temp_dir.path(), 2)?;
    let node = leaf_node(b"trait", b"roundtrip")?;

    let hash = NodeStore::put(&mut store, &node)?;

    assert_eq!(hash, node.hash());
    assert_eq!(
        NodeStore::get(&store, &hash)?,
        Some(std::sync::Arc::new(node))
    );
    Ok(())
}

#[test]
fn delete_removes_file_evicts_cache_and_is_idempotent() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let mut store = DiskStore::with_cache_capacity(temp_dir.path(), 2)?;
    let node = leaf_node(b"delete", b"cached")?;
    let hash = store.put(&node)?;

    assert_eq!(store.get(&hash)?, Some(std::sync::Arc::new(node)));
    assert!(node_path(temp_dir.path(), &hash).is_file());

    store.delete(&hash)?;

    assert!(!node_path(temp_dir.path(), &hash).exists());
    assert_eq!(store.get(&hash)?, None);
    store.delete(&hash)?;
    DeleteNode::delete(&store, &Hash::from_bytes([0xab; 32]))?;
    Ok(())
}

#[test]
fn corrupt_files_return_compression_or_deserialisation_errors() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let store = DiskStore::with_cache_capacity(temp_dir.path(), 2)?;

    let compressed_hash = Hash::from_bytes([4; 32]);
    let compressed_path = node_path(temp_dir.path(), &compressed_hash);
    fs::create_dir_all(
        compressed_path
            .parent()
            .ok_or_else(|| io::Error::other("compressed path should have parent"))?,
    )?;
    fs::write(compressed_path, b"not zstd")?;
    let compressed_error = require_get_error(store.get(&compressed_hash))?;
    assert!(matches!(compressed_error, StoreError::Compression(_)));

    let deserialise_hash = Hash::from_bytes([5; 32]);
    let deserialise_path = node_path(temp_dir.path(), &deserialise_hash);
    fs::create_dir_all(
        deserialise_path
            .parent()
            .ok_or_else(|| io::Error::other("deserialise path should have parent"))?,
    )?;
    let invalid_node = zstd::stream::encode_all([0xff].as_slice(), 0)?;
    fs::write(deserialise_path, invalid_node)?;
    let deserialise_error = require_get_error(store.get(&deserialise_hash))?;
    assert!(matches!(deserialise_error, StoreError::Deserialise(_)));

    Ok(())
}

// ---------------------------------------------------------------------------
// Directory-fsync durability window (HIGH, durability).
//
// `DiskStore::write_compressed_node` fsyncs each node file's DATA before the
// atomic rename that publishes it, but the rename's DIRECTORY ENTRY is only made
// durable by the batched `sync_dirty_dirs` barrier the commit path runs STRICTLY
// BEFORE the WAL committed-root marker. These tests exercise that window at the
// public `DiskStore` + `WalRecovery` boundary (complementing the actor-internal
// wrapper tests in `shard::actor::node_dir_fsync_tests`): if a committed-root
// marker is published over node directory entries that were NOT barriered and are
// then lost on power loss, recovery MUST fail closed (`MissingCommittedRoot`)
// rather than silently serve a tree with an unreachable node.
// ---------------------------------------------------------------------------

type TestResult2<T> = Result<T, Box<dyn std::error::Error>>;

/// Build a committed tree in `store` from `entries`, returning its committed root.
fn commit_tree(store: &mut DiskStore, entries: &[(&[u8], &[u8])]) -> TestResult2<Hash> {
    let empty = store.put(&Node::Leaf(LeafNode::new(Vec::new())?))?;
    let mutations: Vec<(Vec<u8>, Option<Vec<u8>>)> = entries
        .iter()
        .map(|(k, v)| (k.to_vec(), Some(v.to_vec())))
        .collect();
    Ok(batch_mutate(store, empty, mutations.as_slice())?)
}

/// THE DURABILITY WINDOW (falsifier): a committed-root marker published over a
/// node whose directory entry was lost (the rename never reached disk because the
/// barrier had not run) MUST make cold recovery reject with `MissingCommittedRoot`
/// — never silently accept an unreachable committed root.
///
/// We model the precise lost-rename window by DELETING the committed root's node
/// file after the marker is written: that is exactly the on-disk state after a
/// power loss that dropped an un-barriered directory entry. Cold recovery reads
/// the marker, fails to load the root node, and must fail closed.
#[test]
fn lost_node_dir_entry_makes_recovery_reject_missing_root() -> TestResult {
    let dir = tempfile::tempdir()?;
    let nodes_dir = dir.path().join("nodes");
    let wal_path = dir.path().join("shard.wal");

    // Persist a committed tree (node DATA is fsynced by `put`).
    let mut store = DiskStore::new(&nodes_dir)?;
    let root = commit_tree(&mut store, &[(b"durable-key", b"durable-value")])?;

    // Publish the committed-root MARKER into the WAL, as the commit path does
    // AFTER persisting nodes. (In production the dir barrier runs between these;
    // here we deliberately do NOT make the directory entry durable.)
    let mut wal = DurableWal::new(&wal_path, FsyncPolicy::CommitOnly)?;
    wal.commit(root)?;
    drop(wal);

    // Simulate the lost rename: the root node's directory entry never reached disk.
    fs::remove_file(node_path(&nodes_dir, &root))?;

    // Cold recovery over a FRESH store (a restarted process reads only disk, never
    // a warm cache) MUST reject: the marker names a root that is not present.
    let cold = DiskStore::new(&nodes_dir)?;
    match WalRecovery::recover_path(&wal_path, &cold) {
        Err(WalError::MissingCommittedRoot { root: named }) => {
            assert_eq!(
                named, root,
                "recovery must name the marker's now-unreachable committed root"
            );
            Ok(())
        }
        Err(other) => Err(Box::new(other)),
        Ok(_recovered) => Err(boxed_error(
            "expected MissingCommittedRoot when the committed root's dir entry is lost, \
             but recovery succeeded",
        )),
    }
}

/// THE FIX (positive control): when the node files survive the crash (the barrier
/// made their directory entries durable), cold recovery from the SAME marker
/// succeeds and the committed value is readable FROM DISK.
///
/// This is the falsifiable counterpart: it shares the entire setup with the lost-
/// entry test EXCEPT it does not delete the node file, so it proves the rejection
/// above is caused specifically by the lost directory entry, not by an unrelated
/// recovery fault.
#[test]
fn surviving_node_dir_entry_lets_recovery_read_committed_value() -> TestResult {
    let dir = tempfile::tempdir()?;
    let nodes_dir = dir.path().join("nodes");
    let wal_path = dir.path().join("shard.wal");

    let mut store = DiskStore::new(&nodes_dir)?;
    let root = commit_tree(&mut store, &[(b"durable-key", b"durable-value")])?;
    // The real barrier: make the just-written directory entries durable, exactly
    // as the commit path does before the marker.
    store.sync_dirty_dirs()?;

    let mut wal = DurableWal::new(&wal_path, FsyncPolicy::CommitOnly)?;
    wal.commit(root)?;
    drop(wal);

    // The node files survive (we delete nothing). Cold recovery succeeds.
    let cold = DiskStore::new(&nodes_dir)?;
    let recovered = WalRecovery::recover_path(&wal_path, &cold)?;
    assert_eq!(
        recovered.committed_root(),
        Some(root),
        "recovery must adopt the durable committed root"
    );

    // The committed value is reachable from the committed tree alone (cold store).
    let cursor = haematite::Cursor::new(&cold, root);
    assert_eq!(
        cursor.get(b"durable-key")?,
        Some(b"durable-value".to_vec()),
        "the committed value must be readable from disk after recovery"
    );
    Ok(())
}
