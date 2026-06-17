// The DiskStore is filesystem-backed and native-only; this suite does not apply
// to the wasm32 target. (WASM-001 R1)
#![cfg(not(target_arch = "wasm32"))]

use std::fs;
use std::io;
use std::thread;
use std::time::Duration;

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
    result: Result<Option<Node>, StoreError>,
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
    assert_eq!(reader.get(&hash)?, Some(node.clone()));

    fs::remove_file(node_path(temp_dir.path(), &hash))?;
    assert_eq!(reader.get(&hash)?, Some(node));
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

    assert_eq!(store.get(&first_hash)?, Some(first.clone()));

    fs::remove_file(node_path(temp_dir.path(), &first_hash))?;
    fs::remove_file(node_path(temp_dir.path(), &second_hash))?;
    assert_eq!(store.get(&second_hash)?, None);
    assert_eq!(store.get(&first_hash)?, Some(first));
    Ok(())
}

#[test]
fn disk_store_implements_fallible_node_store_trait() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let mut store = DiskStore::with_cache_capacity(temp_dir.path(), 2)?;
    let node = leaf_node(b"trait", b"roundtrip")?;

    let hash = NodeStore::put(&mut store, &node)?;

    assert_eq!(hash, node.hash());
    assert_eq!(NodeStore::get(&store, &hash)?, Some(node));
    Ok(())
}

#[test]
fn delete_removes_file_evicts_cache_and_is_idempotent() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let mut store = DiskStore::with_cache_capacity(temp_dir.path(), 2)?;
    let node = leaf_node(b"delete", b"cached")?;
    let hash = store.put(&node)?;

    assert_eq!(store.get(&hash)?, Some(node));
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
