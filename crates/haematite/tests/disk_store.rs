use std::fs;
use std::io;
use std::thread;
use std::time::Duration;

use haematite::{DiskStore, Hash, LeafNode, Node, NodeError, NodeStore, StoreError};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn leaf_node(key: &[u8], value: &[u8]) -> Result<Node, NodeError> {
    LeafNode::new(vec![(key.to_vec(), value.to_vec())]).map(Node::Leaf)
}

fn boxed_error(message: &'static str) -> Box<dyn std::error::Error> {
    Box::new(io::Error::other(message))
}

fn require_store_error(
    result: Result<DiskStore, StoreError>,
) -> Result<StoreError, Box<dyn std::error::Error>> {
    match result {
        Ok(_store) => Err(boxed_error("expected DiskStore::new to fail")),
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
fn constructor_accepts_only_existing_directories() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let store = DiskStore::new(temp_dir.path().to_path_buf(), 2)?;
    assert_eq!(store.cache_capacity(), 2);
    assert!(format!("{store:?}").contains("DiskStore"));

    let missing = temp_dir.path().join("missing");
    let missing_error = require_store_error(DiskStore::new(missing.clone(), 2))?;
    assert!(matches!(missing_error, StoreError::DirectoryNotFound));
    assert!(!missing.exists());

    let file_path = temp_dir.path().join("not-a-directory");
    fs::write(&file_path, b"file")?;
    let file_error = require_store_error(DiskStore::new(file_path, 2))?;
    assert!(matches!(file_error, StoreError::DirectoryNotFound));

    let zero_path = temp_dir.path().join("zero-missing");
    let zero_error = require_store_error(DiskStore::new(zero_path.clone(), 0))?;
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
fn put_writes_zstd_compressed_node_under_content_hash_without_overwrite() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let mut store = DiskStore::new(temp_dir.path().to_path_buf(), 2)?;
    let node = leaf_node(b"a", b"one")?;
    let hash = store.put(&node)?;

    assert_eq!(hash, node.hash());
    let path = temp_dir.path().join(hash.to_string());
    assert!(path.is_file());

    let compressed = fs::read(&path)?;
    let serialised = zstd::stream::decode_all(compressed.as_slice())?;
    let decoded = Node::deserialise(&serialised)?;
    assert_eq!(decoded, node);

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
    let mut writer = DiskStore::new(temp_dir.path().to_path_buf(), 2)?;
    let missing_hash = Hash::from_bytes([9; 32]);
    assert_eq!(writer.get(&missing_hash)?, None);

    let node = leaf_node(b"a", b"one")?;
    let hash = writer.put(&node)?;

    let reader = DiskStore::new(temp_dir.path().to_path_buf(), 2)?;
    assert_eq!(reader.get(&hash)?, Some(node.clone()));

    fs::remove_file(temp_dir.path().join(hash.to_string()))?;
    assert_eq!(reader.get(&hash)?, Some(node));
    Ok(())
}

#[test]
fn capacity_one_cache_evicts_the_least_recently_used_node() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let mut store = DiskStore::new(temp_dir.path().to_path_buf(), 1)?;
    let first = leaf_node(b"a", b"one")?;
    let second = leaf_node(b"b", b"two")?;
    let first_hash = store.put(&first)?;
    let second_hash = store.put(&second)?;

    fs::remove_file(temp_dir.path().join(first_hash.to_string()))?;
    assert_eq!(store.get(&first_hash)?, None);

    fs::remove_file(temp_dir.path().join(second_hash.to_string()))?;
    assert_eq!(store.get(&second_hash)?, Some(second));
    Ok(())
}

#[test]
fn disk_store_implements_fallible_node_store_trait() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let mut store = DiskStore::new(temp_dir.path().to_path_buf(), 2)?;
    let node = leaf_node(b"trait", b"roundtrip")?;

    let hash = NodeStore::put(&mut store, &node)?;

    assert_eq!(hash, node.hash());
    assert_eq!(NodeStore::get(&store, &hash)?, Some(node));
    Ok(())
}

#[test]
fn corrupt_files_return_compression_or_deserialisation_errors() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let store = DiskStore::new(temp_dir.path().to_path_buf(), 2)?;

    let compressed_hash = Hash::from_bytes([4; 32]);
    fs::write(
        temp_dir.path().join(compressed_hash.to_string()),
        b"not zstd",
    )?;
    let compressed_error = require_get_error(store.get(&compressed_hash))?;
    assert!(matches!(compressed_error, StoreError::Compression(_)));

    let deserialise_hash = Hash::from_bytes([5; 32]);
    let invalid_node = zstd::stream::encode_all([0xff].as_slice(), 0)?;
    fs::write(
        temp_dir.path().join(deserialise_hash.to_string()),
        invalid_node,
    )?;
    let deserialise_error = require_get_error(store.get(&deserialise_hash))?;
    assert!(matches!(deserialise_error, StoreError::Deserialise(_)));

    Ok(())
}
