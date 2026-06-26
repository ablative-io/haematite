use std::cell::RefCell;
use std::collections::BTreeSet;
use std::fmt;
use std::fs;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::store::NodeStore;
use crate::store::cache::{CacheError, LruCache};
use crate::store::gc::DeleteNode;
use crate::tree::{Hash, Node};

const DEFAULT_CACHE_CAPACITY: usize = 1_024;
const HASH_PREFIX_HEX_LEN: usize = 2;

#[derive(Debug)]
pub struct DiskStore {
    dir: PathBuf,
    cache: RefCell<LruCache>,
    /// Parent subdirectories that received a node file since the last
    /// [`Self::sync_dirty_dirs`] barrier (Tier-0 durability fix).
    ///
    /// `write_compressed_node` syncs each node file's DATA but does NOT fsync the
    /// parent directory, so a published rename can be lost on power loss even
    /// though the bytes are durable. Each distinct parent dir is recorded here
    /// (deduped — a commit writes many nodes into few subdirs) and fsync'd ONCE
    /// as a batched barrier by [`Self::sync_dirty_dirs`], which the commit path
    /// invokes strictly before the WAL committed-root marker is written.
    dirty_dirs: RefCell<BTreeSet<PathBuf>>,
}

impl DiskStore {
    pub fn new<P>(dir: P) -> Result<Self, StoreError>
    where
        P: AsRef<Path>,
    {
        Self::with_cache_capacity(dir, DEFAULT_CACHE_CAPACITY)
    }

    pub fn with_cache_capacity<P>(dir: P, cache_capacity: usize) -> Result<Self, StoreError>
    where
        P: AsRef<Path>,
    {
        let dir = dir.as_ref().to_path_buf();
        let cache = LruCache::new(cache_capacity).map_err(StoreError::from)?;
        ensure_directory(&dir)?;

        Ok(Self {
            dir,
            cache: RefCell::new(cache),
            dirty_dirs: RefCell::new(BTreeSet::new()),
        })
    }

    pub fn cache_capacity(&self) -> usize {
        self.cache.borrow().capacity()
    }

    pub fn get(&self, hash: &Hash) -> Result<Option<Arc<Node>>, StoreError> {
        self.read_node(hash)
    }

    pub fn put(&mut self, node: &Node) -> Result<Hash, StoreError> {
        self.write_node(node)
    }

    pub fn delete(&self, hash: &Hash) -> Result<(), StoreError> {
        self.delete_node(hash)
    }

    fn read_node(&self, hash: &Hash) -> Result<Option<Arc<Node>>, StoreError> {
        if let Some(node) = self.cache_get(hash) {
            return Ok(Some(node));
        }

        let path = self.node_path(hash);
        let compressed = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(StoreError::Io(error)),
        };
        let serialised = decompress_node(&compressed)?;
        let node = Arc::new(
            Node::deserialise(&serialised)
                .map_err(|error| StoreError::Deserialise(error.to_string()))?,
        );
        self.cache_put(*hash, Arc::clone(&node));
        Ok(Some(node))
    }

    fn write_node(&self, node: &Node) -> Result<Hash, StoreError> {
        let hash = node.hash();
        let path = self.node_path(&hash);
        if path_exists(&path)? {
            self.cache_put(hash, Arc::new(node.clone()));
            return Ok(hash);
        }

        let serialised = node.serialise();
        let compressed = compress_node(&serialised)?;
        if let Some(parent_dir) = write_compressed_node(&path, &compressed)? {
            // Record the subdirectory that received this node so the commit-path
            // barrier (`sync_dirty_dirs`) can fsync its directory entry ONCE,
            // deduped, before the WAL marker is written. A `None` here means the
            // file already existed (no new directory entry to make durable).
            self.dirty_dirs.borrow_mut().insert(parent_dir);
        }
        self.cache_put(hash, Arc::new(node.clone()));
        Ok(hash)
    }

    /// fsync the directory entry of every subdirectory that received a node
    /// since the last barrier, then clear the dirty set (Tier-0 durability fix).
    ///
    /// Each distinct directory is opened read-only and `sync_all`'d once — the
    /// portable Unix idiom for making a rename's directory entry durable. This
    /// is the batched barrier the commit path runs AFTER all of a commit's nodes
    /// are persisted and STRICTLY BEFORE the WAL committed-root marker is
    /// written.
    fn sync_dirty_directories(&self) -> Result<(), StoreError> {
        let dirs = std::mem::take(&mut *self.dirty_dirs.borrow_mut());
        for dir in dirs {
            let handle = fs::File::open(&dir).map_err(StoreError::Io)?;
            handle.sync_all().map_err(StoreError::Io)?;
        }
        Ok(())
    }

    fn delete_node(&self, hash: &Hash) -> Result<(), StoreError> {
        self.cache_remove(hash);
        match fs::remove_file(self.node_path(hash)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StoreError::Io(error)),
        }
    }

    fn node_path(&self, hash: &Hash) -> PathBuf {
        let hex = hash.to_string();
        let (prefix, file_name) = hex.split_at(HASH_PREFIX_HEX_LEN);
        self.dir.join(prefix).join(file_name)
    }

    fn cache_get(&self, hash: &Hash) -> Option<Arc<Node>> {
        self.cache.borrow_mut().get(hash)
    }

    fn cache_put(&self, hash: Hash, node: Arc<Node>) {
        self.cache.borrow_mut().put(hash, node);
    }

    fn cache_remove(&self, hash: &Hash) -> Option<Arc<Node>> {
        self.cache.borrow_mut().remove(hash)
    }

    /// The distinct parent subdirectories awaiting a directory-entry fsync, in
    /// sorted order (test-support for the durability barrier).
    #[cfg(test)]
    fn pending_dirty_dirs(&self) -> Vec<PathBuf> {
        self.dirty_dirs.borrow().iter().cloned().collect()
    }
}

impl NodeStore for DiskStore {
    type Error = StoreError;

    fn get(&self, hash: &Hash) -> Result<Option<Arc<Node>>, Self::Error> {
        self.read_node(hash)
    }

    fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
        self.write_node(node)
    }

    fn sync_dirty_dirs(&self) -> Result<(), Self::Error> {
        self.sync_dirty_directories()
    }
}

impl DeleteNode for DiskStore {
    type Error = StoreError;

    fn delete(&self, hash: &Hash) -> Result<(), Self::Error> {
        self.delete_node(hash)
    }
}

#[derive(Debug)]
pub enum StoreError {
    DirectoryNotFound,
    NotADirectory { path: PathBuf },
    MissingParentDirectory { path: PathBuf },
    InvalidCapacity,
    Io(std::io::Error),
    Compression(String),
    Deserialise(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DirectoryNotFound => write!(f, "storage directory was not found"),
            Self::NotADirectory { path } => {
                write!(f, "storage path is not a directory: {}", path.display())
            }
            Self::MissingParentDirectory { path } => {
                write!(f, "node path has no parent directory: {}", path.display())
            }
            Self::InvalidCapacity => write!(f, "cache capacity must be greater than zero"),
            Self::Io(error) => write!(f, "disk store I/O error: {error}"),
            Self::Compression(error) => write!(f, "zstd compression error: {error}"),
            Self::Deserialise(error) => write!(f, "node deserialisation error: {error}"),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::DirectoryNotFound
            | Self::NotADirectory { .. }
            | Self::MissingParentDirectory { .. }
            | Self::InvalidCapacity
            | Self::Compression(_)
            | Self::Deserialise(_) => None,
        }
    }
}

impl From<std::io::Error> for StoreError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<CacheError> for StoreError {
    fn from(error: CacheError) -> Self {
        match error {
            CacheError::InvalidCapacity => Self::InvalidCapacity,
        }
    }
}

fn ensure_directory(path: &Path) -> Result<(), StoreError> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_metadata) => Err(StoreError::NotADirectory {
            path: path.to_path_buf(),
        }),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(|error| directory_error(error, path))
        }
        Err(error) => Err(directory_error(error, path)),
    }
}

fn directory_error(error: std::io::Error, path: &Path) -> StoreError {
    match error.kind() {
        ErrorKind::NotFound => StoreError::DirectoryNotFound,
        ErrorKind::NotADirectory | ErrorKind::AlreadyExists => StoreError::NotADirectory {
            path: path.to_path_buf(),
        },
        _ => StoreError::Io(error),
    }
}

/// Atomically publish a compressed node file, syncing its DATA but NOT its
/// parent directory entry.
///
/// The temp file's data is fsync'd (`temp_file.sync_all`) before the atomic
/// `persist_noclobber` rename, so the node bytes are durable. The directory
/// entry created by the rename is deliberately NOT fsync'd here: doing so per
/// node would fsync the same subdirectory once per node. Instead the caller
/// records the returned parent directory and the commit path fsyncs each
/// DISTINCT directory ONCE, as a batched barrier, strictly before the WAL marker
/// (see [`DiskStore::sync_dirty_directories`]).
///
/// Returns `Some(parent_dir)` when a NEW file was persisted (its directory entry
/// still needs the barrier), or `None` when the file already existed (the
/// directory entry is already durable from the prior write).
fn write_compressed_node(
    final_path: &Path,
    compressed: &[u8],
) -> Result<Option<PathBuf>, StoreError> {
    let Some(parent_dir) = final_path.parent() else {
        return Err(StoreError::MissingParentDirectory {
            path: final_path.to_path_buf(),
        });
    };
    fs::create_dir_all(parent_dir).map_err(StoreError::Io)?;

    let mut temp_file = tempfile::Builder::new()
        .prefix(".node-")
        .suffix(".tmp")
        .tempfile_in(parent_dir)
        .map_err(StoreError::Io)?;
    temp_file.write_all(compressed).map_err(StoreError::Io)?;
    temp_file.as_file_mut().sync_all().map_err(StoreError::Io)?;

    match temp_file.persist_noclobber(final_path) {
        Ok(_file) => Ok(Some(parent_dir.to_path_buf())),
        Err(error) if error.error.kind() == ErrorKind::AlreadyExists => Ok(None),
        Err(error) => Err(StoreError::Io(error.error)),
    }
}

fn path_exists(path: &Path) -> Result<bool, StoreError> {
    path.try_exists().map_err(StoreError::Io)
}

fn compress_node(serialised: &[u8]) -> Result<Vec<u8>, StoreError> {
    zstd::stream::encode_all(serialised, 0)
        .map_err(|error| StoreError::Compression(error.to_string()))
}

fn decompress_node(compressed: &[u8]) -> Result<Vec<u8>, StoreError> {
    zstd::stream::decode_all(compressed).map_err(|error| StoreError::Compression(error.to_string()))
}

#[cfg(test)]
mod dirsync_tests {
    use super::{DiskStore, NodeStore};
    use crate::tree::{LeafNode, Node, NodeError};

    fn leaf(key: &[u8], value: &[u8]) -> Result<Node, NodeError> {
        LeafNode::new(vec![(key.to_vec(), value.to_vec())]).map(Node::Leaf)
    }

    /// A freshly persisted node records its DISTINCT parent subdirectory as
    /// dirty, deduped: many nodes landing in the same prefix dir record that dir
    /// exactly once, and nodes in different prefixes are all tracked.
    #[test]
    fn put_records_distinct_parent_dirs_deduped() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let mut store = DiskStore::with_cache_capacity(temp.path(), 64)?;

        // Drive many distinct content hashes; each lands in the subdir named by
        // its 2-hex-char prefix. We assert the dirty set is the EXACT distinct
        // set of parent dirs of the files actually written.
        let mut expected: std::collections::BTreeSet<std::path::PathBuf> =
            std::collections::BTreeSet::new();
        for index in 0..64u32 {
            let node = leaf(format!("k{index}").as_bytes(), b"v")?;
            let hash = store.put(&node)?;
            let path = store.node_path(&hash);
            let parent = path
                .parent()
                .ok_or("node path must have a parent")?
                .to_path_buf();
            expected.insert(parent);
        }

        let pending = store.pending_dirty_dirs();
        // Deduped: no parent dir appears twice.
        let unique: std::collections::BTreeSet<_> = pending.iter().cloned().collect();
        assert_eq!(pending.len(), unique.len(), "dirty dirs must be deduped");
        // Exactly the distinct set of parent dirs of every written node.
        assert_eq!(unique, expected);
        Ok(())
    }

    /// The barrier fsyncs and CLEARS the dirty set: after `sync_dirty_dirs` the
    /// pending set is empty (so the next commit starts a fresh batch), and a
    /// re-put of an already-stored node records NOTHING (its directory entry is
    /// already durable — `write_compressed_node` returns `None`).
    #[test]
    fn barrier_clears_dirty_set_and_duplicate_put_records_nothing()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let mut store = DiskStore::with_cache_capacity(temp.path(), 64)?;

        let node = leaf(b"only", b"value")?;
        store.put(&node)?;
        assert_eq!(
            store.pending_dirty_dirs().len(),
            1,
            "one fresh node, one dir"
        );

        // The barrier fsyncs the dir entry and clears the batch.
        store.sync_dirty_dirs()?;
        assert!(
            store.pending_dirty_dirs().is_empty(),
            "barrier must clear the dirty set"
        );

        // Re-putting an already-stored node persists no new file, so it records
        // no dirty dir (its directory entry is already durable).
        store.put(&node)?;
        assert!(
            store.pending_dirty_dirs().is_empty(),
            "duplicate put must not re-dirty an already-durable dir entry"
        );
        Ok(())
    }
}
