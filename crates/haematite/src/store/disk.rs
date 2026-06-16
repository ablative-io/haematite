use std::cell::RefCell;
use std::fmt;
use std::fs;
use std::io::{ErrorKind, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use lru::LruCache;

use crate::store::NodeStore;
use crate::tree::{Hash, Node};

#[derive(Debug)]
pub struct DiskStore {
    dir: PathBuf,
    cache: RefCell<LruCache<Hash, Node>>,
    cache_capacity: NonZeroUsize,
}

impl DiskStore {
    pub fn new(dir: PathBuf, cache_capacity: usize) -> Result<Self, StoreError> {
        let capacity = NonZeroUsize::new(cache_capacity).ok_or(StoreError::InvalidCapacity)?;
        let metadata = fs::metadata(&dir).map_err(directory_error)?;
        if !metadata.is_dir() {
            return Err(StoreError::DirectoryNotFound);
        }

        Ok(Self {
            dir,
            cache: RefCell::new(LruCache::new(capacity)),
            cache_capacity: capacity,
        })
    }

    pub const fn cache_capacity(&self) -> usize {
        self.cache_capacity.get()
    }

    pub fn get(&self, hash: &Hash) -> Result<Option<Node>, StoreError> {
        self.read_node(hash)
    }

    pub fn put(&mut self, node: &Node) -> Result<Hash, StoreError> {
        self.write_node(node)
    }

    fn read_node(&self, hash: &Hash) -> Result<Option<Node>, StoreError> {
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
        let node = Node::deserialise(&serialised)
            .map_err(|error| StoreError::Deserialise(error.to_string()))?;
        self.cache_put(*hash, node.clone());
        Ok(Some(node))
    }

    fn write_node(&self, node: &Node) -> Result<Hash, StoreError> {
        let hash = node.hash();
        let path = self.node_path(&hash);
        if path_exists(&path)? {
            self.cache_put(hash, node.clone());
            return Ok(hash);
        }

        let serialised = node.serialise();
        let compressed = compress_node(&serialised)?;
        self.write_compressed_node(&path, &compressed)?;
        self.cache_put(hash, node.clone());
        Ok(hash)
    }

    fn write_compressed_node(
        &self,
        final_path: &Path,
        compressed: &[u8],
    ) -> Result<(), StoreError> {
        let mut temp_file = tempfile::Builder::new()
            .prefix(".node-")
            .suffix(".tmp")
            .tempfile_in(&self.dir)
            .map_err(StoreError::Io)?;
        temp_file.write_all(compressed).map_err(StoreError::Io)?;
        temp_file.as_file_mut().sync_all().map_err(StoreError::Io)?;

        match temp_file.persist_noclobber(final_path) {
            Ok(file) => file.sync_all().map_err(StoreError::Io),
            Err(error) if error.error.kind() == ErrorKind::AlreadyExists => Ok(()),
            Err(error) => Err(StoreError::Io(error.error)),
        }
    }

    fn node_path(&self, hash: &Hash) -> PathBuf {
        self.dir.join(hash.to_string())
    }

    fn cache_get(&self, hash: &Hash) -> Option<Node> {
        let mut cache = self.cache.borrow_mut();
        cache.get(hash).cloned()
    }

    fn cache_put(&self, hash: Hash, node: Node) {
        let mut cache = self.cache.borrow_mut();
        cache.put(hash, node);
    }
}

impl NodeStore for DiskStore {
    type Error = StoreError;

    fn get(&self, hash: &Hash) -> Result<Option<Node>, Self::Error> {
        self.read_node(hash)
    }

    fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
        self.write_node(node)
    }
}

#[derive(Debug)]
pub enum StoreError {
    DirectoryNotFound,
    InvalidCapacity,
    Io(std::io::Error),
    Compression(String),
    Deserialise(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DirectoryNotFound => write!(f, "storage directory was not found"),
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

fn directory_error(error: std::io::Error) -> StoreError {
    match error.kind() {
        ErrorKind::NotFound | ErrorKind::NotADirectory => StoreError::DirectoryNotFound,
        _ => StoreError::Io(error),
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
