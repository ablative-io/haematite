use std::cell::RefCell;
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
        write_compressed_node(&path, &compressed)?;
        self.cache_put(hash, Arc::new(node.clone()));
        Ok(hash)
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
}

impl NodeStore for DiskStore {
    type Error = StoreError;

    fn get(&self, hash: &Hash) -> Result<Option<Arc<Node>>, Self::Error> {
        self.read_node(hash)
    }

    fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
        self.write_node(node)
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

fn write_compressed_node(final_path: &Path, compressed: &[u8]) -> Result<(), StoreError> {
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
        Ok(file) => file.sync_all().map_err(StoreError::Io),
        Err(error) if error.error.kind() == ErrorKind::AlreadyExists => Ok(()),
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
