// WASM-001: IndexedDB-backed node store for the browser target.
//
// Each content-addressed node is stored as one IndexedDB object: the 32-byte
// BLAKE3 hash is the key, and zstd-compressed serialised bytes are the value
// (R4, C6 / ADR-006). Compression uses the pure-Rust `ruzstd` encoder, whose
// frames are byte-compatible with the C `zstd` crate used by the native
// `DiskStore`, so a node written on a server can be read in a browser tab with
// no translation (CN6).
//
// IndexedDB is asynchronous and must never block the browser's main thread
// (CN4, C7). The store therefore exposes an async API and runs its transactions
// through a `BlobStore` backend; in the browser that backend issues IndexedDB
// requests on a web worker (see `super::idb_backend`). Decompressed nodes are
// kept hot in an `LruCache` in WASM linear memory, reusing the persistence
// cluster's cache rather than introducing a WASM-specific one (R5, C8).

use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::io::Read;

use ruzstd::decoding::StreamingDecoder;
use ruzstd::encoding::{CompressionLevel, compress_to_vec};

use crate::store::cache::{CacheError, LruCache};
use crate::tree::{Hash, Node};

const DEFAULT_CACHE_CAPACITY: usize = 1_024;

/// Asynchronous key/value backend that persists compressed node bytes.
///
/// This is the seam between the content-addressed store and the underlying
/// transactional storage. The browser implementation drives IndexedDB requests
/// on a web worker; tests use an in-memory map. The trait is intentionally async
/// because IndexedDB transactions can only be awaited, never blocked on, without
/// freezing the UI thread (CN4).
#[allow(async_fn_in_trait)]
pub trait BlobStore {
    type Error: std::error::Error;

    /// Load the raw (compressed) bytes previously stored under `key`, or `None`
    /// if the key is absent.
    async fn load(&self, key: &Hash) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Persist `bytes` under `key`. Content addressing makes the bytes for a
    /// given key immutable, so implementations must be idempotent: storing a key
    /// that already exists overwrites it with identical bytes and must not error.
    /// (`IndexedDbStore::put` checks [`contains`](BlobStore::contains) first as
    /// an optimisation, but that check is not atomic with this write.)
    async fn store(&self, key: &Hash, bytes: Vec<u8>) -> Result<(), Self::Error>;

    /// Report whether `key` is already present, used to make writes idempotent
    /// without re-encoding or re-storing a node that already exists (C10).
    async fn contains(&self, key: &Hash) -> Result<bool, Self::Error>;
}

/// Content-addressed node store backed by a [`BlobStore`] (IndexedDB in the
/// browser) with an in-memory LRU cache of decompressed nodes.
#[derive(Debug)]
pub struct IndexedDbStore<B: BlobStore> {
    backend: B,
    cache: RefCell<LruCache>,
}

impl<B: BlobStore> IndexedDbStore<B> {
    /// Construct a store over `backend` with the default cache capacity.
    pub fn new(backend: B) -> Result<Self, IndexedDbError<B::Error>> {
        Self::with_cache_capacity(backend, DEFAULT_CACHE_CAPACITY)
    }

    /// Construct a store over `backend` with an explicit cache capacity.
    pub fn with_cache_capacity(
        backend: B,
        cache_capacity: usize,
    ) -> Result<Self, IndexedDbError<B::Error>> {
        let cache = LruCache::new(cache_capacity)?;
        Ok(Self {
            backend,
            cache: RefCell::new(cache),
        })
    }

    /// Maximum number of decompressed nodes held in linear memory.
    pub fn cache_capacity(&self) -> usize {
        self.cache.borrow().capacity()
    }

    /// Borrow the underlying blob backend.
    pub const fn backend(&self) -> &B {
        &self.backend
    }

    /// Fetch a node by hash. A cache hit is served from linear memory and never
    /// touches IndexedDB (R5, C8); a miss reads and decompresses the object from
    /// the backend and warms the cache. Returns `None` for an unknown hash.
    pub async fn get(&self, hash: &Hash) -> Result<Option<Node>, IndexedDbError<B::Error>> {
        if let Some(node) = self.cache_get(hash) {
            return Ok(Some(node));
        }

        let Some(compressed) = self
            .backend
            .load(hash)
            .await
            .map_err(IndexedDbError::Backend)?
        else {
            return Ok(None);
        };

        let serialised = decompress_node(&compressed)?;
        let node = Node::deserialise(&serialised)
            .map_err(|error| IndexedDbError::Deserialise(error.to_string()))?;
        self.cache_put(*hash, node.clone());
        Ok(Some(node))
    }

    /// Store a node under its content hash. Writing a hash that already exists —
    /// whether cached or persisted — is a no-op that returns the same hash, so
    /// puts are idempotent (R6, C10).
    pub async fn put(&self, node: &Node) -> Result<Hash, IndexedDbError<B::Error>> {
        let hash = node.hash();

        if self.cache_contains(&hash) {
            return Ok(hash);
        }
        if self
            .backend
            .contains(&hash)
            .await
            .map_err(IndexedDbError::Backend)?
        {
            self.cache_put(hash, node.clone());
            return Ok(hash);
        }

        let compressed = compress_node(&node.serialise());
        self.backend
            .store(&hash, compressed)
            .await
            .map_err(IndexedDbError::Backend)?;
        self.cache_put(hash, node.clone());
        Ok(hash)
    }

    fn cache_get(&self, hash: &Hash) -> Option<Node> {
        self.cache.borrow_mut().get(hash)
    }

    fn cache_contains(&self, hash: &Hash) -> bool {
        self.cache.borrow_mut().get(hash).is_some()
    }

    fn cache_put(&self, hash: Hash, node: Node) {
        self.cache.borrow_mut().put(hash, node);
    }
}

/// Compress serialised node bytes into a standard zstd frame.
///
/// Uses `ruzstd`'s pure-Rust encoder so the WASM build needs no C toolchain. The
/// output is a conformant zstd frame readable by the C `zstd` crate, keeping
/// nodes portable between the browser and native servers (CN6).
fn compress_node(serialised: &[u8]) -> Vec<u8> {
    compress_to_vec(serialised, CompressionLevel::Fastest)
}

/// Decompress a zstd frame produced by either `ruzstd` or the C `zstd` crate.
fn decompress_node<E>(compressed: &[u8]) -> Result<Vec<u8>, IndexedDbError<E>>
where
    E: std::error::Error,
{
    let mut decoder = StreamingDecoder::new(compressed)
        .map_err(|error| IndexedDbError::Decompression(error.to_string()))?;
    let mut serialised = Vec::new();
    decoder
        .read_to_end(&mut serialised)
        .map_err(|error| IndexedDbError::Decompression(error.to_string()))?;
    Ok(serialised)
}

/// Errors raised by [`IndexedDbStore`], parameterised by the backend error.
#[derive(Debug)]
pub enum IndexedDbError<E> {
    /// The configured cache capacity was zero.
    InvalidCapacity,
    /// The underlying blob backend (IndexedDB) failed.
    Backend(E),
    /// A stored object could not be decompressed.
    Decompression(String),
    /// Decompressed bytes were not a valid node encoding.
    Deserialise(String),
}

impl<E> From<CacheError> for IndexedDbError<E> {
    fn from(error: CacheError) -> Self {
        match error {
            CacheError::InvalidCapacity => Self::InvalidCapacity,
        }
    }
}

impl<E: fmt::Display> fmt::Display for IndexedDbError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCapacity => write!(f, "cache capacity must be greater than zero"),
            Self::Backend(error) => write!(f, "IndexedDB backend error: {error}"),
            Self::Decompression(error) => write!(f, "zstd decompression error: {error}"),
            Self::Deserialise(error) => write!(f, "node deserialisation error: {error}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for IndexedDbError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Backend(error) => Some(error),
            Self::InvalidCapacity | Self::Decompression(_) | Self::Deserialise(_) => None,
        }
    }
}

/// In-memory [`BlobStore`] used by tests and as the reference backend.
///
/// It stores the same compressed bytes IndexedDB would, so the store's codec,
/// caching, and idempotency logic exercise the identical path on native and in
/// the browser (R6, C9).
#[derive(Debug, Default)]
pub struct MemoryBlobStore {
    blobs: RefCell<HashMap<Hash, Vec<u8>>>,
}

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of stored objects.
    pub fn len(&self) -> usize {
        self.blobs.borrow().len()
    }

    /// Whether the backend holds no objects.
    pub fn is_empty(&self) -> bool {
        self.blobs.borrow().is_empty()
    }

    /// Borrow the raw stored bytes for a hash, if present.
    pub fn raw(&self, key: &Hash) -> Option<Vec<u8>> {
        self.blobs.borrow().get(key).cloned()
    }

    /// Forget an object, simulating eviction from the durable backend so tests
    /// can prove a cache hit avoids backend access.
    pub fn forget(&self, key: &Hash) -> Option<Vec<u8>> {
        self.blobs.borrow_mut().remove(key)
    }
}

impl BlobStore for MemoryBlobStore {
    type Error = Infallible;

    async fn load(&self, key: &Hash) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.blobs.borrow().get(key).cloned())
    }

    async fn store(&self, key: &Hash, bytes: Vec<u8>) -> Result<(), Self::Error> {
        self.blobs.borrow_mut().insert(*key, bytes);
        Ok(())
    }

    async fn contains(&self, key: &Hash) -> Result<bool, Self::Error> {
        Ok(self.blobs.borrow().contains_key(key))
    }
}

#[cfg(test)]
mod tests {
    use super::{IndexedDbError, IndexedDbStore, MemoryBlobStore};
    use crate::store::indexeddb::BlobStore;
    use crate::tree::{Hash, LeafNode, Node, NodeError};
    use pollster::block_on;

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

    #[test]
    fn with_cache_capacity_rejects_zero() {
        let result = IndexedDbStore::with_cache_capacity(MemoryBlobStore::new(), 0);
        assert!(matches!(result, Err(IndexedDbError::InvalidCapacity)));
    }

    #[test]
    fn new_uses_default_capacity() -> TestResult {
        let store = IndexedDbStore::new(MemoryBlobStore::new())?;
        assert_eq!(store.cache_capacity(), 1_024);
        Ok(())
    }

    #[test]
    fn put_stores_zstd_compressed_bytes_under_content_hash() -> TestResult {
        let store = IndexedDbStore::with_cache_capacity(MemoryBlobStore::new(), 4)?;
        let node = repetitive_leaf_node(80)?;
        let serialised = node.serialise();

        let hash = block_on(store.put(&node))?;
        assert_eq!(hash, node.hash());

        let stored = store
            .backend()
            .raw(&hash)
            .ok_or("node should be stored under its hash")?;
        assert!(stored.len() < serialised.len());
        assert_eq!(
            stored.get(..ZSTD_MAGIC_BYTES.len()),
            Some(ZSTD_MAGIC_BYTES.as_slice())
        );
        Ok(())
    }

    #[test]
    fn node_round_trips_through_put_and_get() -> TestResult {
        let store = IndexedDbStore::with_cache_capacity(MemoryBlobStore::new(), 4)?;
        let node = leaf_node(b"a", b"one")?;

        let hash = block_on(store.put(&node))?;
        assert_eq!(block_on(store.get(&hash))?, Some(node));
        Ok(())
    }

    #[test]
    fn get_returns_none_for_unknown_hash() -> TestResult {
        let store = IndexedDbStore::with_cache_capacity(MemoryBlobStore::new(), 4)?;
        assert_eq!(block_on(store.get(&Hash::from_bytes([7; 32])))?, None);
        Ok(())
    }

    #[test]
    fn duplicate_put_is_idempotent_without_overwriting() -> TestResult {
        let store = IndexedDbStore::with_cache_capacity(MemoryBlobStore::new(), 4)?;
        let node = leaf_node(b"a", b"one")?;

        let first = block_on(store.put(&node))?;
        let stored = store.backend().raw(&first).ok_or("node should be stored")?;

        let second = block_on(store.put(&node))?;
        assert_eq!(second, first);
        assert_eq!(store.backend().len(), 1);
        assert_eq!(store.backend().raw(&first), Some(stored));
        Ok(())
    }

    #[test]
    fn put_into_fresh_store_with_existing_backend_object_is_a_no_op() -> TestResult {
        let node = leaf_node(b"shared", b"node")?;

        // First store persists the object.
        let writer = IndexedDbStore::with_cache_capacity(MemoryBlobStore::new(), 4)?;
        let hash = block_on(writer.put(&node))?;
        let bytes = writer.backend().raw(&hash).ok_or("object should exist")?;

        // A second store whose backend already holds the object must not
        // re-encode or re-store it, but still report success (C10).
        let backend = MemoryBlobStore::new();
        block_on(backend.store(&hash, bytes.clone()))?;
        let reader = IndexedDbStore::with_cache_capacity(backend, 4)?;
        assert_eq!(block_on(reader.put(&node))?, hash);
        assert_eq!(reader.backend().len(), 1);
        assert_eq!(reader.backend().raw(&hash), Some(bytes));
        Ok(())
    }

    #[test]
    fn cache_hit_avoids_backend_access() -> TestResult {
        let store = IndexedDbStore::with_cache_capacity(MemoryBlobStore::new(), 4)?;
        let node = leaf_node(b"a", b"one")?;
        let hash = block_on(store.put(&node))?;

        // Drop the durable object; the decompressed node is still cached in
        // linear memory, so the read is served without touching the backend.
        store.backend().forget(&hash);
        assert_eq!(block_on(store.get(&hash))?, Some(node));
        Ok(())
    }

    #[test]
    fn cold_get_reads_and_decompresses_from_backend() -> TestResult {
        let node = leaf_node(b"a", b"one")?;

        let writer = IndexedDbStore::with_cache_capacity(MemoryBlobStore::new(), 4)?;
        let hash = block_on(writer.put(&node))?;
        let bytes = writer.backend().raw(&hash).ok_or("object should exist")?;

        // A fresh store with a cold cache must reconstruct the node from the
        // stored compressed bytes alone.
        let backend = MemoryBlobStore::new();
        block_on(backend.store(&hash, bytes))?;
        let reader = IndexedDbStore::with_cache_capacity(backend, 4)?;
        assert_eq!(block_on(reader.get(&hash))?, Some(node));
        Ok(())
    }

    #[test]
    fn corrupt_object_returns_decompression_error() -> TestResult {
        let backend = MemoryBlobStore::new();
        let hash = Hash::from_bytes([4; 32]);
        block_on(backend.store(&hash, b"not a zstd frame".to_vec()))?;
        let store = IndexedDbStore::with_cache_capacity(backend, 4)?;

        let result = block_on(store.get(&hash));
        assert!(matches!(result, Err(IndexedDbError::Decompression(_))));
        Ok(())
    }

    // Cross-backend portability (CN6): the WASM store's `ruzstd` frames and the
    // native DiskStore's C `zstd` frames must be mutually decodable, so a node
    // written on one platform reads on the other without translation. These run
    // on native, where both encoders are available.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn ruzstd_frame_decodes_with_native_zstd() -> TestResult {
        use super::compress_node;
        let original = repetitive_leaf_node(80)?.serialise();
        let frame = compress_node(&original);
        let decoded = zstd::stream::decode_all(frame.as_slice())?;
        assert_eq!(decoded, original);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_zstd_frame_decodes_with_ruzstd() -> TestResult {
        use super::decompress_node;
        use std::convert::Infallible;
        let original = repetitive_leaf_node(80)?.serialise();
        let frame = zstd::stream::encode_all(original.as_slice(), 0)?;
        let decoded = decompress_node::<Infallible>(&frame)?;
        assert_eq!(decoded, original);
        Ok(())
    }
}
