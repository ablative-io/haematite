//! IndexedDB-backed [`NodeStore`] for the browser target (R4, R5, R6).
//!
//! Each content-addressed node is stored as an `IndexedDB` record keyed by its
//! hex BLAKE3 hash, with `zstd`-compressed serialised bytes as the value. The
//! `zstd` C crate cannot target `wasm32-unknown-unknown`, so the pure-Rust
//! [`zrip`] implementation is used here; the frame format is standard
//! Zstandard, matching the native store's on-disk encoding (ADR-006).
//!
//! `IndexedDB` transactions are async and must be awaited on a web worker, so the
//! real read/write path is [`IndexedDbStore::get_async`] /
//! [`IndexedDbStore::put_async`]. The synchronous [`NodeStore`] methods only
//! serve the in-memory LRU cache (R5); a cache miss returns
//! [`IndexedDbStoreError::AsyncRequired`] rather than blocking the main thread.

use std::cell::{Cell, RefCell};
use std::fmt;

use crate::store::NodeStore;
use crate::store::cache::{CacheError, LruCache};
use crate::tree::{Hash, Node};

const DEFAULT_CACHE_CAPACITY: usize = 1_024;
const DB_VERSION: u32 = 1;
const NODE_STORE: &str = "nodes";
const COMPRESSION_LEVEL: i32 = 1;

#[cfg(not(all(feature = "wasm", target_arch = "wasm32", target_os = "unknown")))]
compile_error!("store::indexeddb requires the wasm feature on wasm32-unknown-unknown");

mod browser {
    use super::{DB_VERSION, Hash, IndexedDbStoreError, NODE_STORE, Node};
    use indexed_db_futures::database::Database as IdbDatabase;
    use indexed_db_futures::prelude::{Build, BuildPrimitive, QuerySource};
    use indexed_db_futures::transaction::TransactionMode;
    use indexed_db_futures::typed_array::{Uint8Array, Uint8ArraySlice};

    pub(super) async fn open_database(name: String) -> Result<IdbDatabase, IndexedDbStoreError> {
        IdbDatabase::open(name)
            .with_version(DB_VERSION)
            .with_on_upgrade_needed(|_event, db| {
                let has_nodes = db.object_store_names().any(|store| store == NODE_STORE);
                if !has_nodes {
                    db.create_object_store(NODE_STORE).build()?;
                }
                Ok(())
            })
            .await
            .map_err(|error| IndexedDbStoreError::IndexedDb(error.to_string()))
    }

    pub(super) async fn read_node(
        db: &IdbDatabase,
        hash: &Hash,
    ) -> Result<Option<Node>, IndexedDbStoreError> {
        let compressed = fetch_compressed(db, hash).await?;
        compressed
            .map(|bytes| super::decode_compressed_node(bytes.as_ref()))
            .transpose()
    }

    pub(super) async fn has_node(
        db: &IdbDatabase,
        hash: &Hash,
    ) -> Result<bool, IndexedDbStoreError> {
        Ok(fetch_compressed(db, hash).await?.is_some())
    }

    async fn fetch_compressed(
        db: &IdbDatabase,
        hash: &Hash,
    ) -> Result<Option<Uint8Array>, IndexedDbStoreError> {
        let tx = db
            .transaction(NODE_STORE)
            .with_mode(TransactionMode::Readonly)
            .build()
            .map_err(|error| IndexedDbStoreError::IndexedDb(error.to_string()))?;
        let store = tx
            .object_store(NODE_STORE)
            .map_err(|error| IndexedDbStoreError::IndexedDb(error.to_string()))?;
        let compressed: Option<Uint8Array> = store
            .get(hash_key(hash))
            .primitive()
            .map_err(|error| IndexedDbStoreError::IndexedDb(error.to_string()))?
            .await
            .map_err(|error| IndexedDbStoreError::IndexedDb(error.to_string()))?;
        drop(store);
        tx.commit()
            .await
            .map_err(|error| IndexedDbStoreError::IndexedDb(error.to_string()))?;
        Ok(compressed)
    }

    pub(super) async fn write_node(
        db: &IdbDatabase,
        hash: &Hash,
        compressed: &[u8],
    ) -> Result<(), IndexedDbStoreError> {
        let tx = db
            .transaction(NODE_STORE)
            .with_mode(TransactionMode::Readwrite)
            .build()
            .map_err(|error| IndexedDbStoreError::IndexedDb(error.to_string()))?;
        let store = tx
            .object_store(NODE_STORE)
            .map_err(|error| IndexedDbStoreError::IndexedDb(error.to_string()))?;
        store
            .put(Uint8ArraySlice::new(compressed))
            .with_key(hash_key(hash))
            .without_key_type()
            .primitive()
            .map_err(|error| IndexedDbStoreError::IndexedDb(error.to_string()))?
            .await
            .map_err(|error| IndexedDbStoreError::IndexedDb(error.to_string()))?;
        drop(store);
        tx.commit()
            .await
            .map_err(|error| IndexedDbStoreError::IndexedDb(error.to_string()))
    }

    fn hash_key(hash: &Hash) -> String {
        hash.to_string()
    }
}

type BrowserDatabase = indexed_db_futures::database::Database;

/// IndexedDB-backed node store with an in-memory LRU of decompressed nodes.
#[derive(Debug)]
pub struct IndexedDbStore {
    db: BrowserDatabase,
    cache: RefCell<LruCache>,
    transaction_count: Cell<usize>,
}

impl IndexedDbStore {
    /// Open (creating if needed) the `IndexedDB` database `name`.
    pub async fn open(name: impl Into<String>) -> Result<Self, IndexedDbStoreError> {
        Self::open_with_cache_capacity(name, DEFAULT_CACHE_CAPACITY).await
    }

    pub async fn open_with_cache_capacity(
        name: impl Into<String>,
        cache_capacity: usize,
    ) -> Result<Self, IndexedDbStoreError> {
        let cache = LruCache::new(cache_capacity).map_err(IndexedDbStoreError::from)?;
        let db = browser::open_database(name.into()).await?;
        Ok(Self {
            db,
            cache: RefCell::new(cache),
            transaction_count: Cell::new(0),
        })
    }

    pub fn cache_capacity(&self) -> usize {
        self.cache.borrow().capacity()
    }

    /// Number of `IndexedDB` transactions issued, for test assertions that a
    /// cache hit avoided `IndexedDB` access (R5).
    pub const fn indexeddb_transaction_count(&self) -> usize {
        self.transaction_count.get()
    }

    /// Read a node: cache first (R5), then an awaited `IndexedDB` transaction.
    pub async fn get_async(&self, hash: &Hash) -> Result<Option<Node>, IndexedDbStoreError> {
        if let Some(node) = self.cache_get(hash) {
            return Ok(Some(node));
        }

        self.record_transaction();
        let node = browser::read_node(&self.db, hash).await?;
        if let Some(node) = &node {
            self.cache_put(*hash, node.clone());
        }
        Ok(node)
    }

    /// Write a node. Idempotent: an existing hash is a no-op (R6, C10).
    pub async fn put_async(&self, node: &Node) -> Result<Hash, IndexedDbStoreError> {
        let hash = node.hash();
        self.record_transaction();
        if browser::has_node(&self.db, &hash).await? {
            self.cache_put(hash, node.clone());
            return Ok(hash);
        }

        let compressed = encode_node(node)?;
        self.record_transaction();
        browser::write_node(&self.db, &hash, &compressed).await?;
        self.cache_put(hash, node.clone());
        Ok(hash)
    }

    fn cache_get(&self, hash: &Hash) -> Option<Node> {
        self.cache.borrow_mut().get(hash)
    }

    fn cache_put(&self, hash: Hash, node: Node) {
        self.cache.borrow_mut().put(hash, node);
    }

    fn record_transaction(&self) {
        self.transaction_count
            .set(self.transaction_count.get().saturating_add(1));
    }
}

impl NodeStore for IndexedDbStore {
    type Error = IndexedDbStoreError;

    /// Synchronous read serves the cache only; a miss requires
    /// [`IndexedDbStore::get_async`] on a worker (CN4).
    fn get(&self, hash: &Hash) -> Result<Option<Node>, Self::Error> {
        if let Some(node) = self.cache_get(hash) {
            return Ok(Some(node));
        }
        Err(IndexedDbStoreError::AsyncRequired { hash: *hash })
    }

    /// Synchronous write is unsupported: `IndexedDB` transactions must be awaited.
    /// Use [`IndexedDbStore::put_async`] on a worker (CN4).
    fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
        Err(IndexedDbStoreError::AsyncRequired { hash: node.hash() })
    }
}

#[derive(Debug)]
pub enum IndexedDbStoreError {
    InvalidCapacity,
    IndexedDb(String),
    Compression(String),
    Deserialise(String),
    AsyncRequired { hash: Hash },
}

impl fmt::Display for IndexedDbStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCapacity => write!(formatter, "cache capacity must be greater than zero"),
            Self::IndexedDb(error) => write!(formatter, "IndexedDB error: {error}"),
            Self::Compression(error) => write!(formatter, "zstd compression error: {error}"),
            Self::Deserialise(error) => write!(formatter, "node deserialisation error: {error}"),
            Self::AsyncRequired { hash } => write!(
                formatter,
                "IndexedDB NodeStore cache miss for {hash} requires async worker transaction"
            ),
        }
    }
}

impl std::error::Error for IndexedDbStoreError {}

impl From<CacheError> for IndexedDbStoreError {
    fn from(error: CacheError) -> Self {
        match error {
            CacheError::InvalidCapacity => Self::InvalidCapacity,
        }
    }
}

fn encode_node(node: &Node) -> Result<Vec<u8>, IndexedDbStoreError> {
    zrip::compress(node.serialise().as_slice(), COMPRESSION_LEVEL)
        .map_err(|error| IndexedDbStoreError::Compression(error.to_string()))
}

fn decode_compressed_node(compressed: &[u8]) -> Result<Node, IndexedDbStoreError> {
    let serialised = zrip::decompress(compressed)
        .map_err(|error| IndexedDbStoreError::Compression(error.to_string()))?;
    Node::deserialise(&serialised)
        .map_err(|error| IndexedDbStoreError::Deserialise(error.to_string()))
}
