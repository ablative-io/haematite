pub mod api;
pub mod branch;
pub mod shard;
pub mod store;
pub mod sync;
pub mod tree;
pub mod ttl;
pub mod wal;
pub mod wasm;

mod db;
mod error;

pub use db::Database;
pub use error::Error;
pub use store::{
    BlobStore, CacheError, DeleteNode, IndexedDbError, IndexedDbStore, LruCache, MemoryBlobStore,
    MemoryStore, NodeStore,
};

// The filesystem-backed store is excluded from the WASM build, where IndexedDB
// stands in for the disk. (WASM-001 R1)
#[cfg(not(feature = "wasm"))]
pub use store::{DiskStore, StoreError};
pub use tree::{
    BoundaryDetector, Cursor, Hash, InternalNode, LeafNode, Node, NodeError, TreeError,
    batch_mutate, delete, insert,
};
