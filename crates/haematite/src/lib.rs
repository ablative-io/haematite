// The `wasm` feature and the wasm32 target are two sides of the same build:
// the feature cfg-gates the code, the target cfg-gates the dependencies. Tie
// them together so a half-configured build fails loudly instead of silently
// producing a degenerate crate (e.g. no durable storage). (WASM-001 R1, CN1)
#[cfg(all(target_arch = "wasm32", not(feature = "wasm")))]
compile_error!("building haematite for wasm32 requires `--features wasm`");
#[cfg(all(feature = "wasm", not(target_arch = "wasm32")))]
compile_error!("the `wasm` feature is only valid when targeting wasm32");

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
