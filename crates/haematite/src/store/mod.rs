pub mod cache;
pub mod gc;
pub mod indexeddb;
pub mod memory;

// WASM-002: OPFS-backed WAL store (out of scope for WASM-001). Placeholder
// module; hidden from public docs until implemented.
#[doc(hidden)]
pub mod opfs;

// The filesystem-backed node store is native-only: it depends on the C `zstd`
// crate and `tempfile`, neither of which compiles to wasm32. (WASM-001 R1, CN1)
#[cfg(not(feature = "wasm"))]
pub mod disk;

// The concrete IndexedDB blob backend binds to `web_sys` and is browser-only.
#[cfg(target_arch = "wasm32")]
pub mod idb_backend;

pub use cache::{CacheError, LruCache};
pub use gc::DeleteNode;
pub use indexeddb::{BlobStore, IndexedDbError, IndexedDbStore, MemoryBlobStore};
pub use memory::{MemoryStore, NodeStore};

#[cfg(not(feature = "wasm"))]
pub use disk::{DiskStore, StoreError};

#[cfg(target_arch = "wasm32")]
pub use idb_backend::{IdbBlobStore, IdbError};
