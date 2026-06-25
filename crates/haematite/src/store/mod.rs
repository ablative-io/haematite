pub mod cache;
pub mod gc;
pub mod memory;

#[cfg(not(feature = "wasm"))]
pub mod disk;
#[cfg(feature = "wasm")]
pub mod indexeddb;
#[cfg(feature = "wasm")]
pub mod opfs;

pub use cache::{CacheError, LruCache};
pub use gc::DeleteNode;
pub use memory::{MemoryStore, NodeStore};

#[cfg(not(feature = "wasm"))]
pub use disk::{DiskStore, StoreError};
#[cfg(feature = "wasm")]
pub use indexeddb::{IndexedDbStore, IndexedDbStoreError};
