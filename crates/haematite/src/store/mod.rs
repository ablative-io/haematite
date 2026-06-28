pub mod cache;
pub mod gc;
pub mod memory;

#[cfg(not(feature = "wasm"))]
pub mod disk;
#[cfg(feature = "wasm")]
pub mod indexeddb;
// The platform-neutral WAL framing in `opfs::frame` (the load-bearing
// byte-identity layer, WASM-002 R2) compiles on every target so its parity with
// the native `DurableWal` can be proven by native `#[test]`s; the OPFS / IndexedDB
// file I/O inside it is gated to `wasm32` (see the module header).
pub mod opfs;

pub use cache::{CacheError, LruCache};
pub use gc::DeleteNode;
pub use memory::{MemoryStore, NodeStore};

#[cfg(not(feature = "wasm"))]
pub use disk::{DiskStore, StoreError};
#[cfg(feature = "wasm")]
pub use indexeddb::{IndexedDbStore, IndexedDbStoreError};
#[cfg(feature = "wasm")]
pub use opfs::{BrowserWal, IndexedDbWal, OpfsWal, OpfsWalError, WalBackend};
