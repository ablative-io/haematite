pub mod buffer;
pub mod entry;

#[cfg(not(feature = "wasm"))]
pub mod durable;
#[cfg(not(feature = "wasm"))]
pub mod promise;
#[cfg(not(feature = "wasm"))]
pub mod recovery;

pub use buffer::{LookupResult, Mutation, WalBuffer, WalError};
pub use entry::{OperationType, WalEntry};

#[cfg(not(feature = "wasm"))]
pub use durable::{DurableWal, FsyncPolicy, WalFileContents};
#[cfg(not(feature = "wasm"))]
pub use promise::PromiseRecord;
#[cfg(not(feature = "wasm"))]
pub use recovery::{RecoveredWal, WalRecovery};
