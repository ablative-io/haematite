//! Public API facades: the `EventStore`, general KV operations, value types,
//! and error types.

pub mod error;
pub mod event_store;
pub mod kv;
pub mod types;

pub use error::{ApiError, CasMismatch, SequenceConflict};
pub use event_store::{EventStore, decode_stream_key, encode_stream_key};
pub use kv::{KvEntry, KvKey, KvRange, KvValue, ShardRoots};
pub use types::{Event, ScanResult, StreamMeta};
