pub mod api;
pub mod branch;
pub(crate) mod shard;
pub mod store;
pub mod sync;
pub mod tree;
pub mod ttl;
pub mod wal;
pub mod wasm;

pub mod db;
mod error;

pub use branch::{
    BranchError, BranchHandle, BranchRegistry, BranchWalBuffer, CheckoutError, CommitLog,
    CommitLogEntry, ConflictError, ConflictInput, ConflictPolicy, CustomMergeFn, MergeError,
    ReadOnlyView, ShardId, SnapshotEntry, SnapshotError, SnapshotRegistry, Timestamp, checkout,
    current_timestamp, fork, fork_registered, fork_shards, fork_shards_registered, merge,
};

pub use api::{
    ApiError, CasMismatch, Event, EventStore, ScanResult, SequenceConflict, StreamMeta,
    decode_stream_key, encode_stream_key,
};

pub use db::{Database, DatabaseConfig, DatabaseError};

pub use error::Error;

pub use store::{CacheError, DeleteNode, DiskStore, LruCache, MemoryStore, NodeStore, StoreError};

pub use tree::{
    BoundaryDetector, Cursor, Hash, InternalNode, LeafNode, Node, NodeError, RangeIter, TreeError,
    batch_mutate, delete, insert,
};
pub use tree::{DiffEntry, DiffError, diff};

pub use wal::{
    DurableWal, FsyncPolicy, LookupResult, Mutation, OperationType, RecoveredWal, WalBuffer,
    WalEntry, WalError, WalFileContents, WalRecovery,
};
