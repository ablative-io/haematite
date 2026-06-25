pub mod store;
pub mod tree;
pub mod wal;

// Native-only modules. These pull in the actor runtime (beamr/tokio), the
// distribution layer, and filesystem persistence, none of which target
// wasm32-unknown-unknown. They are gated out of the wasm build (R1, CN1).
#[cfg(not(feature = "wasm"))]
pub mod api;
#[cfg(not(feature = "wasm"))]
pub mod branch;
#[cfg(not(feature = "wasm"))]
pub mod db;
#[cfg(not(feature = "wasm"))]
pub(crate) mod shard;
#[cfg(not(feature = "wasm"))]
pub mod sync;
#[cfg(not(feature = "wasm"))]
pub mod ttl;

#[cfg(feature = "wasm")]
pub mod wasm;

mod error;

pub use error::Error;

pub use store::{CacheError, DeleteNode, LruCache, MemoryStore, NodeStore};

#[cfg(not(feature = "wasm"))]
pub use store::{DiskStore, StoreError};
#[cfg(feature = "wasm")]
pub use store::{IndexedDbStore, IndexedDbStoreError};

pub use tree::{
    BoundaryDetector, Cursor, Hash, InternalNode, LeafNode, Node, NodeError, RangeIter, TreeError,
    batch_mutate, delete, insert,
};
pub use tree::{DiffEntry, DiffError, diff};

pub use wal::{LookupResult, Mutation, OperationType, WalBuffer, WalEntry, WalError};

#[cfg(not(feature = "wasm"))]
pub use wal::{DurableWal, FsyncPolicy, RecoveredWal, WalFileContents, WalRecovery};

#[cfg(not(feature = "wasm"))]
pub use branch::{
    BranchError, BranchHandle, BranchRegistry, BranchWalBuffer, CheckoutError, CommitLog,
    CommitLogEntry, ConflictError, ConflictInput, ConflictPolicy, CustomMergeFn, MergeConflict,
    MergeError, MergeReport, PruneError, PruneReport, ReadOnlyView, ShardId, SnapshotEntry,
    SnapshotError, SnapshotRegistry, Timestamp, checkout, current_timestamp, fork, fork_registered,
    fork_shards, fork_shards_registered, merge, merge_with_report, prune,
};

#[cfg(not(feature = "wasm"))]
pub use api::{
    ApiError, CasMismatch, Event, EventStore, KvEntry, KvKey, KvRange, KvValue, ScanResult,
    SequenceConflict, ShardRoots, StreamMeta, decode_stream_key, encode_stream_key,
};

#[cfg(not(feature = "wasm"))]
pub use db::{
    Database, DatabaseConfig, DatabaseError, DistributedDatabaseConfig, respond_to_inbound_writes,
};

#[cfg(not(feature = "wasm"))]
pub use sync::{
    Ack, ConsistencyError, ConsistencyMode, ConvergenceProperties, DistributionEndpoint,
    EventualConsistency, InboundSync, NoopSyncPullTrigger, QuorumOutcome, StrongConsistency,
    SyncMergeError, SyncMergeResult, SyncMergeRoots, SyncNodeId, SyncPair, SyncPullTrigger,
    SyncSchedulerConfig, SyncSchedulerError, SyncSchedulerHandle, SyncSchedulerStats, SyncTopology,
    TopologyError, WriteMembership, execute_with_consistency, merge_synced_roots, quorum_size,
    wait_for_quorum, wait_for_quorum_from_receiver,
};

#[cfg(feature = "wasm-runtime")]
pub use wasm::{WasmRuntime, WasmRuntimeError, WasmShardHandle, WasmShardRuntime};
