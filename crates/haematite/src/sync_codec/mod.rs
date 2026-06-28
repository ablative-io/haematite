//! Platform-neutral sync-protocol message types and wire codec.
//!
//! This module is the wasm-clean half of the sync protocol: the message types a
//! node exchanges and the hand-rolled wire codec that frames them
//! (`encode`/`decode_sync_message`, `encode`/`decode_beamr_sync_frame`). It pulls
//! in NO native dependency (no beamr, tokio, filesystem, or shard layer), so it
//! compiles on `wasm32-unknown-unknown` and a wasm node can encode and decode a
//! [`SyncMessage`] byte-identically to a native peer.
//!
//! The native [`crate::sync`] module depends on this module (never the reverse)
//! and re-exports every type from it, so all existing `crate::sync::*` paths stay
//! stable. The native-only orchestration (endpoint, scheduler, topology,
//! `send_*_via_beamr` transport glue) lives in `crate::sync` and is gated out of
//! the wasm build.

pub mod ballot;
pub mod error;
pub mod ids;
pub mod message;
pub mod missing;
pub mod target;

pub use ballot::{Ballot, Stamp};
pub use error::SyncError;
pub use ids::SyncNodeId;
pub use message::{
    AckOutcome, BatchWriteAck, BatchWriteEntry, BatchWriteProposal, MissingNodes, Nack,
    NodeTransfer, Prepare, Promise, PullRequest, PushResponse, RejectReason, RootExchange,
    RootExchangeRequest, RootExchangeResponse, ShardSyncRequest, SyncDecision, SyncPlan, SyncStats,
    WriteAck, WriteId, WriteProposal, plan_sync,
};
pub use missing::find_missing_nodes;
pub use target::{TargetNodeReader, TargetNodeRequest, TargetNodeResponse, TargetNodeSummary};
