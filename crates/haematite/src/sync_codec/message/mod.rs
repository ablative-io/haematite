//! Sync-protocol message types, split by concern.
//!
//! All types are platform-neutral: their fields bottom out in
//! `ShardId`/`KvKey`/`KvValue`/`SyncNodeId`/`Hash`/`Node`/`Ballot`/`Stamp`/
//! `Duration`, none of which pull in a native dependency. The wire codec
//! ([`crate::sync_codec::wire`]) frames them; the native sync layer re-exports
//! them so every `crate::sync::*` path stays stable.

pub mod election;
pub mod root;
pub mod transfer;
pub mod write;

pub use election::{Nack, Prepare, Promise, ShardSyncRequest};
pub use root::{
    PullRequest, RootExchange, RootExchangeRequest, RootExchangeResponse, SyncDecision, SyncPlan,
    SyncStats, plan_sync,
};
pub use transfer::{MissingNodes, NodeTransfer, PushResponse};
pub use write::{
    AckOutcome, BatchWriteAck, BatchWriteEntry, BatchWriteProposal, RejectReason, WriteAck,
    WriteId, WriteProposal,
};
