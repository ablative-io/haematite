//! Native re-export shim for the sync protocol.
//!
//! The platform-neutral message types, error, target-node abstraction,
//! missing-node discovery, and the wire codec live in the ungated
//! [`crate::sync_codec`] module so they compile on wasm. The native-only beamr
//! transport glue (`send_*_via_beamr`, `register_beamr_sync_handler`) lives in
//! [`crate::sync::transport_glue`]. This module keeps every
//! `crate::sync::protocol::*` path stable by re-exporting both.

#[cfg(test)]
#[path = "protocol/tests.rs"]
mod tests;

pub use crate::sync::transport_glue::{
    register_beamr_sync_handler, send_batch_write_ack_via_beamr,
    send_batch_write_proposal_via_beamr, send_nack_via_beamr, send_prepare_via_beamr,
    send_promise_via_beamr, send_pull_request_via_beamr, send_push_response_via_beamr,
    send_root_exchange_request_via_beamr, send_root_exchange_response_via_beamr,
    send_shard_sync_request_via_beamr, send_sync_message_via_beamr,
    send_target_node_request_via_beamr, send_target_node_response_via_beamr,
    send_write_ack_via_beamr, send_write_proposal_via_beamr,
};
pub use crate::sync_codec::{
    AckOutcome, BatchWriteAck, BatchWriteEntry, BatchWriteProposal, MissingNodes, Nack,
    NodeTransfer, Prepare, Promise, PullRequest, PushResponse, RejectReason, RootExchange,
    RootExchangeRequest, RootExchangeResponse, ShardSyncRequest, SyncDecision, SyncError,
    SyncMessage, SyncPlan, SyncStats, TargetNodeReader, TargetNodeRequest, TargetNodeResponse,
    TargetNodeSummary, WriteAck, WriteId, WriteProposal, decode_beamr_sync_frame,
    decode_sync_message, encode_beamr_sync_frame, encode_sync_message, find_missing_nodes,
    plan_sync,
};
