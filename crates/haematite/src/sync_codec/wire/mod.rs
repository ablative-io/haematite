//! Hand-rolled wire codec for [`SyncMessage`].
//!
//! Platform-neutral (no beamr/tokio/filesystem), so a wasm node encodes and
//! decodes sync frames byte-identically to a native peer. The codec is split into
//! [`encode`], [`decode`], and the [`cursor`] reader; this module owns the
//! [`SyncMessage`] enum, the shared wire constants, and the beamr control-frame
//! wrapper.
//!
//! The native beamr transport glue (`send_*_via_beamr`,
//! `register_beamr_sync_handler`) lives in `crate::sync::transport_glue`, outside
//! this wasm-clean module.

mod cursor;
mod decode;
mod encode;

pub use decode::{decode_beamr_sync_frame, decode_sync_message};
pub use encode::{encode_beamr_sync_frame, encode_sync_message};

pub(crate) use cursor::MessageCursor;
pub(crate) use decode::clamp_capacity;
// Only the native beamr transport glue uses the by-reference PushResponse frame
// encoder; the wasm node frames via the generic `encode_sync_message` path, so
// gate the re-export out of the wasm build to keep that target warning-clean.
#[cfg(not(feature = "wasm"))]
pub(crate) use encode::encode_beamr_push_response_frame;

use crate::sync_codec::message::{
    BatchWriteAck, BatchWriteProposal, Nack, Prepare, Promise, PullRequest, PushResponse,
    RootExchangeRequest, RootExchangeResponse, ShardSyncRequest, WriteAck, WriteProposal,
};
use crate::sync_codec::target::{TargetNodeRequest, TargetNodeResponse};

pub(crate) const SYNC_CONTROL_FRAME: &[u8] = b"haematite.sync.v1";
pub(crate) const SYNC_PROTOCOL_VERSION: u8 = 1;

pub(crate) const MESSAGE_ROOT_REQUEST: u8 = 1;
pub(crate) const MESSAGE_ROOT_RESPONSE: u8 = 2;
pub(crate) const MESSAGE_PULL_REQUEST: u8 = 3;
pub(crate) const MESSAGE_PUSH_RESPONSE: u8 = 4;
pub(crate) const MESSAGE_TARGET_NODE_REQUEST: u8 = 5;
pub(crate) const MESSAGE_TARGET_NODE_RESPONSE: u8 = 6;
pub(crate) const MESSAGE_WRITE_PROPOSAL: u8 = 7;
pub(crate) const MESSAGE_WRITE_ACK: u8 = 8;
pub(crate) const MESSAGE_PREPARE: u8 = 9;
pub(crate) const MESSAGE_PROMISE: u8 = 10;
pub(crate) const MESSAGE_NACK: u8 = 11;
pub(crate) const MESSAGE_SHARD_SYNC_REQUEST: u8 = 12;
pub(crate) const MESSAGE_BATCH_WRITE_PROPOSAL: u8 = 13;
pub(crate) const MESSAGE_BATCH_WRITE_ACK: u8 = 14;

pub(crate) const ACK_OUTCOME_APPLIED: u8 = 0;
pub(crate) const ACK_OUTCOME_REJECTED: u8 = 1;

/// Width of a `usize`/length field on the wire (encoded as a big-endian `u64`).
pub(crate) const WIRE_USIZE_BYTES: usize = 8;

/// Smallest on-wire size of one `NodeTransfer`: a 32-byte content hash plus a
/// u64 node-length prefix (the node body itself may be a single tag byte, but
/// the hash + length prefix alone bound how many transfers a hostile
/// `transfer_count` could describe in the remaining bytes).
pub(crate) const MIN_TRANSFER_BYTES: usize = 32 + WIRE_USIZE_BYTES;

/// Sync protocol messages that can be framed over beamr distribution links.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncMessage {
    RootRequest(RootExchangeRequest),
    RootResponse(RootExchangeResponse),
    PullRequest(PullRequest),
    PushResponse(PushResponse),
    /// Step-3 handoff catch-up request (§2.4): a freshly-elected owner asks a
    /// promiser for the full reachable node set so it can sync to the max
    /// committed root before serving. The reply reuses [`Self::PushResponse`].
    ShardSyncRequest(ShardSyncRequest),
    TargetNodeRequest(TargetNodeRequest),
    TargetNodeResponse(TargetNodeResponse),
    WriteProposal(WriteProposal),
    WriteAck(WriteAck),
    /// A1b: a replicated multi-key append (the batch analogue of
    /// [`Self::WriteProposal`]), applied all-or-nothing through
    /// `apply_durable_batch`.
    BatchWriteProposal(BatchWriteProposal),
    /// A1b: the single all-or-nothing verdict for a [`Self::BatchWriteProposal`].
    BatchWriteAck(BatchWriteAck),
    Prepare(Prepare),
    Promise(Promise),
    Nack(Nack),
}
