use std::sync::Arc;
use std::time::Duration;

use beamr::atom::Atom;
use beamr::distribution::ConnectionManager;
use beamr::distribution::connection::DistConnection;

use crate::branch::ShardId;
use crate::sync::SyncNodeId;
use crate::sync::ballot::Ballot;
use crate::tree::{Hash, Node};

use super::{
    AckOutcome, BatchWriteAck, BatchWriteEntry, BatchWriteProposal, Nack, NodeTransfer, Prepare,
    Promise, PullRequest, PushResponse, RejectReason, RootExchangeRequest, RootExchangeResponse,
    ShardSyncRequest, SyncDecision, SyncError, SyncStats, TargetNodeRequest, TargetNodeResponse,
    TargetNodeSummary, WriteAck, WriteId, WriteProposal,
};
use crate::sync::ballot::Stamp;

const SYNC_CONTROL_FRAME: &[u8] = b"haematite.sync.v1";
const SYNC_PROTOCOL_VERSION: u8 = 1;

const MESSAGE_ROOT_REQUEST: u8 = 1;
const MESSAGE_ROOT_RESPONSE: u8 = 2;
const MESSAGE_PULL_REQUEST: u8 = 3;
const MESSAGE_PUSH_RESPONSE: u8 = 4;
const MESSAGE_TARGET_NODE_REQUEST: u8 = 5;
const MESSAGE_TARGET_NODE_RESPONSE: u8 = 6;
const MESSAGE_WRITE_PROPOSAL: u8 = 7;
const MESSAGE_WRITE_ACK: u8 = 8;
const MESSAGE_PREPARE: u8 = 9;
const MESSAGE_PROMISE: u8 = 10;
const MESSAGE_NACK: u8 = 11;
const MESSAGE_SHARD_SYNC_REQUEST: u8 = 12;
const MESSAGE_BATCH_WRITE_PROPOSAL: u8 = 13;
const MESSAGE_BATCH_WRITE_ACK: u8 = 14;

const ACK_OUTCOME_APPLIED: u8 = 0;
const ACK_OUTCOME_REJECTED: u8 = 1;

/// Width of a `usize`/length field on the wire (encoded as a big-endian `u64`).
const WIRE_USIZE_BYTES: usize = 8;

/// Smallest on-wire size of one `NodeTransfer`: a 32-byte content hash plus a
/// u64 node-length prefix (the node body itself may be a single tag byte, but
/// the hash + length prefix alone bound how many transfers a hostile
/// `transfer_count` could describe in the remaining bytes).
const MIN_TRANSFER_BYTES: usize = 32 + WIRE_USIZE_BYTES;

/// Bound a wire-supplied element count by what the remaining bytes could hold,
/// so a hostile length never triggers an unbounded pre-allocation. Returns the
/// smaller of `count` and the maximum number of `min_element_bytes`-sized
/// elements that could physically fit in `remaining`; the decode loop still
/// grows the Vec normally if the genuine count exceeds the clamp.
fn clamp_capacity(count: usize, remaining: usize, min_element_bytes: usize) -> usize {
    let max_possible = remaining / min_element_bytes.max(1);
    count.min(max_possible)
}

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

/// Encode a sync message payload.
pub fn encode_sync_message(message: &SyncMessage) -> Result<Vec<u8>, SyncError> {
    let mut bytes = Vec::new();
    bytes.push(SYNC_PROTOCOL_VERSION);
    match message {
        SyncMessage::RootRequest(request) => {
            bytes.push(MESSAGE_ROOT_REQUEST);
            append_shard_id(&mut bytes, request.shard_id);
            append_optional_hash(&mut bytes, request.target_root);
        }
        SyncMessage::RootResponse(response) => {
            bytes.push(MESSAGE_ROOT_RESPONSE);
            append_shard_id(&mut bytes, response.shard_id);
            append_optional_hash(&mut bytes, response.source_root);
            append_optional_hash(&mut bytes, response.target_root);
            bytes.push(response.decision.to_wire());
        }
        SyncMessage::PullRequest(request) => {
            bytes.push(MESSAGE_PULL_REQUEST);
            append_shard_id(&mut bytes, request.shard_id);
            append_optional_hash(&mut bytes, request.target_root);
        }
        SyncMessage::PushResponse(response) => encode_push_response(response, &mut bytes),
        SyncMessage::ShardSyncRequest(request) => {
            bytes.push(MESSAGE_SHARD_SYNC_REQUEST);
            append_shard_id(&mut bytes, request.shard_id);
            append_len_prefixed_bytes(&mut bytes, request.requester.as_str().as_bytes());
            append_optional_hash(&mut bytes, request.from_root);
        }
        SyncMessage::TargetNodeRequest(request) => {
            bytes.push(MESSAGE_TARGET_NODE_REQUEST);
            append_shard_id(&mut bytes, request.shard_id);
            bytes.extend_from_slice(request.hash.as_bytes());
        }
        SyncMessage::TargetNodeResponse(response) => {
            bytes.push(MESSAGE_TARGET_NODE_RESPONSE);
            append_shard_id(&mut bytes, response.shard_id);
            bytes.extend_from_slice(response.hash.as_bytes());
            append_optional_target_summary(&mut bytes, response.summary.as_ref());
        }
        SyncMessage::WriteProposal(proposal) => {
            bytes.push(MESSAGE_WRITE_PROPOSAL);
            append_write_id(&mut bytes, &proposal.write_id);
            append_shard_id(&mut bytes, proposal.shard_id);
            append_len_prefixed_bytes(&mut bytes, &proposal.key);
            append_optional_hash(&mut bytes, proposal.expected);
            append_len_prefixed_bytes(&mut bytes, &proposal.value);
            append_optional_duration(&mut bytes, proposal.ttl);
            append_ballot(&mut bytes, &proposal.epoch);
            bytes.extend_from_slice(&proposal.seq.to_be_bytes());
            bytes.push(u8::from(proposal.tombstone));
        }
        SyncMessage::WriteAck(ack) => {
            bytes.push(MESSAGE_WRITE_ACK);
            append_write_id(&mut bytes, &ack.write_id);
            append_len_prefixed_bytes(&mut bytes, ack.acker.as_str().as_bytes());
            bytes.extend_from_slice(&ack.acker_creation.to_be_bytes());
            append_ack_outcome(&mut bytes, ack.outcome);
        }
        SyncMessage::BatchWriteProposal(proposal) => {
            bytes.push(MESSAGE_BATCH_WRITE_PROPOSAL);
            append_write_id(&mut bytes, &proposal.write_id);
            append_shard_id(&mut bytes, proposal.shard_id);
            // Length-prefixed entry vector: count, then each entry's fields in the
            // same order as a single-key proposal (minus the per-entry stamp).
            append_usize(&mut bytes, proposal.entries.len());
            for entry in &proposal.entries {
                append_len_prefixed_bytes(&mut bytes, &entry.key);
                append_optional_hash(&mut bytes, entry.expected);
                append_len_prefixed_bytes(&mut bytes, &entry.value);
                append_optional_duration(&mut bytes, entry.ttl);
            }
            // ONE shared stamp for the whole batch: epoch ballot then seq.
            append_stamp(&mut bytes, &proposal.stamp);
        }
        SyncMessage::BatchWriteAck(ack) => {
            bytes.push(MESSAGE_BATCH_WRITE_ACK);
            append_write_id(&mut bytes, &ack.write_id);
            append_len_prefixed_bytes(&mut bytes, ack.acker.as_str().as_bytes());
            bytes.extend_from_slice(&ack.acker_creation.to_be_bytes());
            append_ack_outcome(&mut bytes, ack.outcome);
        }
        SyncMessage::Prepare(prepare) => {
            bytes.push(MESSAGE_PREPARE);
            append_shard_id(&mut bytes, prepare.shard_id);
            append_ballot(&mut bytes, &prepare.ballot);
        }
        SyncMessage::Promise(promise) => {
            bytes.push(MESSAGE_PROMISE);
            append_shard_id(&mut bytes, promise.shard_id);
            append_ballot(&mut bytes, &promise.ballot);
            append_len_prefixed_bytes(&mut bytes, promise.promiser.as_str().as_bytes());
            append_optional_ballot(&mut bytes, promise.accepted_epoch.as_ref());
            append_optional_hash(&mut bytes, promise.committed_root);
        }
        SyncMessage::Nack(nack) => {
            bytes.push(MESSAGE_NACK);
            append_shard_id(&mut bytes, nack.shard_id);
            append_ballot(&mut bytes, &nack.promised);
        }
    }
    Ok(bytes)
}

/// Decode a sync message payload received from a beamr control frame.
pub fn decode_sync_message(bytes: &[u8]) -> Result<SyncMessage, SyncError> {
    let mut cursor = MessageCursor::new(bytes);
    let version = cursor.read_u8()?;
    if version != SYNC_PROTOCOL_VERSION {
        return Err(SyncError::InvalidMessage);
    }

    let message = match cursor.read_u8()? {
        MESSAGE_ROOT_REQUEST => SyncMessage::RootRequest(RootExchangeRequest {
            shard_id: cursor.read_shard_id()?,
            target_root: cursor.read_optional_hash()?,
        }),
        MESSAGE_ROOT_RESPONSE => SyncMessage::RootResponse(RootExchangeResponse {
            shard_id: cursor.read_shard_id()?,
            source_root: cursor.read_optional_hash()?,
            target_root: cursor.read_optional_hash()?,
            decision: SyncDecision::from_wire(cursor.read_u8()?)?,
        }),
        MESSAGE_PULL_REQUEST => SyncMessage::PullRequest(PullRequest {
            shard_id: cursor.read_shard_id()?,
            target_root: cursor.read_optional_hash()?,
        }),
        MESSAGE_PUSH_RESPONSE => decode_push_response(&mut cursor)?,
        MESSAGE_SHARD_SYNC_REQUEST => SyncMessage::ShardSyncRequest(ShardSyncRequest {
            shard_id: cursor.read_shard_id()?,
            requester: cursor.read_sync_node_id()?,
            from_root: cursor.read_optional_hash()?,
        }),
        MESSAGE_TARGET_NODE_REQUEST => SyncMessage::TargetNodeRequest(TargetNodeRequest {
            shard_id: cursor.read_shard_id()?,
            hash: cursor.read_hash()?,
        }),
        MESSAGE_TARGET_NODE_RESPONSE => SyncMessage::TargetNodeResponse(TargetNodeResponse {
            shard_id: cursor.read_shard_id()?,
            hash: cursor.read_hash()?,
            summary: cursor.read_optional_target_summary()?,
        }),
        MESSAGE_WRITE_PROPOSAL => SyncMessage::WriteProposal(WriteProposal {
            write_id: cursor.read_write_id()?,
            shard_id: cursor.read_shard_id()?,
            key: cursor.read_len_prefixed_bytes()?,
            expected: cursor.read_optional_hash()?,
            value: cursor.read_len_prefixed_bytes()?,
            ttl: cursor.read_optional_duration()?,
            epoch: cursor.read_ballot()?,
            seq: cursor.read_u64()?,
            tombstone: cursor.read_bool()?,
        }),
        MESSAGE_WRITE_ACK => SyncMessage::WriteAck(WriteAck {
            write_id: cursor.read_write_id()?,
            acker: cursor.read_sync_node_id()?,
            acker_creation: cursor.read_u32()?,
            outcome: cursor.read_ack_outcome()?,
        }),
        MESSAGE_BATCH_WRITE_PROPOSAL => {
            let write_id = cursor.read_write_id()?;
            let shard_id = cursor.read_shard_id()?;
            let entry_count = cursor.read_usize()?;
            let mut entries = Vec::new();
            for _ in 0..entry_count {
                entries.push(BatchWriteEntry {
                    key: cursor.read_len_prefixed_bytes()?,
                    expected: cursor.read_optional_hash()?,
                    value: cursor.read_len_prefixed_bytes()?,
                    ttl: cursor.read_optional_duration()?,
                });
            }
            let stamp = cursor.read_stamp()?;
            SyncMessage::BatchWriteProposal(BatchWriteProposal {
                write_id,
                shard_id,
                entries,
                stamp,
            })
        }
        MESSAGE_BATCH_WRITE_ACK => SyncMessage::BatchWriteAck(BatchWriteAck {
            write_id: cursor.read_write_id()?,
            acker: cursor.read_sync_node_id()?,
            acker_creation: cursor.read_u32()?,
            outcome: cursor.read_ack_outcome()?,
        }),
        MESSAGE_PREPARE => SyncMessage::Prepare(Prepare {
            shard_id: cursor.read_shard_id()?,
            ballot: cursor.read_ballot()?,
        }),
        MESSAGE_PROMISE => SyncMessage::Promise(Promise {
            shard_id: cursor.read_shard_id()?,
            ballot: cursor.read_ballot()?,
            promiser: cursor.read_sync_node_id()?,
            accepted_epoch: cursor.read_optional_ballot()?,
            committed_root: cursor.read_optional_hash()?,
        }),
        MESSAGE_NACK => SyncMessage::Nack(Nack {
            shard_id: cursor.read_shard_id()?,
            promised: cursor.read_ballot()?,
        }),
        _ => return Err(SyncError::InvalidMessage),
    };

    cursor.finish()?;
    Ok(message)
}

/// Encode a complete beamr distribution control frame for a sync message.
pub fn encode_beamr_sync_frame(message: &SyncMessage) -> Result<Vec<u8>, SyncError> {
    wrap_beamr_sync_frame(&encode_sync_message(message)?)
}

/// Wrap an already-encoded sync-message payload in the beamr control-frame
/// header (control-tag length, payload length, control tag, payload).
fn wrap_beamr_sync_frame(payload: &[u8]) -> Result<Vec<u8>, SyncError> {
    let control_len =
        u32::try_from(SYNC_CONTROL_FRAME.len()).map_err(|_error| SyncError::MessageTooLarge {
            len: SYNC_CONTROL_FRAME.len(),
        })?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_error| SyncError::MessageTooLarge { len: payload.len() })?;

    let mut frame = Vec::with_capacity(8 + SYNC_CONTROL_FRAME.len() + payload.len());
    frame.extend_from_slice(&control_len.to_be_bytes());
    frame.extend_from_slice(&payload_len.to_be_bytes());
    frame.extend_from_slice(SYNC_CONTROL_FRAME);
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// Encode a [`PushResponse`] frame directly from a borrow.
///
/// `PushResponse` carries the whole transfer set, so cloning it just to wrap it
/// in an owned [`SyncMessage`] for the generic encode path is expensive on the
/// failover/catch-up hot path. This encodes the same bytes by reference.
fn encode_beamr_push_response_frame(response: &PushResponse) -> Result<Vec<u8>, SyncError> {
    let mut payload = Vec::new();
    payload.push(SYNC_PROTOCOL_VERSION);
    encode_push_response(response, &mut payload);
    wrap_beamr_sync_frame(&payload)
}

/// Decode a complete beamr distribution control frame produced by
/// [`encode_beamr_sync_frame`].
pub fn decode_beamr_sync_frame(frame: &[u8]) -> Result<SyncMessage, SyncError> {
    let mut cursor = MessageCursor::new(frame);
    let control_len = cursor.read_u32_as_usize()?;
    let payload_len = cursor.read_u32_as_usize()?;
    let control = cursor.read_exact(control_len)?;
    if control != SYNC_CONTROL_FRAME {
        return Err(SyncError::InvalidMessage);
    }
    let payload = cursor.read_exact(payload_len)?;
    let message = decode_sync_message(payload)?;
    cursor.finish()?;
    Ok(message)
}

/// Send a sync message through beamr's existing distribution connection manager.
///
/// This function never opens its own socket. It requires an already-established
/// beamr distribution connection and hands that connection plus the encoded
/// frame to `write_frame`, allowing the caller's runtime to drive
/// `DistConnection::write_raw` without haematite creating a separate transport.
pub fn send_sync_message_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    message: &SyncMessage,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    let connection = manager
        .get_connection(remote)
        .ok_or(SyncError::TransportConnectionUnavailable)?;
    let frame = encode_beamr_sync_frame(message)?;
    write_frame(connection, frame)
}

pub fn send_root_exchange_request_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    request: RootExchangeRequest,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::RootRequest(request),
        write_frame,
    )
}

pub fn send_root_exchange_response_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    response: RootExchangeResponse,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::RootResponse(response),
        write_frame,
    )
}

pub fn send_pull_request_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    request: PullRequest,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::PullRequest(request),
        write_frame,
    )
}

pub fn send_push_response_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    response: &PushResponse,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    // Encode the (potentially large) PushResponse by reference rather than
    // cloning it into an owned `SyncMessage` just to feed the generic path.
    let connection = manager
        .get_connection(remote)
        .ok_or(SyncError::TransportConnectionUnavailable)?;
    let frame = encode_beamr_push_response_frame(response)?;
    write_frame(connection, frame)
}

pub fn send_shard_sync_request_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    request: ShardSyncRequest,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::ShardSyncRequest(request),
        write_frame,
    )
}

pub fn send_target_node_request_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    request: TargetNodeRequest,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::TargetNodeRequest(request),
        write_frame,
    )
}

pub fn send_target_node_response_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    response: &TargetNodeResponse,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::TargetNodeResponse(response.clone()),
        write_frame,
    )
}

pub fn send_write_proposal_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    proposal: &WriteProposal,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::WriteProposal(proposal.clone()),
        write_frame,
    )
}

pub fn send_write_ack_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    ack: &WriteAck,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::WriteAck(ack.clone()),
        write_frame,
    )
}

pub fn send_batch_write_proposal_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    proposal: &BatchWriteProposal,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::BatchWriteProposal(proposal.clone()),
        write_frame,
    )
}

pub fn send_batch_write_ack_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    ack: &BatchWriteAck,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::BatchWriteAck(ack.clone()),
        write_frame,
    )
}

pub fn send_prepare_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    prepare: &Prepare,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::Prepare(prepare.clone()),
        write_frame,
    )
}

pub fn send_promise_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    promise: &Promise,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::Promise(promise.clone()),
        write_frame,
    )
}

pub fn send_nack_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    nack: &Nack,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::Nack(nack.clone()),
        write_frame,
    )
}

/// Register a beamr control-frame handler for haematite sync messages.
pub fn register_beamr_sync_handler<F>(manager: &ConnectionManager, handler: F)
where
    F: Fn(Result<SyncMessage, SyncError>) + Send + Sync + 'static,
{
    manager.register_control_frame_handler(move |control, payload| {
        if control == SYNC_CONTROL_FRAME {
            handler(decode_sync_message(payload));
        }
    });
}

fn encode_push_response(response: &PushResponse, bytes: &mut Vec<u8>) {
    bytes.push(MESSAGE_PUSH_RESPONSE);
    append_shard_id(bytes, response.shard_id);
    append_optional_hash(bytes, response.source_root);
    append_optional_hash(bytes, response.target_root);
    append_usize(bytes, response.transfers.len());
    for transfer in &response.transfers {
        bytes.extend_from_slice(transfer.hash.as_bytes());
        let node_bytes = transfer.node.serialise();
        append_usize(bytes, node_bytes.len());
        bytes.extend_from_slice(&node_bytes);
    }
}

fn decode_push_response(cursor: &mut MessageCursor<'_>) -> Result<SyncMessage, SyncError> {
    let shard_id = cursor.read_shard_id()?;
    let source_root = cursor.read_optional_hash()?;
    let target_root = cursor.read_optional_hash()?;
    let transfer_count = cursor.read_usize()?;
    // `transfer_count` is attacker-controlled; clamp the pre-allocation to what
    // the remaining bytes could possibly hold (each transfer needs >= a hash and
    // a u64 node-length prefix). The loop still grows the Vec if the bound holds.
    let mut transfers = Vec::with_capacity(clamp_capacity(
        transfer_count,
        cursor.remaining(),
        MIN_TRANSFER_BYTES,
    ));
    // Accumulate the serialised byte total from the on-wire length prefix as we
    // go: the length is already framed, so re-serialising each node a third time
    // (after deserialise + the hash-verify in `from_parts`) just to measure it is
    // pure waste.
    let mut bytes_transferred = 0_usize;
    for _ in 0..transfer_count {
        let hash = cursor.read_hash()?;
        let node_len = cursor.read_usize()?;
        let node_bytes = cursor.read_exact(node_len)?;
        let node = Node::deserialise(node_bytes).map_err(|_error| SyncError::InvalidNodePayload)?;
        transfers.push(NodeTransfer::from_parts(hash, node)?);
        bytes_transferred = bytes_transferred.saturating_add(node_len);
    }
    let stats = SyncStats {
        nodes_transferred: transfers.len(),
        bytes_transferred,
        ..SyncStats::default()
    };
    Ok(SyncMessage::PushResponse(PushResponse::with_stats(
        shard_id,
        source_root,
        target_root,
        transfers,
        stats,
    )))
}

fn append_optional_target_summary(bytes: &mut Vec<u8>, summary: Option<&TargetNodeSummary>) {
    match summary {
        None => bytes.push(0),
        Some(TargetNodeSummary::Leaf) => bytes.push(1),
        Some(TargetNodeSummary::Internal(children)) => {
            bytes.push(2);
            append_usize(bytes, children.len());
            for (separator, hash) in children {
                append_len_prefixed_bytes(bytes, separator);
                bytes.extend_from_slice(hash.as_bytes());
            }
        }
    }
}

fn append_len_prefixed_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    append_usize(output, bytes.len());
    output.extend_from_slice(bytes);
}

fn append_shard_id(bytes: &mut Vec<u8>, shard_id: ShardId) {
    append_usize(bytes, shard_id);
}

fn append_usize(bytes: &mut Vec<u8>, value: usize) {
    bytes.extend_from_slice(&(value as u64).to_be_bytes());
}

fn append_optional_hash(bytes: &mut Vec<u8>, hash: Option<Hash>) {
    match hash {
        Some(hash) => {
            bytes.push(1);
            bytes.extend_from_slice(hash.as_bytes());
        }
        None => bytes.push(0),
    }
}

fn append_write_id(bytes: &mut Vec<u8>, write_id: &WriteId) {
    append_len_prefixed_bytes(bytes, write_id.origin.as_str().as_bytes());
    bytes.extend_from_slice(&write_id.origin_creation.to_be_bytes());
    bytes.extend_from_slice(&write_id.counter.to_be_bytes());
}

/// Wire-encode a [`Ballot`]: `u64` counter (big-endian) followed by the minting
/// node id as length-prefixed bytes (§5). This is the WIRE codec; the WAL ballot
/// codec (`wal/promise.rs`) is a separate, little-endian framing.
fn append_ballot(bytes: &mut Vec<u8>, ballot: &Ballot) {
    bytes.extend_from_slice(&ballot.counter.to_be_bytes());
    append_len_prefixed_bytes(bytes, ballot.node.as_str().as_bytes());
}

/// Wire-encode a [`Stamp`] (A1b): its `epoch` ballot followed by the `seq`
/// (big-endian `u64`). Mirrors the single-key path's `epoch` + `seq` framing on a
/// [`WriteProposal`], grouped here because a batch carries ONE shared stamp.
fn append_stamp(bytes: &mut Vec<u8>, stamp: &Stamp) {
    append_ballot(bytes, &stamp.epoch);
    bytes.extend_from_slice(&stamp.seq.to_be_bytes());
}

fn append_optional_ballot(bytes: &mut Vec<u8>, ballot: Option<&Ballot>) {
    match ballot {
        Some(ballot) => {
            bytes.push(1);
            append_ballot(bytes, ballot);
        }
        None => bytes.push(0),
    }
}

fn append_optional_duration(bytes: &mut Vec<u8>, ttl: Option<Duration>) {
    match ttl {
        Some(duration) => {
            bytes.push(1);
            bytes.extend_from_slice(&duration.as_secs().to_be_bytes());
            bytes.extend_from_slice(&duration.subsec_nanos().to_be_bytes());
        }
        None => bytes.push(0),
    }
}

fn append_ack_outcome(bytes: &mut Vec<u8>, outcome: AckOutcome) {
    match outcome {
        AckOutcome::Applied => bytes.push(ACK_OUTCOME_APPLIED),
        AckOutcome::Rejected(reason) => {
            bytes.push(ACK_OUTCOME_REJECTED);
            bytes.push(reason.to_wire());
        }
    }
}

struct MessageCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> MessageCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    /// Bytes not yet consumed — used to bound pre-allocation against a hostile
    /// element count.
    const fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn read_u8(&mut self) -> Result<u8, SyncError> {
        let bytes = self.read_exact(1)?;
        bytes.first().copied().ok_or(SyncError::InvalidMessage)
    }

    /// Read a single boolean byte (`0` or `1`); any other value is rejected so a
    /// malformed frame fails closed (AA-3-4b tombstone flag).
    fn read_bool(&mut self) -> Result<bool, SyncError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(SyncError::InvalidMessage),
        }
    }

    fn read_u32_as_usize(&mut self) -> Result<usize, SyncError> {
        let bytes = self.read_exact(4)?;
        let mut value = [0_u8; 4];
        value.copy_from_slice(bytes);
        usize::try_from(u32::from_be_bytes(value)).map_err(|_error| SyncError::InvalidMessage)
    }

    fn read_u32(&mut self) -> Result<u32, SyncError> {
        let bytes = self.read_exact(4)?;
        let mut value = [0_u8; 4];
        value.copy_from_slice(bytes);
        Ok(u32::from_be_bytes(value))
    }

    fn read_u64(&mut self) -> Result<u64, SyncError> {
        let bytes = self.read_exact(8)?;
        let mut value = [0_u8; 8];
        value.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(value))
    }

    fn read_usize(&mut self) -> Result<usize, SyncError> {
        let bytes = self.read_exact(8)?;
        let mut value = [0_u8; 8];
        value.copy_from_slice(bytes);
        usize::try_from(u64::from_be_bytes(value)).map_err(|_error| SyncError::InvalidMessage)
    }

    fn read_shard_id(&mut self) -> Result<ShardId, SyncError> {
        self.read_usize()
    }

    fn read_optional_hash(&mut self) -> Result<Option<Hash>, SyncError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_hash().map(Some),
            _ => Err(SyncError::InvalidMessage),
        }
    }

    fn read_optional_target_summary(&mut self) -> Result<Option<TargetNodeSummary>, SyncError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(TargetNodeSummary::Leaf)),
            2 => {
                let child_count = self.read_usize()?;
                // `child_count` is attacker-controlled; clamp the pre-allocation
                // to what the remaining bytes could hold (each child needs >= a
                // u64 separator-length prefix and a 32-byte hash).
                let mut children = Vec::with_capacity(clamp_capacity(
                    child_count,
                    self.remaining(),
                    WIRE_USIZE_BYTES + 32,
                ));
                for _ in 0..child_count {
                    let separator = self.read_len_prefixed_bytes()?;
                    let hash = self.read_hash()?;
                    children.push((separator, hash));
                }
                Ok(Some(TargetNodeSummary::Internal(children)))
            }
            _ => Err(SyncError::InvalidMessage),
        }
    }

    fn read_len_prefixed_bytes(&mut self) -> Result<Vec<u8>, SyncError> {
        let len = self.read_usize()?;
        self.read_exact(len).map(<[u8]>::to_vec)
    }

    fn read_sync_node_id(&mut self) -> Result<SyncNodeId, SyncError> {
        let bytes = self.read_len_prefixed_bytes()?;
        let name = String::from_utf8(bytes).map_err(|_error| SyncError::InvalidMessage)?;
        Ok(SyncNodeId::new(name))
    }

    fn read_ballot(&mut self) -> Result<Ballot, SyncError> {
        let counter = self.read_u64()?;
        let node = self.read_sync_node_id()?;
        Ok(Ballot::new(counter, node))
    }

    fn read_stamp(&mut self) -> Result<Stamp, SyncError> {
        let epoch = self.read_ballot()?;
        let seq = self.read_u64()?;
        Ok(Stamp::new(epoch, seq))
    }

    fn read_optional_ballot(&mut self) -> Result<Option<Ballot>, SyncError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_ballot().map(Some),
            _ => Err(SyncError::InvalidMessage),
        }
    }

    fn read_write_id(&mut self) -> Result<WriteId, SyncError> {
        let origin = self.read_sync_node_id()?;
        let origin_creation = self.read_u32()?;
        let counter = self.read_u64()?;
        Ok(WriteId {
            origin,
            origin_creation,
            counter,
        })
    }

    fn read_optional_duration(&mut self) -> Result<Option<Duration>, SyncError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => {
                let secs = self.read_u64()?;
                let nanos = self.read_u32()?;
                // Reject denormalized sub-second nanos so the `Duration::new`
                // carry can never overflow `secs` and panic on a hostile buffer.
                if nanos >= 1_000_000_000 {
                    return Err(SyncError::InvalidMessage);
                }
                Ok(Some(Duration::new(secs, nanos)))
            }
            _ => Err(SyncError::InvalidMessage),
        }
    }

    fn read_ack_outcome(&mut self) -> Result<AckOutcome, SyncError> {
        match self.read_u8()? {
            ACK_OUTCOME_APPLIED => Ok(AckOutcome::Applied),
            ACK_OUTCOME_REJECTED => {
                let reason = RejectReason::from_wire(self.read_u8()?)?;
                Ok(AckOutcome::Rejected(reason))
            }
            _ => Err(SyncError::InvalidMessage),
        }
    }

    fn read_hash(&mut self) -> Result<Hash, SyncError> {
        let bytes = self.read_exact(32)?;
        let mut hash = [0_u8; 32];
        hash.copy_from_slice(bytes);
        Ok(Hash::from_bytes(hash))
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], SyncError> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(SyncError::InvalidMessage)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(SyncError::InvalidMessage)?;
        self.position = end;
        Ok(bytes)
    }

    const fn finish(&self) -> Result<(), SyncError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(SyncError::InvalidMessage)
        }
    }
}
