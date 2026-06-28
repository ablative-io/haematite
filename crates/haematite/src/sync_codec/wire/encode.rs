//! Encoding half of the sync wire codec.

use std::time::Duration;

use crate::ids::ShardId;
use crate::sync_codec::ballot::{Ballot, Stamp};
use crate::sync_codec::error::SyncError;
use crate::sync_codec::message::write::AckOutcome;
use crate::sync_codec::message::{PushResponse, WriteId};
use crate::sync_codec::target::TargetNodeSummary;
use crate::tree::Hash;

use super::{
    ACK_OUTCOME_APPLIED, ACK_OUTCOME_REJECTED, MESSAGE_BATCH_WRITE_ACK,
    MESSAGE_BATCH_WRITE_PROPOSAL, MESSAGE_NACK, MESSAGE_PREPARE, MESSAGE_PROMISE,
    MESSAGE_PULL_REQUEST, MESSAGE_PUSH_RESPONSE, MESSAGE_ROOT_REQUEST, MESSAGE_ROOT_RESPONSE,
    MESSAGE_SHARD_SYNC_REQUEST, MESSAGE_TARGET_NODE_REQUEST, MESSAGE_TARGET_NODE_RESPONSE,
    MESSAGE_WRITE_ACK, MESSAGE_WRITE_PROPOSAL, SYNC_CONTROL_FRAME, SYNC_PROTOCOL_VERSION,
    SyncMessage,
};

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
/// failover/catch-up hot path. This encodes the same bytes by reference. Only the
/// native beamr transport glue calls it; the wasm node uses the generic encode
/// path, so it is gated out of the wasm build.
#[cfg(not(feature = "wasm"))]
pub fn encode_beamr_push_response_frame(response: &PushResponse) -> Result<Vec<u8>, SyncError> {
    let mut payload = Vec::new();
    payload.push(SYNC_PROTOCOL_VERSION);
    encode_push_response(response, &mut payload);
    wrap_beamr_sync_frame(&payload)
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
/// [`WriteProposal`](crate::sync_codec::WriteProposal), grouped here because a
/// batch carries ONE shared stamp.
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
