//! Decoding half of the sync wire codec.

use crate::sync_codec::error::SyncError;
use crate::sync_codec::message::root::SyncStats;
use crate::sync_codec::message::{
    BatchWriteAck, BatchWriteEntry, BatchWriteProposal, Nack, NodeTransfer, Prepare, Promise,
    PullRequest, PushResponse, RootExchangeRequest, RootExchangeResponse, ShardSyncRequest,
    SyncDecision, WriteAck, WriteProposal,
};
use crate::sync_codec::target::{TargetNodeRequest, TargetNodeResponse};
use crate::tree::Node;

use super::{
    MESSAGE_BATCH_WRITE_ACK, MESSAGE_BATCH_WRITE_PROPOSAL, MESSAGE_NACK, MESSAGE_PREPARE,
    MESSAGE_PROMISE, MESSAGE_PULL_REQUEST, MESSAGE_PUSH_RESPONSE, MESSAGE_ROOT_REQUEST,
    MESSAGE_ROOT_RESPONSE, MESSAGE_SHARD_SYNC_REQUEST, MESSAGE_TARGET_NODE_REQUEST,
    MESSAGE_TARGET_NODE_RESPONSE, MESSAGE_WRITE_ACK, MESSAGE_WRITE_PROPOSAL, MIN_TRANSFER_BYTES,
    MessageCursor, SYNC_CONTROL_FRAME, SYNC_PROTOCOL_VERSION, SyncMessage,
};

/// Bound a wire-supplied element count by what the remaining bytes could hold,
/// so a hostile length never triggers an unbounded pre-allocation. Returns the
/// smaller of `count` and the maximum number of `min_element_bytes`-sized
/// elements that could physically fit in `remaining`; the decode loop still
/// grows the Vec normally if the genuine count exceeds the clamp.
pub fn clamp_capacity(count: usize, remaining: usize, min_element_bytes: usize) -> usize {
    let max_possible = remaining / min_element_bytes.max(1);
    count.min(max_possible)
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

/// Decode a complete beamr distribution control frame produced by
/// [`encode_beamr_sync_frame`](crate::sync_codec::encode_beamr_sync_frame).
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
