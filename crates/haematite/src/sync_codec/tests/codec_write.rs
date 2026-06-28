//! Write/batch message + framing codec round-trip and truncation tests.

use std::time::Duration;

use crate::sync_codec::ballot::{Ballot, Stamp};
use crate::sync_codec::error::SyncError;
use crate::sync_codec::ids::SyncNodeId;
use crate::sync_codec::message::{
    AckOutcome, BatchWriteAck, BatchWriteEntry, BatchWriteProposal, NodeTransfer, PullRequest,
    PushResponse, RejectReason, RootExchangeRequest, RootExchangeResponse, SyncStats, WriteAck,
    WriteId, WriteProposal,
};
use crate::sync_codec::target::{TargetNodeRequest, TargetNodeResponse, TargetNodeSummary};
use crate::sync_codec::wire::{
    SyncMessage, decode_beamr_sync_frame, decode_sync_message, encode_beamr_sync_frame,
    encode_sync_message,
};

use super::{assert_message_round_trips, leaf, sample_hash};

#[test]
fn sync_messages_round_trip_through_beamr_frame_encoding() -> Result<(), Box<dyn std::error::Error>>
{
    let transfer = NodeTransfer::new(leaf(b"a", b"one")?);
    let target_request = TargetNodeRequest::new(5, transfer.hash);
    let target_response = TargetNodeResponse {
        shard_id: 5,
        hash: transfer.hash,
        summary: Some(TargetNodeSummary::Internal(vec![(
            b"".to_vec(),
            transfer.hash,
        )])),
    };
    let response = PushResponse::new(
        5,
        Some(transfer.hash),
        None,
        vec![transfer],
        SyncStats::default(),
    );
    let messages = vec![
        SyncMessage::RootRequest(RootExchangeRequest::new(5, None)),
        SyncMessage::RootResponse(RootExchangeResponse::from_request(
            &RootExchangeRequest::new(5, None),
            response.source_root,
        )),
        SyncMessage::PullRequest(PullRequest::new(5, None)),
        SyncMessage::PushResponse(response),
        SyncMessage::TargetNodeRequest(target_request),
        SyncMessage::TargetNodeResponse(target_response),
    ];

    for message in messages {
        let frame = encode_beamr_sync_frame(&message)?;
        let decoded = decode_beamr_sync_frame(&frame)?;
        assert_eq!(decoded, message);
    }
    Ok(())
}

#[test]
fn wasm_node_push_response_frame_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    // The exact round-trip a wasm node performs: frame a representative
    // PushResponse (carrying a NodeTransfer) alongside a WriteProposal with the
    // full active-active field set, then decode the frames straight back. Proves a
    // wasm node can ENCODE a SyncMessage with `encode_beamr_sync_frame` and DECODE
    // it with `decode_beamr_sync_frame` to an identical message — byte-identical to
    // a native peer.
    let transfer = NodeTransfer::new(leaf(b"failover-key", b"committed-value")?);
    let push = SyncMessage::PushResponse(PushResponse::new(
        9,
        Some(transfer.hash),
        None,
        vec![transfer],
        SyncStats::default(),
    ));
    let proposal = SyncMessage::WriteProposal(WriteProposal {
        write_id: WriteId::new("wasm-origin", 3, 77),
        shard_id: 9,
        key: b"replicated/key".to_vec(),
        expected: Some(sample_hash(b"prev", b"old")?),
        value: b"new-value".to_vec(),
        ttl: Some(Duration::new(30, 500)),
        epoch: Ballot::new(4, SyncNodeId::new("owner-node")),
        seq: 88,
        tombstone: false,
    });

    for message in [push, proposal] {
        let frame = encode_beamr_sync_frame(&message)?;
        let decoded = decode_beamr_sync_frame(&frame)?;
        assert_eq!(decoded, message);
    }
    Ok(())
}

#[test]
fn write_proposal_round_trips_across_field_variations() -> Result<(), Box<dyn std::error::Error>> {
    let expected = sample_hash(b"prev", b"old")?;
    let write_id = WriteId::new("node-origin-name", 7, 42);

    let proposals = vec![
        // empty value, no precondition, no ttl, BOTTOM epoch (un-elected 2a case)
        WriteProposal {
            write_id: write_id.clone(),
            shard_id: 0,
            key: b"k".to_vec(),
            expected: None,
            value: Vec::new(),
            ttl: None,
            epoch: Ballot::bottom(),
            seq: 0,
            tombstone: false,
        },
        // expected Some + ttl Some + a REAL epoch with a multi-byte node tiebreak,
        // and a non-zero `shard_id` so the round-trip proves the shard survives the
        // wire — a routed stamped write (durable timer) co-locates a key on a
        // DIFFERENT shard than its bytes hash to, so a shard_id decode regression
        // would silently mis-route it.
        WriteProposal {
            write_id: write_id.clone(),
            shard_id: 7,
            key: b"another/key".to_vec(),
            expected: Some(expected),
            value: b"hello world".to_vec(),
            ttl: Some(Duration::new(12, 345)),
            epoch: Ballot::new(7, SyncNodeId::new("owner-node-\u{00e9}")),
            seq: 42,
            // A replicated DELETE (tombstone) round-trips its flag too (AA-3-4b).
            tombstone: true,
        },
        // large value + a high-counter epoch (exercises the full u64 counter) + a
        // high seq (exercises the full u64 seq field) + a max `shard_id` (full usize)
        WriteProposal {
            write_id,
            shard_id: usize::MAX,
            key: Vec::new(),
            expected: Some(expected),
            value: vec![0xAB; 64 * 1024],
            ttl: Some(Duration::from_secs(3600)),
            epoch: Ballot::new(u64::MAX, SyncNodeId::new("z")),
            seq: u64::MAX,
            tombstone: false,
        },
    ];

    for proposal in &proposals {
        assert_message_round_trips(&SyncMessage::WriteProposal(proposal.clone()))?;
    }
    Ok(())
}

#[test]
fn write_ack_round_trips_for_every_outcome() -> Result<(), Box<dyn std::error::Error>> {
    let write_id = WriteId::new("origin", 1, 9);
    let outcomes = [
        AckOutcome::Applied,
        AckOutcome::Rejected(RejectReason::CasMismatch),
        AckOutcome::Rejected(RejectReason::ApplyError),
    ];

    for outcome in outcomes {
        let ack = WriteAck {
            write_id: write_id.clone(),
            acker: SyncNodeId::new("multi-byte-acker-name-\u{00e9}"),
            acker_creation: 5,
            outcome,
        };
        assert_message_round_trips(&SyncMessage::WriteAck(ack))?;
    }
    Ok(())
}

#[test]
fn batch_write_proposal_round_trips_across_sizes_and_field_variations()
-> Result<(), Box<dyn std::error::Error>> {
    let h1 = sample_hash(b"prev-a", b"old-a")?;
    let h2 = sample_hash(b"prev-b", b"old-b")?;
    let write_id = WriteId::new("node-origin-name", 7, 42);
    // A REAL (non-bottom) shared stamp with a multi-byte node tiebreak and non-zero
    // seq, so the round-trip exercises the full epoch+seq framing, not just bottom.
    let stamp = Stamp::new(Ballot::new(9, SyncNodeId::new("owner-node-\u{00e9}")), 1234);

    // (a) EMPTY batch (zero entries) — a valid no-op proposal.
    let empty = BatchWriteProposal {
        write_id: write_id.clone(),
        shard_id: 0,
        entries: Vec::new(),
        stamp: Stamp::bottom(),
    };

    // (b) SINGLE entry.
    let single = BatchWriteProposal {
        write_id: write_id.clone(),
        shard_id: 3,
        entries: vec![BatchWriteEntry {
            key: b"only/key".to_vec(),
            expected: None,
            value: b"v".to_vec(),
            ttl: None,
        }],
        stamp: stamp.clone(),
    };

    // (c) SEVERAL entries with mixed expected Some/None and with/without ttl.
    let mixed = BatchWriteProposal {
        write_id: write_id.clone(),
        shard_id: 7,
        entries: vec![
            BatchWriteEntry {
                key: b"stream\0\0\0\0\0\0\0\0\x01".to_vec(),
                expected: None,
                value: b"event-1".to_vec(),
                ttl: Some(Duration::new(12, 345)),
            },
            BatchWriteEntry {
                key: b"stream\0\0\0\0\0\0\0\0\x02".to_vec(),
                expected: Some(h1),
                value: b"event-2".to_vec(),
                ttl: None,
            },
            BatchWriteEntry {
                key: b"stream\xff seq".to_vec(),
                expected: Some(h2),
                value: 2_u64.to_be_bytes().to_vec(),
                ttl: Some(Duration::from_secs(3600)),
            },
        ],
        stamp,
    };

    // (d) LARGE batch: many entries, alternating expected/ttl, a big value.
    let mut large_entries = Vec::new();
    for index in 0..512_u64 {
        large_entries.push(BatchWriteEntry {
            key: {
                let mut key = b"k".to_vec();
                key.extend_from_slice(&index.to_be_bytes());
                key
            },
            expected: if index % 2 == 0 { Some(h1) } else { None },
            value: vec![u8::try_from(index % 256)?; 1 + (index as usize % 17)],
            ttl: if index % 3 == 0 {
                Some(Duration::new(index, u32::try_from(index % 1_000_000_000)?))
            } else {
                None
            },
        });
    }
    // One genuinely large value to exercise multi-KiB length prefixes.
    large_entries.push(BatchWriteEntry {
        key: b"big".to_vec(),
        expected: Some(h2),
        value: vec![0xAB; 64 * 1024],
        ttl: None,
    });
    let large = BatchWriteProposal {
        write_id,
        shard_id: usize::MAX,
        entries: large_entries,
        stamp: Stamp::new(Ballot::new(u64::MAX, SyncNodeId::new("z")), u64::MAX),
    };

    for proposal in [empty, single, mixed, large] {
        assert_message_round_trips(&SyncMessage::BatchWriteProposal(proposal))?;
    }
    Ok(())
}

#[test]
fn batch_write_ack_round_trips_for_every_outcome() -> Result<(), Box<dyn std::error::Error>> {
    let write_id = WriteId::new("origin", 1, 9);
    // Every outcome a batch ack can carry: an accept, and a reject for EACH reason
    // (Fenced and CasMismatch are the two the receiver maps a fence / CAS mismatch
    // onto, plus ApplyError).
    let outcomes = [
        AckOutcome::Applied,
        AckOutcome::Rejected(RejectReason::Fenced),
        AckOutcome::Rejected(RejectReason::CasMismatch),
        AckOutcome::Rejected(RejectReason::ApplyError),
    ];

    for outcome in outcomes {
        let ack = BatchWriteAck {
            write_id: write_id.clone(),
            acker: SyncNodeId::new("multi-byte-acker-name-\u{00e9}"),
            acker_creation: 5,
            outcome,
        };
        assert_message_round_trips(&SyncMessage::BatchWriteAck(ack))?;
    }
    Ok(())
}

#[test]
fn truncated_batch_write_messages_decode_to_clean_error() -> Result<(), Box<dyn std::error::Error>>
{
    let proposal = SyncMessage::BatchWriteProposal(BatchWriteProposal {
        write_id: WriteId::new("origin", 3, 1),
        shard_id: 2,
        entries: vec![
            BatchWriteEntry {
                key: b"k1".to_vec(),
                expected: Some(sample_hash(b"p", b"o")?),
                value: b"v1".to_vec(),
                ttl: Some(Duration::new(1, 1)),
            },
            BatchWriteEntry {
                key: b"k2".to_vec(),
                expected: None,
                value: b"v2".to_vec(),
                ttl: None,
            },
        ],
        stamp: Stamp::new(Ballot::new(9, SyncNodeId::new("owner")), 7),
    });
    let ack = SyncMessage::BatchWriteAck(BatchWriteAck {
        write_id: WriteId::new("origin", 3, 1),
        acker: SyncNodeId::new("acker"),
        acker_creation: 2,
        outcome: AckOutcome::Rejected(RejectReason::Fenced),
    });

    for message in [proposal, ack] {
        let payload = encode_sync_message(&message)?;
        // Every non-empty truncation must be a clean Err, never a panic — this also
        // sweeps truncations inside the entry vector, the per-entry CAS hash, and
        // the trailing shared stamp.
        for len in 0..payload.len() {
            assert!(decode_sync_message(&payload[..len]).is_err());
        }
        // Trailing garbage must also be rejected by the finish() check.
        let mut extended = payload.clone();
        extended.push(0xFF);
        assert!(decode_sync_message(&extended).is_err());
    }
    Ok(())
}

#[test]
fn truncated_write_messages_decode_to_clean_error() -> Result<(), Box<dyn std::error::Error>> {
    let proposal = SyncMessage::WriteProposal(WriteProposal {
        write_id: WriteId::new("origin", 3, 1),
        shard_id: 2,
        key: b"key".to_vec(),
        expected: None,
        value: b"value".to_vec(),
        ttl: Some(Duration::new(1, 1)),
        // A real epoch so the trailing-field truncation sweep below also covers
        // truncations INSIDE the epoch (counter bytes + node-id length prefix):
        // every prefix shorter than the full frame must decode to a clean Err.
        epoch: Ballot::new(9, SyncNodeId::new("owner")),
        seq: 7,
        tombstone: false,
    });
    let ack = SyncMessage::WriteAck(WriteAck {
        write_id: WriteId::new("origin", 3, 1),
        acker: SyncNodeId::new("acker"),
        acker_creation: 2,
        outcome: AckOutcome::Rejected(RejectReason::CasMismatch),
    });

    for message in [proposal, ack] {
        let payload = encode_sync_message(&message)?;
        // Every non-empty truncation must be a clean Err, never a panic.
        for len in 0..payload.len() {
            assert!(decode_sync_message(&payload[..len]).is_err());
        }
        // Trailing garbage must also be rejected by the finish() check.
        let mut extended = payload.clone();
        extended.push(0xFF);
        assert!(decode_sync_message(&extended).is_err());
    }
    Ok(())
}

#[test]
fn denormalized_duration_nanos_decode_to_error() -> Result<(), Box<dyn std::error::Error>> {
    // Bottom epoch trails the ttl on the wire: counter(8) + empty-node
    // len-prefix(8) = 16 bytes, followed by the seq (8 bytes). The nanos field is
    // therefore 16+8+4 from the end.
    const EPOCH_BOTTOM_WIRE_LEN: usize = 8 + 8;
    const SEQ_WIRE_LEN: usize = 8;
    // AA-3-4b adds a 1-byte tombstone flag trailing the seq.
    const TOMBSTONE_WIRE_LEN: usize = 1;
    // origin name len(8) + "origin" + creation(4) + counter(8) =
    // write_id; then key, expected=None, value, ttl flag=1, secs, nanos, epoch.
    let message = SyncMessage::WriteProposal(WriteProposal {
        write_id: WriteId::new("origin", 0, 0),
        shard_id: 0,
        key: Vec::new(),
        expected: None,
        value: Vec::new(),
        ttl: Some(Duration::new(0, 0)),
        epoch: Ballot::bottom(),
        seq: 0,
        tombstone: false,
    });
    let mut payload = encode_sync_message(&message)?;
    // The subsec-nanos field sits just before the 16-byte trailing epoch, the
    // 8-byte trailing seq, and the 1-byte tombstone flag; force it out of range to
    // prove the decoder rejects a denormalized duration.
    let nanos_start = payload.len() - EPOCH_BOTTOM_WIRE_LEN - SEQ_WIRE_LEN - TOMBSTONE_WIRE_LEN - 4;
    payload[nanos_start..nanos_start + 4].copy_from_slice(&1_000_000_000_u32.to_be_bytes());
    assert!(matches!(
        decode_sync_message(&payload),
        Err(SyncError::InvalidMessage)
    ));
    Ok(())
}

#[test]
fn write_proposal_epoch_field_truncation_is_clean_error() -> Result<(), Box<dyn std::error::Error>>
{
    // AA-3-3: a WriteProposal carrying a real (non-bottom) epoch must round-trip,
    // and any truncation that lands INSIDE the trailing epoch field — its 8-byte
    // counter or its length-prefixed node id — must decode to a clean Err, never a
    // panic. We construct the frame, confirm the round-trip, then sweep every
    // truncation from the start of the epoch field to the end of the frame.
    let proposal = WriteProposal {
        write_id: WriteId::new("origin", 4, 11),
        shard_id: 3,
        key: b"shard-key".to_vec(),
        expected: None,
        value: b"v".to_vec(),
        ttl: None,
        epoch: Ballot::new(0x0102_0304_0506_0708, SyncNodeId::new("owner-node")),
        seq: 0x0a0b_0c0d_0e0f_1011,
        tombstone: false,
    };
    let message = SyncMessage::WriteProposal(proposal);
    assert_message_round_trips(&message)?;

    let full = encode_sync_message(&message)?;
    // Wire length of the trailing epoch+seq fields: counter(8) + len-prefix(8) +
    // node bytes + seq(8). Every truncation that cuts into (or just before) the
    // epoch field OR the trailing seq is an Err.
    let epoch_wire_len = 8 + 8 + "owner-node".len();
    let trailing_wire_len = epoch_wire_len + 8;
    let epoch_start = full.len() - trailing_wire_len;
    for len in epoch_start..full.len() {
        assert!(
            decode_sync_message(&full[..len]).is_err(),
            "truncation at {len} inside the epoch/seq fields must be a clean Err"
        );
    }
    Ok(())
}

#[test]
fn unknown_message_tag_is_rejected() {
    // protocol version byte (1) then an unknown message tag.
    let payload = [1_u8, 99];
    assert!(matches!(
        decode_sync_message(&payload),
        Err(SyncError::InvalidMessage)
    ));
}
