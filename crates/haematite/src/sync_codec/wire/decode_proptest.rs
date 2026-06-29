//! Property-based tests for the sync wire codec's DECODER.
//!
//! `decode_sync_message` / `decode_beamr_sync_frame` parse attacker- and
//! peer-controlled bytes, so the two load-bearing properties are:
//!
//! 1. **Stable round-trip.** For every well-formed [`SyncMessage`] we generate,
//!    `decode(encode(x))` reconstructs a message that re-encodes to the IDENTICAL
//!    bytes — and, for the message shapes that are fully value-comparable, to the
//!    IDENTICAL message. This pins the encoder/decoder as exact inverses over
//!    arbitrary `WriteProposal` / `BatchWriteProposal` / election (`Prepare` /
//!    `Promise` / `Nack`) payloads.
//! 2. **No panic on hostile bytes.** `decode_sync_message` and
//!    `decode_beamr_sync_frame` over ARBITRARY random byte strings, and over
//!    TRUNCATIONS of valid frames, must NEVER panic — they always return `Ok` or
//!    `Err`. The crate-wide `panic = deny` lint already forbids an explicit panic
//!    in the codec; this proves the cursor's bounds checks actually hold under
//!    fuzzed lengths (the hostile-`count` pre-allocation clamps, truncated
//!    len-prefixes, denormalised durations, out-of-range tag bytes).

#![allow(clippy::unwrap_used)]
#![allow(clippy::panic)]

use std::time::Duration;

use proptest::prelude::*;

use super::{
    SyncMessage, decode_beamr_sync_frame, decode_sync_message, encode_beamr_sync_frame,
    encode_sync_message,
};
use crate::sync_codec::ballot::{Ballot, Stamp};
use crate::sync_codec::ids::SyncNodeId;
use crate::sync_codec::message::{
    AckOutcome, BatchWriteEntry, BatchWriteProposal, Nack, Prepare, Promise, RejectReason,
    WriteAck, WriteId, WriteProposal,
};
use crate::tree::Hash;

fn arb_node_id() -> impl Strategy<Value = SyncNodeId> {
    "[a-z0-9@._-]{0,16}".prop_map(SyncNodeId::new)
}

fn arb_hash() -> impl Strategy<Value = Hash> {
    any::<[u8; 32]>().prop_map(Hash::from_bytes)
}

fn arb_optional_hash() -> impl Strategy<Value = Option<Hash>> {
    proptest::option::of(arb_hash())
}

fn arb_ballot() -> impl Strategy<Value = Ballot> {
    (any::<u64>(), arb_node_id()).prop_map(|(counter, node)| Ballot::new(counter, node))
}

fn arb_stamp() -> impl Strategy<Value = Stamp> {
    (arb_ballot(), any::<u64>()).prop_map(|(epoch, seq)| Stamp::new(epoch, seq))
}

fn arb_write_id() -> impl Strategy<Value = WriteId> {
    (arb_node_id(), any::<u32>(), any::<u64>())
        .prop_map(|(origin, creation, counter)| WriteId::new(origin, creation, counter))
}

fn arb_bytes() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..32)
}

/// A normalised (carry-free) `Duration` so re-encode is byte-stable: the codec
/// rejects denormalised sub-second nanos, so we only generate valid ones.
fn arb_duration() -> impl Strategy<Value = Option<Duration>> {
    proptest::option::of(
        (any::<u64>(), 0_u32..1_000_000_000).prop_map(|(s, n)| Duration::new(s, n)),
    )
}

fn arb_write_proposal() -> impl Strategy<Value = SyncMessage> {
    (
        arb_write_id(),
        any::<usize>(),
        arb_bytes(),
        arb_optional_hash(),
        arb_bytes(),
        arb_duration(),
        arb_ballot(),
        any::<u64>(),
        any::<bool>(),
    )
        .prop_map(
            |(write_id, shard_id, key, expected, value, ttl, epoch, seq, tombstone)| {
                SyncMessage::WriteProposal(WriteProposal {
                    write_id,
                    shard_id,
                    key,
                    expected,
                    value,
                    ttl,
                    epoch,
                    seq,
                    tombstone,
                })
            },
        )
}

fn arb_batch_entry() -> impl Strategy<Value = BatchWriteEntry> {
    (
        arb_bytes(),
        arb_optional_hash(),
        arb_bytes(),
        arb_duration(),
    )
        .prop_map(|(key, expected, value, ttl)| BatchWriteEntry {
            key,
            expected,
            value,
            ttl,
        })
}

fn arb_batch_proposal() -> impl Strategy<Value = SyncMessage> {
    (
        arb_write_id(),
        any::<usize>(),
        proptest::collection::vec(arb_batch_entry(), 0..6),
        arb_stamp(),
    )
        .prop_map(|(write_id, shard_id, entries, stamp)| {
            SyncMessage::BatchWriteProposal(BatchWriteProposal {
                write_id,
                shard_id,
                entries,
                stamp,
            })
        })
}

fn arb_prepare() -> impl Strategy<Value = SyncMessage> {
    (any::<usize>(), arb_ballot())
        .prop_map(|(shard_id, ballot)| SyncMessage::Prepare(Prepare { shard_id, ballot }))
}

fn arb_promise() -> impl Strategy<Value = SyncMessage> {
    (
        any::<usize>(),
        arb_ballot(),
        arb_node_id(),
        proptest::option::of(arb_ballot()),
        arb_optional_hash(),
    )
        .prop_map(
            |(shard_id, ballot, promiser, accepted_epoch, committed_root)| {
                SyncMessage::Promise(Promise {
                    shard_id,
                    ballot,
                    promiser,
                    accepted_epoch,
                    committed_root,
                })
            },
        )
}

fn arb_nack() -> impl Strategy<Value = SyncMessage> {
    (any::<usize>(), arb_ballot())
        .prop_map(|(shard_id, promised)| SyncMessage::Nack(Nack { shard_id, promised }))
}

fn arb_write_ack() -> impl Strategy<Value = SyncMessage> {
    (
        arb_write_id(),
        arb_node_id(),
        any::<u32>(),
        arb_ack_outcome(),
    )
        .prop_map(|(write_id, acker, acker_creation, outcome)| {
            SyncMessage::WriteAck(WriteAck {
                write_id,
                acker,
                acker_creation,
                outcome,
            })
        })
}

fn arb_ack_outcome() -> impl Strategy<Value = AckOutcome> {
    prop_oneof![
        Just(AckOutcome::Applied),
        Just(AckOutcome::Rejected(RejectReason::CasMismatch)),
        Just(AckOutcome::Rejected(RejectReason::ApplyError)),
        Just(AckOutcome::Rejected(RejectReason::Fenced)),
    ]
}

/// Any of the value-comparable message shapes the task names plus their acks.
fn arb_message() -> impl Strategy<Value = SyncMessage> {
    prop_oneof![
        arb_write_proposal(),
        arb_batch_proposal(),
        arb_prepare(),
        arb_promise(),
        arb_nack(),
        arb_write_ack(),
    ]
}

proptest! {
    /// STABLE ROUND-TRIP: every generated message decodes back to an IDENTICAL
    /// message and to byte-identical re-encoded output, for both the bare payload
    /// codec and the beamr control-frame wrapper.
    #[test]
    fn encode_decode_round_trips(message in arb_message()) {
        let payload = encode_sync_message(&message).unwrap();
        let decoded = decode_sync_message(&payload).unwrap();
        prop_assert_eq!(&decoded, &message, "decode(encode(x)) must reconstruct x");
        let reencoded = encode_sync_message(&decoded).unwrap();
        prop_assert_eq!(reencoded, payload, "re-encode must be byte-stable");

        // The beamr control-frame wrapper is an exact inverse too.
        let frame = encode_beamr_sync_frame(&message).unwrap();
        let from_frame = decode_beamr_sync_frame(&frame).unwrap();
        prop_assert_eq!(&from_frame, &message, "decode_frame(encode_frame(x)) == x");
    }

    /// NO PANIC + Err on TRUNCATIONS: any strict prefix of a valid frame/payload
    /// must be REJECTED (it cannot satisfy `cursor.finish()`), never panic. A
    /// proper prefix can never be a complete message, so decode must return Err.
    #[test]
    fn truncated_frames_are_rejected_without_panic(message in arb_message()) {
        let payload = encode_sync_message(&message).unwrap();
        for cut in 0..payload.len() {
            prop_assert!(
                decode_sync_message(&payload[..cut]).is_err(),
                "a truncated payload must be rejected, not decoded"
            );
        }
        let frame = encode_beamr_sync_frame(&message).unwrap();
        for cut in 0..frame.len() {
            // Must not panic; a strict prefix is never a complete frame.
            prop_assert!(decode_beamr_sync_frame(&frame[..cut]).is_err());
        }
    }

    /// NO PANIC on ARBITRARY bytes: decode of any random byte string returns Ok or
    /// Err, never panics (the cursor's bounds + clamp checks hold under fuzzing).
    #[test]
    fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        // We assert nothing about the verdict — only that neither entry point
        // panics on hostile input. A panic here would unwind out of the test.
        let _ = decode_sync_message(&bytes);
        let _ = decode_beamr_sync_frame(&bytes);
    }
}
