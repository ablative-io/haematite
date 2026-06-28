//! Election (Prepare/Promise/Nack) + shard-sync codec round-trip and truncation
//! tests.

use crate::sync_codec::ballot::Ballot;
use crate::sync_codec::error::SyncError;
use crate::sync_codec::ids::SyncNodeId;
use crate::sync_codec::message::{Nack, Prepare, Promise, ShardSyncRequest};
use crate::sync_codec::wire::{
    SyncMessage, decode_beamr_sync_frame, decode_sync_message, encode_beamr_sync_frame,
    encode_sync_message,
};

use super::{assert_message_round_trips, ballot, sample_hash};

#[test]
fn prepare_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    let messages = [
        SyncMessage::Prepare(Prepare {
            shard_id: 0,
            ballot: Ballot::bottom(),
        }),
        SyncMessage::Prepare(Prepare {
            shard_id: usize::MAX,
            ballot: ballot(7, "node-\u{00e9}-multi-byte"),
        }),
    ];
    for message in &messages {
        assert_message_round_trips(message)?;
    }
    Ok(())
}

#[test]
fn promise_round_trips_with_and_without_options() -> Result<(), Box<dyn std::error::Error>> {
    let root = sample_hash(b"committed", b"root")?;
    let messages = [
        // both options absent
        SyncMessage::Promise(Promise {
            shard_id: 3,
            ballot: ballot(2, "node-a"),
            promiser: SyncNodeId::from("voter-a"),
            accepted_epoch: None,
            committed_root: None,
        }),
        // accepted_epoch present, committed_root absent
        SyncMessage::Promise(Promise {
            shard_id: 3,
            ballot: ballot(9, "node-b"),
            promiser: SyncNodeId::from("voter-\u{1f600}"),
            accepted_epoch: Some(ballot(4, "prior-owner")),
            committed_root: None,
        }),
        // committed_root present, accepted_epoch absent
        SyncMessage::Promise(Promise {
            shard_id: 3,
            ballot: ballot(9, "node-b"),
            promiser: SyncNodeId::from("voter-c"),
            accepted_epoch: None,
            committed_root: Some(root),
        }),
        // both present
        SyncMessage::Promise(Promise {
            shard_id: 1,
            ballot: ballot(11, "node-c"),
            promiser: SyncNodeId::from(""),
            accepted_epoch: Some(ballot(10, "prior-\u{1f600}")),
            committed_root: Some(root),
        }),
    ];
    for message in &messages {
        assert_message_round_trips(message)?;
    }
    Ok(())
}

#[test]
fn nack_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    let messages = [
        SyncMessage::Nack(Nack {
            shard_id: 0,
            promised: Ballot::bottom(),
        }),
        SyncMessage::Nack(Nack {
            shard_id: 42,
            promised: ballot(99, "higher-ballot-owner"),
        }),
    ];
    for message in &messages {
        assert_message_round_trips(message)?;
    }
    Ok(())
}

#[test]
fn shard_sync_request_round_trips_with_and_without_root() -> Result<(), Box<dyn std::error::Error>>
{
    let root = sample_hash(b"catch", b"up")?;
    let messages = [
        // No from_root (source has no committed data).
        SyncMessage::ShardSyncRequest(ShardSyncRequest::new(
            0,
            SyncNodeId::from("requester-a"),
            None,
        )),
        // from_root present, multibyte requester, max shard id.
        SyncMessage::ShardSyncRequest(ShardSyncRequest::new(
            usize::MAX,
            SyncNodeId::from("requester-\u{1f600}"),
            Some(root),
        )),
        // Empty requester id (boundary on the length-prefixed string).
        SyncMessage::ShardSyncRequest(ShardSyncRequest::new(7, SyncNodeId::from(""), Some(root))),
    ];
    for message in &messages {
        assert_message_round_trips(message)?;
    }
    Ok(())
}

#[test]
fn ballot_round_trips_for_multibyte_and_empty_node() -> Result<(), Box<dyn std::error::Error>> {
    // Multi-byte UTF-8 node id and the empty-string bottom ballot both survive
    // a Prepare round-trip (the ballot is exercised inside the message codec).
    let messages = [
        SyncMessage::Prepare(Prepare {
            shard_id: 5,
            ballot: ballot(u64::MAX, "\u{00e9}\u{1f600}\u{4e2d}\u{6587}"),
        }),
        SyncMessage::Prepare(Prepare {
            shard_id: 5,
            ballot: Ballot::bottom(),
        }),
        SyncMessage::Prepare(Prepare {
            shard_id: 5,
            ballot: ballot(1, ""),
        }),
    ];
    for message in &messages {
        assert_message_round_trips(message)?;
    }
    Ok(())
}

#[test]
fn truncated_election_messages_decode_to_clean_error() -> Result<(), Box<dyn std::error::Error>> {
    let root = sample_hash(b"c", b"r")?;
    let messages = [
        // Prepare: cuts land inside shard, inside ballot counter, inside the
        // node-length prefix, and inside the node bytes.
        SyncMessage::Prepare(Prepare {
            shard_id: 7,
            ballot: ballot(0x0102_0304_0506_0708, "node-name"),
        }),
        // Promise with both options present: extra cut points across the
        // option presence tags, the inner ballot, and the hash.
        SyncMessage::Promise(Promise {
            shard_id: 7,
            ballot: ballot(5, "owner"),
            promiser: SyncNodeId::from("voter"),
            accepted_epoch: Some(ballot(4, "prior")),
            committed_root: Some(root),
        }),
        SyncMessage::Nack(Nack {
            shard_id: 7,
            promised: ballot(3, "promised-node"),
        }),
    ];
    for message in messages {
        let payload = encode_sync_message(&message)?;
        // Every non-empty truncation must be a clean Err, never a panic. This
        // sweeps a cut through every field boundary including inside the ballot
        // counter, the node-length prefix, and the node bytes.
        for len in 0..payload.len() {
            assert!(matches!(
                decode_sync_message(&payload[..len]),
                Err(SyncError::InvalidMessage)
            ));
        }
        // Trailing garbage is rejected by the finish() check.
        let mut extended = payload.clone();
        extended.push(0xFF);
        assert!(matches!(
            decode_sync_message(&extended),
            Err(SyncError::InvalidMessage)
        ));
    }
    Ok(())
}

#[test]
fn election_ballot_node_length_overflow_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    // A node-length prefix claiming more bytes than remain must error (the DoS
    // guard), never over-allocate or panic. Encode a Prepare, then overwrite the
    // 8-byte node-length prefix (which follows the version+tag+shard+counter)
    // with a huge value.
    let message = SyncMessage::Prepare(Prepare {
        shard_id: 1,
        ballot: ballot(1, "n"),
    });
    let mut payload = encode_sync_message(&message)?;
    // layout: version(1) tag(1) shard(4) counter(8) node_len(8) node_bytes...
    let node_len_start = 1 + 1 + 4 + 8;
    payload[node_len_start..node_len_start + 8].copy_from_slice(&u64::MAX.to_be_bytes());
    assert!(matches!(
        decode_sync_message(&payload),
        Err(SyncError::InvalidMessage)
    ));
    Ok(())
}

#[test]
fn election_optional_ballot_bad_presence_tag_is_rejected() -> Result<(), Box<dyn std::error::Error>>
{
    // The accepted_epoch presence tag must be 0 or 1; anything else is a clean
    // error rather than a misread.
    let message = SyncMessage::Promise(Promise {
        shard_id: 1,
        ballot: ballot(1, "n"),
        promiser: SyncNodeId::from(""),
        accepted_epoch: None,
        committed_root: None,
    });
    let mut payload = encode_sync_message(&message)?;
    // layout: version(1) tag(1) shard(4) counter(8) node_len(8) "n"(1)
    //         promiser_len(8) promiser(0)
    //         accepted_epoch_tag(1) committed_root_tag(1)
    let accepted_tag = 1 + 1 + 4 + 8 + 8 + 1 + 8;
    payload[accepted_tag] = 2;
    assert!(matches!(
        decode_sync_message(&payload),
        Err(SyncError::InvalidMessage)
    ));
    Ok(())
}

#[test]
fn election_messages_round_trip_through_beamr_frame() -> Result<(), Box<dyn std::error::Error>> {
    let root = sample_hash(b"c", b"r")?;
    let messages = [
        SyncMessage::Prepare(Prepare {
            shard_id: 2,
            ballot: ballot(8, "node-a"),
        }),
        SyncMessage::Promise(Promise {
            shard_id: 2,
            ballot: ballot(8, "node-a"),
            promiser: SyncNodeId::from("node-b"),
            accepted_epoch: Some(ballot(7, "node-b")),
            committed_root: Some(root),
        }),
        SyncMessage::Nack(Nack {
            shard_id: 2,
            promised: ballot(9, "node-c"),
        }),
    ];
    for message in &messages {
        let frame = encode_beamr_sync_frame(message)?;
        assert_eq!(&decode_beamr_sync_frame(&frame)?, message);
    }
    Ok(())
}
