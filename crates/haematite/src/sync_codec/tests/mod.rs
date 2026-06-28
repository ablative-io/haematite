//! Parity tests for the platform-neutral sync codec.
//!
//! These run on BOTH the native host and (via `cargo test`) exercise the exact
//! encode/decode round-trip a wasm node performs, so the wire format stays
//! byte-identical across targets. Split by concern to stay within the per-file
//! line cap.

mod codec_election;
mod codec_write;
mod discovery;

use crate::sync_codec::ballot::Ballot;
use crate::sync_codec::ids::SyncNodeId;
use crate::sync_codec::wire::{
    SyncMessage, decode_beamr_sync_frame, decode_sync_message, encode_beamr_sync_frame,
    encode_sync_message,
};
use crate::tree::{Hash, LeafNode, Node};

/// Build a single-entry leaf node for fixtures.
fn leaf(key: &[u8], value: &[u8]) -> Result<Node, Box<dyn std::error::Error>> {
    Ok(Node::Leaf(LeafNode::new(vec![(
        key.to_vec(),
        value.to_vec(),
    )])?))
}

/// The content hash of a single-entry leaf, used as a CAS-precondition fixture.
fn sample_hash(key: &[u8], value: &[u8]) -> Result<Hash, Box<dyn std::error::Error>> {
    Ok(leaf(key, value)?.hash())
}

/// Build a ballot from a counter and node-id string.
fn ballot(counter: u64, node: &str) -> Ballot {
    Ballot::new(counter, SyncNodeId::new(node))
}

/// Assert a message survives both the raw payload codec and the full beamr frame
/// codec unchanged — the round-trip a wasm node performs.
fn assert_message_round_trips(message: &SyncMessage) -> Result<(), Box<dyn std::error::Error>> {
    let payload = encode_sync_message(message)?;
    assert_eq!(&decode_sync_message(&payload)?, message);

    let frame = encode_beamr_sync_frame(message)?;
    assert_eq!(&decode_beamr_sync_frame(&frame)?, message);
    Ok(())
}
