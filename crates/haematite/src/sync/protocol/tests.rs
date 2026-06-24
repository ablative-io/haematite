use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::time::Duration;

use super::*;
use crate::store::MemoryStore;
use crate::sync::ballot::Ballot;
use crate::tree::{InternalNode, LeafNode};

#[derive(Debug)]
struct CountingStore<'a> {
    inner: &'a MemoryStore,
    reads: Cell<usize>,
}

impl<'a> CountingStore<'a> {
    const fn new(inner: &'a MemoryStore) -> Self {
        Self {
            inner,
            reads: Cell::new(0),
        }
    }

    fn reads(&self) -> usize {
        self.reads.get()
    }
}

impl NodeStore for CountingStore<'_> {
    type Error = Infallible;

    fn get(&self, hash: &Hash) -> Result<Option<Node>, Self::Error> {
        self.reads.set(self.reads.get().saturating_add(1));
        Ok(self.inner.get(hash))
    }

    fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
        Ok(node.hash())
    }
}

#[derive(Debug, Default)]
struct RemoteTargetReader {
    summaries: BTreeMap<Hash, TargetNodeSummary>,
    reads: Cell<usize>,
}

impl RemoteTargetReader {
    fn insert(&mut self, hash: Hash, node: &Node) {
        self.summaries
            .insert(hash, TargetNodeSummary::from_node(node));
    }
}

impl TargetNodeReader for RemoteTargetReader {
    fn read_target_node(&self, hash: Hash) -> Result<Option<TargetNodeSummary>, SyncError> {
        self.reads.set(self.reads.get().saturating_add(1));
        Ok(self.summaries.get(&hash).cloned())
    }
}

fn leaf(key: &[u8], value: &[u8]) -> Result<Node, Box<dyn std::error::Error>> {
    Ok(Node::Leaf(LeafNode::new(vec![(
        key.to_vec(),
        value.to_vec(),
    )])?))
}

fn store_node(store: &mut MemoryStore, node: &Node) -> Hash {
    store.put(node)
}

fn separator(index: usize) -> Vec<u8> {
    format!("k{index:03}").into_bytes()
}

#[test]
fn identical_roots_short_circuit_without_tree_walk() -> Result<(), Box<dyn std::error::Error>> {
    let root_node = leaf(b"a", b"one")?;
    let root = root_node.hash();
    let plan = plan_sync(5, Some(root), Some(root));

    assert_eq!(plan.exchange.shard_id, 5);
    assert_eq!(plan.exchange.decision, SyncDecision::AlreadySynced);
    assert!(!plan.requires_tree_walk());
    assert_eq!(plan.stats.root_hashes_exchanged, 1);
    assert_eq!(plan.stats.tree_walks, 0);
    assert_eq!(plan.stats.nodes_transferred, 0);
    Ok(())
}

#[test]
fn differing_roots_trigger_tree_walk() -> Result<(), Box<dyn std::error::Error>> {
    let source_root = leaf(b"a", b"one")?.hash();
    let target_root = leaf(b"a", b"two")?.hash();
    let plan = plan_sync(5, Some(source_root), Some(target_root));

    assert_eq!(plan.exchange.decision, SyncDecision::WalkTrees);
    assert!(plan.requires_tree_walk());
    assert_eq!(plan.stats.tree_walks, 1);
    assert_eq!(plan.stats.nodes_transferred, 0);
    Ok(())
}

#[test]
fn tree_walk_skips_matching_children_and_transfers_missing_nodes_post_order()
-> Result<(), Box<dyn std::error::Error>> {
    let shared_left = leaf(b"a", b"shared-left")?;
    let shared_right = leaf(b"z", b"shared-right")?;
    let source_middle = leaf(b"m", b"source")?;
    let target_middle = leaf(b"m", b"target")?;

    let mut source_store = MemoryStore::new();
    let mut target_store = MemoryStore::new();

    let shared_left_hash = store_node(&mut source_store, &shared_left);
    store_node(&mut target_store, &shared_left);
    let shared_right_hash = store_node(&mut source_store, &shared_right);
    store_node(&mut target_store, &shared_right);
    let source_middle_hash = store_node(&mut source_store, &source_middle);
    let target_middle_hash = store_node(&mut target_store, &target_middle);

    let source_root_node = Node::Internal(InternalNode::new(vec![
        (b"".to_vec(), shared_left_hash),
        (b"m".to_vec(), source_middle_hash),
        (b"z".to_vec(), shared_right_hash),
    ])?);
    let target_root_node = Node::Internal(InternalNode::new(vec![
        (b"".to_vec(), shared_left_hash),
        (b"m".to_vec(), target_middle_hash),
        (b"z".to_vec(), shared_right_hash),
    ])?);
    let source_root = store_node(&mut source_store, &source_root_node);
    let target_root = store_node(&mut target_store, &target_root_node);

    let counted_source = CountingStore::new(&source_store);
    let missing = find_missing_nodes(
        &counted_source,
        &target_store,
        5,
        Some(source_root),
        Some(target_root),
    )?;

    let transfer_hashes: Vec<_> = missing
        .transfers
        .iter()
        .map(|transfer| transfer.hash)
        .collect();
    assert_eq!(transfer_hashes, vec![source_middle_hash, source_root]);
    assert!(!transfer_hashes.contains(&shared_left_hash));
    assert!(!transfer_hashes.contains(&shared_right_hash));
    assert_eq!(missing.stats.nodes_transferred, 2);
    assert_eq!(missing.stats.matching_subtrees_skipped, 2);
    assert_eq!(counted_source.reads(), 2);
    Ok(())
}

#[test]
fn mostly_shared_trees_transfer_only_differing_leaf_and_new_root()
-> Result<(), Box<dyn std::error::Error>> {
    let mut source_store = MemoryStore::new();
    let mut target_store = MemoryStore::new();
    let mut source_children = Vec::new();
    let mut target_children = Vec::new();
    let mut shared_hashes = Vec::new();

    for index in 0..100 {
        let key = separator(index);
        if index == 42 {
            let source_leaf = leaf(&key, b"source")?;
            let target_leaf = leaf(&key, b"target")?;
            let source_hash = store_node(&mut source_store, &source_leaf);
            let target_hash = store_node(&mut target_store, &target_leaf);
            source_children.push((key.clone(), source_hash));
            target_children.push((key, target_hash));
        } else {
            let shared_leaf = leaf(&key, b"shared")?;
            let shared_hash = store_node(&mut source_store, &shared_leaf);
            store_node(&mut target_store, &shared_leaf);
            source_children.push((key.clone(), shared_hash));
            target_children.push((key, shared_hash));
            shared_hashes.push(shared_hash);
        }
    }

    let source_root_node = Node::Internal(InternalNode::new(source_children)?);
    let target_root_node = Node::Internal(InternalNode::new(target_children)?);
    let source_root = store_node(&mut source_store, &source_root_node);
    let target_root = store_node(&mut target_store, &target_root_node);

    let missing = find_missing_nodes(
        &source_store,
        &target_store,
        5,
        Some(source_root),
        Some(target_root),
    )?;
    let transferred: BTreeSet<_> = missing
        .transfers
        .iter()
        .map(|transfer| transfer.hash)
        .collect();

    assert_eq!(missing.stats.nodes_transferred, 2);
    assert_eq!(missing.stats.matching_subtrees_skipped, 99);
    assert!(transferred.contains(&source_root));
    assert!(shared_hashes.iter().all(|hash| !transferred.contains(hash)));
    Ok(())
}

#[test]
fn tree_walk_can_use_remote_target_node_summaries() -> Result<(), Box<dyn std::error::Error>> {
    let shared = leaf(b"a", b"shared")?;
    let source_only = leaf(b"z", b"source")?;
    let target_only = leaf(b"z", b"target")?;

    let mut source_store = MemoryStore::new();
    let mut target_store = MemoryStore::new();
    let shared_hash = store_node(&mut source_store, &shared);
    store_node(&mut target_store, &shared);
    let source_only_hash = store_node(&mut source_store, &source_only);
    let target_only_hash = store_node(&mut target_store, &target_only);

    let source_root_node = Node::Internal(InternalNode::new(vec![
        (b"".to_vec(), shared_hash),
        (b"z".to_vec(), source_only_hash),
    ])?);
    let target_root_node = Node::Internal(InternalNode::new(vec![
        (b"".to_vec(), shared_hash),
        (b"z".to_vec(), target_only_hash),
    ])?);
    let source_root = store_node(&mut source_store, &source_root_node);
    let target_root = store_node(&mut target_store, &target_root_node);

    let mut remote_target = RemoteTargetReader::default();
    remote_target.insert(target_root, &target_root_node);
    remote_target.insert(target_only_hash, &target_only);

    let missing = find_missing_nodes(
        &source_store,
        &remote_target,
        5,
        Some(source_root),
        Some(target_root),
    )?;
    let transfer_hashes: Vec<_> = missing
        .transfers
        .iter()
        .map(|transfer| transfer.hash)
        .collect();

    assert_eq!(transfer_hashes, vec![source_only_hash, source_root]);
    assert_eq!(missing.stats.nodes_transferred, 2);
    assert!(remote_target.reads.get() > 0);
    Ok(())
}

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

fn sample_hash(key: &[u8], value: &[u8]) -> Result<Hash, Box<dyn std::error::Error>> {
    Ok(leaf(key, value)?.hash())
}

fn assert_message_round_trips(message: &SyncMessage) -> Result<(), Box<dyn std::error::Error>> {
    let payload = encode_sync_message(message)?;
    assert_eq!(&decode_sync_message(&payload)?, message);

    let frame = encode_beamr_sync_frame(message)?;
    assert_eq!(&decode_beamr_sync_frame(&frame)?, message);
    Ok(())
}

#[test]
fn write_proposal_round_trips_across_field_variations()
-> Result<(), Box<dyn std::error::Error>> {
    let expected = sample_hash(b"prev", b"old")?;
    let write_id = WriteId::new("node-origin-name", 7, 42);

    let proposals = vec![
        // empty value, no precondition, no ttl
        WriteProposal {
            write_id: write_id.clone(),
            key: b"k".to_vec(),
            expected: None,
            value: Vec::new(),
            ttl: None,
        },
        // expected Some + ttl Some + multi-byte node name already in write_id
        WriteProposal {
            write_id: write_id.clone(),
            key: b"another/key".to_vec(),
            expected: Some(expected),
            value: b"hello world".to_vec(),
            ttl: Some(Duration::new(12, 345)),
        },
        // large value
        WriteProposal {
            write_id,
            key: Vec::new(),
            expected: Some(expected),
            value: vec![0xAB; 64 * 1024],
            ttl: Some(Duration::from_secs(3600)),
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
fn truncated_write_messages_decode_to_clean_error() -> Result<(), Box<dyn std::error::Error>> {
    let proposal = SyncMessage::WriteProposal(WriteProposal {
        write_id: WriteId::new("origin", 3, 1),
        key: b"key".to_vec(),
        expected: None,
        value: b"value".to_vec(),
        ttl: Some(Duration::new(1, 1)),
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
    // origin name len(8) + "origin" + creation(4) + counter(8) =
    // write_id; then key, expected=None, value, ttl flag=1, secs, nanos.
    let message = SyncMessage::WriteProposal(WriteProposal {
        write_id: WriteId::new("origin", 0, 0),
        key: Vec::new(),
        expected: None,
        value: Vec::new(),
        ttl: Some(Duration::new(0, 0)),
    });
    let mut payload = encode_sync_message(&message)?;
    // The last 4 bytes are the subsec-nanos field; force them out of range.
    let nanos_start = payload.len() - 4;
    payload[nanos_start..].copy_from_slice(&1_000_000_000_u32.to_be_bytes());
    assert!(matches!(
        decode_sync_message(&payload),
        Err(SyncError::InvalidMessage)
    ));
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

fn ballot(counter: u64, node: &str) -> Ballot {
    Ballot::new(counter, SyncNodeId::new(node))
}

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
fn promise_round_trips_with_and_without_options()
-> Result<(), Box<dyn std::error::Error>> {
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
fn ballot_round_trips_for_multibyte_and_empty_node()
-> Result<(), Box<dyn std::error::Error>> {
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
fn truncated_election_messages_decode_to_clean_error()
-> Result<(), Box<dyn std::error::Error>> {
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
fn election_ballot_node_length_overflow_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
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
fn election_optional_ballot_bad_presence_tag_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
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
fn election_messages_round_trip_through_beamr_frame()
-> Result<(), Box<dyn std::error::Error>> {
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
