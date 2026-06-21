use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;

use super::*;
use crate::store::MemoryStore;
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
