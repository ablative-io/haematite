use std::collections::BTreeSet;

use crate::branch::ShardId;
use crate::store::NodeStore;
use crate::tree::{Hash, Node};

#[path = "protocol/wire.rs"]
mod wire;

#[path = "protocol/error.rs"]
mod error;

#[path = "protocol/target.rs"]
mod target;

#[cfg(test)]
#[path = "protocol/tests.rs"]
mod tests;

pub use error::SyncError;
pub use target::{TargetNodeReader, TargetNodeRequest, TargetNodeResponse, TargetNodeSummary};
pub use wire::{
    SyncMessage, decode_beamr_sync_frame, decode_sync_message, encode_beamr_sync_frame,
    encode_sync_message, register_beamr_sync_handler, send_pull_request_via_beamr,
    send_push_response_via_beamr, send_root_exchange_request_via_beamr,
    send_root_exchange_response_via_beamr, send_sync_message_via_beamr,
    send_target_node_request_via_beamr, send_target_node_response_via_beamr,
};

/// Decision made by the root-hash exchange for one shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDecision {
    /// The source and target roots are identical, so no node transfer is needed.
    AlreadySynced,
    /// The roots differ and the protocol must walk the content-addressed trees.
    WalkTrees,
}

impl SyncDecision {
    #[must_use]
    pub const fn requires_tree_walk(self) -> bool {
        matches!(self, Self::WalkTrees)
    }

    pub(crate) const fn to_wire(self) -> u8 {
        match self {
            Self::AlreadySynced => 0,
            Self::WalkTrees => 1,
        }
    }

    pub(crate) const fn from_wire(value: u8) -> Result<Self, SyncError> {
        match value {
            0 => Ok(Self::AlreadySynced),
            1 => Ok(Self::WalkTrees),
            _ => Err(SyncError::InvalidMessage),
        }
    }
}

/// Statistics reported by one per-shard sync operation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SyncStats {
    /// Root-hash exchanges performed before any tree walk.
    pub root_hashes_exchanged: usize,
    /// Differential tree walks started because roots differed.
    pub tree_walks: usize,
    /// Source-store nodes read while discovering missing content.
    pub source_nodes_read: usize,
    /// Target-store probes performed while discovering missing content.
    pub target_nodes_checked: usize,
    /// Subtrees skipped because the source and target child hashes matched.
    pub matching_subtrees_skipped: usize,
    /// Subtrees skipped because the target already had the source hash.
    pub existing_subtrees_skipped: usize,
    /// Content-addressed nodes included in the transfer or applied locally.
    pub nodes_transferred: usize,
    /// Serialized bytes included in the transfer or applied locally.
    pub bytes_transferred: usize,
}

impl SyncStats {
    pub(crate) const fn record_transfer_bytes(&mut self, byte_len: usize) {
        self.nodes_transferred = self.nodes_transferred.saturating_add(1);
        self.bytes_transferred = self.bytes_transferred.saturating_add(byte_len);
    }

    #[must_use]
    pub(crate) const fn without_transfer_counts(self) -> Self {
        Self {
            root_hashes_exchanged: self.root_hashes_exchanged,
            tree_walks: self.tree_walks,
            source_nodes_read: self.source_nodes_read,
            target_nodes_checked: self.target_nodes_checked,
            matching_subtrees_skipped: self.matching_subtrees_skipped,
            existing_subtrees_skipped: self.existing_subtrees_skipped,
            nodes_transferred: 0,
            bytes_transferred: 0,
        }
    }
}

/// Result of exchanging roots between source and target for one shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RootExchange {
    pub shard_id: ShardId,
    pub source_root: Option<Hash>,
    pub target_root: Option<Hash>,
    pub decision: SyncDecision,
}

impl RootExchange {
    #[must_use]
    pub fn new(shard_id: ShardId, source_root: Option<Hash>, target_root: Option<Hash>) -> Self {
        let decision = if source_root == target_root {
            SyncDecision::AlreadySynced
        } else {
            SyncDecision::WalkTrees
        };

        Self {
            shard_id,
            source_root,
            target_root,
            decision,
        }
    }

    #[must_use]
    pub const fn requires_tree_walk(&self) -> bool {
        self.decision.requires_tree_walk()
    }
}

/// Initial sync plan after the source and target roots have been compared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncPlan {
    pub exchange: RootExchange,
    pub stats: SyncStats,
}

impl SyncPlan {
    #[must_use]
    pub const fn requires_tree_walk(&self) -> bool {
        self.exchange.requires_tree_walk()
    }
}

/// Create a per-shard plan by exchanging source and target root hashes.
#[must_use]
pub fn plan_sync(
    shard_id: ShardId,
    source_root: Option<Hash>,
    target_root: Option<Hash>,
) -> SyncPlan {
    let exchange = RootExchange::new(shard_id, source_root, target_root);
    let mut stats = SyncStats {
        root_hashes_exchanged: 1,
        ..SyncStats::default()
    };
    if exchange.requires_tree_walk() {
        stats.tree_walks = 1;
    }

    SyncPlan { exchange, stats }
}

/// A target-to-source root exchange request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RootExchangeRequest {
    pub shard_id: ShardId,
    pub target_root: Option<Hash>,
}

impl RootExchangeRequest {
    #[must_use]
    pub const fn new(shard_id: ShardId, target_root: Option<Hash>) -> Self {
        Self {
            shard_id,
            target_root,
        }
    }
}

/// A source-to-target root exchange response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RootExchangeResponse {
    pub shard_id: ShardId,
    pub source_root: Option<Hash>,
    pub target_root: Option<Hash>,
    pub decision: SyncDecision,
}

impl RootExchangeResponse {
    #[must_use]
    pub fn from_request(request: &RootExchangeRequest, source_root: Option<Hash>) -> Self {
        let exchange = RootExchange::new(request.shard_id, source_root, request.target_root);
        Self {
            shard_id: request.shard_id,
            source_root,
            target_root: request.target_root,
            decision: exchange.decision,
        }
    }
}

/// Target-initiated request for the source to send nodes missing from the target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PullRequest {
    pub shard_id: ShardId,
    pub target_root: Option<Hash>,
}

impl PullRequest {
    #[must_use]
    pub const fn new(shard_id: ShardId, target_root: Option<Hash>) -> Self {
        Self {
            shard_id,
            target_root,
        }
    }

    #[must_use]
    pub const fn root_exchange_request(self) -> RootExchangeRequest {
        RootExchangeRequest::new(self.shard_id, self.target_root)
    }
}

/// One content-addressed node to transfer from source to target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeTransfer {
    pub hash: Hash,
    pub node: Node,
}

impl NodeTransfer {
    #[must_use]
    pub fn new(node: Node) -> Self {
        Self {
            hash: node.hash(),
            node,
        }
    }

    pub fn from_parts(hash: Hash, node: Node) -> Result<Self, SyncError> {
        let actual = node.hash();
        if actual != hash {
            return Err(SyncError::HashMismatch {
                expected: hash,
                actual,
            });
        }
        Ok(Self { hash, node })
    }

    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.node.serialise().len()
    }
}

/// Source response containing the source root and exactly the nodes the target lacks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushResponse {
    pub shard_id: ShardId,
    pub source_root: Option<Hash>,
    pub target_root: Option<Hash>,
    pub transfers: Vec<NodeTransfer>,
    pub stats: SyncStats,
}

impl PushResponse {
    #[must_use]
    pub fn new(
        shard_id: ShardId,
        source_root: Option<Hash>,
        target_root: Option<Hash>,
        transfers: Vec<NodeTransfer>,
        mut stats: SyncStats,
    ) -> Self {
        stats.nodes_transferred = transfers.len();
        stats.bytes_transferred = transfers.iter().map(NodeTransfer::byte_len).sum();
        Self {
            shard_id,
            source_root,
            target_root,
            transfers,
            stats,
        }
    }
}

/// Missing-node discovery result for one shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingNodes {
    pub shard_id: ShardId,
    pub source_root: Option<Hash>,
    pub target_root: Option<Hash>,
    pub decision: SyncDecision,
    pub transfers: Vec<NodeTransfer>,
    pub stats: SyncStats,
}

/// Discover the source nodes missing from the target for one shard.
///
/// The result is ordered children-before-parents so a crash during pull never
/// leaves a newly visible source root without the descendants already written.
pub fn find_missing_nodes<S, T>(
    source_store: &S,
    target_store: &T,
    shard_id: ShardId,
    source_root: Option<Hash>,
    target_root: Option<Hash>,
) -> Result<MissingNodes, SyncError>
where
    S: NodeStore + ?Sized,
    T: TargetNodeReader + ?Sized,
{
    let plan = plan_sync(shard_id, source_root, target_root);
    let mut stats = plan.stats;
    let mut transfers = Vec::new();

    if !plan.requires_tree_walk() {
        return Ok(MissingNodes {
            shard_id,
            source_root,
            target_root,
            decision: plan.exchange.decision,
            transfers,
            stats,
        });
    }

    if let Some(source_hash) = source_root {
        let mut visited = BTreeSet::new();
        collect_missing_node(
            source_store,
            target_store,
            source_hash,
            target_root,
            &mut transfers,
            &mut visited,
            &mut stats,
        )?;
    }

    stats.nodes_transferred = transfers.len();
    stats.bytes_transferred = transfers.iter().map(NodeTransfer::byte_len).sum();

    Ok(MissingNodes {
        shard_id,
        source_root,
        target_root,
        decision: plan.exchange.decision,
        transfers,
        stats,
    })
}

fn collect_missing_node<S, T>(
    source_store: &S,
    target_store: &T,
    source_hash: Hash,
    target_hash: Option<Hash>,
    transfers: &mut Vec<NodeTransfer>,
    visited: &mut BTreeSet<Hash>,
    stats: &mut SyncStats,
) -> Result<(), SyncError>
where
    S: NodeStore + ?Sized,
    T: TargetNodeReader + ?Sized,
{
    if target_hash == Some(source_hash) {
        stats.matching_subtrees_skipped = stats.matching_subtrees_skipped.saturating_add(1);
        return Ok(());
    }

    if !visited.insert(source_hash) {
        return Ok(());
    }

    stats.target_nodes_checked = stats.target_nodes_checked.saturating_add(1);
    if target_store.read_target_node(source_hash)?.is_some() {
        stats.existing_subtrees_skipped = stats.existing_subtrees_skipped.saturating_add(1);
        return Ok(());
    }

    stats.source_nodes_read = stats.source_nodes_read.saturating_add(1);
    let source_node = source_store
        .get(&source_hash)
        .map_err(|_error| SyncError::SourceStoreRead { hash: source_hash })?
        .ok_or(SyncError::MissingSourceNode { hash: source_hash })?;
    let actual_hash = source_node.hash();
    if actual_hash != source_hash {
        return Err(SyncError::HashMismatch {
            expected: source_hash,
            actual: actual_hash,
        });
    }

    let target_node = match target_hash {
        Some(hash) => {
            stats.target_nodes_checked = stats.target_nodes_checked.saturating_add(1);
            target_store.read_target_node(hash)?
        }
        None => None,
    };

    if let Node::Internal(internal) = &source_node {
        for (separator, child_hash) in internal.children() {
            let target_child_hash = target_node
                .as_ref()
                .and_then(|node| node.child_hash(separator.as_slice()));
            collect_missing_node(
                source_store,
                target_store,
                *child_hash,
                target_child_hash,
                transfers,
                visited,
                stats,
            )?;
        }
    }

    let transfer = NodeTransfer::from_parts(source_hash, source_node)?;
    stats.record_transfer_bytes(transfer.byte_len());
    transfers.push(transfer);
    Ok(())
}
