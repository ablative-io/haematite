use crate::branch::ShardId;
use crate::store::NodeStore;
use crate::tree::{Hash, Node};

use super::SyncError;

/// Minimal target-side node information needed for a source to prune a tree walk.
///
/// The source never needs target values while discovering source nodes missing
/// from the target. Leaf presence is enough for equality/containment checks, and
/// internal summaries expose only child separator hashes for the simultaneous
/// walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetNodeSummary {
    Leaf,
    Internal(Vec<(Vec<u8>, Hash)>),
}

impl TargetNodeSummary {
    #[must_use]
    pub fn from_node(node: &Node) -> Self {
        match node {
            Node::Leaf(_) => Self::Leaf,
            Node::Internal(internal) => Self::Internal(internal.children().to_vec()),
        }
    }

    #[must_use]
    pub fn child_hash(&self, separator: &[u8]) -> Option<Hash> {
        let Self::Internal(children) = self else {
            return None;
        };

        children
            .iter()
            .find(|(target_separator, _hash)| target_separator.as_slice() == separator)
            .map(|(_separator, hash)| *hash)
    }
}

/// Source-to-target request for the target node summary needed during a remote walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetNodeRequest {
    pub shard_id: ShardId,
    pub hash: Hash,
}

impl TargetNodeRequest {
    #[must_use]
    pub const fn new(shard_id: ShardId, hash: Hash) -> Self {
        Self { shard_id, hash }
    }
}

/// Target-to-source response containing only structural information for a target node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetNodeResponse {
    pub shard_id: ShardId,
    pub hash: Hash,
    pub summary: Option<TargetNodeSummary>,
}

impl TargetNodeResponse {
    #[must_use]
    pub const fn missing(shard_id: ShardId, hash: Hash) -> Self {
        Self {
            shard_id,
            hash,
            summary: None,
        }
    }

    #[must_use]
    pub fn present(shard_id: ShardId, hash: Hash, node: &Node) -> Self {
        Self {
            shard_id,
            hash,
            summary: Some(TargetNodeSummary::from_node(node)),
        }
    }

    pub fn from_store<T>(request: TargetNodeRequest, target_store: &T) -> Result<Self, SyncError>
    where
        T: NodeStore + ?Sized,
    {
        let node = target_store
            .get(&request.hash)
            .map_err(|_error| SyncError::TargetStoreRead { hash: request.hash })?;
        Ok(node.map_or_else(
            || Self::missing(request.shard_id, request.hash),
            |node| Self::present(request.shard_id, request.hash, &node),
        ))
    }
}

/// Abstraction over target-node knowledge used by source-side missing-node discovery.
///
/// A local `NodeStore` implements this trait directly for deterministic tests and
/// same-process sync. A remote implementation can satisfy each read by sending a
/// [`TargetNodeRequest`] over beamr and returning the corresponding
/// [`TargetNodeResponse`], so the source does not need to own or track target
/// state.
pub trait TargetNodeReader {
    fn read_target_node(&self, hash: Hash) -> Result<Option<TargetNodeSummary>, SyncError>;
}

impl<T> TargetNodeReader for T
where
    T: NodeStore + ?Sized,
{
    fn read_target_node(&self, hash: Hash) -> Result<Option<TargetNodeSummary>, SyncError> {
        self.get(&hash)
            .map_err(|_error| SyncError::TargetStoreRead { hash })
            .map(|node| node.as_ref().map(TargetNodeSummary::from_node))
    }
}
