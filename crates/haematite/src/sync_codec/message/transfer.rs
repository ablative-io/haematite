//! Node-transfer and push-response message types.

use crate::ids::ShardId;
use crate::sync_codec::error::SyncError;
use crate::sync_codec::message::root::{SyncDecision, SyncStats};
use crate::tree::{Hash, Node};

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

    /// Construct from already-computed stats, trusting the caller's
    /// `nodes_transferred` / `bytes_transferred` instead of re-deriving them by
    /// re-serialising every node.
    ///
    /// The wire-decode path (`decode_push_response`) already knows each node's
    /// serialised length from the on-wire length prefix, so it sums those
    /// directly; this constructor avoids the redundant re-serialise that
    /// [`Self::new`] performs via [`NodeTransfer::byte_len`].
    #[must_use]
    pub const fn with_stats(
        shard_id: ShardId,
        source_root: Option<Hash>,
        target_root: Option<Hash>,
        transfers: Vec<NodeTransfer>,
        stats: SyncStats,
    ) -> Self {
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
