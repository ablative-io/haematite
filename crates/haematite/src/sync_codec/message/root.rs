//! Root-exchange and pull-request message types plus the per-shard sync plan.

use crate::ids::ShardId;
use crate::sync_codec::error::SyncError;
use crate::tree::Hash;

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
