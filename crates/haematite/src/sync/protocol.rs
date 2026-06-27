use std::collections::BTreeSet;
use std::time::Duration;

use crate::api::kv::{KvKey, KvValue};
use crate::branch::ShardId;
use crate::store::NodeStore;
use crate::sync::SyncNodeId;
use crate::sync::ballot::{Ballot, Stamp};
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
    encode_sync_message, register_beamr_sync_handler, send_batch_write_ack_via_beamr,
    send_batch_write_proposal_via_beamr, send_nack_via_beamr, send_prepare_via_beamr,
    send_promise_via_beamr, send_pull_request_via_beamr, send_push_response_via_beamr,
    send_root_exchange_request_via_beamr, send_root_exchange_response_via_beamr,
    send_shard_sync_request_via_beamr, send_sync_message_via_beamr,
    send_target_node_request_via_beamr, send_target_node_response_via_beamr,
    send_write_ack_via_beamr, send_write_proposal_via_beamr,
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

/// Incarnation-safe correlation id for a single active-active write.
///
/// The id embeds the originating node's restart incarnation (`origin_creation`)
/// so that a slow acknowledgement for a *prior* writer incarnation cannot
/// satisfy a *post-restart* write that happened to reuse the same in-memory
/// `counter`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WriteId {
    pub origin: SyncNodeId,
    pub origin_creation: u32,
    pub counter: u64,
}

impl WriteId {
    #[must_use]
    pub fn new(origin: impl Into<SyncNodeId>, origin_creation: u32, counter: u64) -> Self {
        Self {
            origin: origin.into(),
            origin_creation,
            counter,
        }
    }
}

/// Reason a [`WriteProposal`] was rejected by a receiving replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// The CAS precondition did not match the replica's current value hash.
    CasMismatch,
    /// The replica failed to apply the write for a non-CAS reason.
    ApplyError,
    /// The replica fenced the write: its `promised[shard]` ballot strictly
    /// exceeds the proposal's `epoch` (§2.3), so a stale/deposed owner's write
    /// is rejected. Like [`Self::CasMismatch`] this is a *vote-against*, not a
    /// transport fault — it erodes possible-accepts toward a fence/quorum
    /// failure, never a retryable infrastructure error.
    Fenced,
}

impl RejectReason {
    const fn to_wire(self) -> u8 {
        match self {
            Self::CasMismatch => 0,
            Self::ApplyError => 1,
            Self::Fenced => 2,
        }
    }

    const fn from_wire(value: u8) -> Result<Self, SyncError> {
        match value {
            0 => Ok(Self::CasMismatch),
            1 => Ok(Self::ApplyError),
            2 => Ok(Self::Fenced),
            _ => Err(SyncError::InvalidMessage),
        }
    }
}

/// Outcome of a receiving replica's conditional apply of a [`WriteProposal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    /// The replica conditionally and durably applied the write.
    Applied,
    /// The replica did not apply the write; the reason is a vote-against.
    Rejected(RejectReason),
}

/// Active-active proposal: a CAS-conditioned write replicated to a peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteProposal {
    pub write_id: WriteId,
    /// The explicit owning shard for this write (mirrors
    /// [`BatchWriteProposal::shard_id`]). A bare single-key proposal would route by
    /// hashing its `key`, but a ROUTED stamped write (the durable-timer primitive,
    /// `Database::replicate_write_routed`) co-locates a physical `key` on a
    /// DIFFERENT key's shard, so the shard is named directly here and the receiver
    /// routes by `shard_id` rather than re-deriving it from `key`. For the ordinary
    /// (non-routed) caller this is exactly `shard_for(&key)`, so the on-disk routing
    /// is byte-identical to the pre-`shard_id` behaviour.
    pub shard_id: ShardId,
    pub key: KvKey,
    /// CAS precondition: the prior value hash (`None` means create-if-absent).
    pub expected: Option<Hash>,
    pub value: KvValue,
    pub ttl: Option<Duration>,
    /// The owner epoch this write is stamped with (§2.3). The receiver fences it
    /// (rejects) iff `epoch < promised[shard]`. With no election the stamp is
    /// [`Ballot::bottom`] and every node's `promised` is also bottom, so the
    /// fence is a no-op (2a sequential semantics preserved unchanged).
    pub epoch: Ballot,
    /// The owner-assigned per-(shard, live-epoch) sequence number (AA-3-4a,
    /// R-SEQ). Drawn ONCE by the owner from an atomic counter and carried here so
    /// EVERY replica stores the identical commit stamp `(epoch, seq)` for this
    /// write (§2.4). With no live election the owner stamps `seq = 0` over the
    /// bottom epoch (2a-compat). The receiver stores `(epoch, seq)` verbatim,
    /// never inventing its own.
    pub seq: u64,
    /// AA-3-4b: when `true`, this proposal is a DELETE — the receiver applies a
    /// stamped TOMBSTONE (not the `value`/`ttl`, which are empty) through the same
    /// fence + CAS + stamp path. `expected` is the hash of the value being deleted
    /// (`None` to delete an absent/tombstoned key). A delete is the one fenced,
    /// stamped, replicated write a put is — there is no second delete path.
    pub tombstone: bool,
}

impl WriteProposal {
    /// The commit stamp `(epoch, seq)` this write carries (AA-3-4a). The receiver
    /// stores THIS stamp verbatim so every replica's copy is byte-identical.
    #[must_use]
    pub fn stamp(&self) -> crate::sync::ballot::Stamp {
        crate::sync::ballot::Stamp::new(self.epoch.clone(), self.seq)
    }
}

/// Acknowledgement of a [`WriteProposal`] from a receiving replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteAck {
    pub write_id: WriteId,
    pub acker: SyncNodeId,
    pub acker_creation: u32,
    pub outcome: AckOutcome,
}

/// One entry of a [`BatchWriteProposal`] (A1b): a single key's CAS-conditioned
/// put within a replicated multi-key append.
///
/// Mirrors the per-key fields of a [`WriteProposal`] — `key`, the CAS
/// precondition `expected` (`None` = create-if-absent), `value`, and `ttl` — but
/// carries NO per-entry stamp or epoch: the WHOLE batch shares ONE `stamp` on the
/// enclosing [`BatchWriteProposal`] (§2.4), so every key in an applied batch lands
/// the IDENTICAL commit stamp in ONE atomic fsync. The wire-codec encodes the
/// entries length-prefixed; the field order matches
/// [`crate::shard::actor::handle::BatchItem`] so the receiver can hand them
/// straight to `apply_durable_batch` without re-ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchWriteEntry {
    pub key: KvKey,
    /// CAS precondition for THIS key (`None` means create-if-absent). Each entry
    /// has its own precondition; any single mismatch rejects the WHOLE batch.
    pub expected: Option<Hash>,
    pub value: KvValue,
    pub ttl: Option<Duration>,
}

/// Active-active BATCH proposal (A1b): the replicated multi-key analogue of a
/// [`WriteProposal`].
///
/// A whole stream-append's entries (event keys + the sequence key) are proposed as
/// ONE all-or-nothing unit applied through `apply_durable_batch`.
///
/// Routing/metadata mirror the single-key path, with the multi-key
/// generalisations:
///
/// * `shard_id` — the explicit owning shard for the WHOLE batch. A
///   [`WriteProposal`] routes by hashing its single `key`; a batch spans many keys
///   that (by construction at the proposer in A1c, exactly as a stream append's
///   keys do) all map to ONE shard, so the shard is named directly here — the same
///   way election messages ([`Prepare`]) name their shard rather than re-deriving
///   it. The receiver routes by `shard_id`.
/// * `entries` — the per-key puts, each its own [`BatchWriteEntry`] (with its own
///   CAS `expected`). May be empty (a no-op batch) or large.
/// * `stamp` — ONE shared commit stamp `(epoch, seq)` for the WHOLE batch (§2.4).
///   Its `epoch` is the fence epoch (the receiver rejects the whole batch iff
///   `epoch < promised[shard]`); its `seq` is the owner-assigned per-(shard,epoch)
///   sequence drawn ONCE. Every replica stores the identical stamp on every key.
/// * `write_id` — the SAME incarnation-safe correlation id a [`WriteProposal`]
///   carries, so the batch ack's incarnation gate matches verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchWriteProposal {
    pub write_id: WriteId,
    pub shard_id: ShardId,
    pub entries: Vec<BatchWriteEntry>,
    /// The ONE commit stamp `(epoch, seq)` shared by every entry (§2.4). The
    /// receiver fences the whole batch iff `stamp.epoch < promised[shard]`, and
    /// stores THIS stamp verbatim on every applied key.
    pub stamp: Stamp,
}

impl BatchWriteProposal {
    /// The whole batch's shared commit stamp `(epoch, seq)`.
    #[must_use]
    pub fn stamp(&self) -> Stamp {
        self.stamp.clone()
    }
}

/// Acknowledgement of a [`BatchWriteProposal`] from a receiving replica (A1b).
///
/// Same shape as [`WriteAck`] (echoes `write_id`, carries `acker`/`acker_creation`,
/// and an [`AckOutcome`]) but for a whole batch. Because `apply_durable_batch` is
/// all-or-nothing, the `outcome` is a single verdict for the ENTIRE batch:
/// [`AckOutcome::Applied`] iff EVERY key was durably applied under the shared
/// stamp, otherwise [`AckOutcome::Rejected`] with the reason a fence
/// ([`RejectReason::Fenced`]) or any single CAS mismatch
/// ([`RejectReason::CasMismatch`]) — in which case NOTHING was written. There is no
/// per-entry partial outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchWriteAck {
    pub write_id: WriteId,
    pub acker: SyncNodeId,
    pub acker_creation: u32,
    pub outcome: AckOutcome,
}

/// Step-3 Phase-1 Prepare: a candidate asks every node to promise its ballot
/// for `shard` (§2.2). The receiver promises iff `ballot` exceeds its current
/// `promised[shard]`, otherwise replies [`Nack`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepare {
    pub shard_id: ShardId,
    pub ballot: Ballot,
}

/// Step-3 Phase-1 Promise: a node's grant of a [`Prepare`] (§2.2).
///
/// It carries the promiser's last-accepted epoch and last-committed root so the
/// new owner can state-sync (§2.4). Both are `Option` because a fresh node has
/// neither.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Promise {
    pub shard_id: ShardId,
    pub ballot: Ballot,
    /// The node that granted this promise. The `ballot` echoes the CANDIDATE's
    /// ballot (so the candidate can confirm the reply is for its attempt), so it
    /// cannot identify the promiser; `promiser` carries the granting node's id so
    /// the candidate can count promises from a strict majority of DISTINCT nodes
    /// (§2.2 step 4) and dedup duplicate frames. Mirrors [`WriteAck::acker`].
    pub promiser: SyncNodeId,
    /// The highest epoch the promiser previously accepted, if any.
    pub accepted_epoch: Option<Ballot>,
    /// The promiser's last committed root for `shard`, if any (§2.4).
    pub committed_root: Option<Hash>,
}

/// Step-3 Phase-1 Nack: a node's refusal of a [`Prepare`] whose ballot did not
/// exceed its already-`promised` ballot (§2.2), surfacing that higher ballot so
/// the candidate can retry above it or back off.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Nack {
    pub shard_id: ShardId,
    pub promised: Ballot,
}

/// Step-3 handoff catch-up request (§2.4, AA-3-4).
///
/// A freshly-elected owner asks a promiser for every content-addressed node
/// reachable from its committed root for `shard_id`, so it can sync its local
/// committed state up to the max `committed_root` carried in its Promise majority
/// BEFORE serving.
///
/// Unlike a [`PullRequest`] (which carries only a `target_root` and no requester),
/// this request names the `requester` so the source can route the [`PushResponse`]
/// reply back over the live transport — the requester/response correlation a
/// blind pull lacks. `from_root` is the source's expected committed root (the one
/// the requester saw in the Promise); the source answers from its CURRENT committed
/// root regardless, and the requester adopts whatever `source_root` the response
/// carries. `target_root` is intentionally `None`: the new owner asks for the FULL
/// reachable set (correct-over-clever, §2.4), letting the idempotent
/// content-addressed `put` skip the nodes it already holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardSyncRequest {
    pub shard_id: ShardId,
    /// The node making the request, so the source can route the reply back.
    pub requester: SyncNodeId,
    /// The committed root the requester saw in the promiser's Promise, if any.
    pub from_root: Option<Hash>,
}

impl ShardSyncRequest {
    #[must_use]
    pub const fn new(shard_id: ShardId, requester: SyncNodeId, from_root: Option<Hash>) -> Self {
        Self {
            shard_id,
            requester,
            from_root,
        }
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

    if let Node::Internal(internal) = &*source_node {
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

    let transfer = NodeTransfer::from_parts(source_hash, Node::clone(&source_node))?;
    stats.record_transfer_bytes(transfer.byte_len());
    transfers.push(transfer);
    Ok(())
}
