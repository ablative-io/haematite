//! Step-3 epoch-fence election message types plus the handoff catch-up request.

use crate::ids::ShardId;
use crate::sync_codec::ballot::Ballot;
use crate::sync_codec::ids::SyncNodeId;
use crate::tree::Hash;

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
    /// (§2.2 step 4) and dedup duplicate frames. Mirrors
    /// [`WriteAck::acker`](crate::sync_codec::WriteAck).
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
/// Unlike a [`PullRequest`](crate::sync_codec::PullRequest) (which carries only a
/// `target_root` and no requester), this request names the `requester` so the
/// source can route the [`PushResponse`](crate::sync_codec::PushResponse) reply
/// back over the live transport — the requester/response correlation a blind pull
/// lacks. `from_root` is the source's expected committed root (the one the
/// requester saw in the Promise); the source answers from its CURRENT committed
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
