//! Per-operation consistency policy helpers for distribution.
//!
//! DIST-002 deliberately keeps this layer transport-agnostic: eventual mode
//! records bounded sync intervals and never requires write-path acknowledgments,
//! while strong mode waits for quorum acknowledgments supplied by the existing
//! sync/beamr path. Topology and periodic task orchestration remain with DIST-003.

use std::collections::HashSet;
use std::fmt;
use std::hash::Hash as StdHash;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventualConsistency {
    sync_interval: Duration,
}

impl EventualConsistency {
    pub const fn new(sync_interval: Duration) -> Self {
        Self { sync_interval }
    }

    pub const fn sync_interval(self) -> Duration {
        self.sync_interval
    }

    pub const fn write_requires_ack(self) -> bool {
        false
    }

    pub fn next_sync_after(self, last_sync: Instant) -> Instant {
        last_sync + self.sync_interval
    }

    pub fn sync_due(self, last_sync: Instant, now: Instant) -> bool {
        now.duration_since(last_sync) >= self.sync_interval
    }

    pub fn trigger_if_due<E, Trigger>(
        self,
        last_sync: &mut Instant,
        now: Instant,
        trigger: Trigger,
    ) -> Result<bool, E>
    where
        Trigger: FnOnce() -> Result<(), E>,
    {
        if !self.sync_due(*last_sync, now) {
            return Ok(false);
        }
        trigger()?;
        *last_sync = now;
        Ok(true)
    }

    pub fn intervals_elapsed(self, last_sync: Instant, now: Instant) -> u128 {
        let interval_millis = self.sync_interval.as_millis();
        if interval_millis == 0 {
            return 0;
        }
        now.duration_since(last_sync).as_millis() / interval_millis
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrongConsistency {
    total_nodes: usize,
    timeout: Duration,
    count_local_ack: bool,
}

impl StrongConsistency {
    pub const fn new(total_nodes: usize, timeout: Duration) -> Self {
        Self {
            total_nodes,
            timeout,
            count_local_ack: true,
        }
    }

    pub const fn remote_only(total_nodes: usize, timeout: Duration) -> Self {
        Self {
            total_nodes,
            timeout,
            count_local_ack: false,
        }
    }

    pub const fn total_nodes(self) -> usize {
        self.total_nodes
    }

    pub const fn timeout(self) -> Duration {
        self.timeout
    }

    pub const fn counts_local_ack(self) -> bool {
        self.count_local_ack
    }

    pub const fn write_requires_ack(self) -> bool {
        true
    }

    pub const fn quorum(self) -> Result<usize, ConsistencyError> {
        quorum_size(self.total_nodes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistencyMode {
    Eventual(EventualConsistency),
    Strong(StrongConsistency),
}

impl ConsistencyMode {
    pub const fn eventual(sync_interval: Duration) -> Self {
        Self::Eventual(EventualConsistency::new(sync_interval))
    }

    pub const fn strong(total_nodes: usize, timeout: Duration) -> Self {
        Self::Strong(StrongConsistency::new(total_nodes, timeout))
    }

    pub const fn write_requires_ack(self) -> bool {
        match self {
            Self::Eventual(config) => config.write_requires_ack(),
            Self::Strong(config) => config.write_requires_ack(),
        }
    }
}

impl Default for ConsistencyMode {
    fn default() -> Self {
        Self::Eventual(EventualConsistency::new(Duration::from_secs(60)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsistencyError {
    InvalidNodeCount,
    QuorumUnavailable {
        required: usize,
        possible: usize,
    },
    QuorumTimeout {
        required: usize,
        acknowledged: usize,
        timeout: Duration,
    },
    AckFailed,
    /// The writer-side coordinator could not use the distribution transport to
    /// drive the write to quorum. This covers two cases the coordinator must fail
    /// closed on rather than silently self-quorum: the dedicated distribution
    /// runtime is gone, or the BLOCKING coordinator was invoked from within an
    /// async runtime context (where parking a worker can deadlock the runtime).
    /// Distinct from [`Self::AckFailed`] (a peer could not be reached) — this is a
    /// local precondition failure, not a remote vote.
    TransportUnavailable,
    /// A CAS write was deterministically out-voted: enough replicas rejected the
    /// proposal that a quorum of accepts is no longer reachable, so the writer is
    /// fenced. This is a clean, deterministic loss — distinct from a transport
    /// `AckFailed` (a node could not be reached) and from a `QuorumTimeout` (acks
    /// never arrived in time). It only ever arises from the CAS-aware tally.
    Fenced {
        required: usize,
        possible_accepts: usize,
    },
    /// A replicated CAS write was deterministically out-voted by *value-CAS
    /// mismatches alone* (no epoch fence in the loss set): we are still the live
    /// owner, but enough replicas refused the precondition that a quorum of accepts
    /// is no longer reachable. Distinct from [`Self::Fenced`] (a conflicting owner
    /// with a higher ballot deposed us — the stronger signal) and from
    /// [`Self::AckFailed`] (a retryable transport failure). A caller can retry a
    /// `CasConflict` by re-reading and re-CAS-ing; a `Fenced` caller must instead
    /// re-resolve ownership.
    CasConflict {
        required: usize,
        possible_accepts: usize,
    },
}

impl fmt::Display for ConsistencyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidNodeCount => formatter.write_str("total node count must be at least 1"),
            Self::QuorumUnavailable { required, possible } => write!(
                formatter,
                "quorum cannot be reached: required {required} acknowledgments, only {possible} possible"
            ),
            Self::QuorumTimeout {
                required,
                acknowledged,
                timeout,
            } => write!(
                formatter,
                "timed out after {timeout:?} waiting for quorum: required {required}, acknowledged {acknowledged}"
            ),
            Self::AckFailed => formatter.write_str("sync acknowledgment failed"),
            Self::TransportUnavailable => {
                formatter.write_str("distribution transport unavailable for quorum write")
            }
            Self::Fenced {
                required,
                possible_accepts,
            } => write!(
                formatter,
                "fenced by CAS rejects: required {required} accepts, only {possible_accepts} still possible"
            ),
            Self::CasConflict {
                required,
                possible_accepts,
            } => write!(
                formatter,
                "lost CAS by value mismatch: required {required} accepts, only {possible_accepts} still possible"
            ),
        }
    }
}

impl std::error::Error for ConsistencyError {}

pub const fn quorum_size(total_nodes: usize) -> Result<usize, ConsistencyError> {
    if total_nodes == 0 {
        Err(ConsistencyError::InvalidNodeCount)
    } else {
        Ok(total_nodes / 2 + 1)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumOutcome<NodeId> {
    pub required: usize,
    pub acknowledged: usize,
    pub acknowledged_nodes: Vec<NodeId>,
}

impl<NodeId> QuorumOutcome<NodeId> {
    pub const fn reached(&self) -> bool {
        self.acknowledged >= self.required
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ack<NodeId> {
    Received(NodeId),
    Failed(NodeId),
}

impl<NodeId> Ack<NodeId> {
    pub const fn received(node_id: NodeId) -> Self {
        Self::Received(node_id)
    }

    pub const fn failed(node_id: NodeId) -> Self {
        Self::Failed(node_id)
    }
}

pub fn wait_for_quorum<NodeId, Acks>(
    strong: StrongConsistency,
    acks: Acks,
) -> Result<QuorumOutcome<NodeId>, ConsistencyError>
where
    NodeId: Clone + Eq + StdHash,
    Acks: IntoIterator<Item = Ack<NodeId>>,
{
    let required = strong.quorum()?;
    let local_ack_count = usize::from(strong.counts_local_ack());
    let remote_capacity = strong.total_nodes().saturating_sub(1);
    let possible = local_ack_count.saturating_add(remote_capacity);
    if possible < required {
        return Err(ConsistencyError::QuorumUnavailable { required, possible });
    }

    let deadline = Instant::now() + strong.timeout();
    let mut acknowledged_nodes = Vec::new();
    let mut seen = HashSet::new();
    let mut acknowledged = local_ack_count;

    if acknowledged >= required {
        return Ok(QuorumOutcome {
            required,
            acknowledged,
            acknowledged_nodes,
        });
    }

    for ack in acks {
        if Instant::now() > deadline {
            return Err(ConsistencyError::QuorumTimeout {
                required,
                acknowledged,
                timeout: strong.timeout(),
            });
        }

        match ack {
            Ack::Received(node_id) => {
                if seen.insert(node_id.clone()) {
                    acknowledged = acknowledged.saturating_add(1);
                    acknowledged_nodes.push(node_id);
                    if acknowledged >= required {
                        return Ok(QuorumOutcome {
                            required,
                            acknowledged,
                            acknowledged_nodes,
                        });
                    }
                }
            }
            Ack::Failed(_node_id) => return Err(ConsistencyError::AckFailed),
        }
    }

    Err(ConsistencyError::QuorumTimeout {
        required,
        acknowledged,
        timeout: strong.timeout(),
    })
}

pub fn wait_for_quorum_from_receiver<NodeId>(
    strong: StrongConsistency,
    receiver: &std::sync::mpsc::Receiver<Ack<NodeId>>,
) -> Result<QuorumOutcome<NodeId>, ConsistencyError>
where
    NodeId: Clone + Eq + StdHash,
{
    let required = strong.quorum()?;
    let local_ack_count = usize::from(strong.counts_local_ack());
    let remote_capacity = strong.total_nodes().saturating_sub(1);
    let possible = local_ack_count.saturating_add(remote_capacity);
    if possible < required {
        return Err(ConsistencyError::QuorumUnavailable { required, possible });
    }

    let deadline = Instant::now() + strong.timeout();
    let mut acknowledged_nodes = Vec::new();
    let mut seen = HashSet::new();
    let mut acknowledged = local_ack_count;

    if acknowledged >= required {
        return Ok(QuorumOutcome {
            required,
            acknowledged,
            acknowledged_nodes,
        });
    }

    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| ConsistencyError::QuorumTimeout {
                required,
                acknowledged,
                timeout: strong.timeout(),
            })?;

        match receiver.recv_timeout(remaining) {
            Ok(Ack::Received(node_id)) => {
                if seen.insert(node_id.clone()) {
                    acknowledged = acknowledged.saturating_add(1);
                    acknowledged_nodes.push(node_id);
                    if acknowledged >= required {
                        return Ok(QuorumOutcome {
                            required,
                            acknowledged,
                            acknowledged_nodes,
                        });
                    }
                }
            }
            Ok(Ack::Failed(_node_id)) => return Err(ConsistencyError::AckFailed),
            Err(
                std::sync::mpsc::RecvTimeoutError::Timeout
                | std::sync::mpsc::RecvTimeoutError::Disconnected,
            ) => {
                return Err(ConsistencyError::QuorumTimeout {
                    required,
                    acknowledged,
                    timeout: strong.timeout(),
                });
            }
        }
    }
}

/// Why a replica deterministically voted AGAINST a CAS proposal.
///
/// Both kinds are legitimate vote-againsts (never transport faults), but they carry
/// different strength when a write is provably lost: an [`RejectKind::EpochFence`]
/// means a conflicting owner with a strictly-higher ballot deposed us (the §2.3
/// fence — the stronger signal), whereas an [`RejectKind::CasMismatch`] is a benign
/// value-CAS race (we are still the owner, the precondition merely lost a write
/// ordering). The tally keeps them distinct so a deposed survivor is never confused
/// with a value-CAS loser.
///
/// This enum lives HERE (not in `sync_codec`) on purpose: the wire-level
/// [`crate::sync_codec::message::write::RejectReason`] is mapped onto it at the
/// endpoint boundary, so the consistency layer never takes a dependency on the
/// codec (avoiding a cycle).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectKind {
    /// A conflicting owner promised a strictly-higher ballot: we are fenced.
    EpochFence,
    /// A value-CAS precondition mismatch: a benign race, not a deposition.
    CasMismatch,
}

/// A single vote in a CAS-aware quorum tally.
///
/// CAS writes need a third signal beyond the accept/fault distinction of [`Ack`]:
/// a replica that is ahead of (or conflicts with) the proposal votes *against* it
/// deterministically. The three votes are semantically distinct and must not be
/// collapsed:
///
/// * [`CasVote::Accept`] — the replica conditionally + durably applied the write.
/// * [`CasVote::Reject`] — the replica says *no* (carrying a [`RejectKind`]): a
///   legitimate deterministic vote-against, NOT a transport fault. The kind
///   distinguishes an epoch fence (we were deposed) from a value-CAS mismatch.
/// * [`CasVote::Fault`] — the replica could not be reached / failed to apply for a
///   transport reason; this poisons the tally exactly like [`Ack::Failed`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasVote<NodeId> {
    Accept(NodeId),
    Reject(NodeId, RejectKind),
    Fault(NodeId),
}

impl<NodeId> CasVote<NodeId> {
    pub const fn accept(node_id: NodeId) -> Self {
        Self::Accept(node_id)
    }

    /// Construct an epoch-fence reject. Kept as the single-arg `reject` so the
    /// stronger fence signal is the default vote-against (the historical
    /// behaviour: a bare reject fences).
    pub const fn reject(node_id: NodeId) -> Self {
        Self::Reject(node_id, RejectKind::EpochFence)
    }

    /// Construct a reject carrying an explicit [`RejectKind`].
    pub const fn reject_kind(node_id: NodeId, kind: RejectKind) -> Self {
        Self::Reject(node_id, kind)
    }

    pub const fn fault(node_id: NodeId) -> Self {
        Self::Fault(node_id)
    }
}

/// Classify a CAS write that can no longer reach a quorum of accepts, by WHY the
/// accepts dried up. The three reasons form a strict precedence:
///
/// 1. An epoch fence anywhere in the loss set ⇒ [`ConsistencyError::Fenced`] — a
///    conflicting owner with a strictly-higher ballot deposed us. This is the
///    STRONGEST signal and WINS even if value-CAS mismatches are also present (a
///    deposed survivor must re-resolve ownership, not merely retry the CAS).
/// 2. Value-CAS mismatches but no fence ⇒ [`ConsistencyError::CasConflict`] — we
///    are still the live owner; the precondition simply lost a write ordering and
///    the caller can re-read and retry.
/// 3. Faults alone (no deterministic vote-against at all) ⇒
///    [`ConsistencyError::AckFailed`] — a retryable infrastructure failure.
const fn decline_outcome(
    had_epoch_fence: bool,
    had_cas_mismatch: bool,
    required: usize,
    possible_accepts: usize,
) -> ConsistencyError {
    if had_epoch_fence {
        ConsistencyError::Fenced {
            required,
            possible_accepts,
        }
    } else if had_cas_mismatch {
        ConsistencyError::CasConflict {
            required,
            possible_accepts,
        }
    } else {
        ConsistencyError::AckFailed
    }
}

/// CAS-aware quorum tally over a stream of [`CasVote`]s.
///
/// This is a SEPARATE primitive from [`wait_for_quorum`]: non-CAS callers keep
/// using the accept/fault-only tally unchanged. The semantics, exactly:
///
/// * `required = quorum_size(total_nodes)`; `possible = local_ack + (total-1)`.
/// * The existing static `possible < required` short-circuit is the ONLY
///   availability short-circuit (no liveness/reachability fast-fail): a transient
///   blip must never abort a write the majority should win.
/// * Distinct **accepts** (deduped by node id, plus the local ack) count toward
///   `acknowledged`; reaching `required` yields a committed [`QuorumOutcome`].
/// * Both **rejects** and **faults** (deduped together by node id — a node that
///   will not accept, whether by CAS-reject or by failure) shrink the reachable
///   accept ceiling `possible_accepts = possible - declined`. A single fault does
///   NOT abort a write the rest of the cluster can still carry; the tally only
///   gives up when `possible_accepts < required`.
/// * When `possible_accepts < required`, the verdict depends on WHY (see
///   [`decline_outcome`]): an [`RejectKind::EpochFence`] anywhere wins →
///   [`ConsistencyError::Fenced`]; value-CAS mismatches with no fence →
///   [`ConsistencyError::CasConflict`]; **faults alone** → an infrastructure
///   failure [`ConsistencyError::AckFailed`] (retryable), not a fence.
/// * If neither a quorum of accepts nor a declined verdict is reached before the
///   deadline, return [`ConsistencyError::QuorumTimeout`].
pub fn wait_for_cas_quorum<NodeId, Votes>(
    strong: StrongConsistency,
    votes: Votes,
) -> Result<QuorumOutcome<NodeId>, ConsistencyError>
where
    NodeId: Clone + Eq + StdHash,
    Votes: IntoIterator<Item = CasVote<NodeId>>,
{
    let required = strong.quorum()?;
    let local_ack_count = usize::from(strong.counts_local_ack());
    let remote_capacity = strong.total_nodes().saturating_sub(1);
    let possible = local_ack_count.saturating_add(remote_capacity);
    if possible < required {
        return Err(ConsistencyError::QuorumUnavailable { required, possible });
    }

    let deadline = Instant::now() + strong.timeout();
    let mut acknowledged_nodes = Vec::new();
    let mut accepted = HashSet::new();
    // Nodes that will not contribute an accept — CAS rejects AND faults unioned,
    // so a node that both rejects and faults erodes the accept ceiling once.
    let mut declined = HashSet::new();
    let mut had_epoch_fence = false;
    let mut had_cas_mismatch = false;
    let mut acknowledged = local_ack_count;

    if acknowledged >= required {
        return Ok(QuorumOutcome {
            required,
            acknowledged,
            acknowledged_nodes,
        });
    }

    for vote in votes {
        if Instant::now() > deadline {
            return Err(ConsistencyError::QuorumTimeout {
                required,
                acknowledged,
                timeout: strong.timeout(),
            });
        }

        match vote {
            CasVote::Accept(node_id) => {
                if accepted.insert(node_id.clone()) {
                    acknowledged = acknowledged.saturating_add(1);
                    acknowledged_nodes.push(node_id);
                    if acknowledged >= required {
                        return Ok(QuorumOutcome {
                            required,
                            acknowledged,
                            acknowledged_nodes,
                        });
                    }
                }
            }
            CasVote::Reject(node_id, kind) => {
                match kind {
                    RejectKind::EpochFence => had_epoch_fence = true,
                    RejectKind::CasMismatch => had_cas_mismatch = true,
                }
                if declined.insert(node_id) {
                    let possible_accepts = possible.saturating_sub(declined.len());
                    if possible_accepts < required {
                        return Err(decline_outcome(
                            had_epoch_fence,
                            had_cas_mismatch,
                            required,
                            possible_accepts,
                        ));
                    }
                }
            }
            CasVote::Fault(node_id) => {
                if declined.insert(node_id) {
                    let possible_accepts = possible.saturating_sub(declined.len());
                    if possible_accepts < required {
                        return Err(decline_outcome(
                            had_epoch_fence,
                            had_cas_mismatch,
                            required,
                            possible_accepts,
                        ));
                    }
                }
            }
        }
    }

    Err(ConsistencyError::QuorumTimeout {
        required,
        acknowledged,
        timeout: strong.timeout(),
    })
}

/// CAS-aware quorum tally that blocks on a receiver of [`CasVote`]s.
///
/// The blocking analogue of [`wait_for_cas_quorum`], mirroring
/// [`wait_for_quorum_from_receiver`] for the non-CAS path. The writer-side
/// coordinator (2a-3) feeds votes from inbound `WriteAck` handlers into the
/// sender; this is NOT wired to any live send/apply path in this increment.
pub fn wait_for_cas_quorum_from_receiver<NodeId>(
    strong: StrongConsistency,
    receiver: &std::sync::mpsc::Receiver<CasVote<NodeId>>,
) -> Result<QuorumOutcome<NodeId>, ConsistencyError>
where
    NodeId: Clone + Eq + StdHash,
{
    let required = strong.quorum()?;
    let local_ack_count = usize::from(strong.counts_local_ack());
    let remote_capacity = strong.total_nodes().saturating_sub(1);
    let possible = local_ack_count.saturating_add(remote_capacity);
    if possible < required {
        return Err(ConsistencyError::QuorumUnavailable { required, possible });
    }

    let deadline = Instant::now() + strong.timeout();
    let mut acknowledged_nodes = Vec::new();
    let mut accepted = HashSet::new();
    // Nodes that will not contribute an accept — CAS rejects AND faults unioned,
    // so a node that both rejects and faults erodes the accept ceiling once.
    let mut declined = HashSet::new();
    let mut had_epoch_fence = false;
    let mut had_cas_mismatch = false;
    let mut acknowledged = local_ack_count;

    if acknowledged >= required {
        return Ok(QuorumOutcome {
            required,
            acknowledged,
            acknowledged_nodes,
        });
    }

    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| ConsistencyError::QuorumTimeout {
                required,
                acknowledged,
                timeout: strong.timeout(),
            })?;

        match receiver.recv_timeout(remaining) {
            Ok(CasVote::Accept(node_id)) => {
                if accepted.insert(node_id.clone()) {
                    acknowledged = acknowledged.saturating_add(1);
                    acknowledged_nodes.push(node_id);
                    if acknowledged >= required {
                        return Ok(QuorumOutcome {
                            required,
                            acknowledged,
                            acknowledged_nodes,
                        });
                    }
                }
            }
            Ok(CasVote::Reject(node_id, kind)) => {
                match kind {
                    RejectKind::EpochFence => had_epoch_fence = true,
                    RejectKind::CasMismatch => had_cas_mismatch = true,
                }
                if declined.insert(node_id) {
                    let possible_accepts = possible.saturating_sub(declined.len());
                    if possible_accepts < required {
                        return Err(decline_outcome(
                            had_epoch_fence,
                            had_cas_mismatch,
                            required,
                            possible_accepts,
                        ));
                    }
                }
            }
            Ok(CasVote::Fault(node_id)) => {
                if declined.insert(node_id) {
                    let possible_accepts = possible.saturating_sub(declined.len());
                    if possible_accepts < required {
                        return Err(decline_outcome(
                            had_epoch_fence,
                            had_cas_mismatch,
                            required,
                            possible_accepts,
                        ));
                    }
                }
            }
            Err(
                std::sync::mpsc::RecvTimeoutError::Timeout
                | std::sync::mpsc::RecvTimeoutError::Disconnected,
            ) => {
                return Err(ConsistencyError::QuorumTimeout {
                    required,
                    acknowledged,
                    timeout: strong.timeout(),
                });
            }
        }
    }
}

pub fn execute_with_consistency<Write, WriteResult, AckNode, Acks>(
    mode: ConsistencyMode,
    write: Write,
    acks: Acks,
) -> Result<WriteResult, ConsistencyError>
where
    Write: FnOnce() -> Result<WriteResult, ConsistencyError>,
    AckNode: Clone + Eq + StdHash,
    Acks: IntoIterator<Item = Ack<AckNode>>,
{
    let result = write()?;
    if let ConsistencyMode::Strong(strong) = mode {
        wait_for_quorum(strong, acks).map(drop)?;
    }
    Ok(result)
}

#[cfg(test)]
#[path = "consistency_tests.rs"]
mod tests;
