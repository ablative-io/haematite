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
    /// A CAS write was deterministically out-voted: enough replicas rejected the
    /// proposal that a quorum of accepts is no longer reachable, so the writer is
    /// fenced. This is a clean, deterministic loss — distinct from a transport
    /// `AckFailed` (a node could not be reached) and from a `QuorumTimeout` (acks
    /// never arrived in time). It only ever arises from the CAS-aware tally.
    Fenced {
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
            Self::Fenced {
                required,
                possible_accepts,
            } => write!(
                formatter,
                "fenced by CAS rejects: required {required} accepts, only {possible_accepts} still possible"
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

/// A single vote in a CAS-aware quorum tally.
///
/// CAS writes need a third signal beyond the accept/fault distinction of [`Ack`]:
/// a replica that is ahead of (or conflicts with) the proposal votes *against* it
/// deterministically. The three votes are semantically distinct and must not be
/// collapsed:
///
/// * [`CasVote::Accept`] — the replica conditionally + durably applied the write.
/// * [`CasVote::Reject`] — the replica says *no* (e.g. a CAS-precondition
///   mismatch): a legitimate deterministic vote-against, NOT a transport fault.
/// * [`CasVote::Fault`] — the replica could not be reached / failed to apply for a
///   transport reason; this poisons the tally exactly like [`Ack::Failed`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasVote<NodeId> {
    Accept(NodeId),
    Reject(NodeId),
    Fault(NodeId),
}

impl<NodeId> CasVote<NodeId> {
    pub const fn accept(node_id: NodeId) -> Self {
        Self::Accept(node_id)
    }

    pub const fn reject(node_id: NodeId) -> Self {
        Self::Reject(node_id)
    }

    pub const fn fault(node_id: NodeId) -> Self {
        Self::Fault(node_id)
    }
}

/// Classify a CAS write that can no longer reach a quorum of accepts. A prior
/// CAS reject means a conflicting owner deterministically out-voted us
/// ([`ConsistencyError::Fenced`]); a loss to faults alone is an infrastructure
/// failure ([`ConsistencyError::AckFailed`], retryable) — not a fence.
const fn decline_outcome(
    had_reject: bool,
    required: usize,
    possible_accepts: usize,
) -> ConsistencyError {
    if had_reject {
        ConsistencyError::Fenced {
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
/// * When `possible_accepts < required`, the verdict depends on WHY: if any CAS
///   **reject** occurred, a conflicting owner deterministically out-voted us →
///   [`ConsistencyError::Fenced`]; if the loss is to **faults alone**, it is an
///   infrastructure failure → [`ConsistencyError::AckFailed`] (retryable), not a
///   fence.
/// * If neither a quorum of accepts nor a fenced verdict is reached before the
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
    let mut had_reject = false;
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
            CasVote::Reject(node_id) => {
                had_reject = true;
                if declined.insert(node_id) {
                    let possible_accepts = possible.saturating_sub(declined.len());
                    if possible_accepts < required {
                        return Err(ConsistencyError::Fenced {
                            required,
                            possible_accepts,
                        });
                    }
                }
            }
            CasVote::Fault(node_id) => {
                if declined.insert(node_id) {
                    let possible_accepts = possible.saturating_sub(declined.len());
                    if possible_accepts < required {
                        return Err(decline_outcome(had_reject, required, possible_accepts));
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
    let mut had_reject = false;
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
            Ok(CasVote::Reject(node_id)) => {
                had_reject = true;
                if declined.insert(node_id) {
                    let possible_accepts = possible.saturating_sub(declined.len());
                    if possible_accepts < required {
                        return Err(ConsistencyError::Fenced {
                            required,
                            possible_accepts,
                        });
                    }
                }
            }
            Ok(CasVote::Fault(node_id)) => {
                if declined.insert(node_id) {
                    let possible_accepts = possible.saturating_sub(declined.len());
                    if possible_accepts < required {
                        return Err(decline_outcome(had_reject, required, possible_accepts));
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
