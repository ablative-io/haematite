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
