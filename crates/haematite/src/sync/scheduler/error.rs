use std::fmt;

use crate::branch::ShardId;

use super::super::topology::{SyncNodeId, TopologyError};

/// Errors surfaced by the host-side sync scheduler handle.
#[derive(Debug)]
pub enum SyncSchedulerError {
    ActorUnavailable {
        pid: u64,
    },
    ReplyDisconnected {
        pid: u64,
    },
    ReplyTimeout {
        pid: u64,
    },
    Spawn(String),
    InvalidInterval,
    InvalidShardCount,
    Topology(TopologyError),
    Trigger {
        partner: SyncNodeId,
        shard_id: ShardId,
        message: String,
    },
}

impl fmt::Display for SyncSchedulerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ActorUnavailable { pid } => {
                write!(formatter, "sync scheduler actor {pid} is unavailable")
            }
            Self::ReplyDisconnected { pid } => {
                write!(
                    formatter,
                    "sync scheduler actor {pid} reply channel disconnected"
                )
            }
            Self::ReplyTimeout { pid } => {
                write!(
                    formatter,
                    "timed out waiting for sync scheduler actor {pid}"
                )
            }
            Self::Spawn(message) => write!(formatter, "sync scheduler spawn failed: {message}"),
            Self::InvalidInterval => write!(formatter, "sync interval must be greater than zero"),
            Self::InvalidShardCount => {
                write!(formatter, "sync scheduler shard_count must be at least 1")
            }
            Self::Topology(error) => write!(formatter, "invalid sync topology: {error}"),
            Self::Trigger {
                partner,
                shard_id,
                message,
            } => write!(
                formatter,
                "scheduled pull from partner `{partner}` for shard {shard_id} failed: {message}"
            ),
        }
    }
}

impl std::error::Error for SyncSchedulerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Topology(error) => Some(error),
            Self::ActorUnavailable { .. }
            | Self::ReplyDisconnected { .. }
            | Self::ReplyTimeout { .. }
            | Self::Spawn(_)
            | Self::InvalidInterval
            | Self::InvalidShardCount
            | Self::Trigger { .. } => None,
        }
    }
}

impl From<TopologyError> for SyncSchedulerError {
    fn from(error: TopologyError) -> Self {
        Self::Topology(error)
    }
}
