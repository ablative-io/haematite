use std::fmt;

use crate::branch::ShardId;
use crate::tree::Hash;

/// Errors raised by the hash-based sync protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncError {
    MissingSourceNode {
        hash: Hash,
    },
    SourceStoreRead {
        hash: Hash,
    },
    TargetStoreRead {
        hash: Hash,
    },
    TargetStoreWrite {
        hash: Hash,
    },
    HashMismatch {
        expected: Hash,
        actual: Hash,
    },
    InvalidNodePayload,
    InvalidMessage,
    MessageTooLarge {
        len: usize,
    },
    ShardMismatch {
        expected: ShardId,
        actual: ShardId,
    },
    TargetRootMismatch {
        expected: Option<Hash>,
        actual: Option<Hash>,
    },
    TransportConnectionUnavailable,
    TransportConnectFailed,
    TransportWrite,
    /// The dedicated distribution runtime could not be constructed.
    TransportRuntimeUnavailable,
    /// Binding the distribution listener failed.
    TransportBind(String),
    /// The inbound-sync drain has no senders left (endpoint torn down).
    TransportDrainDisconnected,
}

impl fmt::Display for SyncError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSourceNode { hash } => {
                write!(formatter, "source is missing tree node {hash}")
            }
            Self::SourceStoreRead { hash } => {
                write!(formatter, "failed to read source tree node {hash}")
            }
            Self::TargetStoreRead { hash } => {
                write!(formatter, "failed to read target tree node {hash}")
            }
            Self::TargetStoreWrite { hash } => {
                write!(formatter, "failed to store target tree node {hash}")
            }
            Self::HashMismatch { expected, actual } => write!(
                formatter,
                "node hash mismatch: expected {expected}, computed {actual}"
            ),
            Self::InvalidNodePayload => {
                formatter.write_str("sync message contains invalid node bytes")
            }
            Self::InvalidMessage => formatter.write_str("sync message is malformed"),
            Self::MessageTooLarge { len } => {
                write!(formatter, "sync message is too large to frame: {len} bytes")
            }
            Self::ShardMismatch { expected, actual } => write!(
                formatter,
                "sync response was for shard {actual}, expected shard {expected}"
            ),
            Self::TargetRootMismatch { expected, actual } => write!(
                formatter,
                "sync response target root {actual:?} did not match request target root {expected:?}"
            ),
            Self::TransportConnectionUnavailable => {
                formatter.write_str("beamr distribution connection is unavailable")
            }
            Self::TransportConnectFailed => {
                formatter.write_str("beamr distribution connection could not be established")
            }
            Self::TransportWrite => {
                formatter.write_str("failed to write sync frame to beamr connection")
            }
            Self::TransportRuntimeUnavailable => {
                formatter.write_str("distribution runtime could not be constructed")
            }
            Self::TransportBind(message) => {
                write!(formatter, "distribution listener bind failed: {message}")
            }
            Self::TransportDrainDisconnected => {
                formatter.write_str("distribution inbound drain is disconnected")
            }
        }
    }
}

impl std::error::Error for SyncError {}
