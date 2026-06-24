use std::fmt;
use std::io;

/// Errors surfaced by the top-level database handle.
#[derive(Debug)]
pub enum DatabaseError {
    DirectoryCreate(io::Error),
    ConfigWrite(io::Error),
    ConfigRead(io::Error),
    ConfigParse(String),
    InvalidShardCount,
    ShardSpawn(String),
    SweepSpawn(String),
    ShardError(String),
    SweepError(String),
    SyncSchedulerSpawn(String),
    SyncSchedulerError(String),
    IoError(io::Error),
    MissingSweepInterval,
    InvalidSweepInterval,
    MissingSyncTopology,
    InvalidSyncInterval,
    SequenceConflict {
        expected: u64,
        actual: u64,
    },
    CasMismatch {
        expected: Option<u64>,
        actual: Option<u64>,
    },
    ConsistencyError(String),
    /// A live distribution-endpoint operation failed (no endpoint attached, a
    /// transport send/connect failure, or a disconnected inbound drain).
    Distribution(String),
    /// A replicated write reached peer-quorum but the proposer could not durably
    /// apply its OWN committed value locally (see [`crate::db::Database::replicate_write`]).
    ///
    /// This is reported, never swallowed: a committed write that is absent on its
    /// own writer is a correctness hazard (it reopens the heal-mid-write
    /// split-brain hole). Under single-owner-per-key (the step-3 epoch fence) the
    /// local CAS can never mismatch, so this only ever surfaces a genuine local
    /// storage/IO fault.
    LocalCommitFailed(String),
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DirectoryCreate(error) => {
                write!(formatter, "failed to create database directory: {error}")
            }
            Self::ConfigWrite(error) => {
                write!(formatter, "failed to write database config: {error}")
            }
            Self::ConfigRead(error) => write!(formatter, "failed to read database config: {error}"),
            Self::ConfigParse(message) => {
                write!(formatter, "failed to parse database config: {message}")
            }
            Self::InvalidShardCount => write!(formatter, "database shard_count must be at least 1"),
            Self::ShardSpawn(message) => {
                write!(formatter, "failed to spawn shard actor: {message}")
            }
            Self::SweepSpawn(message) => {
                write!(formatter, "failed to spawn sweep actor: {message}")
            }
            Self::ShardError(message) => write!(formatter, "shard operation failed: {message}"),
            Self::SweepError(message) => write!(formatter, "sweep operation failed: {message}"),
            Self::SyncSchedulerSpawn(message) => {
                write!(formatter, "failed to spawn sync scheduler: {message}")
            }
            Self::SyncSchedulerError(message) => {
                write!(formatter, "sync scheduler failed: {message}")
            }
            Self::IoError(error) => write!(formatter, "database I/O error: {error}"),
            Self::MissingSweepInterval => write!(formatter, "ttl writes require sweep_interval"),
            Self::InvalidSweepInterval => {
                write!(formatter, "sweep_interval must be greater than zero")
            }
            Self::MissingSyncTopology => {
                write!(formatter, "distributed database requires sync topology")
            }
            Self::InvalidSyncInterval => {
                write!(formatter, "sync_interval must be greater than zero")
            }
            Self::SequenceConflict { expected, actual } => write!(
                formatter,
                "sequence conflict on append: expected {expected}, actual {actual}"
            ),
            Self::CasMismatch { expected, actual } => write!(
                formatter,
                "cas mismatch: expected {expected:?}, actual {actual:?}"
            ),
            Self::ConsistencyError(message) => {
                write!(formatter, "consistency requirement failed: {message}")
            }
            Self::Distribution(message) => {
                write!(formatter, "distribution endpoint error: {message}")
            }
            Self::LocalCommitFailed(message) => write!(
                formatter,
                "replicated write reached quorum but local durable commit failed: {message}"
            ),
        }
    }
}

impl std::error::Error for DatabaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::DirectoryCreate(error)
            | Self::ConfigWrite(error)
            | Self::ConfigRead(error)
            | Self::IoError(error) => Some(error),
            Self::ConfigParse(_)
            | Self::InvalidShardCount
            | Self::ShardSpawn(_)
            | Self::SweepSpawn(_)
            | Self::ShardError(_)
            | Self::SweepError(_)
            | Self::SyncSchedulerSpawn(_)
            | Self::SyncSchedulerError(_)
            | Self::MissingSweepInterval
            | Self::InvalidSweepInterval
            | Self::MissingSyncTopology
            | Self::InvalidSyncInterval
            | Self::SequenceConflict { .. }
            | Self::CasMismatch { .. }
            | Self::ConsistencyError(_)
            | Self::Distribution(_)
            | Self::LocalCommitFailed(_) => None,
        }
    }
}

impl From<io::Error> for DatabaseError {
    fn from(error: io::Error) -> Self {
        Self::IoError(error)
    }
}
