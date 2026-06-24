//! Shard-local operation errors for event append and compare-and-swap.
//!
//! These are the in-actor error types; they convert into the public
//! [`ShardError`] (see [`super::handle`]) at the actor/handle boundary.

use std::fmt;

use crate::sync::ballot::Ballot;
use crate::tree::Hash;
use crate::wal::WalError;

use super::handle::ShardError;

/// Errors returned by shard-local event append operations.
#[derive(Debug)]
pub(super) enum AppendError {
    SequenceConflict { expected: u64, actual: u64 },
    Wal(WalError),
}

impl fmt::Display for AppendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SequenceConflict { expected, actual } => write!(
                formatter,
                "sequence conflict on append: expected {expected}, actual {actual}"
            ),
            Self::Wal(error) => write!(formatter, "append WAL error: {error}"),
        }
    }
}

impl std::error::Error for AppendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Wal(error) => Some(error),
            Self::SequenceConflict { .. } => None,
        }
    }
}

impl From<WalError> for AppendError {
    fn from(error: WalError) -> Self {
        Self::Wal(error)
    }
}

impl From<AppendError> for ShardError {
    fn from(error: AppendError) -> Self {
        match error {
            AppendError::SequenceConflict { expected, actual } => {
                Self::SequenceConflict { expected, actual }
            }
            AppendError::Wal(error) => Self::from(error),
        }
    }
}

/// Errors returned by shard-local compare-and-swap operations.
#[derive(Debug)]
pub(super) enum CasError {
    Mismatch {
        expected: Option<u64>,
        actual: Option<u64>,
    },
    Wal(WalError),
}

impl fmt::Display for CasError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mismatch { expected, actual } => write!(
                formatter,
                "cas mismatch: expected {expected:?}, actual {actual:?}"
            ),
            Self::Wal(error) => write!(formatter, "cas WAL error: {error}"),
        }
    }
}

impl std::error::Error for CasError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Wal(error) => Some(error),
            Self::Mismatch { .. } => None,
        }
    }
}

impl From<WalError> for CasError {
    fn from(error: WalError) -> Self {
        Self::Wal(error)
    }
}

impl From<CasError> for ShardError {
    fn from(error: CasError) -> Self {
        match error {
            CasError::Mismatch { expected, actual } => Self::CasMismatch { expected, actual },
            CasError::Wal(error) => Self::from(error),
        }
    }
}

/// Errors returned by the receiver-side conditional-durable apply
/// ([`super::ShardActor::apply_durable`], active-active 2a-4).
///
/// Unlike [`CasError`], the precondition is a tree value HASH (not a scalar
/// `u64`), so a mismatch carries `Option<Hash>` — what the proposing writer
/// expected vs what this replica actually holds. The distinction matters at the
/// quorum tally: a [`Self::HashMismatch`] is a CAS *vote-against* (the replica is
/// ahead and the writer lost the race), whereas [`Self::Wal`] is a genuine apply
/// fault.
#[derive(Debug)]
pub(super) enum HashCasError {
    HashMismatch {
        expected: Option<Hash>,
        actual: Option<Hash>,
    },
    /// The epoch fence rejected the write (AA-3-3, §2.3): the write's `attempted`
    /// epoch was strictly below this shard's actor-local `promised` ballot, so a
    /// stale/deposed owner's write was refused and NOTHING was applied. Like
    /// [`Self::HashMismatch`] this is a vote-against at the quorum tally, never an
    /// apply fault.
    Fenced {
        promised: Ballot,
        attempted: Ballot,
    },
    Wal(WalError),
}

impl fmt::Display for HashCasError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HashMismatch { expected, actual } => write!(
                formatter,
                "hash cas mismatch: expected {expected:?}, actual {actual:?}"
            ),
            Self::Fenced {
                promised,
                attempted,
            } => write!(
                formatter,
                "epoch fence: attempted {attempted:?} < promised {promised:?}"
            ),
            Self::Wal(error) => write!(formatter, "apply WAL error: {error}"),
        }
    }
}

impl std::error::Error for HashCasError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Wal(error) => Some(error),
            Self::HashMismatch { .. } | Self::Fenced { .. } => None,
        }
    }
}

impl From<WalError> for HashCasError {
    fn from(error: WalError) -> Self {
        Self::Wal(error)
    }
}

impl From<HashCasError> for ShardError {
    fn from(error: HashCasError) -> Self {
        match error {
            HashCasError::HashMismatch { expected, actual } => {
                Self::CasHashMismatch { expected, actual }
            }
            HashCasError::Fenced {
                promised,
                attempted,
            } => Self::Fenced {
                promised,
                attempted,
            },
            HashCasError::Wal(error) => Self::from(error),
        }
    }
}
