//! Shard-local operation errors for event append and compare-and-swap.
//!
//! These are the in-actor error types; they convert into the public
//! [`ShardError`] (see [`super::handle`]) at the actor/handle boundary.

use std::fmt;

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
