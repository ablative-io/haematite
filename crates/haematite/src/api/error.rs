//! API-001: `EventStore` error types.
//!
//! These are the public, stable errors the `EventStore` (and the KV/TTL briefs
//! that build on it) surface to callers. [`SequenceConflict`] and
//! [`CasMismatch`] are the two optimistic-concurrency failures; [`ApiError`]
//! is the umbrella error every `EventStore` operation returns, wrapping the
//! underlying [`DatabaseError`] for storage-layer failures.

use std::fmt;

use crate::db::DatabaseError;

/// Optimistic-concurrency failure on `append`: the caller's `expected_seq` did
/// not match the stream's current sequence number. The stream is unmodified.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequenceConflict {
    pub expected: u64,
    pub actual: u64,
}

impl SequenceConflict {
    /// Construct a [`SequenceConflict`] from the expected and actual sequences.
    #[must_use]
    pub const fn new(expected: u64, actual: u64) -> Self {
        Self { expected, actual }
    }
}

impl fmt::Display for SequenceConflict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "sequence conflict: expected {}, actual {}",
            self.expected, self.actual
        )
    }
}

impl std::error::Error for SequenceConflict {}

/// Optimistic-concurrency failure on `cas`: the current scalar value did not
/// match the caller's `expected`. The key is unmodified.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CasMismatch {
    pub expected: Option<u64>,
    pub actual: Option<u64>,
}

impl CasMismatch {
    /// Construct a [`CasMismatch`] from the expected and actual values.
    #[must_use]
    pub const fn new(expected: Option<u64>, actual: Option<u64>) -> Self {
        Self { expected, actual }
    }
}

impl fmt::Display for CasMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cas mismatch: expected {:?}, actual {:?}",
            self.expected, self.actual
        )
    }
}

impl std::error::Error for CasMismatch {}

/// A stream existed, but the readable event history requested by the caller was
/// compacted away by TTL expiry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryCompacted {
    pub stream_key: Vec<u8>,
}

impl HistoryCompacted {
    /// Construct a [`HistoryCompacted`] error for `stream_key`.
    #[must_use]
    pub const fn new(stream_key: Vec<u8>) -> Self {
        Self { stream_key }
    }
}

impl fmt::Display for HistoryCompacted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "event history compacted for stream {:?}",
            self.stream_key
        )
    }
}

impl std::error::Error for HistoryCompacted {}

/// Umbrella error for every `EventStore` operation.
///
/// Optimistic-concurrency failures are surfaced as their dedicated variants so
/// callers can match them precisely; everything else (timeouts, WAL/tree/store
/// failures, invalid stored metadata) is a [`Self::Storage`] wrapping the
/// underlying [`DatabaseError`].
#[derive(Debug)]
pub enum ApiError {
    /// `append` saw a stale `expected_seq`; the stream was not modified.
    SequenceConflict(SequenceConflict),
    /// `cas` saw a non-matching current value; the key was not modified.
    CasMismatch(CasMismatch),
    /// A stream exists but its requested history has expired.
    HistoryCompacted(HistoryCompacted),
    /// A stored event value or sequence header was malformed on read.
    CorruptEvent(String),
    /// An error from the underlying storage layer.
    Storage(DatabaseError),
}

impl fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SequenceConflict(conflict) => write!(formatter, "{conflict}"),
            Self::CasMismatch(mismatch) => write!(formatter, "{mismatch}"),
            Self::HistoryCompacted(compacted) => write!(formatter, "{compacted}"),
            Self::CorruptEvent(message) => {
                write!(formatter, "corrupt event record: {message}")
            }
            Self::Storage(error) => write!(formatter, "storage error: {error}"),
        }
    }
}

impl std::error::Error for ApiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SequenceConflict(conflict) => Some(conflict),
            Self::CasMismatch(mismatch) => Some(mismatch),
            Self::HistoryCompacted(compacted) => Some(compacted),
            Self::Storage(error) => Some(error),
            Self::CorruptEvent(_) => None,
        }
    }
}

impl From<SequenceConflict> for ApiError {
    fn from(conflict: SequenceConflict) -> Self {
        Self::SequenceConflict(conflict)
    }
}

impl From<CasMismatch> for ApiError {
    fn from(mismatch: CasMismatch) -> Self {
        Self::CasMismatch(mismatch)
    }
}

impl From<HistoryCompacted> for ApiError {
    fn from(compacted: HistoryCompacted) -> Self {
        Self::HistoryCompacted(compacted)
    }
}

impl From<DatabaseError> for ApiError {
    fn from(error: DatabaseError) -> Self {
        match error {
            DatabaseError::SequenceConflict { expected, actual } => {
                Self::SequenceConflict(SequenceConflict { expected, actual })
            }
            other => Self::Storage(other),
        }
    }
}
