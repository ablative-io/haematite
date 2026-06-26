//! API-001 / PERF-002: full-shard sequence scan used by `EventStore::scan`.
//!
//! The production scan path no longer walks the committed tree. It reads the
//! actor-owned ordered stream index, so enumeration is O(streams) while keeping
//! the same per-shard lexicographic stream-key ordering.

use super::handle::{ShardError, StreamSeq};
use super::stream_index::{self, LiveStreamIndex, SequenceIndexErrors};

/// Return every stream's decoded `(stream_key, next_seq)` from the secondary
/// index in per-shard lexicographic order.
pub(super) fn scan_sequences(
    index: &LiveStreamIndex,
    errors: &SequenceIndexErrors,
) -> Result<Vec<StreamSeq>, ShardError> {
    stream_index::scan_index(index, errors)
}
