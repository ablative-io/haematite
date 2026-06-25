//! Internal helpers for [`crate::db::Database`]: shard-error mapping, ordered
//! commit-hash collection, single-shard range collection, and event-key range
//! bounds. Kept separate to keep `db.rs` focused on the public surface.

use std::time::Duration;

use crate::shard::actor::{RangeItem, ShardError, ShardHandle};
use crate::tree::Hash;

use super::{DatabaseError, DbRange, ShardCommitResult};

/// Map a spawn-time shard error, preserving the spawn variant.
pub(super) fn map_spawn_error(error: ShardError) -> DatabaseError {
    match error {
        ShardError::Spawn(message) => DatabaseError::ShardSpawn(message),
        other => map_shard_error(other),
    }
}

/// Map a runtime shard error into the public [`DatabaseError`], preserving the
/// optimistic-concurrency variants so callers can match them precisely.
pub fn map_shard_error(error: ShardError) -> DatabaseError {
    match error {
        ShardError::SequenceConflict { expected, actual } => {
            DatabaseError::SequenceConflict { expected, actual }
        }
        ShardError::CasMismatch { expected, actual } => {
            DatabaseError::CasMismatch { expected, actual }
        }
        ShardError::Spawn(message) => DatabaseError::ShardSpawn(message),
        other => DatabaseError::ShardError(other.to_string()),
    }
}

/// Reassemble per-shard commit results into a shard-ordered hash vector.
pub fn ordered_hashes(
    results: Vec<ShardCommitResult>,
    shard_count: usize,
) -> Result<Vec<Hash>, DatabaseError> {
    let mut ordered = vec![None; shard_count];
    for (index, result) in results {
        match result {
            Ok(hash) => {
                if let Some(slot) = ordered.get_mut(index) {
                    *slot = Some(hash);
                }
            }
            Err(error) => return Err(map_shard_error(error)),
        }
    }
    let mut hashes = Vec::with_capacity(shard_count);
    for hash in ordered {
        let Some(hash) = hash else {
            return Err(DatabaseError::ShardError(
                "missing shard commit result".to_owned(),
            ));
        };
        hashes.push(hash);
    }
    Ok(hashes)
}

/// Run a `[from, to)` range against one shard handle and collect its entries.
pub fn range_on_handle(
    handle: &ShardHandle,
    from: &[u8],
    to: &[u8],
    timeout: Duration,
) -> Result<DbRange, DatabaseError> {
    let items = handle
        .range(from.to_vec(), to.to_vec(), timeout)
        .map_err(map_shard_error)?;
    collect_range_items(items)
}

/// Collect streamed range items up to the [`RangeItem::Done`] sentinel.
fn collect_range_items(items: Vec<RangeItem>) -> Result<DbRange, DatabaseError> {
    let mut entries = Vec::new();
    for item in items {
        match item {
            RangeItem::Entry { key, value } => entries.push((key, value)),
            RangeItem::Done => return Ok(entries),
        }
    }
    Err(DatabaseError::ShardError(
        "range result missing Done".to_owned(),
    ))
}

/// Inclusive lower bound for an event range: `key || 0x00 || seq.to_be_bytes()`.
pub(super) fn event_range_start(key: &[u8], seq: u64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len().saturating_add(9));
    encoded.extend_from_slice(key);
    encoded.push(0);
    encoded.extend_from_slice(&seq.to_be_bytes());
    encoded
}

/// Exclusive upper bound for an event range: `key || 0x01`, which sorts after
/// every `key || 0x00 || ...` event key but before the `0xff` sequence key.
pub(super) fn event_range_end(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len().saturating_add(1));
    encoded.extend_from_slice(key);
    encoded.push(1);
    encoded
}

/// The per-stream sequence-counter key: `key || 0xff "seq"`.
///
/// This is byte-identical to the shard actor's `sequence_key` (its `SEQ_SUFFIX`)
/// and to the legacy `db::event_sequence_key`; it is the metadata key whose 8-byte
/// big-endian value is the stream's next-sequence. The single shared definition so
/// the local `append`, `read_stream_next_seq`, and the replicated
/// [`replicate_append`](crate::db::Database::replicate_append) all address the SAME
/// key (§ event-store layout).
pub(super) fn event_sequence_key(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len().saturating_add(4));
    encoded.extend_from_slice(key);
    encoded.extend_from_slice(&[0xff, b's', b'e', b'q']);
    encoded
}

pub fn run_indexed_parallel<Item, Output, Work>(
    items: Vec<Item>,
    work: Work,
) -> Result<Vec<(usize, Output)>, DatabaseError>
where
    Item: Send,
    Output: Send,
    Work: Fn(Item) -> Output + Sync,
{
    std::thread::scope(|scope| {
        let mut joins = Vec::with_capacity(items.len());
        for (index, item) in items.into_iter().enumerate() {
            let work = &work;
            joins.push(scope.spawn(move || (index, work(item))));
        }
        let mut results = Vec::with_capacity(joins.len());
        for join in joins {
            match join.join() {
                Ok(result) => results.push(result),
                Err(_) => {
                    return Err(DatabaseError::ShardError(
                        "parallel worker thread panicked".to_owned(),
                    ));
                }
            }
        }
        Ok(results)
    })
}
