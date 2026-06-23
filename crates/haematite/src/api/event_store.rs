//! API-001: the `EventStore` over the real [`Database`].
//!
//! The `EventStore` is a thin, typed facade over a persistent
//! [`crate::db::Database`]. Every operation routes through the shard router to
//! the single-threaded shard actors, so the atomicity guarantees of `append`
//! and `cas` are the actors' (see [`crate::shard::actor`]) — this layer adds
//! only the public event envelope and key encoding.
//!
//! # Event stream key format (R2)
//!
//! An event is stored at the tree key [`encode_stream_key`] produces:
//!
//! ```text
//! stream_key || 0x00 || seq.to_be_bytes()   (9 trailing bytes)
//! ```
//!
//! The `0x00` separator and big-endian sequence make the encoding sort
//! lexicographically by `stream_key` first, then by `seq`, so a tree range over
//! one stream yields its events in sequence order. This MUST match the encoding
//! the shard actor writes ([`crate::shard::actor`]'s `event_key`); a divergence
//! would make appended events unreadable, so both are exercised by tests.
//!
//! # Event value envelope
//!
//! The sequence number is recovered from the *key*. The timestamp is stored in
//! the *value*, as an eight-byte big-endian header before the payload:
//!
//! ```text
//! timestamp.to_be_bytes() || payload
//! ```
//!
//! so `read`/`read_from` recover payload + sequence + timestamp after a reopen.
//!
//! # Keyspace separation (caller invariant)
//!
//! The `EventStore` and the scalar [`EventStore::cas`]/[`EventStore::read_value`]
//! API share one flat tree keyspace with three disjoint regions per logical key:
//! a CAS scalar at the raw `key`, events at `key || 0x00 || seq`, and the
//! per-stream sequence counter at `key || 0xff`. Callers MUST NOT use a stream
//! name and a CAS key that collide across these encodings (e.g. a CAS key whose
//! bytes equal another stream's `key || 0x00 || seq`), or one region's value
//! could be read through the other's decoder. Aion's usage keeps event-stream
//! keys and CAS keys in separate, non-overlapping namespaces.

use std::time::Duration;

use crate::api::error::{ApiError, CasMismatch, HistoryCompacted, SequenceConflict};
use crate::api::types::{Event, ScanResult, StreamMeta};
use crate::branch::{Timestamp, current_timestamp};
use crate::db::{Database, DatabaseError};

/// Separator placed between a stream key and its big-endian sequence number.
const EVENT_SEPARATOR: u8 = 0x00;
/// Width in bytes of the big-endian sequence number appended to event keys.
const SEQ_WIDTH: usize = 8;
/// Width in bytes of the big-endian timestamp header on each stored value.
const TS_WIDTH: usize = 8;

/// A typed `EventStore` facade over a persistent [`Database`].
#[derive(Debug)]
pub struct EventStore {
    db: Database,
}

impl EventStore {
    /// Wrap an open [`Database`] as an `EventStore`.
    #[must_use]
    pub const fn new(db: Database) -> Self {
        Self { db }
    }

    /// Borrow the underlying database.
    #[must_use]
    pub const fn database(&self) -> &Database {
        &self.db
    }

    /// Consume the `EventStore` and return the wrapped database.
    #[must_use]
    pub fn into_database(self) -> Database {
        self.db
    }

    /// Atomically append `payload` to `stream_key` under optimistic concurrency.
    ///
    /// `expected_seq` is the stream's current next-sequence (its event count);
    /// on a match the event is appended and the new next-sequence is returned.
    /// On a mismatch the stream is left untouched and [`ApiError::SequenceConflict`]
    /// is returned. The sequence check and append are atomic inside the shard
    /// actor; see [`crate::shard::actor::ShardActor`].
    ///
    /// # Errors
    /// [`ApiError::SequenceConflict`] on a stale `expected_seq`, otherwise a
    /// storage error.
    pub fn append(
        &self,
        stream_key: &[u8],
        payload: &[u8],
        expected_seq: u64,
    ) -> Result<u64, ApiError> {
        self.append_batch_with_ttl(stream_key, &[payload], expected_seq, None)
    }

    /// Atomically append `payload` with optional TTL metadata.
    pub fn append_with_ttl(
        &self,
        stream_key: &[u8],
        payload: &[u8],
        expected_seq: u64,
        ttl: Option<Duration>,
    ) -> Result<u64, ApiError> {
        self.append_batch_with_ttl(stream_key, &[payload], expected_seq, ttl)
    }

    /// Atomically append many `payloads` to `stream_key` as one tree commit.
    ///
    /// Returns the stream's next-sequence after the batch (`expected_seq +
    /// payloads.len()`). An empty batch is a no-op that returns `expected_seq`.
    ///
    /// # Errors
    /// As [`Self::append`].
    pub fn append_batch(
        &self,
        stream_key: &[u8],
        payloads: &[&[u8]],
        expected_seq: u64,
    ) -> Result<u64, ApiError> {
        self.append_batch_with_ttl(stream_key, payloads, expected_seq, None)
    }

    /// Atomically append many `payloads` with optional TTL metadata.
    pub fn append_batch_with_ttl(
        &self,
        stream_key: &[u8],
        payloads: &[&[u8]],
        expected_seq: u64,
        ttl: Option<Duration>,
    ) -> Result<u64, ApiError> {
        let timestamp = current_timestamp();
        let entries: Vec<Vec<u8>> = payloads
            .iter()
            .map(|payload| encode_value(timestamp, payload))
            .collect();
        match self
            .db
            .append_with_ttl(stream_key.to_vec(), entries, expected_seq, ttl)
        {
            Ok(next_seq) => Ok(next_seq),
            Err(DatabaseError::SequenceConflict { expected, actual }) => {
                Err(ApiError::SequenceConflict(SequenceConflict {
                    expected,
                    actual,
                }))
            }
            Err(error) => Err(ApiError::from(error)),
        }
    }

    /// Read the full event stream for `stream_key` in sequence order.
    ///
    /// Returns an empty `Vec` for a stream with no events.
    ///
    /// # Errors
    /// A storage error, or [`ApiError::CorruptEvent`] if a stored value is
    /// malformed.
    pub fn read(&self, stream_key: &[u8]) -> Result<Vec<Event>, ApiError> {
        self.read_from(stream_key, 0)
    }

    /// Read events for `stream_key` with sequence `>= from_seq`, in order.
    ///
    /// Uses a tree range starting at `from_seq` (not a full read + filter), so
    /// events before `from_seq` are never loaded. Returns an empty `Vec` when
    /// `from_seq` is at or beyond the end of the stream.
    ///
    /// # Errors
    /// As [`Self::read`].
    pub fn read_from(&self, stream_key: &[u8], from_seq: u64) -> Result<Vec<Event>, ApiError> {
        // The engine stores the first event of a stream at engine-key seq 1
        // (1-based); the public API is 0-based, so API seq `s` maps to engine
        // key seq `s + 1`. Translate the range start to skip events before
        // `from_seq` at the tree level (R5: a range query, not read+filter).
        let engine_from = from_seq.saturating_add(1);
        let entries = self.db.read_event_entries_from(stream_key, engine_from)?;
        let next_seq = self.db.read_stream_next_seq(stream_key)?;
        let mut events = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let seq = decode_event_seq(stream_key, &key)?;
            let (timestamp, payload) = decode_value(&value)?;
            events.push(Event::new(seq, payload, timestamp));
        }
        if events.is_empty() && from_seq == 0 && next_seq.is_some_and(|next_seq| next_seq > 0) {
            return Err(ApiError::HistoryCompacted(HistoryCompacted::new(
                stream_key.to_vec(),
            )));
        }
        Ok(events)
    }

    /// Read the scalar `u64` value at `key`, or `None` if unset.
    ///
    /// # Errors
    /// A storage error, including a malformed (non eight-byte) stored value.
    pub fn read_value(&self, key: &[u8]) -> Result<Option<u64>, ApiError> {
        Ok(self.db.read_value(key)?)
    }

    /// Atomically compare-and-swap the scalar `u64` value at `key`.
    ///
    /// `expected == None` requires the key to be currently unset (and creates
    /// it). On a value mismatch nothing is written and [`ApiError::CasMismatch`]
    /// carries the actual current value. The read-compare-write runs inside the
    /// owning shard's single-threaded actor, so concurrent CAS calls on one key
    /// cannot race; see [`crate::shard::actor::ShardActor`].
    ///
    /// # Errors
    /// [`ApiError::CasMismatch`] on a non-matching current value, otherwise a
    /// storage error.
    pub fn cas(&self, key: &[u8], expected: Option<u64>, new: u64) -> Result<(), ApiError> {
        match self.db.cas(key.to_vec(), expected, new) {
            Ok(()) => Ok(()),
            Err(DatabaseError::CasMismatch { expected, actual }) => {
                Err(ApiError::CasMismatch(CasMismatch { expected, actual }))
            }
            Err(error) => Err(ApiError::from(error)),
        }
    }

    /// Walk every shard, applying `predicate` to each stream's metadata.
    ///
    /// This is O(total streams across all shards): the brief's intentionally
    /// unindexed scan. Streams whose metadata satisfies `predicate` are
    /// returned as [`ScanResult`]s, in no particular cross-shard order.
    ///
    /// # Errors
    /// A storage error from any shard.
    pub fn scan<P>(&self, mut predicate: P) -> Result<Vec<ScanResult>, ApiError>
    where
        P: FnMut(StreamMeta<'_>) -> bool,
    {
        let streams = self.db.scan_sequence_keys()?;
        let mut matches = Vec::new();
        for (stream_key, next_seq) in &streams {
            let meta = StreamMeta {
                stream_key,
                next_seq: *next_seq,
            };
            if predicate(meta) && self.db.stream_has_live_events(stream_key)? {
                matches.push(ScanResult::new(stream_key.clone(), *next_seq));
            }
        }
        Ok(matches)
    }

    /// Flush every shard's buffered writes to durable storage.
    ///
    /// Maps to [`Database::commit`]: appended events and CAS writes are already
    /// committed per-operation, so this is for callers that buffer general
    /// writes elsewhere; it is a genuine persist, not a no-op.
    ///
    /// # Errors
    /// A storage error from any shard.
    pub fn flush(&self) -> Result<(), ApiError> {
        self.db.commit()?;
        Ok(())
    }
}

/// Encode the tree key for event `seq` of `stream_key` (R2).
///
/// Layout: `stream_key || 0x00 || seq.to_be_bytes()`. Sorts by `stream_key`
/// then `seq`. Matches the shard actor's `event_key`.
#[must_use]
pub fn encode_stream_key(stream_key: &[u8], seq: u64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(stream_key.len().saturating_add(1 + SEQ_WIDTH));
    encoded.extend_from_slice(stream_key);
    encoded.push(EVENT_SEPARATOR);
    encoded.extend_from_slice(&seq.to_be_bytes());
    encoded
}

/// Decode an event tree key back into `(stream_key, seq)`.
///
/// Returns `None` if `encoded` is too short or lacks the `0x00` separator in
/// the expected position.
#[must_use]
pub fn decode_stream_key(encoded: &[u8]) -> Option<(Vec<u8>, u64)> {
    let split = encoded.len().checked_sub(SEQ_WIDTH)?;
    let separator_index = split.checked_sub(1)?;
    if encoded.get(separator_index) != Some(&EVENT_SEPARATOR) {
        return None;
    }
    let stream_key = encoded.get(..separator_index)?.to_vec();
    let seq_bytes: [u8; SEQ_WIDTH] = encoded.get(split..)?.try_into().ok()?;
    Some((stream_key, u64::from_be_bytes(seq_bytes)))
}

/// Decode the public 0-based sequence of an event key known to belong to
/// `stream_key`.
///
/// The engine stores the first event at engine-key seq 1; the public API is
/// 0-based, so this subtracts one. An engine seq of 0 is impossible for an
/// event key (it is the empty-stream sentinel) and is treated as corruption.
fn decode_event_seq(stream_key: &[u8], encoded: &[u8]) -> Result<u64, ApiError> {
    match decode_stream_key(encoded) {
        Some((decoded_key, engine_seq)) if decoded_key == stream_key => engine_seq
            .checked_sub(1)
            .ok_or_else(|| ApiError::CorruptEvent(format!("event key has zero seq: {encoded:?}"))),
        _ => Err(ApiError::CorruptEvent(format!(
            "event key does not encode stream {stream_key:?}: {encoded:?}"
        ))),
    }
}

/// Encode a stored event value: `timestamp.to_be_bytes() || payload`.
fn encode_value(timestamp: Timestamp, payload: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(TS_WIDTH.saturating_add(payload.len()));
    encoded.extend_from_slice(&timestamp.to_be_bytes());
    encoded.extend_from_slice(payload);
    encoded
}

/// Decode a stored event value back into `(timestamp, payload)`.
fn decode_value(value: &[u8]) -> Result<(Timestamp, Vec<u8>), ApiError> {
    let header: [u8; TS_WIDTH] = value
        .get(..TS_WIDTH)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| {
            ApiError::CorruptEvent(format!(
                "event value shorter than timestamp header: {value:?}"
            ))
        })?;
    let timestamp = u64::from_be_bytes(header);
    let payload = value.get(TS_WIDTH..).unwrap_or_default().to_vec();
    Ok((timestamp, payload))
}

#[cfg(test)]
#[path = "event_store_tests.rs"]
mod tests;
