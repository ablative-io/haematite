//! API-001: Public `EventStore` value types.
//!
//! [`Event`] is the unit returned by every read path: it carries the
//! sequence number assigned at append time, the original payload bytes, and a
//! wall-clock timestamp captured when the event was appended. The sequence is
//! recovered from the event's tree key (see [`crate::api::event_store`]); the
//! timestamp is recovered from a fixed-width header prepended to the stored
//! value so both survive a `Database` reopen.

use crate::branch::Timestamp;

/// One appended event: its assigned sequence, payload, and append timestamp.
///
/// `seq` is the 0-based position of this event within its stream (the first
/// appended event has `seq == 0`). `timestamp` is nanoseconds since the Unix
/// epoch, captured by the `EventStore` when the event was appended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub seq: u64,
    pub payload: Vec<u8>,
    pub timestamp: Timestamp,
}

impl Event {
    /// Construct an [`Event`] from its parts.
    #[must_use]
    pub const fn new(seq: u64, payload: Vec<u8>, timestamp: Timestamp) -> Self {
        Self {
            seq,
            payload,
            timestamp,
        }
    }
}

/// One match produced by [`crate::api::event_store::EventStore::scan`]: a
/// stream key whose current sequence-number metadata satisfied the predicate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanResult {
    pub stream_key: Vec<u8>,
    pub next_seq: u64,
}

impl ScanResult {
    /// Construct a [`ScanResult`] from a stream key and its next sequence.
    #[must_use]
    pub const fn new(stream_key: Vec<u8>, next_seq: u64) -> Self {
        Self {
            stream_key,
            next_seq,
        }
    }
}

/// Metadata handed to a [`crate::api::event_store::EventStore::scan`] predicate
/// for one stream: the stream key and its current next-sequence number (equal
/// to the number of events appended to the stream).
#[derive(Clone, Copy, Debug)]
pub struct StreamMeta<'meta> {
    pub stream_key: &'meta [u8],
    pub next_seq: u64,
}
