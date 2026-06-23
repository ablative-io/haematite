//! API-001 `EventStore` tests over the real persistent [`Database`].
//!
//! Every test drives a real [`Database`] backed by on-disk shard actors. No
//! `BTreeMap` or in-memory mock stands in for the engine; round-trips, conflicts,
//! the concurrent-CAS race, and persistence-across-reopen are all exercised
//! against the genuine storage path.

use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use tempfile::TempDir;

use super::{Event, EventStore, decode_stream_key, encode_stream_key};
use crate::api::error::ApiError;
use crate::api::types::StreamMeta;
use crate::db::{Database, DatabaseConfig};

type TestResult = Result<(), Box<dyn Error>>;

fn new_store(dir: &TempDir, shard_count: usize) -> Result<EventStore, Box<dyn Error>> {
    let config = DatabaseConfig {
        data_dir: dir.path().to_path_buf(),
        shard_count,
        sweep_interval: None,
    };
    Ok(EventStore::new(Database::create(config)?))
}

fn new_ttl_store(dir: &TempDir, shard_count: usize) -> Result<EventStore, Box<dyn Error>> {
    let config = DatabaseConfig {
        data_dir: dir.path().to_path_buf(),
        shard_count,
        // Read-time-filtering tests: a long interval keeps the sweep from firing
        // during the sub-second test (its first tick is `interval` away), avoiding
        // scheduler load. Physical sweeping is covered by ttl::sweep's own test.
        sweep_interval: Some(60_000),
    };
    Ok(EventStore::new(Database::create(config)?))
}

// ---- R2: event stream key format -----------------------------------------

#[test]
fn encode_decode_round_trips() -> TestResult {
    let encoded = encode_stream_key(b"orders", 42);
    let (key, seq) = decode_stream_key(&encoded).ok_or("decode failed")?;
    assert_eq!(key, b"orders");
    assert_eq!(seq, 42);
    Ok(())
}

#[test]
fn same_stream_sorts_by_sequence() {
    let low = encode_stream_key(b"s", 1);
    let high = encode_stream_key(b"s", 2);
    assert!(low < high);
    let very_high = encode_stream_key(b"s", u64::from(u32::MAX) + 1);
    assert!(high < very_high);
}

#[test]
fn different_streams_sort_by_stream_key_first() {
    // Even with a tiny seq on stream "a" and a huge seq on stream "b",
    // stream_key dominates the ordering.
    let a_high = encode_stream_key(b"a", u64::MAX);
    let b_low = encode_stream_key(b"b", 0);
    assert!(a_high < b_low);
}

#[test]
fn decode_rejects_short_or_unseparated_keys() {
    assert!(decode_stream_key(b"short").is_none());
    // 9 trailing bytes but no 0x00 separator at the right spot.
    let mut bogus = vec![b'x'; 9];
    bogus[0] = 0x01;
    assert!(decode_stream_key(&bogus).is_none());
}

// ---- R3: atomic append with optimistic concurrency ------------------------

#[test]
fn append_round_trip_recovers_payload_seq_and_timestamp() -> TestResult {
    let dir = TempDir::new()?;
    let store = new_store(&dir, 2)?;

    let next = store.append(b"stream", b"hello", 0)?;
    assert_eq!(next, 1);

    let events = store.read(b"stream")?;
    assert_eq!(events.len(), 1);
    let event = events.first().ok_or("missing event")?;
    assert_eq!(event.seq, 0);
    assert_eq!(event.payload, b"hello");
    assert!(event.timestamp > 0, "timestamp must be recoverable on read");
    Ok(())
}

#[test]
fn append_then_read_ten_events_in_order() -> TestResult {
    let dir = TempDir::new()?;
    let store = new_store(&dir, 3)?;

    for index in 0..10_u64 {
        let payload = format!("event-{index}");
        let next = store.append(b"log", payload.as_bytes(), index)?;
        assert_eq!(next, index + 1);
    }

    let events = store.read(b"log")?;
    assert_eq!(events.len(), 10);
    for (index, event) in events.iter().enumerate() {
        let index = u64::try_from(index)?;
        assert_eq!(event.seq, index);
        assert_eq!(event.payload, format!("event-{index}").into_bytes());
    }
    Ok(())
}

#[test]
fn stale_expected_seq_returns_conflict_without_mutating() -> TestResult {
    let dir = TempDir::new()?;
    let store = new_store(&dir, 1)?;

    store.append(b"acct", b"first", 0)?;
    store.append(b"acct", b"second", 1)?;

    // Stale: stream is at 2, caller thinks it is at 1.
    let result = store.append(b"acct", b"clobber", 1);
    match result {
        Err(ApiError::SequenceConflict(conflict)) => {
            assert_eq!(conflict.expected, 1);
            assert_eq!(conflict.actual, 2);
        }
        other => return Err(format!("expected SequenceConflict, got {other:?}").into()),
    }

    // The stream must be UNMODIFIED: still exactly the two original events.
    let events = store.read(b"acct")?;
    let payloads: Vec<&[u8]> = events
        .iter()
        .map(|event| event.payload.as_slice())
        .collect();
    assert_eq!(payloads, vec![b"first".as_slice(), b"second".as_slice()]);
    Ok(())
}

#[test]
fn batch_append_advances_sequence_by_batch_len() -> TestResult {
    let dir = TempDir::new()?;
    let store = new_store(&dir, 1)?;

    let payloads: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e"];
    let next = store.append_batch(b"batch", &payloads, 0)?;
    assert_eq!(next, 5, "5 events => next seq is expected_seq + len");

    let events = store.read(b"batch")?;
    assert_eq!(events.len(), 5);
    assert_eq!(events.last().ok_or("missing")?.seq, 4);
    Ok(())
}

// ---- R4 / R5: read and read_from ------------------------------------------

#[test]
fn read_unknown_stream_is_empty() -> TestResult {
    let dir = TempDir::new()?;
    let store = new_store(&dir, 2)?;
    assert!(store.read(b"nope")?.is_empty());
    Ok(())
}

#[test]
fn read_from_returns_only_tail() -> TestResult {
    let dir = TempDir::new()?;
    let store = new_store(&dir, 2)?;
    for index in 0..10_u64 {
        store.append(b"s", format!("v{index}").as_bytes(), index)?;
    }

    let tail = store.read_from(b"s", 5)?;
    assert_eq!(tail.len(), 5);
    assert_eq!(tail.first().ok_or("missing")?.seq, 5);
    assert_eq!(tail.last().ok_or("missing")?.seq, 9);

    assert_eq!(store.read_from(b"s", 0)?.len(), 10);
    assert!(store.read_from(b"s", 100)?.is_empty());
    Ok(())
}

// ---- R6: scan across all shards -------------------------------------------

#[test]
fn scan_matches_subset_across_shards() -> TestResult {
    let dir = TempDir::new()?;
    let store = new_store(&dir, 4)?;

    // 100 distinct streams; mark 3 with a longer history.
    for index in 0..100_u64 {
        let key = format!("stream-{index:03}");
        store.append(key.as_bytes(), b"x", 0)?;
    }
    for key in [b"stream-007".as_slice(), b"stream-042", b"stream-099"] {
        // append a second event so next_seq == 2 for exactly these three.
        store.append(key, b"y", 1)?;
    }

    let matches = store.scan(|meta: StreamMeta<'_>| meta.next_seq == 2)?;
    assert_eq!(matches.len(), 3, "exactly three streams have two events");
    let mut keys: Vec<Vec<u8>> = matches.iter().map(|m| m.stream_key.clone()).collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            b"stream-007".to_vec(),
            b"stream-042".to_vec(),
            b"stream-099".to_vec()
        ]
    );

    // A predicate matching nothing yields empty.
    assert!(store.scan(|meta| meta.next_seq == 9999)?.is_empty());
    Ok(())
}

#[test]
fn scan_visits_every_shard() -> TestResult {
    let dir = TempDir::new()?;
    let store = new_store(&dir, 8)?;
    // Enough distinct keys that, by hashing, every one of 8 shards is hit.
    let mut expected = Vec::new();
    for index in 0..200_u64 {
        let key = format!("k-{index}");
        store.append(key.as_bytes(), b"v", 0)?;
        expected.push(key.into_bytes());
    }
    let all = store.scan(|_| true)?;
    assert_eq!(all.len(), expected.len(), "scan returns every stream");
    Ok(())
}

// ---- R7: compare-and-swap -------------------------------------------------

#[test]
fn cas_creates_then_swaps() -> TestResult {
    let dir = TempDir::new()?;
    let store = new_store(&dir, 2)?;

    assert_eq!(store.read_value(b"counter")?, None);
    // Create with expected = None.
    store.cas(b"counter", None, 1)?;
    assert_eq!(store.read_value(b"counter")?, Some(1));
    // Swap 1 -> 2.
    store.cas(b"counter", Some(1), 2)?;
    assert_eq!(store.read_value(b"counter")?, Some(2));
    Ok(())
}

#[test]
fn cas_mismatch_reports_actual_and_does_not_write() -> TestResult {
    let dir = TempDir::new()?;
    let store = new_store(&dir, 1)?;
    store.cas(b"k", None, 10)?;

    match store.cas(b"k", Some(99), 20) {
        Err(ApiError::CasMismatch(mismatch)) => {
            assert_eq!(mismatch.expected, Some(99));
            assert_eq!(mismatch.actual, Some(10));
        }
        other => return Err(format!("expected CasMismatch, got {other:?}").into()),
    }
    // Unchanged.
    assert_eq!(store.read_value(b"k")?, Some(10));
    Ok(())
}

/// Two threads race the SAME key with the SAME expected value; exactly one
/// must win. This is the load-bearing atomicity proof: a get-then-put from the
/// Database side would let both read the old value and both succeed. Because
/// the read-compare-write happens inside the single-threaded shard actor, only
/// one wins.
#[test]
fn concurrent_cas_exactly_one_winner() -> TestResult {
    let dir = TempDir::new()?;
    let store = Arc::new(new_store(&dir, 1)?);
    store.cas(b"race", None, 0)?;

    let rounds = 200_u64;
    for round in 0..rounds {
        let barrier = Arc::new(Barrier::new(2));
        let winners = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let winners = Arc::clone(&winners);
            handles.push(thread::spawn(move || {
                barrier.wait();
                // Both threads attempt round -> round+1 from the same expected.
                if store.cas(b"race", Some(round), round + 1).is_ok() {
                    winners.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }
        for handle in handles {
            handle.join().map_err(|_| "cas thread panicked")?;
        }
        assert_eq!(
            winners.load(Ordering::SeqCst),
            1,
            "round {round}: exactly one CAS must win the race"
        );
        assert_eq!(store.read_value(b"race")?, Some(round + 1));
    }
    Ok(())
}

// ---- persistence across reopen --------------------------------------------

#[test]
fn events_and_cas_survive_reopen() -> TestResult {
    let dir = TempDir::new()?;
    let path = dir.path().to_path_buf();

    {
        let store = new_store(&dir, 2)?;
        store.append(b"durable", b"one", 0)?;
        store.append(b"durable", b"two", 1)?;
        store.cas(b"scalar", None, 7)?;
        store.flush()?;
        // drop store (and database) -> shards shut down.
    }

    // Reopen from disk: a brand-new Database over the same directory.
    let store = EventStore::new(Database::open(&path)?);
    let events: Vec<Event> = store.read(b"durable")?;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].payload, b"one");
    assert_eq!(events[1].payload, b"two");
    assert_eq!(events[1].seq, 1);
    assert_eq!(store.read_value(b"scalar")?, Some(7));
    Ok(())
}

#[test]
fn event_value_envelope_preserves_empty_payload() -> TestResult {
    let dir = TempDir::new()?;
    let store = new_store(&dir, 1)?;
    store.append(b"empty", b"", 0)?;
    let events = store.read(b"empty")?;
    assert_eq!(events.len(), 1);
    assert!(events[0].payload.is_empty());
    Ok(())
}

#[test]
fn event_store_ttl_filters_expired_events_and_reports_compaction() -> TestResult {
    let dir = TempDir::new()?;
    let store = new_ttl_store(&dir, 1)?;

    store.append_with_ttl(b"expired", b"gone", 0, Some(Duration::ZERO))?;
    match store.read(b"expired") {
        Err(ApiError::HistoryCompacted(compacted)) => {
            assert_eq!(compacted.stream_key, b"expired".to_vec());
        }
        other => return Err(format!("expected HistoryCompacted, got {other:?}").into()),
    }

    assert!(store.read(b"missing")?.is_empty());
    Ok(())
}

#[test]
fn event_store_ttl_reports_compaction_across_shards() -> TestResult {
    // Regression: a stream's next-seq metadata is written into the shard of the
    // STREAM key (where `append` routes), so reading it must route on the stream
    // key too — not on the differently-hashed sequence-metadata key. With more
    // than one shard, routing the read on the seq key lands on the wrong shard,
    // reads None, and a fully-expired stream silently returns empty instead of
    // HistoryCompacted (violating R5). Sixteen keys over four shards make the
    // divergent-shard case certain to be exercised.
    let dir = TempDir::new()?;
    let store = new_ttl_store(&dir, 4)?;

    for index in 0..16u32 {
        let key = format!("stream-{index}").into_bytes();
        store.append_with_ttl(key.as_slice(), b"gone", 0, Some(Duration::ZERO))?;
        match store.read(key.as_slice()) {
            Err(ApiError::HistoryCompacted(compacted)) => {
                assert_eq!(compacted.stream_key, key);
            }
            other => {
                return Err(
                    format!("stream {index}: expected HistoryCompacted, got {other:?}").into(),
                );
            }
        }
    }
    Ok(())
}

#[test]
fn event_store_ttl_keeps_live_tail_without_compaction_error() -> TestResult {
    let dir = TempDir::new()?;
    let store = new_ttl_store(&dir, 1)?;

    store.append_with_ttl(b"mixed", b"old", 0, Some(Duration::ZERO))?;
    store.append_with_ttl(b"mixed", b"live", 1, Some(Duration::from_secs(60)))?;

    let events = store.read(b"mixed")?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].seq, 1);
    assert_eq!(events[0].payload, b"live");
    Ok(())
}

#[test]
fn event_store_scan_excludes_streams_with_no_live_events() -> TestResult {
    let dir = TempDir::new()?;
    let store = new_ttl_store(&dir, 2)?;

    store.append_with_ttl(b"gone", b"expired", 0, Some(Duration::ZERO))?;
    store.append_with_ttl(b"live", b"visible", 0, Some(Duration::from_secs(60)))?;

    let mut streams: Vec<Vec<u8>> = store
        .scan(|_| true)?
        .into_iter()
        .map(|result| result.stream_key)
        .collect();
    streams.sort();
    assert_eq!(streams, vec![b"live".to_vec()]);
    Ok(())
}
