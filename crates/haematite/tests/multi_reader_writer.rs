//! End-to-end demonstration that a single-node haematite `Database` supports
//! many concurrent writers spread across shards together with many concurrent
//! readers, with no lost updates and no inconsistent reads.
//!
//! This is a real-OS-thread test: every writer and reader is its own
//! `std::thread`, all sharing one `Arc<Database>`. It proves three things:
//!   1. The handle is genuinely shareable across threads (Send + Sync).
//!   2. Writers whose keys hash to different shards run against different shard
//!      actors — verified by asserting the key set covers every shard.
//!   3. Under concurrent reads, every key still reaches its exact final value
//!      (no lost updates) and readers never observe an out-of-range/garbage
//!      value (consistent reads off the content-addressed tree).
//!
//! Marked `#[ignore]`: every live op is a shard-actor mailbox round-trip, so a
//! thorough soak runs for tens of seconds — too slow for the default suite
//! (the lib's `concurrent_cas_exactly_one_winner` etc. cover that). Run on
//! demand: `cargo test -p haematite --test multi_reader_writer -- --ignored`.

use std::collections::BTreeSet;
use std::sync::{Arc, Barrier};
use std::thread;

use haematite::db::{Database, DatabaseConfig};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const SHARDS: usize = 4;
const WRITERS: usize = 6;
const READERS: usize = 4;
const KEYS_PER_WRITER: u64 = 24;
const ROUNDS: u64 = 20;
const READER_PASSES: usize = 10;

fn key_for(writer: usize, index: u64) -> Vec<u8> {
    format!("w{writer:02}-k{index:04}").into_bytes()
}

fn config(dir: &std::path::Path) -> DatabaseConfig {
    DatabaseConfig {
        data_dir: dir.to_path_buf(),
        shard_count: SHARDS,
        sweep_interval: None,
        distributed: None,
    }
}

#[test]
#[ignore = "concurrency soak: tens of seconds of shard-actor round-trips; run with --ignored"]
fn many_writers_across_shards_with_concurrent_readers() -> TestResult {
    let dir = tempfile::tempdir()?;
    let db = Arc::new(Database::create(config(dir.path()))?);

    // Sanity: the full key set must actually span every shard, otherwise this
    // would not be testing cross-shard parallelism at all.
    let mut shards_hit = BTreeSet::new();
    for w in 0..WRITERS {
        for i in 0..KEYS_PER_WRITER {
            shards_hit.insert(db.shard_for(&key_for(w, i)));
        }
    }
    assert_eq!(
        shards_hit.len(),
        SHARDS,
        "test keys must cover all {SHARDS} shards (covered: {shards_hit:?})"
    );

    // All writers and readers start together so reads genuinely overlap writes.
    let start = Arc::new(Barrier::new(WRITERS + READERS));
    let mut handles: Vec<thread::JoinHandle<Result<(), String>>> = Vec::new();

    // Writers: each owns a DISJOINT key range (no inter-writer contention), and
    // advances each of its keys 0 -> ROUNDS via compare-and-swap. Because the
    // ranges are disjoint, every CAS must succeed; any lost update would leave a
    // key below ROUNDS and fail the final assertion.
    for w in 0..WRITERS {
        let db = Arc::clone(&db);
        let start = Arc::clone(&start);
        handles.push(thread::spawn(move || -> Result<(), String> {
            start.wait();
            for i in 0..KEYS_PER_WRITER {
                let key = key_for(w, i);
                db.cas(key.clone(), None, 0)
                    .map_err(|e| format!("create: {e}"))?;
                for round in 0..ROUNDS {
                    db.cas(key.clone(), Some(round), round + 1)
                        .map_err(|e| format!("advance: {e}"))?;
                }
            }
            Ok(())
        }));
    }

    // Readers: hammer the whole key space concurrently with the writers. A key
    // may not exist yet (None) — fine. Whenever a value IS present it must be a
    // valid sequence in 0..=ROUNDS; a torn/garbage read would fall outside that.
    for _ in 0..READERS {
        let db = Arc::clone(&db);
        let start = Arc::clone(&start);
        handles.push(thread::spawn(move || -> Result<(), String> {
            start.wait();
            for _pass in 0..READER_PASSES {
                for w in 0..WRITERS {
                    for i in 0..KEYS_PER_WRITER {
                        let observed = db
                            .read_value(&key_for(w, i))
                            .map_err(|e| format!("read: {e}"))?;
                        match observed {
                            Some(value) if value > ROUNDS => {
                                return Err(format!("out-of-range value {value} (> {ROUNDS})"));
                            }
                            _ => {}
                        }
                    }
                }
            }
            Ok(())
        }));
    }

    for handle in handles {
        handle
            .join()
            .map_err(|_| "worker thread panicked")?
            .map_err(|e| format!("worker failed: {e}"))?;
    }

    // No lost updates: every key reached its exact final value despite all the
    // concurrent reads and cross-shard parallelism.
    for w in 0..WRITERS {
        for i in 0..KEYS_PER_WRITER {
            let key = key_for(w, i);
            assert_eq!(
                db.read_value(&key)?,
                Some(ROUNDS),
                "key {} did not reach final value",
                String::from_utf8_lossy(&key)
            );
        }
    }
    Ok(())
}
