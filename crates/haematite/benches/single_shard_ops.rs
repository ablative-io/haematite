//! `single_shard_ops` — micro-benchmarks for the core single-node operations on a
//! one-shard [`Database`].
//!
//! ## What this measures (and what it does NOT)
//!
//! Each benchmark times ONE logical operation against a real on-disk store in a
//! temp directory. The numbers are dominated by:
//!
//! * **put (buffered)** — append one mutation to the in-memory WAL buffer + the
//!   durable WAL. This includes the per-op WAL append/fsync of the operation log,
//!   but NOT a tree flush. It is the cheap path.
//! * **commit (fsync)** — flush the buffered writes into the prolly tree and FSYNC
//!   the new committed root. This is the EXPENSIVE part of the write path and is
//!   measured explicitly and separately from `put`: we buffer one put, then time the
//!   `commit` alone. On a real disk this is a physical `fsync` and is the dominant
//!   cost; on a machine with a fast SSD / write-back cache it is far cheaper than on
//!   spinning rust. Treat the absolute number as MACHINE-DEPENDENT.
//! * **get (warm)** — read one key already committed to the tree, served from the
//!   warm in-memory node cache. No fsync.
//! * **cas** — one scalar compare-and-swap (read-compare-write inside the shard
//!   actor), which durably persists the new value.
//! * **append (single)** — append one event (OCC sequence check + durable apply).
//! * **append (batch-16)** — append 16 events as ONE atomic commit; the per-event
//!   amortized cost is far below a single append because they share one fsync.
//! * **range (32)** — a shard-local `[from, to)` scan returning 32 committed keys.
//!
//! ## Honesty notes
//!
//! * These are SINGLE-NODE numbers: no replication, no network. Replicated paths are
//!   in `replicated_write` / `replicated_append`.
//! * Absolute timings depend heavily on the host's fsync latency (disk, filesystem,
//!   and whether the OS honours the flush). The SHAPE (commit ≫ put, batch ≪ N×single)
//!   is the portable takeaway, not the nanosecond counts.
//! * Each measured iteration writes to a UNIQUE key/stream where the operation is
//!   destructive (put/cas/append), so repeated iterations don't degenerate into
//!   no-ops or unbounded stream growth within a single sample batch.

#![allow(
    clippy::print_stdout,
    clippy::doc_markdown,
    clippy::missing_panics_doc,
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::significant_drop_tightening
)]

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use haematite::{Database, DatabaseConfig};

/// Create a fresh single-shard store in a temp dir. The returned `TempDir` guard
/// must be kept alive for the store's lifetime.
fn fresh_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::create(DatabaseConfig {
        data_dir: dir.path().join("db"),
        shard_count: 1,
        sweep_interval: None,
        distributed: None,
    })
    .expect("create db");
    (db, dir)
}

fn bench_single_shard(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_shard_ops");

    // -- put (buffered): one WAL-buffer append, no tree flush ------------------
    group.bench_function("put_buffered", |b| {
        let (db, _dir) = fresh_db();
        let mut counter = 0_u64;
        b.iter(|| {
            let key = format!("k:{counter:016}").into_bytes();
            counter += 1;
            db.put(black_box(key), black_box(b"value".to_vec()))
                .expect("put");
        });
    });

    // -- commit (fsync): the EXPENSIVE part, measured alone --------------------
    // Each iteration buffers exactly one put, then times the commit (the flush +
    // root fsync) only. The put is done OUTSIDE the timed closure via `iter_batched`
    // so the measurement isolates the commit/fsync cost.
    group.bench_function("commit_fsync", |b| {
        let (db, _dir) = fresh_db();
        let mut counter = 0_u64;
        b.iter_batched(
            || {
                let key = format!("c:{counter:016}").into_bytes();
                counter += 1;
                db.put(key, b"value".to_vec()).expect("put");
            },
            |()| {
                db.commit().expect("commit");
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // -- get (warm): read a committed key from the warm cache ------------------
    group.bench_function("get_warm", |b| {
        let (db, _dir) = fresh_db();
        db.put(b"hot".to_vec(), b"value".to_vec()).expect("put");
        db.commit().expect("commit");
        // Warm the cache.
        let _ = db.get(b"hot").expect("get");
        b.iter(|| {
            let value = db.get(black_box(b"hot")).expect("get");
            black_box(value);
        });
    });

    // -- cas: one scalar compare-and-swap (durable) ---------------------------
    group.bench_function("cas", |b| {
        let (db, _dir) = fresh_db();
        // Seed the counter; each iteration swaps prev -> prev+1 on a unique key so it
        // always matches and always writes.
        let mut counter = 0_u64;
        b.iter(|| {
            let key = format!("cas:{counter:016}").into_bytes();
            counter += 1;
            db.cas(black_box(key), None, 1).expect("cas");
        });
    });

    // -- append (single event) ------------------------------------------------
    group.bench_function("append_single", |b| {
        let (db, _dir) = fresh_db();
        let mut counter = 0_u64;
        b.iter(|| {
            // A fresh stream per iteration so expected_seq is always 0 and the append
            // always commits one event.
            let stream = format!("s:{counter:016}").into_bytes();
            counter += 1;
            let next = db
                .append(black_box(stream), black_box(vec![b"event".to_vec()]), 0)
                .expect("append");
            black_box(next);
        });
    });

    // -- append (batch of 16): one atomic commit for 16 events ----------------
    group.bench_function("append_batch_16", |b| {
        let (db, _dir) = fresh_db();
        let payloads: Vec<Vec<u8>> = (0..16).map(|i| format!("event-{i}").into_bytes()).collect();
        let mut counter = 0_u64;
        b.iter(|| {
            let stream = format!("b:{counter:016}").into_bytes();
            counter += 1;
            let next = db
                .append(black_box(stream), black_box(payloads.clone()), 0)
                .expect("append batch");
            black_box(next);
        });
    });

    // -- range (32 committed keys) --------------------------------------------
    group.bench_function("range_32", |b| {
        let (db, dir) = fresh_db();
        // Seed 32 keys under one prefix that all live on shard 0 (single-shard store).
        for i in 0..32_u64 {
            db.put(format!("r:{i:016}").into_bytes(), b"v".to_vec())
                .expect("put");
        }
        db.commit().expect("commit");
        let from = b"r:".to_vec();
        let to = b"r;".to_vec();
        b.iter(|| {
            let entries = db.range(black_box(&from), black_box(&to)).expect("range");
            black_box(entries);
        });
        drop(dir);
    });

    // -- concurrent same-shard writers: the GROUP COMMIT win (audit E) ----------
    // N client threads each issue ONE durable CAS against the SAME single-shard
    // store at once, so several groupable writes are QUEUED before the shard actor
    // drains them. With group commit the actor coalesces a queued run into ONE
    // fsync instead of one fsync per write — the win only shows under concurrent
    // queueing, which the blocking single-op benches above never produce. We time
    // the wall-clock for all N writes to complete. Before group commit this was
    // ~N × one fsync; after, it trends toward ~one fsync per drained group.
    bench_concurrent_writers(&mut group, 8);
    bench_concurrent_writers(&mut group, 32);

    group.finish();
}

/// Time `writers` client threads each committing one durable CAS to the SAME
/// single-shard store concurrently, so the writes queue and the shard coalesces
/// them into group commits.
fn bench_concurrent_writers(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    writers: usize,
) {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    group.bench_function(format!("concurrent_writers_{writers}"), |b| {
        let (db, _dir) = fresh_db();
        let db = Arc::new(db);
        // A monotonic key counter so every CAS targets a distinct (always-absent)
        // key on the one shard and therefore always matches `expected = None`.
        let next_key = AtomicU64::new(0);
        b.iter(|| {
            std::thread::scope(|scope| {
                for _ in 0..writers {
                    let db = Arc::clone(&db);
                    let id = next_key.fetch_add(1, Ordering::Relaxed);
                    scope.spawn(move || {
                        let key = format!("cw:{id:016}").into_bytes();
                        db.cas(black_box(key), None, 1).expect("cas");
                    });
                }
            });
        });
    });
}

criterion_group! {
    name = benches;
    // A short, low-overhead config so a `cargo bench` smoke run produces numbers
    // quickly. For a full statistical run, raise sample_size / measurement_time.
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .sample_size(30);
    targets = bench_single_shard
}
criterion_main!(benches);
