//! `multi_shard_scaling` — how the core ops behave as the shard count grows
//! across {1, 4, 16} shards.
//!
//! ## What this measures (and the honest finding)
//!
//! haematite shards the keyspace by `BLAKE3(key) % shard_count`, and EACH shard has
//! its own WAL and its own committed prolly tree that it FSYNCs INDEPENDENTLY at
//! commit time. That has a direct, measurable consequence:
//!
//! * **`commit_all` gets SLOWER as the shard count grows.** `Database::commit` fans
//!   out to every shard, and each shard does its own fsync. With more shards there
//!   are more independent fsyncs per commit, so an empty-ish commit's cost scales
//!   with the shard count rather than shrinking. This is a REAL property of the
//!   per-shard durability design, and this benchmark keeps it visible — it does NOT
//!   hide it behind a single aggregate number.
//!
//! * **`put` (buffered) is roughly shard-count-independent.** A single put routes to
//!   exactly one shard and appends to that shard's buffer; the other shards are
//!   untouched. So per-op put cost should stay flat as shards grow (any change is the
//!   routing hash + cache effects, not fan-out).
//!
//! So the two curves tell opposite stories on purpose: routed single-key writes do
//! not pay for extra shards, but a fan-out `commit` does. If your workload commits
//! frequently with few buffered writes, MORE shards costs MORE fsync.
//!
//! ## Honesty notes
//!
//! * Absolute numbers are MACHINE-DEPENDENT (fsync latency dominates `commit_all`).
//!   The portable result is the *trend*: commit_all rising with shard count.
//! * `commit_all` here commits a store that buffered one put per measured iteration
//!   (so the commit has real work on one shard and an empty flush on the rest — which
//!   is exactly the per-shard-fsync cost we want to surface).

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

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use haematite::{Database, DatabaseConfig};

const SHARD_COUNTS: [usize; 3] = [1, 4, 16];

fn fresh_db(shard_count: usize) -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::create(DatabaseConfig {
        data_dir: dir.path().join("db"),
        shard_count,
        sweep_interval: None,
        distributed: None,
    })
    .expect("create db");
    (db, dir)
}

fn bench_multi_shard(c: &mut Criterion) {
    // -- put (buffered): expected ~flat across shard counts -------------------
    let mut put_group = c.benchmark_group("multi_shard_put_buffered");
    for &shards in &SHARD_COUNTS {
        put_group.bench_with_input(
            BenchmarkId::from_parameter(shards),
            &shards,
            |b, &shards| {
                let (db, _dir) = fresh_db(shards);
                let mut counter = 0_u64;
                b.iter(|| {
                    let key = format!("k:{counter:016}").into_bytes();
                    counter += 1;
                    db.put(black_box(key), black_box(b"value".to_vec()))
                        .expect("put");
                });
            },
        );
    }
    put_group.finish();

    // -- commit_all (fsync fan-out): expected to RISE with shard count --------
    // Each iteration buffers ONE put (lands on a single shard) then times the
    // fan-out commit, which fsyncs every shard. More shards ⇒ more fsyncs ⇒ slower.
    let mut commit_group = c.benchmark_group("multi_shard_commit_all");
    for &shards in &SHARD_COUNTS {
        commit_group.bench_with_input(
            BenchmarkId::from_parameter(shards),
            &shards,
            |b, &shards| {
                let (db, _dir) = fresh_db(shards);
                let mut counter = 0_u64;
                b.iter_batched(
                    || {
                        let key = format!("c:{counter:016}").into_bytes();
                        counter += 1;
                        db.put(key, b"value".to_vec()).expect("put");
                    },
                    |()| {
                        let roots = db.commit().expect("commit");
                        black_box(roots);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    commit_group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .sample_size(30);
    targets = bench_multi_shard
}
criterion_main!(benches);
