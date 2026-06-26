//! `read_heavy` — read-path micro-benchmarks against a LARGE, multi-level tree.
//!
//! The existing `single_shard_ops` read benches (`get_warm`, `range_32`) build a
//! tiny tree (one or a handful of small leaves with 1–5 byte values), so a
//! point read descends ~one node and a range touches ~one leaf. That cannot
//! surface per-node allocation costs on the read path. This bench seeds a tree
//! large enough to be multi-level with realistically-sized values, so a warm
//! point read descends several cached nodes and a range spans many leaves —
//! exercising the node materialisation on every traversal step.
//!
//! Absolute numbers are machine-dependent; use this to compare a before/after of
//! a read-path change on the SAME machine.

#![allow(
    clippy::print_stdout,
    clippy::doc_markdown,
    clippy::missing_panics_doc,
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic
)]

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use haematite::{Database, DatabaseConfig};

const KEYS: u64 = 50_000;
const VALUE_LEN: usize = 256;

/// Build a single-shard store preloaded with `KEYS` committed keys, each with a
/// `VALUE_LEN`-byte value, in one commit. Returns a key known to exist near the
/// middle for point reads.
fn seeded_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::create(DatabaseConfig {
        data_dir: dir.path().join("db"),
        shard_count: 1,
        sweep_interval: None,
        distributed: None,
    })
    .expect("create db");
    let value = vec![0xAB_u8; VALUE_LEN];
    for i in 0..KEYS {
        db.put(format!("k:{i:016}").into_bytes(), value.clone())
            .expect("put");
    }
    db.commit().expect("commit");
    (db, dir)
}

fn bench_read_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_heavy");

    // -- get (warm, deep tree): point read descending a multi-level tree from a
    //    warm cache. The path to a single hot key stays resident, so this isolates
    //    the per-node materialisation cost of a descent, not disk/fsync. --------
    group.bench_function("get_warm_deep", |b| {
        let (db, _dir) = seeded_db();
        let hot = format!("k:{:016}", KEYS / 2).into_bytes();
        // Warm the path.
        let _ = db.get(&hot).expect("get");
        b.iter(|| {
            let value = db.get(black_box(&hot)).expect("get");
            black_box(value);
        });
    });

    // -- range (512 keys across many leaves): a [from,to) scan that spans many
    //    leaves of the large tree, so every traversed leaf is materialised. -----
    group.bench_function("range_512", |b| {
        let (db, _dir) = seeded_db();
        let from = format!("k:{:016}", 10_000_u64).into_bytes();
        let to = format!("k:{:016}", 10_512_u64).into_bytes();
        b.iter(|| {
            let entries = db.range(black_box(&from), black_box(&to)).expect("range");
            black_box(entries);
        });
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(50);
    targets = bench_read_heavy
}
criterion_main!(benches);
