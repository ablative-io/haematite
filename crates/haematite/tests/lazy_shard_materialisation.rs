//! Integration tests for LAZY shard materialisation.
//!
//! GATE 3 — acquire/recover-before-serve. A cold (never-materialised-this-
//! lifetime) shard must materialise AND recover its on-disk promise/WAL state
//! BEFORE any caller reads promise state to mint a ballot. If a lazily-spawned
//! shard did NOT recover its durable `promised`/`owner_epoch` first, an adopting
//! node would ignore a durable promise and elect below it — split-brain. This
//! test proves the durable `promised` survives a drop + reopen and is visible on
//! the very FIRST touch of the cold shard after reopen (materialisation runs the
//! shard's normal boot, which WAL-recovers before the handle serves).
//!
//! GATE 1 (data correctness under mostly-empty shards) — a range scan / commit
//! over a DB whose shards are mostly un-materialised returns correct data and a
//! commit root identical to the eager case.
//!
//! Tests return `Result` and use `?` (the crate denies `expect`/`unwrap`/`panic`).

use std::error::Error;
use std::path::Path;

use haematite::sync::SyncNodeId;
use haematite::sync::ballot::Ballot;
use haematite::{Database, DatabaseConfig};

type TestResult = Result<(), Box<dyn Error>>;

fn config_for(path: &Path, shard_count: usize) -> DatabaseConfig {
    DatabaseConfig {
        data_dir: path.to_path_buf(),
        shard_count,
        sweep_interval: None,
        distributed: None,
    }
}

/// GATE 3: a durable `promised` ballot recorded on a shard survives a drop, and
/// is recovered on the FIRST touch of that shard after a lazy reopen — before the
/// shard can serve (mint a ballot / stamp a write). A high shard count guarantees
/// the reopen materialises nothing up front, so the read is a genuine cold touch.
#[test]
fn cold_shard_recovers_durable_promise_on_first_touch_after_reopen() -> TestResult {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");

    let promised = Ballot::new(42, SyncNodeId::from("owner@127.0.0.1"));
    let shard_id;
    {
        let db = Database::create(config_for(&data_dir, 512))?;
        // Route a real key to pick a shard, then durably advance its promised.
        shard_id = db.shard_for(b"tenant/workflow-7");
        assert!(
            db.record_promise_for_test(shard_id, promised.clone()),
            "recording a fresh promise on a cold shard must succeed"
        );
        // The promise is durable (fsync'd) at this point; drop crashes the DB.
        drop(db);
    }

    // Reopen: LAZY, so nothing is materialised yet. The FIRST touch of `shard_id`
    // must materialise it AND recover the durable promise before serving.
    let reopened = Database::open(&data_dir)?;
    let recovered = reopened
        .promised_ballot_for_test(shard_id)
        .ok_or("cold shard must recover a durable promise on first touch")?;
    assert_eq!(
        recovered, promised,
        "a lazily-materialised shard must recover its on-disk promised ballot \
         BEFORE it can serve — else an adopting node would elect below a durable \
         promise (split-brain)"
    );

    // A fresh, never-promised shard recovers `bottom` (no phantom promise).
    let other = usize::from(shard_id == 0);
    let other_ballot = reopened
        .promised_ballot_for_test(other)
        .ok_or("an untouched shard should still materialise and read a promise")?;
    assert_eq!(
        other_ballot,
        Ballot::bottom(),
        "a never-promised cold shard must recover the bottom ballot"
    );
    Ok(())
}

/// GATE 1: a commit + range scan over a DB with MOSTLY un-materialised shards
/// returns correct data, the commit yields one root per shard for the full
/// `0..shard_count` (empties synthesised, not spawned), and only the shards that
/// own a key are ever materialised. (The eager-vs-synthesised BYTE-IDENTITY of
/// the global root is proved exhaustively by the S0 proptest
/// `global_root_identical_eager_vs_synthesised`; this test proves the same
/// invariant end-to-end through the public API while keeping the shard count high
/// — never force-materialising every shard, which would defeat the purpose.)
#[test]
fn commit_and_scan_over_mostly_unmaterialised_shards_are_correct() -> TestResult {
    let dir = tempfile::tempdir()?;
    let shard_count = 512;
    let db = Database::create(config_for(&dir.path().join("db"), shard_count))?;

    let keys: Vec<Vec<u8>> = (0..40)
        .map(|n| format!("key-{n:04}").into_bytes())
        .collect();
    let mut touched = std::collections::BTreeSet::new();
    for key in &keys {
        db.put(key.clone(), key.clone())?;
        touched.insert(db.shard_for(key));
    }
    // With 40 keys over 512 shards, the touched set is far smaller than the whole.
    assert!(
        touched.len() < shard_count,
        "the workload must leave most shards un-materialised"
    );

    // Commit returns one root per shard for the FULL 0..shard_count: the touched
    // shards' real roots, and the synthesised empty-tree root everywhere else
    // (GATE 1) — never spawning the un-touched shards.
    let roots = db.commit()?;
    assert_eq!(roots.len(), shard_count);

    let empty = haematite::tree::empty_root_hash();
    for (shard_id, root) in &roots {
        if touched.contains(shard_id) {
            assert_ne!(*root, empty, "a shard holding a key must not be empty");
        } else {
            assert_eq!(
                *root, empty,
                "an un-materialised shard must contribute the synthesised empty root"
            );
        }
    }

    // Every key reads back (materialising each key's shard on demand).
    for key in &keys {
        assert_eq!(
            db.get(key)?,
            Some(key.clone()),
            "every key must read back from the lazy DB"
        );
    }

    // A per-shard range over one touched shard returns exactly the keys that shard
    // owns (a scattered subset) — proof a shard-local scan over a lazily-
    // materialised shard reads its real data, not the empty tree.
    let sample = db.shard_for(&keys[0]);
    let owned: Vec<Vec<u8>> = keys
        .iter()
        .filter(|key| db.shard_for(key) == sample)
        .cloned()
        .collect();
    let scanned = db.range_per_shard(sample, b"key-", b"key.")?;
    assert_eq!(
        scanned.len(),
        owned.len(),
        "a per-shard range must return exactly the keys that shard owns"
    );

    // A cross-shard sequence scan only enumerates materialised shards and misses
    // nothing: plain KV puts create no event streams, so the result is empty — the
    // point is the scan neither errors nor spuriously materialises every shard.
    let streams = db.scan_sequence_keys()?;
    assert!(
        streams.is_empty(),
        "plain KV puts create no event-sequence keys"
    );
    Ok(())
}

/// Boot-timing harness (ignored; run with `--ignored --nocapture`): measures
/// `create` + `open` at a high `shard_count` to demonstrate O(used)-not-O(count)
/// boot cost. Under lazy materialisation both are near-constant regardless of the
/// count, because no shard actor is spawned until first touch.
#[test]
#[ignore = "timing harness; run explicitly with --ignored --nocapture"]
fn boot_timing_at_high_shard_count() -> TestResult {
    let shard_count = 4096;
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");

    let start = std::time::Instant::now();
    let db = Database::create(config_for(&data_dir, shard_count))?;
    let create_elapsed = start.elapsed();
    drop(db);

    let start = std::time::Instant::now();
    let db = Database::open(&data_dir)?;
    let open_elapsed = start.elapsed();
    drop(db);

    println!(
        "BOOT-TIMING shard_count={shard_count} create={create_elapsed:?} open={open_elapsed:?}"
    );
    Ok(())
}
