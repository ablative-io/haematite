//! S0 SPIKE / GATE 1 — the blocking correctness gate for lazy shard
//! materialisation.
//!
//! The committed GLOBAL ROOT is defined over ALL `shard_count` shards:
//! `Database::commit` returns one root hash per shard id `0..shard_count`
//! (`api/kv.rs`), and `ordered_hashes` reassembles them into a dense, ordered
//! vector (`db/helpers.rs`). Lazy materialisation only spawns the shards that
//! are actually written; an un-materialised shard must therefore contribute its
//! deterministic EMPTY-tree root so the global root is BYTE-IDENTICAL to the
//! eager case.
//!
//! This spike proves that invariant WITHOUT any lazy machinery, so it gates the
//! whole feature:
//!
//! * `empty_shard_commit_equals_empty_root_constant` — a real, eagerly-spawned
//!   shard that never received a write commits to exactly `empty_root_hash()`.
//!   This is the fact that makes synthesis sound: the constant we plan to
//!   substitute for an un-materialised shard is the value the actor WOULD have
//!   produced.
//! * `global_root_identical_eager_vs_synthesised` (proptest) — for random key
//!   sets across a high `shard_count`, the ordered per-shard root vector is
//!   byte-identical whether every shard is committed eagerly, or only the
//!   non-empty shards are committed and every empty slot is filled with the
//!   synthesised `empty_root_hash()` constant.
//!
//! If either fails, lazy materialisation MUST NOT be built: the global root
//! would diverge between an eager and a lazy DB holding the same data.

use std::collections::BTreeMap;
use std::error::Error;

use proptest::prelude::*;

use super::{Database, DatabaseConfig};
use crate::tree::{Hash, empty_root_hash};

fn config_for(path: &std::path::Path, shard_count: usize) -> DatabaseConfig {
    DatabaseConfig {
        data_dir: path.to_path_buf(),
        shard_count,
        sweep_interval: None,
        distributed: None,
    }
}

#[test]
fn empty_shard_commit_equals_empty_root_constant() -> Result<(), Box<dyn Error>> {
    // A high shard count with only a handful of keys guarantees most shards are
    // empty; every empty shard's committed root must equal the store-free
    // `empty_root_hash()` constant an un-materialised shard would contribute.
    let dir = tempfile::tempdir()?;
    let shard_count = 64;
    let db = Database::create(config_for(&dir.path().join("db"), shard_count))?;

    db.put(b"alpha".to_vec(), b"1".to_vec())?;
    db.put(b"beta".to_vec(), b"2".to_vec())?;
    db.put(b"gamma".to_vec(), b"3".to_vec())?;

    let mut written = std::collections::BTreeSet::new();
    for key in [b"alpha".as_slice(), b"beta".as_slice(), b"gamma".as_slice()] {
        written.insert(db.shard_for(key));
    }

    let roots = db.commit()?;
    assert_eq!(roots.len(), shard_count);

    let empty = empty_root_hash();
    let mut saw_empty = false;
    for (shard_id, root) in &roots {
        if written.contains(shard_id) {
            assert_ne!(*root, empty, "written shard {shard_id} must not be empty");
        } else {
            assert_eq!(*root, empty, "empty shard {shard_id} must equal constant");
            saw_empty = true;
        }
    }
    assert!(saw_empty, "with 3 keys over 64 shards some shard is empty");
    Ok(())
}

/// Commit `db` and return the dense ordered root vector, the artefact the global
/// root is folded from.
fn eager_ordered_roots(db: &Database) -> Result<Vec<Hash>, Box<dyn Error>> {
    let roots: BTreeMap<usize, Hash> = db.commit()?;
    Ok(roots.into_values().collect())
}

/// Reconstruct the SAME ordered root vector the LAZY path would produce: only the
/// shards that actually own a key contribute their committed root; every other
/// slot is filled with the synthesised `empty_root_hash()` constant, never
/// committed.
fn synthesised_ordered_roots(shard_count: usize, non_empty: &BTreeMap<usize, Hash>) -> Vec<Hash> {
    let empty = empty_root_hash();
    (0..shard_count)
        .map(|shard_id| non_empty.get(&shard_id).copied().unwrap_or(empty))
        .collect()
}

proptest! {
    // Each case spins a fresh multi-shard DB (each shard is a real beamr native
    // process), so the case count and shard ceiling are kept modest: the gate is
    // a CORRECTNESS proof (empty slots synthesise identically), not a load test —
    // 24..48 shards already guarantees the mostly-empty regime the invariant
    // protects, without exhausting the scheduler under proptest fan-out.
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// GATE 1: the global root (the ordered per-shard root vector) is
    /// byte-identical whether every shard is committed eagerly, or only the
    /// non-empty shards are committed and the rest contribute the synthesised
    /// empty-root constant.
    #[test]
    fn global_root_identical_eager_vs_synthesised(
        keys in prop::collection::btree_set(
            prop::collection::vec(any::<u8>(), 1..24),
            0..24,
        ),
        shard_count in 24usize..=48,
    ) {
        let dir = tempfile::tempdir().map_err(fail)?;
        let db = Database::create(config_for(&dir.path().join("db"), shard_count))
            .map_err(fail)?;

        // Which shards genuinely own a key (the only ones a lazy DB materialises).
        let mut touched = std::collections::BTreeSet::new();
        for key in &keys {
            db.put(key.clone(), key.clone()).map_err(fail)?;
            touched.insert(db.shard_for(key));
        }

        // The eager reference: commit ALL shards and take the dense ordered roots.
        let eager = eager_ordered_roots(&db).map_err(fail)?;
        prop_assert_eq!(eager.len(), shard_count);

        // The non-empty shards' actual committed roots, keyed by id.
        let non_empty: BTreeMap<usize, Hash> = eager
            .iter()
            .copied()
            .enumerate()
            .filter(|(shard_id, _)| touched.contains(shard_id))
            .collect();

        // The lazy reconstruction: non-empty roots + synthesised empties.
        let synthesised = synthesised_ordered_roots(shard_count, &non_empty);

        prop_assert_eq!(eager, synthesised);
    }
}

/// Map any setup error inside a proptest body into a `TestCaseError` so the case
/// can propagate it with `?` (keeping the file free of `unwrap`/`expect`).
fn fail<E: std::fmt::Display>(error: E) -> proptest::test_runner::TestCaseError {
    proptest::test_runner::TestCaseError::fail(error.to_string())
}
