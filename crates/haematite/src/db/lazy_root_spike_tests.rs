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
//! * `slot_fill_maps_non_empty_roots_and_synthesises_empties` (proptest) — for
//!   random key sets across a high `shard_count`, the ordered per-shard root
//!   vector `commit()` returns is byte-identical to a hand-reconstruction that
//!   places each non-empty shard's committed root at its slot and fills every
//!   other slot with the synthesised `empty_root_hash()` constant. This
//!   validates the SLOT-FILL ARITHMETIC of the lazy `commit()` (id→slot mapping
//!   plus empty synthesis); because both sides derive from the same
//!   already-lazy `commit()`, it is a self-consistency check of that mapping,
//!   NOT an independent eager-vs-lazy diff.
//! * `global_root_identical_lazy_vs_force_materialised` — the genuine
//!   eager-vs-lazy diff. Two independent DBs receive the same keys: DB-A commits
//!   lazily (untouched shards are never spawned and contribute the SYNTHESISED
//!   empty root), while DB-B force-materialises EVERY shard id (spawning every
//!   actor so each empty slot is a REALLY-COMMITTED empty-shard root) before
//!   committing. Their committed global roots must be byte-identical, proving
//!   the synthesised empty root equals a really-committed empty shard's root at
//!   the global-root level — the evidence the self-consistency proptest cannot
//!   supply.
//!
//! If any fails, lazy materialisation MUST NOT be built: the global root
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

/// GATE 1 — the GENUINE eager-vs-lazy diff.
///
/// The self-consistency proptest reconstructs the "eager" side from the same
/// already-lazy `commit()`, so it can only prove the slot-fill arithmetic is
/// internally coherent — it never commits a real empty shard. This test closes
/// that gap by building the eager side for real:
///
/// * DB-A (LAZY): write the keys, then `commit()`. Only the shards that own a
///   key are ever materialised; every untouched slot is filled by the store-free
///   SYNTHESISED `empty_root_hash()` — its actor is never spawned.
/// * DB-B (EAGER): write the SAME keys, then FORCE-MATERIALISE every shard id in
///   `0..shard_count` via `handle_for_shard` (which spawns the actor and runs its
///   real boot), so `commit()` sends a real `Commit` to EVERY shard. Each empty
///   slot is now a REALLY-COMMITTED empty-shard root, not a synthesised constant.
///
/// The committed global roots (the full ordered per-shard root maps) must be
/// byte-identical. That is the fact synthesis depends on and that the
/// self-consistency proptest cannot establish: a really-committed empty shard's
/// root equals the synthesised `empty_root_hash()` at the global-root level.
#[test]
fn global_root_identical_lazy_vs_force_materialised() -> Result<(), Box<dyn Error>> {
    // A handful of key distributions over a low shard count. Low `shard_count`
    // keeps the force-materialise fan-out cheap (every actor is really spawned in
    // DB-B) while still leaving untouched slots that must synthesise/really-commit
    // to the same empty root.
    let shard_count = 8;
    let distributions: [&[&[u8]]; 4] = [
        &[],
        &[b"alpha"],
        &[b"alpha", b"beta", b"gamma"],
        &[b"a", b"bb", b"ccc", b"dddd", b"eeeee", b"ffffff"],
    ];

    for keys in distributions {
        let dir = tempfile::tempdir()?;

        // DB-A: lazy commit. Untouched shards contribute the synthesised empty
        // root — their actors are never spawned.
        let db_a = Database::create(config_for(&dir.path().join("a"), shard_count))?;
        for key in keys {
            db_a.put(key.to_vec(), key.to_vec())?;
        }
        // Sanity: the lazy DB materialised only the shards that own a key, so at
        // least one slot in the committed root IS a synthesised empty (never a
        // committed actor) whenever the keys don't span every shard.
        let mut touched = std::collections::BTreeSet::new();
        for key in keys {
            touched.insert(db_a.shard_for(key));
        }
        assert_eq!(db_a.materialised_shard_ids(), touched_ids(&touched));
        let lazy_roots = db_a.commit()?;

        // DB-B: eager commit. Force-materialise EVERY shard so `commit()` really
        // commits each one — no synthesis is exercised.
        let db_b = Database::create(config_for(&dir.path().join("b"), shard_count))?;
        for key in keys {
            db_b.put(key.to_vec(), key.to_vec())?;
        }
        for shard_id in 0..shard_count {
            // Spawns the actor and runs its real boot; drop the handle — its only
            // purpose is to make the shard materialised so `commit()` visits it.
            let _handle = db_b.handle_for_shard(shard_id)?;
        }
        assert_eq!(
            db_b.materialised_shard_ids(),
            (0..shard_count).collect::<Vec<_>>()
        );
        let eager_roots = db_b.commit()?;

        // The load-bearing assertion: the SYNTHESISED empty slots (DB-A) equal the
        // REALLY-COMMITTED empty shards (DB-B) at the global-root level.
        assert_eq!(
            lazy_roots,
            eager_roots,
            "lazy synthesised global root must equal force-materialised eager root \
             for {} key(s)",
            keys.len(),
        );
        // And the empty case is genuinely exercised on both sides: unless the keys
        // span all 8 shards, some slot was synthesised in DB-A and really-committed
        // in DB-B, yet the two agree.
        assert!(touched.len() < shard_count || keys.len() >= shard_count);
    }
    Ok(())
}

/// The materialised-shard ids a lazy DB should report after touching exactly
/// `touched`: ascending, deduplicated — the shape `materialised_shard_ids`
/// returns.
fn touched_ids(touched: &std::collections::BTreeSet<usize>) -> Vec<usize> {
    touched.iter().copied().collect()
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

    /// GATE 1 (slot-fill arithmetic): the ordered per-shard root vector that the
    /// already-lazy `commit()` returns is byte-identical to a hand-reconstruction
    /// that maps each non-empty shard's committed root to its slot and fills every
    /// other slot with the synthesised empty-root constant. Both sides derive from
    /// the same `commit()`, so this validates the id→slot mapping + empty synthesis
    /// (a self-consistency check), NOT a genuine eager-vs-lazy diff — that is
    /// `global_root_identical_lazy_vs_force_materialised`.
    #[test]
    fn slot_fill_maps_non_empty_roots_and_synthesises_empties(
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
