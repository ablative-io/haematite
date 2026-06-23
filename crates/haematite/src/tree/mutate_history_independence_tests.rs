//! Property-based stress test for the MOST IMPORTANT correctness invariant of
//! the prolly tree: **history-independence**.
//!
//! History-independence means the root hash for a given SET of key->value pairs
//! MUST be identical regardless of the ORDER of the insertions/deletions that
//! produced it. Fork, merge and sync all depend on this: if two replicas reach
//! the same logical state via different operation orders but get different root
//! hashes, then merge/sync/equality are all broken.
//!
//! The suspected danger zone is MULTI-LEAF trees. The production `batch_mutate`
//! path hard-codes `BoundaryDetector::default()` (target_size 4096), so small
//! test trees stay single-leaf and never exercise node splitting/rewriting.
//! These tests use the test-only `set_test_target_size` seam (see `mutate.rs`)
//! to drive the REAL mutate code path with a small target_size, so that a few
//! dozen keys produce genuinely multi-leaf trees.

#![allow(clippy::unwrap_used)]
#![allow(clippy::panic)]

use std::collections::BTreeMap;

use proptest::prelude::*;

use super::mutate::{batch_mutate, set_test_target_size};
use super::node::{Hash, Node};
use crate::store::MemoryStore;
use crate::tree::cursor::load_node;

/// Build an empty tree (an empty leaf) and return (store, root_hash).
fn empty_tree() -> (MemoryStore, Hash) {
    let mut store = MemoryStore::new();
    // An empty leaf is the canonical empty root.
    let leaf = super::node::LeafNode::new(Vec::new()).unwrap();
    let hash = store.put(&Node::Leaf(leaf));
    (store, hash)
}

/// Apply a sequence of operations (Some(value) = put, None = delete) one at a
/// time, returning the final root hash. Applying ops one-at-a-time (rather than
/// a single batch) is what stresses the *incremental* rewrite path.
fn apply_ops(target_size: usize, ops: &[(Vec<u8>, Option<Vec<u8>>)]) -> Hash {
    set_test_target_size(Some(target_size));
    let (mut store, mut root) = empty_tree();
    for (key, value) in ops {
        let mutation = [(key.clone(), value.clone())];
        root = batch_mutate(&mut store, root, &mutation).unwrap();
    }
    set_test_target_size(None);
    root
}

/// Read every key->value pair out of a tree by walking all leaves.
fn collect_map(target_size: usize, root: Hash, ops: &[(Vec<u8>, Option<Vec<u8>>)]) -> ReadBack {
    set_test_target_size(Some(target_size));
    // Rebuild the store deterministically so we can read leaves directly.
    let (mut store, mut r) = empty_tree();
    for (key, value) in ops {
        let mutation = [(key.clone(), value.clone())];
        r = batch_mutate(&mut store, r, &mutation).unwrap();
    }
    assert_eq!(r, root);
    let mut map = BTreeMap::new();
    let mut leaf_count = 0usize;
    walk(&store, root, &mut map, &mut leaf_count);
    set_test_target_size(None);
    ReadBack { map, leaf_count, root }
}

struct ReadBack {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
    leaf_count: usize,
    root: Hash,
}

fn walk(
    store: &MemoryStore,
    hash: Hash,
    out: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    leaf_count: &mut usize,
) {
    match load_node(store, hash).unwrap() {
        Node::Leaf(leaf) => {
            *leaf_count += 1;
            for (k, v) in leaf.entries() {
                out.insert(k.clone(), v.clone());
            }
        }
        Node::Internal(internal) => {
            for (_sep, child) in internal.children() {
                walk(store, *child, out, leaf_count);
            }
        }
    }
}

/// True if the root node is internal with > 1 child (i.e. genuinely multi-leaf
/// at the top level).
fn root_is_multi_leaf(target_size: usize, ops: &[(Vec<u8>, Option<Vec<u8>>)]) -> bool {
    set_test_target_size(Some(target_size));
    let (mut store, mut root) = empty_tree();
    for (key, value) in ops {
        let mutation = [(key.clone(), value.clone())];
        root = batch_mutate(&mut store, root, &mutation).unwrap();
    }
    let result = matches!(load_node(&store, root).unwrap(), Node::Internal(i) if i.children().len() > 1);
    set_test_target_size(None);
    result
}

/// Generate `n` distinct keys with random values. Keys are 4-byte big-endian
/// integers so they are easy to make distinct and to permute.
fn key_value_set(
    n_range: std::ops::Range<usize>,
) -> impl Strategy<Value = Vec<(Vec<u8>, Vec<u8>)>> {
    proptest::collection::btree_map(
        proptest::collection::vec(any::<u8>(), 1..6),
        proptest::collection::vec(any::<u8>(), 0..6),
        n_range,
    )
    .prop_map(|m| m.into_iter().collect::<Vec<_>>())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 300, max_shrink_iters: 20000, ..ProptestConfig::default() })]

    /// PROPERTY A — insertion order independence.
    /// The same SET of key->value pairs inserted in two different random
    /// permutations must yield the same root hash.
    #[test]
    fn prop_a_insertion_order_independence(
        target_size in prop::sample::select(vec![4usize, 8, 16, 4096]),
        pairs in key_value_set(1..200),
        perm1 in any::<prop::sample::Index>(),
        perm2 in any::<prop::sample::Index>(),
    ) {
        // Two independent random permutations of the same pair set.
        let order1 = shuffle_with(&pairs, perm1.index(usize::MAX));
        let order2 = shuffle_with(&pairs, perm2.index(usize::MAX).wrapping_add(1));

        let ops1: Vec<(Vec<u8>, Option<Vec<u8>>)> =
            order1.iter().map(|(k, v)| (k.clone(), Some(v.clone()))).collect();
        let ops2: Vec<(Vec<u8>, Option<Vec<u8>>)> =
            order2.iter().map(|(k, v)| (k.clone(), Some(v.clone()))).collect();

        let h1 = apply_ops(target_size, &ops1);
        let h2 = apply_ops(target_size, &ops2);

        prop_assert_eq!(
            h1, h2,
            "insertion order changed the root hash (target_size={}, n={})\norder1={:?}\norder2={:?}",
            target_size, pairs.len(), order1, order2
        );
    }

    /// PROPERTY B — insert+delete history independence.
    /// Two different put/delete sequences that result in the SAME final
    /// key->value map must yield the same root hash.
    #[test]
    fn prop_b_insert_delete_history_independence(
        target_size in prop::sample::select(vec![4usize, 8, 16, 4096]),
        final_pairs in key_value_set(1..120),
        noise in proptest::collection::vec(
            (proptest::collection::vec(any::<u8>(), 1..6), proptest::collection::vec(any::<u8>(), 0..6)),
            0..80,
        ),
        seed1 in any::<u64>(),
        seed2 in any::<u64>(),
    ) {
        // Sequence 1: insert all final pairs in one order (with extra noise keys
        // that are later deleted), ending at the target map.
        let ops1 = build_sequence_to_map(&final_pairs, &noise, seed1);
        // Sequence 2: a different order / different noise interleaving, same map.
        let ops2 = build_sequence_to_map(&final_pairs, &noise, seed2);

        // Sanity: both sequences must actually reach the target map.
        let target: BTreeMap<Vec<u8>, Vec<u8>> = final_pairs.iter().cloned().collect();
        prop_assert_eq!(replay(&ops1), target.clone(), "seq1 did not reach target map");
        prop_assert_eq!(replay(&ops2), target.clone(), "seq2 did not reach target map");

        let h1 = apply_ops(target_size, &ops1);
        let h2 = apply_ops(target_size, &ops2);

        prop_assert_eq!(
            h1, h2,
            "put/delete history changed the root hash (target_size={}, n={})\nops1={:?}\nops2={:?}",
            target_size, final_pairs.len(), ops1, ops2
        );
    }

    /// PROPERTY C — sanity: distinct maps must (essentially always) yield
    /// distinct roots. Guards against a degenerate always-equal hash that would
    /// make A and B trivially pass.
    #[test]
    fn prop_c_distinct_maps_distinct_roots(
        target_size in prop::sample::select(vec![4usize, 8, 16, 4096]),
        pairs in key_value_set(1..120),
        extra_key in proptest::collection::vec(any::<u8>(), 1..6),
        extra_val in proptest::collection::vec(any::<u8>(), 0..6),
    ) {
        let map: BTreeMap<Vec<u8>, Vec<u8>> = pairs.iter().cloned().collect();
        // Make a genuinely different map: insert a key not present (or with a
        // different value if present).
        prop_assume!(!map.is_empty());
        let ops_base: Vec<(Vec<u8>, Option<Vec<u8>>)> =
            pairs.iter().map(|(k, v)| (k.clone(), Some(v.clone()))).collect();

        let mut ops_diff = ops_base.clone();
        let differs = if let Some(existing) = map.get(&extra_key) {
            if existing == &extra_val { false }
            else { ops_diff.push((extra_key.clone(), Some(extra_val.clone()))); true }
        } else {
            ops_diff.push((extra_key.clone(), Some(extra_val.clone()))); true
        };
        prop_assume!(differs);

        let h_base = apply_ops(target_size, &ops_base);
        let h_diff = apply_ops(target_size, &ops_diff);
        prop_assert_ne!(h_base, h_diff, "two distinct maps produced the same root hash");
    }
}

/// Deterministically shuffle a slice using a simple LCG seeded by `seed`.
fn shuffle_with(pairs: &[(Vec<u8>, Vec<u8>)], seed: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut v = pairs.to_vec();
    let mut state = (seed as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15).wrapping_add(1);
    // Fisher-Yates.
    for i in (1..v.len()).rev() {
        state = state.wrapping_mul(0x5851_f42d_4c95_7f2d).wrapping_add(0x1405_7b7e_f767_814f);
        let j = (state >> 33) as usize % (i + 1);
        v.swap(i, j);
    }
    v
}

/// Build a put/delete sequence that ends at exactly `final_pairs` as the final
/// map. It inserts the final pairs (in a seed-dependent order), interleaves
/// `noise` puts for keys that are NOT in the final map, then deletes that noise
/// at the end. This produces genuinely different histories that converge.
fn build_sequence_to_map(
    final_pairs: &[(Vec<u8>, Vec<u8>)],
    noise: &[(Vec<u8>, Vec<u8>)],
    seed: u64,
) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let final_map: BTreeMap<Vec<u8>, Vec<u8>> = final_pairs.iter().cloned().collect();
    // Noise keys must not collide with final keys (else final value would be lost).
    let noise: Vec<(Vec<u8>, Vec<u8>)> = noise
        .iter()
        .filter(|(k, _)| !final_map.contains_key(k))
        .cloned()
        .collect();

    let order = shuffle_with(final_pairs, seed as usize);
    let mut ops: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();

    // Interleave: put final keys and noise keys in a seed-dependent braid.
    let mut state = seed.wrapping_add(0xdead_beef);
    let mut fi = 0usize;
    let mut ni = 0usize;
    while fi < order.len() || ni < noise.len() {
        state = state.wrapping_mul(0x5851_f42d_4c95_7f2d).wrapping_add(1);
        let pick_final = (state >> 40) & 1 == 0;
        if (pick_final && fi < order.len()) || ni >= noise.len() {
            let (k, v) = &order[fi];
            ops.push((k.clone(), Some(v.clone())));
            fi += 1;
        } else if ni < noise.len() {
            let (k, v) = &noise[ni];
            ops.push((k.clone(), Some(v.clone())));
            ni += 1;
        }
    }

    // Overwrite some final keys with a wrong value, then correct them, to add
    // more history divergence (still converges to the same map).
    for (idx, (k, v)) in order.iter().enumerate() {
        if idx % 3 == 0 {
            let mut wrong = v.clone();
            wrong.push(0xff);
            ops.push((k.clone(), Some(wrong)));
            ops.push((k.clone(), Some(v.clone())));
        }
    }

    // Finally delete all noise keys, so the converged map is exactly final_map.
    for (k, _) in &noise {
        ops.push((k.clone(), None));
    }

    ops
}

/// Replay a put/delete sequence into a plain BTreeMap to compute the expected
/// final logical state independently of the tree.
fn replay(ops: &[(Vec<u8>, Option<Vec<u8>>)]) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut map = BTreeMap::new();
    for (k, v) in ops {
        match v {
            Some(value) => {
                map.insert(k.clone(), value.clone());
            }
            None => {
                map.remove(k);
            }
        }
    }
    map
}

/// Deterministic regression for the minimal shrunk counterexample found by
/// `prop_a_insertion_order_independence`.
///
/// Over the key SET {0, 1, 8, 19} (target_size = 4), inserting in order
/// [19, 1, 0, 8] yields a DIFFERENT root hash than inserting [8, 19, 0, 1]
/// (the latter matches the canonical single-batch build). The first ordering
/// transiently produces a 3-child root, which `root_children` bisects
/// positionally (at len/2) rather than at content-defined boundaries, wrapping
/// {8},{19} in a spurious internal node. THIS BREAKS HISTORY-INDEPENDENCE.
///
/// This test is `#[ignore]` because it is EXPECTED TO FAIL against current
/// production code; it exists to pin the exact counterexample for the human
/// reviewing the proposed fix. Remove `#[ignore]` once the root_children /
/// internal re-chunking path is made content-defined.
#[test]
#[ignore = "documents a known history-independence bug; expected to fail until root_children is fixed"]
fn regression_minimal_insertion_order_counterexample() {
    let order1: Vec<(Vec<u8>, Option<Vec<u8>>)> =
        [19u8, 1, 0, 8].iter().map(|&k| (vec![k], Some(vec![]))).collect();
    let order2: Vec<(Vec<u8>, Option<Vec<u8>>)> =
        [8u8, 19, 0, 1].iter().map(|&k| (vec![k], Some(vec![]))).collect();

    let h1 = apply_ops(4, &order1);
    let h2 = apply_ops(4, &order2);
    assert_eq!(
        h1, h2,
        "history-independence violated: {h1} (order [19,1,0,8]) != {h2} (order [8,19,0,1])"
    );
}

#[test]
fn confirms_small_target_size_produces_multi_leaf_trees() {
    // Deterministic evidence that the test harness genuinely builds multi-leaf
    // trees at small target_size. We insert 4-byte keys 0..N and assert the
    // root is internal with > 1 child.
    let mut ops: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    for i in 0u32..120 {
        ops.push((i.to_be_bytes().to_vec(), Some(vec![i as u8])));
    }
    assert!(
        root_is_multi_leaf(4, &ops),
        "expected a multi-leaf root at target_size=4 for 120 keys"
    );

    // Also report the leaf count for the record.
    let root = apply_ops(4, &ops);
    let read = collect_map(4, root, &ops);
    assert!(read.leaf_count > 1, "expected >1 leaf, got {}", read.leaf_count);
    assert_eq!(read.map.len(), 120);
    assert_eq!(read.root, root);
    println!(
        "multi-leaf evidence: target_size=4, 120 keys -> {} leaves",
        read.leaf_count
    );
}
