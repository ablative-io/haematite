//! AA-3-4c: tests for the ancestor-free 2-way union + max-stamp merge.
//!
//! This is the algorithmic heart of step-3 handoff (§2.4), so the tests are
//! exhaustive on the resolver's contract: union of disjoint keys, per-key
//! max-stamp, tombstones as first-class participants (winning AND losing),
//! tombstone-vs-never-written, commutativity (identical root hash regardless of
//! argument order), and an order-independence PROPERTY test over all permutations
//! of >=3 forked states (the property prolly history was once bitten by).

use std::collections::BTreeMap;
use std::error::Error;

use super::{HandoffMergeError, merge_committed_union};
use crate::store::MemoryStore;
use crate::sync::ballot::{Ballot, Stamp};
use crate::sync::topology::SyncNodeId;
use crate::tree::{Cursor, Hash, LeafNode, Node, batch_mutate};
use crate::ttl::entry::{StampedEntry, encode_stamped, encode_stamped_tombstone};

type TestResult = Result<(), Box<dyn Error>>;

/// A fork spec entry: `(key, stamp, Some(value) | None for a tombstone)`.
type SpecEntry<'a> = (&'a [u8], Stamp, Option<&'a [u8]>);

fn stamp(counter: u64, node: &str, seq: u64) -> Stamp {
    Stamp::new(Ballot::new(counter, SyncNodeId::new(node)), seq)
}

/// Canonical empty root (an empty leaf), matching the commit path.
fn empty_root(store: &mut MemoryStore) -> Result<Hash, Box<dyn Error>> {
    let leaf = LeafNode::new(Vec::new())?;
    Ok(store.put(&Node::Leaf(leaf)))
}

/// A stamped VALUE entry's raw stored bytes.
fn value_bytes(value: &[u8], stamp: Stamp) -> Vec<u8> {
    encode_stamped(value.to_vec(), stamp, None)
}

/// A stamped TOMBSTONE entry's raw stored bytes.
fn tombstone_bytes(stamp: Stamp) -> Vec<u8> {
    encode_stamped_tombstone(stamp)
}

/// Build a committed tree from a key -> stored-bytes set and return its root.
fn build_tree(
    store: &mut MemoryStore,
    entries: &[(&[u8], Vec<u8>)],
) -> Result<Hash, Box<dyn Error>> {
    let root = empty_root(store)?;
    let mutations: Vec<(Vec<u8>, Option<Vec<u8>>)> = entries
        .iter()
        .map(|(key, bytes)| (key.to_vec(), Some(bytes.clone())))
        .collect();
    Ok(batch_mutate(store, root, mutations.as_slice())?)
}

fn build_from_spec(
    store: &mut MemoryStore,
    spec: &[SpecEntry<'_>],
) -> Result<Hash, Box<dyn Error>> {
    let entries: Vec<(&[u8], Vec<u8>)> = spec
        .iter()
        .map(|(key, stamp, value)| {
            let bytes = value.as_ref().map_or_else(
                || tombstone_bytes(stamp.clone()),
                |v| value_bytes(v, stamp.clone()),
            );
            (*key, bytes)
        })
        .collect();
    build_tree(store, entries.as_slice())
}

/// Read every STORED entry (raw bytes, tombstones included) out of a tree, as a
/// map. Used to assert exactly which entries survived the merge.
fn read_stored(
    store: &MemoryStore,
    root: Hash,
) -> Result<BTreeMap<Vec<u8>, StampedEntry>, Box<dyn Error>> {
    let mut out = BTreeMap::new();
    collect(store, root, &mut out)?;
    Ok(out)
}

fn collect(
    store: &MemoryStore,
    hash: Hash,
    out: &mut BTreeMap<Vec<u8>, StampedEntry>,
) -> Result<(), Box<dyn Error>> {
    match &*store.get(&hash).ok_or("missing node")? {
        Node::Leaf(leaf) => {
            for (key, bytes) in leaf.entries() {
                let entry = StampedEntry::decode(bytes)?.ok_or("not stamped")?;
                out.insert(key.clone(), entry);
            }
        }
        Node::Internal(internal) => {
            for (_separator, child) in internal.children() {
                collect(store, *child, out)?;
            }
        }
    }
    Ok(())
}

/// Logical read of a key in a merged tree: `Some(bytes)` for a live value,
/// `None` if absent (never-written OR tombstoned).
fn logical_get(
    store: &MemoryStore,
    root: Hash,
    key: &[u8],
) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
    let cursor = Cursor::new(store, root);
    let Some(raw) = cursor.get(key)? else {
        return Ok(None);
    };
    let entry = StampedEntry::decode(&raw)?.ok_or("not stamped")?;
    Ok(entry.into_value())
}

/// True when the key is STORED (present in the tree), regardless of visibility —
/// i.e. distinguishes a tombstone from a never-written key.
fn is_stored(store: &MemoryStore, root: Hash, key: &[u8]) -> Result<bool, Box<dyn Error>> {
    Ok(Cursor::new(store, root).get(key)?.is_some())
}

fn require(root: Option<Hash>) -> Result<Hash, Box<dyn Error>> {
    root.ok_or_else(|| "merge produced an empty (None) root".into())
}

// ---------------------------------------------------------------------------
// 1. Forked union (the §2.4 gate): disjoint keys from both sides survive.
// ---------------------------------------------------------------------------

#[test]
fn forked_union_keeps_both_disjoint_keys() -> TestResult {
    let mut store = MemoryStore::new();
    let a = build_tree(&mut store, &[(b"k2", value_bytes(b"v2", stamp(1, "A", 0)))])?;
    let b = build_tree(&mut store, &[(b"k3", value_bytes(b"v3", stamp(1, "A", 1)))])?;

    let merged = require(merge_committed_union(Some(a), Some(b), &mut store)?)?;

    assert_eq!(logical_get(&store, merged, b"k2")?, Some(b"v2".to_vec()));
    assert_eq!(logical_get(&store, merged, b"k3")?, Some(b"v3".to_vec()));
    Ok(())
}

// ---------------------------------------------------------------------------
// 2. Same-key max-stamp: higher (epoch, seq) wins, order-independent.
// ---------------------------------------------------------------------------

#[test]
fn same_key_keeps_max_stamp_either_arg_order() -> TestResult {
    let mut store = MemoryStore::new();
    let a = build_tree(&mut store, &[(b"k", value_bytes(b"old", stamp(7, "A", 5)))])?;
    let b = build_tree(&mut store, &[(b"k", value_bytes(b"new", stamp(7, "A", 9)))])?;

    let ab = require(merge_committed_union(Some(a), Some(b), &mut store)?)?;
    let ba = require(merge_committed_union(Some(b), Some(a), &mut store)?)?;

    assert_eq!(logical_get(&store, ab, b"k")?, Some(b"new".to_vec()));
    assert_eq!(logical_get(&store, ba, b"k")?, Some(b"new".to_vec()));
    assert_eq!(ab, ba, "argument order must not change the root hash");
    Ok(())
}

#[test]
fn same_key_higher_epoch_beats_higher_seq() -> TestResult {
    // Epoch dominates seq: (epoch 8, seq 0) beats (epoch 7, seq u64::MAX).
    let mut store = MemoryStore::new();
    let a = build_tree(
        &mut store,
        &[(b"k", value_bytes(b"old", stamp(7, "Z", u64::MAX)))],
    )?;
    let b = build_tree(&mut store, &[(b"k", value_bytes(b"new", stamp(8, "A", 0)))])?;

    let merged = require(merge_committed_union(Some(a), Some(b), &mut store)?)?;
    assert_eq!(logical_get(&store, merged, b"k")?, Some(b"new".to_vec()));
    Ok(())
}

// ---------------------------------------------------------------------------
// 3. Tombstone wins / loses, both directions.
// ---------------------------------------------------------------------------

#[test]
fn tombstone_with_higher_stamp_wins_and_reads_absent() -> TestResult {
    let mut store = MemoryStore::new();
    let value_side = build_tree(&mut store, &[(b"k", value_bytes(b"v", stamp(1, "A", 5)))])?;
    let tomb_side = build_tree(&mut store, &[(b"k", tombstone_bytes(stamp(1, "A", 9)))])?;

    // Both directions: the higher-stamp tombstone wins, key reads absent, and the
    // tombstone is WRITTEN into the merged tree (stored, not dropped).
    for (x, y) in [(value_side, tomb_side), (tomb_side, value_side)] {
        let merged = require(merge_committed_union(Some(x), Some(y), &mut store)?)?;
        assert_eq!(
            logical_get(&store, merged, b"k")?,
            None,
            "tombstone hides the value"
        );
        assert!(
            is_stored(&store, merged, b"k")?,
            "tombstone persists in the merged tree"
        );
        let entry = read_stored(&store, merged)?
            .remove(b"k".as_slice())
            .ok_or("missing k")?;
        assert!(entry.is_tombstone());
        assert_eq!(entry.stamp(), &stamp(1, "A", 9));
    }
    Ok(())
}

#[test]
fn value_with_higher_stamp_beats_tombstone_both_directions() -> TestResult {
    let mut store = MemoryStore::new();
    let tomb_side = build_tree(&mut store, &[(b"k", tombstone_bytes(stamp(1, "A", 5)))])?;
    let value_side = build_tree(&mut store, &[(b"k", value_bytes(b"v", stamp(1, "A", 9)))])?;

    for (x, y) in [(tomb_side, value_side), (value_side, tomb_side)] {
        let merged = require(merge_committed_union(Some(x), Some(y), &mut store)?)?;
        assert_eq!(logical_get(&store, merged, b"k")?, Some(b"v".to_vec()));
        let entry = read_stored(&store, merged)?
            .remove(b"k".as_slice())
            .ok_or("missing k")?;
        assert!(!entry.is_tombstone());
        assert_eq!(entry.stamp(), &stamp(1, "A", 9));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 4. Tombstone vs never-written: union KEEPS the tombstone (no resurrection).
// ---------------------------------------------------------------------------

#[test]
fn tombstone_vs_never_written_keeps_tombstone() -> TestResult {
    let mut store = MemoryStore::new();
    // Side A has a committed delete on `k`; side B never wrote `k` (it has another
    // key so the tree is non-trivial). The delete MUST survive (not resurrected).
    let a = build_tree(&mut store, &[(b"k", tombstone_bytes(stamp(2, "A", 3)))])?;
    let b = build_tree(
        &mut store,
        &[(b"other", value_bytes(b"x", stamp(2, "A", 4)))],
    )?;

    let merged = require(merge_committed_union(Some(a), Some(b), &mut store)?)?;
    assert!(
        is_stored(&store, merged, b"k")?,
        "tombstone must not vanish"
    );
    assert_eq!(
        logical_get(&store, merged, b"k")?,
        None,
        "key stays deleted"
    );
    let entry = read_stored(&store, merged)?
        .remove(b"k".as_slice())
        .ok_or("missing k")?;
    assert!(entry.is_tombstone());
    // The unrelated key is also preserved (union).
    assert_eq!(logical_get(&store, merged, b"other")?, Some(b"x".to_vec()));
    Ok(())
}

#[test]
fn none_side_is_treated_as_empty() -> TestResult {
    let mut store = MemoryStore::new();
    let a = build_tree(&mut store, &[(b"k", value_bytes(b"v", stamp(1, "A", 0)))])?;

    // None on one side == empty: the present side survives intact.
    let merged = require(merge_committed_union(Some(a), None, &mut store)?)?;
    assert_eq!(logical_get(&store, merged, b"k")?, Some(b"v".to_vec()));

    // Both None == nothing to adopt.
    assert_eq!(merge_committed_union(None, None, &mut store)?, None);
    Ok(())
}

// ---------------------------------------------------------------------------
// 5. Commutativity: merge(A,B) root == merge(B,A) root across mixed fixtures.
// ---------------------------------------------------------------------------

#[test]
fn commutativity_identical_root_across_mixed_fixtures() -> TestResult {
    let fixtures: &[(&[SpecEntry<'_>], &[SpecEntry<'_>])] = &[
        // disjoint
        (
            &[(b"a", stamp(1, "A", 0), Some(b"1"))],
            &[(b"b", stamp(1, "A", 1), Some(b"2"))],
        ),
        // overlapping values, different stamps
        (
            &[(b"k", stamp(2, "A", 1), Some(b"lo"))],
            &[(b"k", stamp(2, "A", 9), Some(b"hi"))],
        ),
        // tombstone vs value
        (
            &[(b"k", stamp(3, "A", 4), None)],
            &[(b"k", stamp(3, "A", 7), Some(b"v"))],
        ),
        // mixed multi-key with tombstones on both sides
        (
            &[
                (b"x", stamp(4, "A", 1), Some(b"x1")),
                (b"y", stamp(4, "A", 2), None),
                (b"z", stamp(5, "A", 0), Some(b"z1")),
            ],
            &[
                (b"x", stamp(4, "A", 9), None),
                (b"y", stamp(4, "A", 1), Some(b"y0")),
                (b"w", stamp(6, "A", 3), Some(b"w1")),
            ],
        ),
    ];

    for (left, right) in fixtures {
        let mut store = MemoryStore::new();
        let a = build_from_spec(&mut store, left)?;
        let b = build_from_spec(&mut store, right)?;
        let ab = merge_committed_union(Some(a), Some(b), &mut store)?;
        let ba = merge_committed_union(Some(b), Some(a), &mut store)?;
        assert_eq!(ab, ba, "merge must be commutative on the root hash");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 6. Associativity / order-independence over >=3 forks: ALL permutations of the
//    merge order produce the IDENTICAL root hash. This is a real permutation
//    test, not an assertion. (The prolly history-independence property.)
// ---------------------------------------------------------------------------

/// Fold a left-to-right reduction of `merge_committed_union` over a list of roots.
fn fold_merge(store: &mut MemoryStore, roots: &[Hash]) -> Result<Option<Hash>, Box<dyn Error>> {
    let mut acc: Option<Hash> = None;
    for &root in roots {
        acc = merge_committed_union(acc, Some(root), store)?;
    }
    Ok(acc)
}

fn permutations(items: Vec<Hash>) -> Vec<Vec<Hash>> {
    if items.len() <= 1 {
        return vec![items];
    }
    let mut out = Vec::new();
    for index in 0..items.len() {
        let mut rest = items.clone();
        let head = rest.remove(index);
        for mut perm in permutations(rest) {
            perm.insert(0, head);
            out.push(perm);
        }
    }
    out
}

#[test]
fn associativity_all_permutations_yield_identical_root() -> TestResult {
    let mut store = MemoryStore::new();

    // Five forked committed states with OVERLAPPING keys at different stamps,
    // including tombstones that both win and lose, and disjoint keys.
    let forks: Vec<Hash> = vec![
        build_from_spec(
            &mut store,
            &[
                (b"shared", stamp(1, "A", 0), Some(b"a-lo")),
                (b"only-a", stamp(1, "A", 1), Some(b"a")),
                (b"del", stamp(2, "A", 0), Some(b"will-die")),
            ],
        )?,
        build_from_spec(
            &mut store,
            &[
                (b"shared", stamp(3, "B", 4), Some(b"b-mid")),
                (b"only-b", stamp(3, "B", 1), Some(b"b")),
                (b"del", stamp(9, "B", 7), None), // highest stamp for `del`: tombstone wins
            ],
        )?,
        build_from_spec(
            &mut store,
            &[
                (b"shared", stamp(5, "C", 2), Some(b"c-hi")), // highest for `shared`: wins
                (b"only-c", stamp(5, "C", 9), None),          // a tombstone with no rival
            ],
        )?,
        build_from_spec(
            &mut store,
            &[
                (b"shared", stamp(2, "D", 8), Some(b"d")),
                (b"del", stamp(4, "D", 0), Some(b"resurrect-attempt")), // lower than B's tombstone
            ],
        )?,
        build_from_spec(
            &mut store,
            &[
                (b"only-e", stamp(6, "E", 0), Some(b"e")),
                (b"shared", stamp(4, "E", 0), Some(b"e-shared")),
            ],
        )?,
    ];

    let perms = permutations(forks.clone());
    assert_eq!(perms.len(), 120, "5! permutations");

    let baseline = require(fold_merge(&mut store, &forks)?)?;
    for perm in &perms {
        let root = require(fold_merge(&mut store, perm)?)?;
        assert_eq!(
            root, baseline,
            "every merge permutation must yield the identical root"
        );
    }

    // And the winners are exactly the per-key chain tips:
    assert_eq!(
        logical_get(&store, baseline, b"shared")?,
        Some(b"c-hi".to_vec())
    ); // epoch 5 wins
    assert_eq!(
        logical_get(&store, baseline, b"only-a")?,
        Some(b"a".to_vec())
    );
    assert_eq!(
        logical_get(&store, baseline, b"only-b")?,
        Some(b"b".to_vec())
    );
    assert_eq!(logical_get(&store, baseline, b"del")?, None); // B's tombstone (epoch 9) wins
    assert!(
        is_stored(&store, baseline, b"del")?,
        "the winning tombstone persists"
    );
    assert_eq!(logical_get(&store, baseline, b"only-c")?, None); // lone tombstone survives
    assert!(is_stored(&store, baseline, b"only-c")?);
    assert_eq!(
        logical_get(&store, baseline, b"only-e")?,
        Some(b"e".to_vec())
    );

    // Explicit associativity shape: merge(merge(A,B),C) == merge(A,merge(B,C)).
    let (a, b, c) = (forks[0], forks[1], forks[2]);
    let ab = merge_committed_union(Some(a), Some(b), &mut store)?;
    let ab_c = merge_committed_union(ab, Some(c), &mut store)?;
    let bc = merge_committed_union(Some(b), Some(c), &mut store)?;
    let a_bc = merge_committed_union(Some(a), bc, &mut store)?;
    assert_eq!(ab_c, a_bc, "(A·B)·C == A·(B·C)");
    Ok(())
}

// ---------------------------------------------------------------------------
// 7. No committed write lost: every committed tip (value or tombstone) survives.
// ---------------------------------------------------------------------------

#[test]
fn no_committed_tip_is_lost() -> TestResult {
    let mut store = MemoryStore::new();
    // Construct forks where each key's tip is on at least one side. Tips:
    //   k1 -> value "v1b" (stamp (2,A,0))   [tip on side B]
    //   k2 -> tombstone   (stamp (3,A,0))   [tip on side A]
    //   k3 -> value "v3a" (stamp (5,A,0))   [tip on side A]
    //   k4 -> value "v4b" (stamp (1,A,9))   [only on side B]
    let a = build_from_spec(
        &mut store,
        &[
            (b"k1", stamp(1, "A", 0), Some(b"v1a")), // older
            (b"k2", stamp(3, "A", 0), None),         // tip (tombstone)
            (b"k3", stamp(5, "A", 0), Some(b"v3a")), // tip
        ],
    )?;
    let b = build_from_spec(
        &mut store,
        &[
            (b"k1", stamp(2, "A", 0), Some(b"v1b")), // tip
            (b"k2", stamp(2, "A", 0), Some(b"v2b")), // older than A's tombstone
            (b"k4", stamp(1, "A", 9), Some(b"v4b")), // only on B
        ],
    )?;

    let merged = require(merge_committed_union(Some(a), Some(b), &mut store)?)?;

    assert_eq!(logical_get(&store, merged, b"k1")?, Some(b"v1b".to_vec()));
    assert_eq!(logical_get(&store, merged, b"k2")?, None); // tombstone tip
    assert!(is_stored(&store, merged, b"k2")?);
    assert_eq!(logical_get(&store, merged, b"k3")?, Some(b"v3a".to_vec()));
    assert_eq!(logical_get(&store, merged, b"k4")?, Some(b"v4b".to_vec()));
    Ok(())
}

// ---------------------------------------------------------------------------
// Invariant: equal stamp + DIFFERENT bytes is a loud error (R-LE/R-SEQ).
// Equal stamp + identical bytes is fine (idempotent replication).
// ---------------------------------------------------------------------------

#[test]
fn equal_stamp_different_bytes_is_an_error() -> TestResult {
    let mut store = MemoryStore::new();
    let s = stamp(4, "A", 2);
    let a = build_tree(&mut store, &[(b"k", value_bytes(b"one", s.clone()))])?;
    let b = build_tree(&mut store, &[(b"k", value_bytes(b"two", s.clone()))])?;

    let result = merge_committed_union(Some(a), Some(b), &mut store);
    assert_eq!(
        result,
        Err(HandoffMergeError::DuplicateStamp {
            key: b"k".to_vec(),
            stamp: s
        })
    );
    Ok(())
}

#[test]
fn equal_stamp_identical_bytes_is_idempotent() -> TestResult {
    let mut store = MemoryStore::new();
    let s = stamp(4, "A", 2);
    // Same write replicated to both promisers: identical stamp AND bytes.
    let a = build_tree(&mut store, &[(b"k", value_bytes(b"same", s.clone()))])?;
    let b = build_tree(&mut store, &[(b"k", value_bytes(b"same", s))])?;

    let merged = require(merge_committed_union(Some(a), Some(b), &mut store)?)?;
    assert_eq!(logical_get(&store, merged, b"k")?, Some(b"same".to_vec()));
    Ok(())
}
