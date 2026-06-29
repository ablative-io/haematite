//! Property-based commutativity / associativity tests for the ancestor-free
//! union + max-stamp merge (AA-3-4c, §2.4).
//!
//! `handoff_merge_tests.rs` proves the resolver's contract on FIXED fixtures.
//! This module proves the SAME invariants over RANDOM inputs: it generates an
//! arbitrary set of `(key, stamp, value | tombstone)` writes, splits them
//! ARBITRARILY across two or three committed roots, and asserts that EVERY merge
//! ordering (all argument orders and all fold permutations) yields:
//!
//! * the IDENTICAL root hash, and
//! * the IDENTICAL logical reads,
//!
//! AND that those reads are exactly the per-key max-`(epoch, seq)` chain tip
//! computed by an independent reference. This is the same bug-class as the
//! prolly-tree history-independence property (the root being a pure function of
//! the winning key->entry SET regardless of build/merge order), one layer up at
//! the active-active convergence primitive.

#![allow(clippy::unwrap_used)]
#![allow(clippy::panic)]

use std::collections::BTreeMap;

use proptest::prelude::*;

use super::{HandoffMergeError, merge_committed_union};
use crate::store::MemoryStore;
use crate::sync::ballot::{Ballot, Stamp};
use crate::sync::topology::SyncNodeId;
use crate::tree::{Cursor, Hash, LeafNode, Node, batch_mutate};
use crate::ttl::entry::{StampedEntry, encode_stamped, encode_stamped_tombstone};

/// One generated write: a key, its commit stamp, and either a value or (None) a
/// tombstone.
#[derive(Clone, Debug)]
struct Write {
    key: Vec<u8>,
    stamp: Stamp,
    value: Option<Vec<u8>>,
}

/// Canonical empty root (an empty leaf), matching the commit path.
fn empty_root(store: &mut MemoryStore) -> Hash {
    let leaf = LeafNode::new(Vec::new()).unwrap();
    store.put(&Node::Leaf(leaf))
}

/// The raw stored bytes for a write (stamped value or stamped tombstone).
fn stored_bytes(write: &Write) -> Vec<u8> {
    write.value.as_ref().map_or_else(
        || encode_stamped_tombstone(write.stamp.clone()),
        |value| encode_stamped(value.clone(), write.stamp.clone(), None),
    )
}

/// Build a committed tree from a slice of writes. A LATER write to the same key
/// in the slice overwrites the earlier stored bytes (deterministic build input);
/// the merge itself is what we are testing, not this constructor.
fn build_tree(store: &mut MemoryStore, writes: &[Write]) -> Hash {
    let mut by_key: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for write in writes {
        by_key.insert(write.key.clone(), stored_bytes(write));
    }
    let root = empty_root(store);
    let mutations: Vec<(Vec<u8>, Option<Vec<u8>>)> =
        by_key.into_iter().map(|(k, v)| (k, Some(v))).collect();
    batch_mutate(store, root, mutations.as_slice()).unwrap()
}

/// Logical read of a key in a merged tree: `Some(bytes)` for a live value,
/// `None` if absent (never-written OR tombstoned).
fn logical_get(store: &MemoryStore, root: Hash, key: &[u8]) -> Option<Vec<u8>> {
    let cursor = Cursor::new(store, root);
    let raw = cursor.get(key).unwrap()?;
    StampedEntry::decode(&raw).unwrap().unwrap().into_value()
}

/// Independent reference: fold every write through a per-key max-`(epoch, seq)`
/// join (the same semilattice the merge implements) and return the winning
/// logical value per key. A tombstone tip reads as absent.
fn reference_winners(all: &[Write]) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
    let mut winners: BTreeMap<Vec<u8>, (Stamp, Option<Vec<u8>>)> = BTreeMap::new();
    for write in all {
        match winners.get(&write.key) {
            Some((existing_stamp, _)) if *existing_stamp >= write.stamp => {}
            _ => {
                winners.insert(
                    write.key.clone(),
                    (write.stamp.clone(), write.value.clone()),
                );
            }
        }
    }
    winners
        .into_iter()
        .map(|(key, (_stamp, value))| (key, value))
        .collect()
}

/// All permutations of a small list of roots (n! — bounded to <=3 here).
fn permutations(items: &[Hash]) -> Vec<Vec<Hash>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    let mut out = Vec::new();
    for index in 0..items.len() {
        let mut rest = items.to_vec();
        let head = rest.remove(index);
        for mut perm in permutations(&rest) {
            perm.insert(0, head);
            out.push(perm);
        }
    }
    out
}

/// Left-to-right fold of `merge_committed_union` over a list of roots.
fn fold_merge(store: &mut MemoryStore, roots: &[Hash]) -> Result<Option<Hash>, HandoffMergeError> {
    let mut acc: Option<Hash> = None;
    for &root in roots {
        acc = merge_committed_union(acc, Some(root), store)?;
    }
    Ok(acc)
}

/// A stamp whose `(epoch, seq)` are globally unique within a generated case:
/// the `epoch.counter` and `seq` are derived from a per-write unique `index`, so
/// no two distinct writes ever collide on a stamp (which would be the loud
/// `DuplicateStamp` invariant violation, not a convergence question).
fn unique_stamp(index: u64) -> Stamp {
    // Spread the index across both the epoch counter and seq so a wide range of
    // stamps is exercised, while staying injective in `index`.
    let epoch = Ballot::new(index / 4 + 1, SyncNodeId::new("p"));
    Stamp::new(epoch, index % 4)
}

prop_compose! {
    /// A single write over a SMALL key space (so collisions on a key across roots
    /// are common — that is what exercises max-stamp) with a unique stamp and an
    /// even chance of being a tombstone.
    fn arb_write(index: u64)(
        key_index in 0_u8..6,
        is_tombstone in any::<bool>(),
        value in proptest::collection::vec(any::<u8>(), 0..8),
    ) -> Write {
        Write {
            key: vec![b'k', key_index],
            stamp: unique_stamp(index),
            value: if is_tombstone { None } else { Some(value) },
        }
    }
}

/// A set of writes with GLOBALLY UNIQUE stamps (the `index` makes each stamp
/// distinct), of size 1..=12.
fn arb_writes() -> impl Strategy<Value = Vec<Write>> {
    (1_usize..=12).prop_flat_map(|count| {
        let per_write: Vec<_> = (0..count).map(|i| arb_write(i as u64)).collect();
        per_write
    })
}

/// Split a write set across `parts` roots by round-robin assignment, so every
/// write lands on exactly one root and the roots are non-trivially populated.
fn split(writes: &[Write], parts: usize) -> Vec<Vec<Write>> {
    let mut buckets = vec![Vec::new(); parts];
    for (i, write) in writes.iter().enumerate() {
        buckets[i % parts].push(write.clone());
    }
    buckets
}

/// Assert convergence over a fixed number of roots: build each root, merge in
/// EVERY permutation, and check all roots equal the baseline AND the logical
/// reads match the independent reference for every key.
fn assert_converges(writes: &[Write], parts: usize) -> Result<(), TestCaseError> {
    let mut store = MemoryStore::new();
    let buckets = split(writes, parts);
    let roots: Vec<Hash> = buckets.iter().map(|b| build_tree(&mut store, b)).collect();

    let baseline = fold_merge(&mut store, &roots)
        .map_err(|e| TestCaseError::fail(format!("baseline merge failed: {e}")))?
        .ok_or_else(|| TestCaseError::fail("baseline merge produced an empty root"))?;

    for perm in permutations(&roots) {
        let root = fold_merge(&mut store, &perm)
            .map_err(|e| TestCaseError::fail(format!("merge failed: {e}")))?
            .ok_or_else(|| TestCaseError::fail("merge produced an empty root"))?;
        prop_assert_eq!(
            root,
            baseline,
            "every merge order must yield the identical root"
        );
    }

    // Logical reads match the independent per-key max-stamp reference.
    let reference = reference_winners(writes);
    for (key, expected) in &reference {
        prop_assert_eq!(
            logical_get(&store, baseline, key),
            expected.clone(),
            "merged read must equal the per-key max-stamp chain tip"
        );
    }
    Ok(())
}

proptest! {
    /// COMMUTATIVITY + convergence over TWO roots: merge(A,B) == merge(B,A), and
    /// the reads are the per-key max-stamp tips.
    #[test]
    fn two_way_merge_is_order_independent(writes in arb_writes()) {
        assert_converges(&writes, 2)?;
    }

    /// ASSOCIATIVITY + convergence over THREE roots: ALL 3! fold orders yield the
    /// identical root, and reads match the reference.
    #[test]
    fn three_way_merge_is_order_independent(writes in arb_writes()) {
        assert_converges(&writes, 3)?;
    }
}
