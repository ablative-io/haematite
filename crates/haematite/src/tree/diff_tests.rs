//! Tests for the structural tree diff (CORE-003), kept in a sidecar module so
//! `diff.rs` stays within the 500-line limit (CN1).
use std::cell::RefCell;
use std::collections::HashSet;
use std::convert::Infallible;

use super::{DiffEntry, DiffError, diff};
use crate::store::{MemoryStore, NodeStore};
use crate::tree::node::{Hash, InternalNode, LeafNode, Node};
use crate::tree::{batch_mutate, insert};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Wraps a store to count `get` calls and fail loudly if the diff descends
/// into a hash it was told to treat as a shared, skippable subtree.
#[derive(Debug)]
struct ObservingStore {
    inner: MemoryStore,
    gets: RefCell<usize>,
    forbidden: HashSet<Hash>,
}

impl ObservingStore {
    fn new(inner: MemoryStore) -> Self {
        Self {
            inner,
            gets: RefCell::new(0),
            forbidden: HashSet::new(),
        }
    }

    fn forbid(&mut self, hash: Hash) {
        self.forbidden.insert(hash);
    }

    fn gets(&self) -> usize {
        *self.gets.borrow()
    }
}

impl NodeStore for ObservingStore {
    type Error = Infallible;

    fn get(&self, hash: &Hash) -> Result<Option<Node>, Self::Error> {
        assert!(
            !self.forbidden.contains(hash),
            "diff descended into shared subtree {hash}"
        );
        *self.gets.borrow_mut() += 1;
        Ok(self.inner.get(hash))
    }

    fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
        Ok(self.inner.put(node))
    }
}

fn key(index: usize) -> Vec<u8> {
    (index as u32).to_be_bytes().to_vec()
}

fn kv(k: &[u8], v: &[u8]) -> (Vec<u8>, Vec<u8>) {
    (k.to_vec(), v.to_vec())
}

fn added(k: &[u8], v: &[u8]) -> DiffEntry {
    DiffEntry::Added {
        key: k.to_vec(),
        value: v.to_vec(),
    }
}

fn removed(k: &[u8], v: &[u8]) -> DiffEntry {
    DiffEntry::Removed {
        key: k.to_vec(),
        value: v.to_vec(),
    }
}

fn modified(k: &[u8], old: &[u8], new: &[u8]) -> DiffEntry {
    DiffEntry::Modified {
        key: k.to_vec(),
        old_value: old.to_vec(),
        new_value: new.to_vec(),
    }
}

fn empty_root(store: &mut MemoryStore) -> Result<Hash, Box<dyn std::error::Error>> {
    Ok(store.put(&Node::Leaf(LeafNode::new(Vec::new())?)))
}

fn leaf(
    store: &mut MemoryStore,
    entries: &[(Vec<u8>, Vec<u8>)],
) -> Result<Hash, Box<dyn std::error::Error>> {
    Ok(store.put(&Node::Leaf(LeafNode::new(entries.to_vec())?)))
}

fn internal(
    store: &mut MemoryStore,
    children: Vec<(Vec<u8>, Hash)>,
) -> Result<Hash, Box<dyn std::error::Error>> {
    Ok(store.put(&Node::Internal(InternalNode::new(children)?)))
}

fn build(
    store: &mut MemoryStore,
    entries: &[(Vec<u8>, Vec<u8>)],
) -> Result<Hash, Box<dyn std::error::Error>> {
    let root = empty_root(store)?;
    let mutations: Vec<(Vec<u8>, Option<Vec<u8>>)> = entries
        .iter()
        .map(|(k, v)| (k.clone(), Some(v.clone())))
        .collect();
    Ok(batch_mutate(store, root, &mutations)?)
}

// R1: every variant is constructible and supports Debug + PartialEq.
#[test]
fn diff_entry_variants_are_constructible_and_comparable() {
    let a = added(b"k", b"v");
    assert_eq!(a, a.clone());
    assert_ne!(a, removed(b"k", b"v"));
    assert!(!format!("{:?}", modified(b"k", b"old", b"new")).is_empty());
}

// R2: identical roots short-circuit without consulting the store.
#[test]
fn identical_roots_return_empty_without_touching_store() -> TestResult {
    let mut backing = MemoryStore::new();
    let root = build(&mut backing, &[kv(b"a", b"1"), kv(b"b", b"2")])?;
    let store = ObservingStore::new(backing);

    assert_eq!(diff(&store, &root, &root), Ok(Vec::new()));
    assert_eq!(store.gets(), 0);
    Ok(())
}

// R2: an absent node hash surfaces as MissingNode.
#[test]
fn missing_node_is_reported() {
    let store = MemoryStore::new();
    let present = Hash::from_bytes([1; 32]);
    let absent = Hash::from_bytes([2; 32]);
    assert_eq!(
        diff(&store, &present, &absent),
        Err(DiffError::MissingNode(present))
    );
}

// R2: DiffError is a std::error::Error with Debug + Display.
#[test]
fn diff_error_is_a_std_error() {
    fn assert_error<E: std::error::Error>() {}
    assert_error::<DiffError>();
    let error = DiffError::MissingNode(Hash::from_bytes([3; 32]));
    assert!(!format!("{error}").is_empty());
    assert!(!format!("{error:?}").is_empty());
}

// R4: two-pointer leaf merge yields Added/Removed in key order.
#[test]
fn leaf_added_and_removed_in_key_order() -> TestResult {
    let mut store = MemoryStore::new();
    let root_a = leaf(&mut store, &[kv(b"b", b"2"), kv(b"c", b"3")])?;
    let root_b = leaf(&mut store, &[kv(b"a", b"1"), kv(b"c", b"3")])?;
    assert_eq!(
        diff(&store, &root_a, &root_b)?,
        vec![added(b"a", b"1"), removed(b"b", b"2")]
    );
    Ok(())
}

// R4: a changed value yields Modified; an unchanged one yields nothing.
#[test]
fn modified_and_identical_values() -> TestResult {
    let mut store = MemoryStore::new();
    let root_a = leaf(&mut store, &[kv(b"k", b"old")])?;
    let root_b = leaf(&mut store, &[kv(b"k", b"new")])?;
    assert_eq!(
        diff(&store, &root_a, &root_b)?,
        vec![modified(b"k", b"old", b"new")]
    );

    let same = leaf(&mut store, &[kv(b"k", b"old")])?;
    assert_eq!(diff(&store, &root_a, &same)?, Vec::new());
    Ok(())
}

// R3: a subtree whose hash matches the opposing side is never fetched, and
// only the entries beneath the differing leaf are reported.
#[test]
fn shared_subtree_is_skipped() -> TestResult {
    let mut backing = MemoryStore::new();
    let shared = leaf(&mut backing, &[kv(b"a", b"1"), kv(b"b", b"2")])?;
    let right_a = leaf(&mut backing, &[kv(b"m", b"1"), kv(b"n", b"2")])?;
    let right_b = leaf(&mut backing, &[kv(b"m", b"1"), kv(b"n", b"changed")])?;
    let root_a = internal(
        &mut backing,
        vec![(b"a".to_vec(), shared), (b"m".to_vec(), right_a)],
    )?;
    let root_b = internal(
        &mut backing,
        vec![(b"a".to_vec(), shared), (b"m".to_vec(), right_b)],
    )?;

    let mut store = ObservingStore::new(backing);
    store.forbid(shared);
    assert_eq!(
        diff(&store, &root_a, &root_b)?,
        vec![modified(b"n", b"2", b"changed")]
    );
    Ok(())
}

// R3 + R7: one edit in a 1000-entry tree fetches far fewer than 20 nodes.
#[test]
fn single_modification_in_large_tree_is_cheap() -> TestResult {
    let mut backing = MemoryStore::new();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..1000).map(|i| kv(&key(i), b"v")).collect();
    let root_a = build(&mut backing, &entries)?;
    let root_b = insert(&mut backing, root_a, key(500), b"changed")?;

    let store = ObservingStore::new(backing);
    assert_eq!(
        diff(&store, &root_a, &root_b)?,
        vec![modified(&key(500), b"v", b"changed")]
    );
    assert!(
        store.gets() < 20,
        "expected < 20 gets, got {}",
        store.gets()
    );
    Ok(())
}

// R5: a leaf opposite an internal node surfaces the extra keys as Added.
#[test]
fn leaf_versus_internal_yields_added_for_extra_keys() -> TestResult {
    let mut store = MemoryStore::new();
    let root_a = leaf(&mut store, &[kv(b"a", b"1"), kv(b"b", b"2")])?;
    let left = leaf(&mut store, &[kv(b"a", b"1"), kv(b"b", b"2")])?;
    let right = leaf(&mut store, &[kv(b"c", b"3"), kv(b"d", b"4")])?;
    let root_b = internal(
        &mut store,
        vec![(b"a".to_vec(), left), (b"c".to_vec(), right)],
    )?;
    assert_eq!(
        diff(&store, &root_a, &root_b)?,
        vec![added(b"c", b"3"), added(b"d", b"4")]
    );
    Ok(())
}

// R5: the same key set in different node shapes yields no differences.
#[test]
fn same_keys_different_shape_yields_empty() -> TestResult {
    let mut store = MemoryStore::new();
    let root_a = leaf(
        &mut store,
        &[kv(b"a", b"1"), kv(b"b", b"2"), kv(b"c", b"3")],
    )?;
    let left = leaf(&mut store, &[kv(b"a", b"1"), kv(b"b", b"2")])?;
    let right = leaf(&mut store, &[kv(b"c", b"3")])?;
    let root_b = internal(
        &mut store,
        vec![(b"a".to_vec(), left), (b"c".to_vec(), right)],
    )?;
    assert_eq!(diff(&store, &root_a, &root_b)?, Vec::new());
    assert_eq!(diff(&store, &root_b, &root_a)?, Vec::new());
    Ok(())
}

// R7: three edits in two 10k-entry trees report exactly three diffs while
// fetching far fewer than 100 nodes.
#[test]
fn three_differences_in_ten_thousand_entries() -> TestResult {
    let mut backing = MemoryStore::new();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..10_000).map(|i| kv(&key(i), b"v")).collect();
    let root_a = build(&mut backing, &entries)?;
    let edits = vec![
        (key(10), Some(b"x".to_vec())),
        (key(5000), Some(b"y".to_vec())),
        (key(9999), Some(b"z".to_vec())),
    ];
    let root_b = batch_mutate(&mut backing, root_a, &edits)?;

    let store = ObservingStore::new(backing);
    let result = diff(&store, &root_a, &root_b)?;
    assert_eq!(result.len(), 3);
    assert!(
        store.gets() < 100,
        "expected < 100 gets, got {}",
        store.gets()
    );
    Ok(())
}

// R7: entirely disjoint trees report every key on both sides.
#[test]
fn disjoint_trees_report_all_keys() -> TestResult {
    let mut store = MemoryStore::new();
    let entries_a: Vec<(Vec<u8>, Vec<u8>)> = (0..100).map(|i| kv(&key(i), b"v")).collect();
    let entries_b: Vec<(Vec<u8>, Vec<u8>)> = (100..200).map(|i| kv(&key(i), b"v")).collect();
    let root_a = build(&mut store, &entries_a)?;
    let root_b = build(&mut store, &entries_b)?;

    let result = diff(&store, &root_a, &root_b)?;
    let added_count = result
        .iter()
        .filter(|e| matches!(e, DiffEntry::Added { .. }))
        .count();
    let removed_count = result
        .iter()
        .filter(|e| matches!(e, DiffEntry::Removed { .. }))
        .count();
    assert_eq!(result.len(), 200);
    assert_eq!(added_count, 100);
    assert_eq!(removed_count, 100);
    Ok(())
}

// R7: empty against empty is nothing; empty against populated is all Added.
#[test]
fn empty_trees_diff_to_nothing() -> TestResult {
    let mut store = MemoryStore::new();
    let root_a = empty_root(&mut store)?;
    let root_b = empty_root(&mut store)?;
    assert_eq!(diff(&store, &root_a, &root_b)?, Vec::new());

    let populated = build(&mut store, &[kv(b"a", b"1"), kv(b"b", b"2")])?;
    assert_eq!(
        diff(&store, &root_a, &populated)?,
        vec![added(b"a", b"1"), added(b"b", b"2")]
    );
    Ok(())
}

// Diff results stay in ascending key order across scattered insert/delete edits.
#[test]
fn results_are_ascending_by_key() -> TestResult {
    let mut backing = MemoryStore::new();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..500).map(|i| kv(&key(i), b"v")).collect();
    let root_a = build(&mut backing, &entries)?;
    let edits = vec![
        (key(3), Some(b"x".to_vec())),
        (key(250), None),
        (key(600), Some(b"new".to_vec())),
        (key(100), Some(b"y".to_vec())),
    ];
    let root_b = batch_mutate(&mut backing, root_a, &edits)?;

    let result = diff(&backing, &root_a, &root_b)?;
    let keys: Vec<&[u8]> = result.iter().map(diff_key).collect();
    let mut sorted = keys.clone();
    sorted.sort_unstable();
    assert_eq!(keys, sorted);
    Ok(())
}

fn diff_key(entry: &DiffEntry) -> &[u8] {
    match entry {
        DiffEntry::Added { key, .. }
        | DiffEntry::Removed { key, .. }
        | DiffEntry::Modified { key, .. } => key,
    }
}
