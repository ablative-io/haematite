use std::cell::Cell;
use std::error::Error;

use haematite::{
    DiffEntry, DiffError, Hash, InternalNode, LeafNode, MemoryStore, Node, NodeStore, batch_mutate,
    diff,
};

type KeyValue = (Vec<u8>, Vec<u8>);

#[derive(Debug, Default)]
struct CountingStore {
    inner: MemoryStore,
    reads: Cell<usize>,
}

impl CountingStore {
    fn new() -> Self {
        Self::default()
    }

    const fn read_count(&self) -> usize {
        self.reads.get()
    }

    fn reset_reads(&self) {
        self.reads.set(0);
    }
}

impl NodeStore for CountingStore {
    type Error = std::convert::Infallible;

    fn get(&self, hash: &Hash) -> Result<Option<Node>, Self::Error> {
        self.reads.set(self.reads.get().saturating_add(1));
        Ok(self.inner.get(hash))
    }

    fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
        Ok(self.inner.put(node))
    }
}

#[derive(Debug, Default)]
struct PanicOnHashStore {
    inner: MemoryStore,
    panic_on: Vec<Hash>,
}

impl PanicOnHashStore {
    fn panic_on(&mut self, hash: Hash) {
        self.panic_on.push(hash);
    }
}

impl NodeStore for PanicOnHashStore {
    type Error = std::convert::Infallible;

    fn get(&self, hash: &Hash) -> Result<Option<Node>, Self::Error> {
        assert!(
            !self.panic_on.contains(hash),
            "diff attempted to fetch shared subtree {hash}"
        );
        Ok(self.inner.get(hash))
    }

    fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
        Ok(self.inner.put(node))
    }
}

fn store_node<S>(store: &mut S, node: &Node) -> Result<Hash, Box<dyn Error>>
where
    S: NodeStore + ?Sized,
    S::Error: Error + 'static,
{
    store.put(node).map_err(Into::into)
}

fn store_leaf<S>(store: &mut S, entries: &[KeyValue]) -> Result<Hash, Box<dyn Error>>
where
    S: NodeStore + ?Sized,
    S::Error: Error + 'static,
{
    let leaf = LeafNode::new(entries.to_vec())?;
    store_node(store, &Node::Leaf(leaf))
}

fn store_leaf_bytes<S>(store: &mut S, entries: &[(&[u8], &[u8])]) -> Result<Hash, Box<dyn Error>>
where
    S: NodeStore + ?Sized,
    S::Error: Error + 'static,
{
    let entries = entries
        .iter()
        .map(|(key, value)| ((*key).to_vec(), (*value).to_vec()))
        .collect::<Vec<_>>();
    store_leaf(store, entries.as_slice())
}

fn store_internal<S>(store: &mut S, children: Vec<(Vec<u8>, Hash)>) -> Result<Hash, Box<dyn Error>>
where
    S: NodeStore + ?Sized,
    S::Error: Error + 'static,
{
    let internal = InternalNode::new(children)?;
    store_node(store, &Node::Internal(internal))
}

fn empty_root<S>(store: &mut S) -> Result<Hash, Box<dyn Error>>
where
    S: NodeStore + ?Sized,
    S::Error: Error + 'static,
{
    store_leaf(store, &[])
}

fn numbered_entries(prefix: &str, count: usize) -> Vec<KeyValue> {
    (0..count)
        .map(|index| {
            (
                format!("{prefix}{index:05}").into_bytes(),
                format!("v{index:05}").into_bytes(),
            )
        })
        .collect()
}

fn manual_tree<S>(
    store: &mut S,
    entries: &[KeyValue],
    leaf_size: usize,
) -> Result<Hash, Box<dyn Error>>
where
    S: NodeStore + ?Sized,
    S::Error: Error + 'static,
{
    if entries.is_empty() {
        return empty_root(store);
    }

    let mut children = Vec::new();
    for chunk in entries.chunks(leaf_size) {
        let Some((separator, _value)) = chunk.first() else {
            return Err("manual tree chunk was empty".into());
        };
        let child_hash = store_leaf(store, chunk)?;
        children.push((separator.clone(), child_hash));
    }

    if children.len() == 1 {
        children
            .first()
            .map(|(_separator, hash)| *hash)
            .ok_or_else(|| "manual tree did not produce a child".into())
    } else {
        store_internal(store, children)
    }
}

#[test]
fn crate_root_exports_diff_api_and_error_type() -> Result<(), Box<dyn Error>> {
    fn assert_error<E: std::error::Error>() {}
    assert_error::<DiffError>();

    let mut store = MemoryStore::new();
    let root = empty_root(&mut store)?;
    let entries: Vec<DiffEntry> = diff(&store, &root, &root)?;
    assert_eq!(entries, Vec::new());

    let trait_store: &dyn NodeStore<Error = std::convert::Infallible> = &store;
    assert_eq!(diff(trait_store, &root, &root)?, Vec::<DiffEntry>::new());
    Ok(())
}

#[test]
fn equal_roots_return_empty_without_store_reads() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::new();
    let root = empty_root(&mut store)?;
    store.reset_reads();

    assert_eq!(diff(&store, &root, &root)?, Vec::new());
    assert_eq!(store.read_count(), 0);
    Ok(())
}

#[test]
fn missing_node_is_reported_by_hash() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::new();
    let root = empty_root(&mut store)?;
    let missing = Hash::from_bytes([9; 32]);

    assert!(matches!(
        diff(&store, &missing, &root),
        Err(DiffError::MissingNode(hash)) if hash == missing
    ));
    Ok(())
}

#[test]
fn leaf_diff_uses_sorted_two_pointer_scan() -> Result<(), Box<dyn Error>> {
    let mut store = MemoryStore::new();
    let root_a = store_leaf_bytes(&mut store, &[(b"b", b"2"), (b"c", b"3")])?;
    let root_b = store_leaf_bytes(&mut store, &[(b"a", b"1"), (b"c", b"3")])?;

    assert_eq!(
        diff(&store, &root_a, &root_b)?,
        vec![
            DiffEntry::Added {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
            DiffEntry::Removed {
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            },
        ]
    );
    Ok(())
}

#[test]
fn leaf_diff_reports_modified_and_suppresses_identical_values() -> Result<(), Box<dyn Error>> {
    let mut store = MemoryStore::new();
    let old = store_leaf_bytes(&mut store, &[(b"k", b"old")])?;
    let new = store_leaf_bytes(&mut store, &[(b"k", b"new")])?;
    assert_eq!(
        diff(&store, &old, &new)?,
        vec![DiffEntry::Modified {
            key: b"k".to_vec(),
            old_value: b"old".to_vec(),
            new_value: b"new".to_vec(),
        }]
    );

    let same_a = store_leaf_bytes(&mut store, &[(b"k", b"v")])?;
    let same_b = store_leaf_bytes(&mut store, &[(b"k", b"v")])?;
    assert_eq!(diff(&store, &same_a, &same_b)?, Vec::new());
    Ok(())
}

#[test]
fn shared_child_hashes_are_not_loaded_or_reported() -> Result<(), Box<dyn Error>> {
    let mut store = PanicOnHashStore::default();
    let shared = store_leaf_bytes(&mut store, &[(b"a", b"one"), (b"b", b"two")])?;
    let old_leaf = store_leaf_bytes(&mut store, &[(b"m", b"old")])?;
    let new_leaf = store_leaf_bytes(&mut store, &[(b"m", b"new")])?;
    let root_a = store_internal(
        &mut store,
        vec![(b"a".to_vec(), shared), (b"m".to_vec(), old_leaf)],
    )?;
    let root_b = store_internal(
        &mut store,
        vec![(b"a".to_vec(), shared), (b"m".to_vec(), new_leaf)],
    )?;
    store.panic_on(shared);

    assert_eq!(
        diff(&store, &root_a, &root_b)?,
        vec![DiffEntry::Modified {
            key: b"m".to_vec(),
            old_value: b"old".to_vec(),
            new_value: b"new".to_vec(),
        }]
    );
    Ok(())
}

#[test]
fn mostly_shared_thousand_entry_tree_reads_only_differing_path() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::new();
    let base = numbered_entries("k", 1_000);
    let mut changed = base.clone();
    changed[510].1 = b"changed".to_vec();
    let root_a = manual_tree(&mut store, base.as_slice(), 20)?;
    let root_b = manual_tree(&mut store, changed.as_slice(), 20)?;
    store.reset_reads();

    let entries = diff(&store, &root_a, &root_b)?;
    assert_eq!(entries.len(), 1);
    assert!(store.read_count() < 20, "read {} nodes", store.read_count());
    Ok(())
}

#[test]
fn leaf_internal_shape_divergence_adds_missing_keys() -> Result<(), Box<dyn Error>> {
    let mut store = MemoryStore::new();
    let root_a = store_leaf_bytes(&mut store, &[(b"a", b"1"), (b"b", b"2")])?;
    let left = store_leaf_bytes(&mut store, &[(b"a", b"1"), (b"b", b"2")])?;
    let right = store_leaf_bytes(&mut store, &[(b"c", b"3"), (b"d", b"4")])?;
    let root_b = store_internal(
        &mut store,
        vec![(b"a".to_vec(), left), (b"c".to_vec(), right)],
    )?;

    assert_eq!(
        diff(&store, &root_a, &root_b)?,
        vec![
            DiffEntry::Added {
                key: b"c".to_vec(),
                value: b"3".to_vec(),
            },
            DiffEntry::Added {
                key: b"d".to_vec(),
                value: b"4".to_vec(),
            },
        ]
    );
    Ok(())
}

#[test]
fn leaf_internal_shape_divergence_with_same_keys_is_empty() -> Result<(), Box<dyn Error>> {
    let mut store = MemoryStore::new();
    let root_a = store_leaf_bytes(&mut store, &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")])?;
    let left = store_leaf_bytes(&mut store, &[(b"a", b"1"), (b"b", b"2")])?;
    let right = store_leaf_bytes(&mut store, &[(b"c", b"3")])?;
    let root_b = store_internal(
        &mut store,
        vec![(b"a".to_vec(), left), (b"c".to_vec(), right)],
    )?;

    assert_eq!(diff(&store, &root_a, &root_b)?, Vec::new());
    Ok(())
}

#[test]
fn ten_thousand_entries_with_three_differences_reads_under_threshold() -> Result<(), Box<dyn Error>>
{
    let mut store = CountingStore::new();
    let base = numbered_entries("k", 10_000);
    let mut changed = base.clone();
    for index in [123_usize, 4_567, 9_001] {
        changed[index].1 = format!("changed{index}").into_bytes();
    }
    let root_a = manual_tree(&mut store, base.as_slice(), 50)?;
    let root_b = manual_tree(&mut store, changed.as_slice(), 50)?;
    store.reset_reads();

    let entries = diff(&store, &root_a, &root_b)?;
    assert_eq!(entries.len(), 3);
    assert!(
        entries
            .iter()
            .all(|entry| matches!(entry, DiffEntry::Modified { .. })),
        "expected exactly three modified entries: {entries:?}"
    );
    assert!(
        store.read_count() < 100,
        "diff fetched {} nodes for three changed keys",
        store.read_count()
    );
    Ok(())
}

#[test]
fn disjoint_trees_report_every_added_and_removed_entry() -> Result<(), Box<dyn Error>> {
    let mut store = MemoryStore::new();
    let root_a = manual_tree(&mut store, numbered_entries("a", 100).as_slice(), 10)?;
    let root_b = manual_tree(&mut store, numbered_entries("b", 100).as_slice(), 10)?;

    let entries = diff(&store, &root_a, &root_b)?;
    let added = entries
        .iter()
        .filter(|entry| matches!(entry, DiffEntry::Added { .. }))
        .count();
    let removed = entries
        .iter()
        .filter(|entry| matches!(entry, DiffEntry::Removed { .. }))
        .count();
    assert_eq!(entries.len(), 200);
    assert_eq!(added, 100);
    assert_eq!(removed, 100);
    Ok(())
}

#[test]
fn empty_tree_diff_is_empty() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::new();
    let root_a = empty_root(&mut store)?;
    let root_b = empty_root(&mut store)?;
    store.reset_reads();

    assert_eq!(diff(&store, &root_a, &root_b)?, Vec::new());
    assert_eq!(store.read_count(), 0);
    Ok(())
}

#[test]
fn batch_built_roots_can_be_diffed_through_node_store() -> Result<(), Box<dyn Error>> {
    let mut store = MemoryStore::new();
    let root = empty_root(&mut store)?;
    let root_a = batch_mutate(
        &mut store,
        root,
        &[
            (b"a".to_vec(), Some(b"1".to_vec())),
            (b"b".to_vec(), Some(b"2".to_vec())),
        ],
    )?;
    let root_b = batch_mutate(
        &mut store,
        root_a,
        &[
            (b"b".to_vec(), Some(b"changed".to_vec())),
            (b"c".to_vec(), Some(b"3".to_vec())),
        ],
    )?;

    assert_eq!(
        diff(&store, &root_a, &root_b)?,
        vec![
            DiffEntry::Modified {
                key: b"b".to_vec(),
                old_value: b"2".to_vec(),
                new_value: b"changed".to_vec(),
            },
            DiffEntry::Added {
                key: b"c".to_vec(),
                value: b"3".to_vec(),
            },
        ]
    );
    Ok(())
}
