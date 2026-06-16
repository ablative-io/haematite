use std::cell::Cell;
use std::error::Error;

use haematite::{
    BoundaryDetector, Cursor, Hash, InternalNode, LeafNode, MemoryStore, Node, NodeStore,
    TreeError, batch_mutate, delete, insert,
};

type KeyValue = (Vec<u8>, Vec<u8>);

#[derive(Debug, Default)]
struct CountingStore {
    inner: MemoryStore,
    reads: Cell<usize>,
    writes: usize,
}

impl CountingStore {
    fn new() -> Self {
        Self::default()
    }

    const fn write_count(&self) -> usize {
        self.writes
    }

    const fn read_count(&self) -> usize {
        self.reads.get()
    }

    fn reset_counts(&mut self) {
        self.reads.set(0);
        self.writes = 0;
    }
}

impl NodeStore for CountingStore {
    type Error = std::convert::Infallible;

    fn get(&self, hash: &Hash) -> Result<Option<Node>, Self::Error> {
        self.reads.set(self.reads.get().saturating_add(1));
        Ok(self.inner.get(hash))
    }

    fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
        self.writes = self.writes.saturating_add(1);
        Ok(self.inner.put(node))
    }
}

fn store_node(store: &mut impl NodeStore, node: &Node) -> Hash {
    store.put(node).unwrap_or_else(|_| unreachable!())
}

fn store_leaf(
    store: &mut impl NodeStore,
    entries: &[(&[u8], &[u8])],
) -> Result<Hash, Box<dyn Error>> {
    let entries = entries
        .iter()
        .map(|(key, value)| ((*key).to_vec(), (*value).to_vec()))
        .collect();
    let leaf = LeafNode::new(entries)?;
    Ok(store_node(store, &Node::Leaf(leaf)))
}

fn store_internal(
    store: &mut impl NodeStore,
    children: Vec<(&[u8], Hash)>,
) -> Result<Hash, Box<dyn Error>> {
    let children = children
        .into_iter()
        .map(|(key, hash)| (key.to_vec(), hash))
        .collect();
    let internal = InternalNode::new(children)?;
    Ok(store_node(store, &Node::Internal(internal)))
}

fn empty_root(store: &mut impl NodeStore) -> Result<Hash, Box<dyn Error>> {
    store_leaf(store, &[])
}

fn collect_range<S: NodeStore + ?Sized>(
    cursor: &Cursor<'_, S>,
    from: &[u8],
    to: &[u8],
) -> Result<Vec<KeyValue>, TreeError> {
    cursor.range(from, to).collect()
}

fn non_boundary_keys(count: usize) -> Vec<Vec<u8>> {
    let detector = BoundaryDetector::default();
    let mut keys = Vec::with_capacity(count);
    let mut next = 0_u64;
    while keys.len() < count {
        let key = format!("k{next:020}").into_bytes();
        if !detector.is_boundary(key.as_slice()) {
            keys.push(key);
        }
        next = next.saturating_add(1);
    }
    keys
}

fn boundary_key_with_successor() -> (Vec<u8>, Vec<u8>) {
    let detector = BoundaryDetector::default();
    let mut next = 0_u64;
    loop {
        let key = format!("b{next:020}").into_bytes();
        let successor = format!("b{:020}", next.saturating_add(1)).into_bytes();
        if detector.is_boundary(key.as_slice()) && key < successor {
            return (key, successor);
        }
        next = next.saturating_add(1);
    }
}

fn boundary_keys(count: usize) -> Vec<Vec<u8>> {
    let detector = BoundaryDetector::default();
    let mut keys = Vec::with_capacity(count);
    let mut next = 0_u64;
    while keys.len() < count {
        let key = format!("s{next:020}").into_bytes();
        if detector.is_boundary(key.as_slice()) {
            keys.push(key);
        }
        next = next.saturating_add(1);
    }
    keys
}

#[test]
fn cursor_is_shared_lazy_and_debuggable() -> Result<(), Box<dyn Error>> {
    fn assert_error<E: std::error::Error>() {}
    assert_error::<TreeError>();

    let mut store = CountingStore::new();
    let root = empty_root(&mut store)?;
    store.reset_counts();

    let cursor: Cursor<'_, CountingStore> = Cursor::new(&store, root);
    let trait_store: &dyn NodeStore<Error = std::convert::Infallible> = &store;
    let object_cursor: Cursor<'_, dyn NodeStore<Error = std::convert::Infallible>> = Cursor::new(trait_store, root);

    assert_eq!(cursor.root_hash(), root);
    assert_eq!(object_cursor.root_hash(), root);
    assert_eq!(store.read_count(), 0);
    assert!(format!("{cursor:?}").contains("root_hash"));
    Ok(())
}

#[test]
fn cursor_get_follows_only_the_direct_path() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::new();
    let leaf_a = store_leaf(&mut store, &[(b"a", b"one")])?;
    let leaf_m = store_leaf(&mut store, &[(b"m", b"two")])?;
    let leaf_z = store_leaf(&mut store, &[(b"z", b"three")])?;
    let internal_left = store_internal(&mut store, vec![(b"a", leaf_a), (b"m", leaf_m)])?;
    let internal_right = store_internal(&mut store, vec![(b"z", leaf_z)])?;
    let root = store_internal(
        &mut store,
        vec![(b"a", internal_left), (b"z", internal_right)],
    )?;
    store.reset_counts();

    let cursor = Cursor::new(&store, root);
    assert_eq!(cursor.get(b"m")?, Some(b"two".to_vec()));
    assert_eq!(store.read_count(), 3);
    assert_eq!(cursor.get(b"n")?, None);
    Ok(())
}

#[test]
fn cursor_get_reports_missing_nodes_and_empty_leaf_absence() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::new();
    let missing = Hash::from_bytes([9; 32]);
    let root = store_internal(&mut store, vec![(b"a", missing)])?;
    let cursor = Cursor::new(&store, root);
    assert!(matches!(
        cursor.get(b"a"),
        Err(TreeError::MissingNode { hash }) if hash == missing
    ));

    let empty = empty_root(&mut store)?;
    let cursor = Cursor::new(&store, empty);
    assert_eq!(cursor.get(b"absent")?, None);
    Ok(())
}

#[test]
fn range_is_sorted_half_open_and_lazy_across_leaves() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::new();
    let leaf_a = store_leaf(&mut store, &[(b"a", b"one")])?;
    let leaf_m = store_leaf(&mut store, &[(b"m", b"two")])?;
    let leaf_x = store_leaf(&mut store, &[(b"x", b"three")])?;
    let root = store_internal(
        &mut store,
        vec![(b"a", leaf_a), (b"m", leaf_m), (b"x", leaf_x)],
    )?;
    store.reset_counts();

    let cursor = Cursor::new(&store, root);
    let mut iter = cursor.range(b"a", b"z");
    assert_eq!(store.read_count(), 0);
    assert_eq!(
        iter.next().transpose()?,
        Some((b"a".to_vec(), b"one".to_vec()))
    );
    assert_eq!(store.read_count(), 2);
    assert_eq!(
        iter.next().transpose()?,
        Some((b"m".to_vec(), b"two".to_vec()))
    );
    assert_eq!(
        iter.next().transpose()?,
        Some((b"x".to_vec(), b"three".to_vec()))
    );
    assert_eq!(iter.next().transpose()?, None);

    let all = collect_range(&cursor, b"", b"\xff")?;
    assert_eq!(
        all,
        vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"m".to_vec(), b"two".to_vec()),
            (b"x".to_vec(), b"three".to_vec()),
        ]
    );
    assert!(collect_range(&cursor, b"a", b"a")?.is_empty());
    assert!(collect_range(&cursor, b"z", b"a")?.is_empty());
    Ok(())
}

#[test]
fn insert_rewrites_immutably_splits_roots_and_is_history_independent() -> Result<(), Box<dyn Error>>
{
    let mut store = MemoryStore::new();
    let root = empty_root(&mut store)?;
    let (boundary, successor) = boundary_key_with_successor();

    let first = insert(&mut store, root, boundary.as_slice(), b"boundary")?;
    assert_ne!(first, root);
    let split = insert(&mut store, first, successor.as_slice(), b"successor")?;
    assert_ne!(split, first);

    let cursor = Cursor::new(&store, split);
    assert_eq!(
        cursor.get(successor.as_slice())?,
        Some(b"successor".to_vec())
    );
    let replaced = insert(&mut store, split, successor.as_slice(), b"second")?;
    assert_ne!(replaced, split);
    assert_eq!(
        Cursor::new(&store, replaced).get(successor.as_slice())?,
        Some(b"second".to_vec())
    );

    let Some(Node::Internal(internal)) = store.get(&split) else {
        return Err("split root was not an internal node".into());
    };
    assert_eq!(internal.children().len(), 2);
    for (_separator, child_hash) in internal.children() {
        assert!(matches!(store.get(child_hash), Some(Node::Leaf(_))));
    }

    let mut reverse_split_store = MemoryStore::new();
    let reverse_split_root = empty_root(&mut reverse_split_store)?;
    let reverse_split_root = insert(
        &mut reverse_split_store,
        reverse_split_root,
        successor.as_slice(),
        b"successor",
    )?;
    let reverse_split = insert(
        &mut reverse_split_store,
        reverse_split_root,
        boundary.as_slice(),
        b"boundary",
    )?;
    assert_eq!(split, reverse_split);

    let mut sibling_store = MemoryStore::new();
    let left = store_leaf(&mut sibling_store, &[(b"a", b"one")])?;
    let right = store_leaf(&mut sibling_store, &[(b"m", b"two")])?;
    let manual_root = store_internal(&mut sibling_store, vec![(b"a", left), (b"m", right)])?;
    let rewritten = insert(&mut sibling_store, manual_root, b"b", b"inserted")?;
    let Some(Node::Internal(internal)) = sibling_store.get(&rewritten) else {
        return Err("rewritten root was not an internal node".into());
    };
    assert!(
        internal
            .children()
            .iter()
            .any(|(_key, hash)| *hash == right)
    );

    let split_keys = boundary_keys(12);
    let mut split_store = MemoryStore::new();
    let mut split_root = empty_root(&mut split_store)?;
    for key in &split_keys {
        split_root = insert(&mut split_store, split_root, key.as_slice(), b"value")?;
    }
    let Some(Node::Internal(internal)) = split_store.get(&split_root) else {
        return Err("multi-boundary split root was not an internal node".into());
    };
    assert_eq!(internal.children().len(), 2);

    let mut split_reverse_store = MemoryStore::new();
    let mut split_reverse = empty_root(&mut split_reverse_store)?;
    for key in split_keys.iter().rev() {
        split_reverse = insert(
            &mut split_reverse_store,
            split_reverse,
            key.as_slice(),
            b"value",
        )?;
    }
    assert_eq!(split_root, split_reverse);

    let split_mutations: Vec<(Vec<u8>, Option<Vec<u8>>)> = split_keys
        .iter()
        .map(|key| (key.clone(), Some(b"value".to_vec())))
        .collect();
    let mut split_batch_store = MemoryStore::new();
    let split_batch_root = empty_root(&mut split_batch_store)?;
    let split_batch = batch_mutate(
        &mut split_batch_store,
        split_batch_root,
        split_mutations.as_slice(),
    )?;
    assert_eq!(split_batch, split_root);

    let keys = non_boundary_keys(100);
    let mut forward_store = MemoryStore::new();
    let mut forward = empty_root(&mut forward_store)?;
    for key in &keys {
        forward = insert(&mut forward_store, forward, key.as_slice(), b"value")?;
    }

    let mut reverse_store = MemoryStore::new();
    let mut reverse = empty_root(&mut reverse_store)?;
    for key in keys.iter().rev() {
        reverse = insert(&mut reverse_store, reverse, key.as_slice(), b"value")?;
    }
    assert_eq!(forward, reverse);
    Ok(())
}

#[test]
fn delete_is_idempotent_collapses_empty_nodes_and_preserves_canonical_roots()
-> Result<(), Box<dyn Error>> {
    let mut store = MemoryStore::new();
    let mut root = empty_root(&mut store)?;
    root = insert(&mut store, root, b"a", b"one")?;
    root = insert(&mut store, root, b"b", b"two")?;
    let deleted = delete(&mut store, root, b"a")?;
    assert_ne!(deleted, root);
    assert_eq!(Cursor::new(&store, deleted).get(b"a")?, None);
    assert_eq!(delete(&mut store, deleted, b"absent")?, deleted);
    let restored = insert(&mut store, deleted, b"a", b"one")?;
    assert_eq!(restored, root);

    let mut single_store = MemoryStore::new();
    let single = empty_root(&mut single_store)?;
    let single = insert(&mut single_store, single, b"only", b"value")?;
    let empty = delete(&mut single_store, single, b"only")?;
    assert!(
        matches!(single_store.get(&empty), Some(Node::Leaf(ref leaf)) if leaf.entries().is_empty())
    );

    let mut parent_store = MemoryStore::new();
    let left = store_leaf(&mut parent_store, &[(b"a", b"one")])?;
    let middle = store_leaf(&mut parent_store, &[(b"m", b"two")])?;
    let right = store_leaf(&mut parent_store, &[(b"z", b"three")])?;
    let root = store_internal(
        &mut parent_store,
        vec![(b"a", left), (b"m", middle), (b"z", right)],
    )?;
    let without_middle = delete(&mut parent_store, root, b"m")?;
    let Some(Node::Internal(internal)) = parent_store.get(&without_middle) else {
        return Err("root after middle delete was not internal".into());
    };
    assert_eq!(
        internal.children(),
        &[(b"a".to_vec(), left), (b"z".to_vec(), right)]
    );
    Ok(())
}

#[test]
fn batch_mutate_matches_individual_operations_and_writes_fewer_nodes() -> Result<(), Box<dyn Error>>
{
    let keys = non_boundary_keys(100);
    let mutations: Vec<(Vec<u8>, Option<Vec<u8>>)> = keys
        .iter()
        .map(|key| (key.clone(), Some(b"value".to_vec())))
        .collect();

    let mut individual_store = CountingStore::new();
    let mut individual = empty_root(&mut individual_store)?;
    individual_store.reset_counts();
    for key in &keys {
        individual = insert(&mut individual_store, individual, key.as_slice(), b"value")?;
    }
    let individual_writes = individual_store.write_count();

    let mut batch_store = CountingStore::new();
    let batch_root = empty_root(&mut batch_store)?;
    batch_store.reset_counts();
    let batch = batch_mutate(&mut batch_store, batch_root, mutations.as_slice())?;
    let batch_writes = batch_store.write_count();
    assert_eq!(batch, individual);
    assert!(batch_writes < individual_writes);
    assert_eq!(batch_mutate(&mut batch_store, batch, &[])?, batch);

    let two = &mut MemoryStore::new();
    let two_root = empty_root(two)?;
    let batched_two = batch_mutate(
        two,
        two_root,
        &[
            (b"k1".to_vec(), Some(b"v1".to_vec())),
            (b"k2".to_vec(), Some(b"v2".to_vec())),
        ],
    )?;
    let one_insert = insert(two, two_root, b"k1", b"v1")?;
    let individual_two = insert(two, one_insert, b"k2", b"v2")?;
    assert_eq!(batched_two, individual_two);

    let delete_batch = batch_mutate(two, individual_two, &[(b"k1".to_vec(), None)])?;
    assert_eq!(delete_batch, delete(two, individual_two, b"k1")?);

    let interleaved = batch_mutate(
        two,
        individual_two,
        &[
            (b"k1".to_vec(), None),
            (b"k2".to_vec(), Some(b"v2b".to_vec())),
            (b"k3".to_vec(), Some(b"v3".to_vec())),
        ],
    )?;
    let mut sequential = individual_two;
    sequential = delete(two, sequential, b"k1")?;
    sequential = insert(two, sequential, b"k2", b"v2b")?;
    sequential = insert(two, sequential, b"k3", b"v3")?;
    assert_eq!(interleaved, sequential);
    Ok(())
}
