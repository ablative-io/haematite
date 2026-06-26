use std::cell::Cell;
use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::branch::conflict::ConflictPolicy;
use crate::store::NodeStore;
use crate::tree::{Cursor, Hash, LeafNode, Node, batch_mutate};

use super::{MergeError, merge};

static CUSTOM_CALLS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Default)]
struct CountingStore {
    nodes: HashMap<Hash, Vec<u8>>,
    gets: Cell<usize>,
    leaf_gets: Cell<usize>,
    puts: Cell<usize>,
}

impl NodeStore for CountingStore {
    type Error = Infallible;

    fn get(&self, hash: &Hash) -> Result<Option<std::sync::Arc<Node>>, Self::Error> {
        self.gets.set(self.gets.get().saturating_add(1));
        let node = self
            .nodes
            .get(hash)
            .and_then(|serialised| Node::deserialise(serialised).ok())
            .map(std::sync::Arc::new);
        if matches!(node.as_deref(), Some(Node::Leaf(_))) {
            self.leaf_gets.set(self.leaf_gets.get().saturating_add(1));
        }
        Ok(node)
    }

    fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
        self.puts.set(self.puts.get().saturating_add(1));
        let hash = node.hash();
        self.nodes.insert(hash, node.serialise());
        Ok(hash)
    }
}

impl CountingStore {
    fn reset_counts(&self) {
        self.gets.set(0);
        self.leaf_gets.set(0);
        self.puts.set(0);
    }

    fn get_count(&self) -> usize {
        self.gets.get()
    }

    fn leaf_get_count(&self) -> usize {
        self.leaf_gets.get()
    }

    fn put_count(&self) -> usize {
        self.puts.get()
    }

    fn leaf_count_for_root(&self, root: Hash) -> Result<usize, MergeError> {
        match self.raw_node(root)? {
            Node::Leaf(_) => Ok(1),
            Node::Internal(internal) => {
                internal
                    .children()
                    .iter()
                    .try_fold(0_usize, |count, (_separator, child_hash)| {
                        self.leaf_count_for_root(*child_hash)
                            .map(|child_count| count.saturating_add(child_count))
                    })
            }
        }
    }

    fn raw_node(&self, hash: Hash) -> Result<Node, MergeError> {
        self.nodes
            .get(&hash)
            .and_then(|serialised| Node::deserialise(serialised).ok())
            .ok_or(MergeError::MissingNode { hash })
    }
}

fn custom_counting_resolution(
    key: &[u8],
    ancestor_value: Option<&[u8]>,
    parent_value: Option<&[u8]>,
    branch_value: Option<&[u8]>,
) -> Option<Vec<u8>> {
    CUSTOM_CALLS.fetch_add(1, Ordering::SeqCst);
    if key.is_empty()
        && ancestor_value.is_none()
        && parent_value.is_none()
        && branch_value.is_none()
    {
        None
    } else {
        Some(b"custom".to_vec())
    }
}

fn custom_argument_resolution(
    key: &[u8],
    ancestor_value: Option<&[u8]>,
    parent_value: Option<&[u8]>,
    branch_value: Option<&[u8]>,
) -> Option<Vec<u8>> {
    if key.is_empty() {
        return None;
    }

    let mut resolved = Vec::new();
    resolved.extend_from_slice(key);
    resolved.push(b'|');
    resolved.extend_from_slice(ancestor_value.unwrap_or(b"none"));
    resolved.push(b'|');
    resolved.extend_from_slice(parent_value.unwrap_or(b"none"));
    resolved.push(b'|');
    resolved.extend_from_slice(branch_value.unwrap_or(b"none"));
    Some(resolved)
}

fn custom_delete_resolution(
    key: &[u8],
    ancestor_value: Option<&[u8]>,
    parent_value: Option<&[u8]>,
    branch_value: Option<&[u8]>,
) -> Option<Vec<u8>> {
    if key == b"delete-me"
        || (ancestor_value.is_none() && parent_value.is_none() && branch_value.is_none())
    {
        None
    } else {
        Some(b"kept".to_vec())
    }
}

fn store_node(store: &mut CountingStore, node: &Node) -> Hash {
    match store.put(node) {
        Ok(hash) => hash,
        Err(error) => match error {},
    }
}

fn empty_root(store: &mut CountingStore) -> Result<Hash, Box<dyn Error>> {
    let leaf = Node::Leaf(LeafNode::new(Vec::new())?);
    Ok(store_node(store, &leaf))
}

fn build_root(
    store: &mut CountingStore,
    mutations: &[(Vec<u8>, Option<Vec<u8>>)],
) -> Result<Hash, Box<dyn Error>> {
    let root = empty_root(store)?;
    Ok(batch_mutate(store, root, mutations)?)
}

fn value(store: &CountingStore, root: Hash, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
    Ok(Cursor::new(store, root).get(key)?)
}

fn put_mutation(key: &[u8], value: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
    (key.to_vec(), Some(value.to_vec()))
}

fn delete_mutation(key: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
    (key.to_vec(), None)
}

fn numbered_put(index: u32, prefix: &str) -> (Vec<u8>, Option<Vec<u8>>) {
    (
        format!("key-{index:06}").into_bytes(),
        Some(format!("{prefix}-{index:06}").into_bytes()),
    )
}

#[test]
fn identical_roots_return_without_node_reads() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::default();
    let root = build_root(&mut store, &[put_mutation(b"a", b"one")])?;
    store.reset_counts();

    let merged = merge(&mut store, root, root, root, &ConflictPolicy::Lww)?;

    assert_eq!(merged, root);
    assert_eq!(store.get_count(), 0);
    assert_eq!(store.put_count(), 0);
    Ok(())
}

#[test]
fn branch_only_modification_is_applied_without_custom_policy() -> Result<(), Box<dyn Error>> {
    CUSTOM_CALLS.store(0, Ordering::SeqCst);
    let mut store = CountingStore::default();
    let ancestor = build_root(&mut store, &[put_mutation(b"k", b"base")])?;
    let parent = ancestor;
    let branch = batch_mutate(&mut store, ancestor, &[put_mutation(b"k", b"branch")])?;

    let merged = merge(
        &mut store,
        parent,
        branch,
        ancestor,
        &ConflictPolicy::Custom(custom_counting_resolution),
    )?;

    assert_eq!(value(&store, merged, b"k")?, Some(b"branch".to_vec()));
    assert_eq!(CUSTOM_CALLS.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn parent_only_modification_remains_without_custom_policy() -> Result<(), Box<dyn Error>> {
    CUSTOM_CALLS.store(0, Ordering::SeqCst);
    let mut store = CountingStore::default();
    let ancestor = build_root(&mut store, &[put_mutation(b"k", b"base")])?;
    let parent = batch_mutate(&mut store, ancestor, &[put_mutation(b"k", b"parent")])?;
    let branch = ancestor;

    let merged = merge(
        &mut store,
        parent,
        branch,
        ancestor,
        &ConflictPolicy::Custom(custom_counting_resolution),
    )?;

    assert_eq!(value(&store, merged, b"k")?, Some(b"parent".to_vec()));
    assert_eq!(CUSTOM_CALLS.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn branch_only_delete_removes_key() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::default();
    let ancestor = build_root(&mut store, &[put_mutation(b"k", b"base")])?;
    let branch = batch_mutate(&mut store, ancestor, &[delete_mutation(b"k")])?;

    let merged = merge(&mut store, ancestor, branch, ancestor, &ConflictPolicy::Lww)?;

    assert_eq!(value(&store, merged, b"k")?, None);
    Ok(())
}

#[test]
fn lww_routes_modify_modify_conflict_to_branch_value() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::default();
    let ancestor = build_root(&mut store, &[put_mutation(b"k", b"base")])?;
    let parent = batch_mutate(&mut store, ancestor, &[put_mutation(b"k", b"parent")])?;
    let branch = batch_mutate(&mut store, ancestor, &[put_mutation(b"k", b"branch")])?;

    let merged = merge(&mut store, parent, branch, ancestor, &ConflictPolicy::Lww)?;

    assert_eq!(value(&store, merged, b"k")?, Some(b"branch".to_vec()));
    Ok(())
}

#[test]
fn delete_modify_conflict_is_routed_to_policy() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::default();
    let ancestor = build_root(&mut store, &[put_mutation(b"k", b"base")])?;
    let parent = batch_mutate(&mut store, ancestor, &[delete_mutation(b"k")])?;
    let branch = batch_mutate(&mut store, ancestor, &[put_mutation(b"k", b"branch")])?;

    let merged = merge(
        &mut store,
        parent,
        branch,
        ancestor,
        &ConflictPolicy::Custom(custom_argument_resolution),
    )?;

    assert_eq!(
        value(&store, merged, b"k")?,
        Some(b"k|base|none|branch".to_vec())
    );
    Ok(())
}

#[test]
fn custom_policy_returning_none_deletes_key() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::default();
    let ancestor = build_root(&mut store, &[put_mutation(b"delete-me", b"base")])?;
    let parent = batch_mutate(
        &mut store,
        ancestor,
        &[put_mutation(b"delete-me", b"parent")],
    )?;
    let branch = batch_mutate(
        &mut store,
        ancestor,
        &[put_mutation(b"delete-me", b"branch")],
    )?;

    let merged = merge(
        &mut store,
        parent,
        branch,
        ancestor,
        &ConflictPolicy::Custom(custom_delete_resolution),
    )?;

    assert_eq!(value(&store, merged, b"delete-me")?, None);
    Ok(())
}

#[test]
fn vector_clock_surfaces_true_conflicts_as_unimplemented() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::default();
    let ancestor = build_root(&mut store, &[put_mutation(b"k", b"base")])?;
    let parent = batch_mutate(&mut store, ancestor, &[put_mutation(b"k", b"parent")])?;
    let branch = batch_mutate(&mut store, ancestor, &[put_mutation(b"k", b"branch")])?;

    let result = merge(
        &mut store,
        parent,
        branch,
        ancestor,
        &ConflictPolicy::VectorClock,
    );

    assert_eq!(
        result,
        Err(MergeError::Unimplemented {
            feature: "vector-clock conflict resolution"
        })
    );
    Ok(())
}

#[test]
fn vector_clock_allows_clean_merges_without_conflicts() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::default();
    let ancestor = build_root(&mut store, &[put_mutation(b"k", b"base")])?;
    let branch = batch_mutate(&mut store, ancestor, &[put_mutation(b"k", b"branch")])?;

    let merged = merge(
        &mut store,
        ancestor,
        branch,
        ancestor,
        &ConflictPolicy::VectorClock,
    )?;

    assert_eq!(value(&store, merged, b"k")?, Some(b"branch".to_vec()));
    Ok(())
}

#[test]
fn same_roots_can_be_merged_with_different_policies() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::default();
    let ancestor = build_root(&mut store, &[put_mutation(b"k", b"base")])?;
    let parent = batch_mutate(&mut store, ancestor, &[put_mutation(b"k", b"parent")])?;
    let branch = batch_mutate(&mut store, ancestor, &[put_mutation(b"k", b"branch")])?;

    let lww = merge(&mut store, parent, branch, ancestor, &ConflictPolicy::Lww)?;
    let custom = merge(
        &mut store,
        parent,
        branch,
        ancestor,
        &ConflictPolicy::Custom(custom_argument_resolution),
    )?;

    assert_eq!(value(&store, lww, b"k")?, Some(b"branch".to_vec()));
    assert_eq!(
        value(&store, custom, b"k")?,
        Some(b"k|base|parent|branch".to_vec())
    );
    Ok(())
}

#[test]
fn merge_accumulates_resolutions_into_one_batch() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::default();
    let ancestor = build_root(&mut store, &[put_mutation(b"a", b"base")])?;
    let parent = batch_mutate(&mut store, ancestor, &[put_mutation(b"a", b"parent")])?;
    let branch_mutations: Vec<_> = (0_u32..50)
        .map(|index| numbered_put(index, "branch"))
        .chain([put_mutation(b"a", b"branch")])
        .collect();
    let branch = batch_mutate(&mut store, ancestor, branch_mutations.as_slice())?;

    let expected_mutations: Vec<_> = (0_u32..50)
        .map(|index| numbered_put(index, "branch"))
        .chain([put_mutation(b"a", b"branch")])
        .collect();
    let mut reference = CountingStore::default();
    let reference_ancestor = build_root(&mut reference, &[put_mutation(b"a", b"base")])?;
    let reference_parent = batch_mutate(
        &mut reference,
        reference_ancestor,
        &[put_mutation(b"a", b"parent")],
    )?;
    reference.reset_counts();
    let expected_root = batch_mutate(
        &mut reference,
        reference_parent,
        expected_mutations.as_slice(),
    )?;
    let expected_puts = reference.put_count();

    store.reset_counts();
    let merged = merge(&mut store, parent, branch, ancestor, &ConflictPolicy::Lww)?;
    let merge_puts = store.put_count();

    assert_eq!(merged, expected_root);
    assert_eq!(merge_puts, expected_puts);
    assert!(merge_puts < 51);
    Ok(())
}

#[test]
fn sparse_merge_reads_only_changed_paths_not_all_leaves() -> Result<(), Box<dyn Error>> {
    let mut store = CountingStore::default();
    let ancestor_mutations: Vec<_> = (0_u32..100_000)
        .map(|index| numbered_put(index, "base"))
        .collect();
    let ancestor = build_root(&mut store, ancestor_mutations.as_slice())?;
    let parent_mutations: Vec<_> = (0_u32..10)
        .map(|index| numbered_put(index, "parent"))
        .collect();
    let branch_mutations: Vec<_> = (10_u32..20)
        .map(|index| numbered_put(index, "branch"))
        .collect();
    let parent = batch_mutate(&mut store, ancestor, parent_mutations.as_slice())?;
    let branch = batch_mutate(&mut store, ancestor, branch_mutations.as_slice())?;
    let total_leaves = store.leaf_count_for_root(ancestor)?;
    store.reset_counts();

    let merged = merge(&mut store, parent, branch, ancestor, &ConflictPolicy::Lww)?;

    assert_eq!(
        value(&store, merged, b"key-000000")?,
        Some(b"parent-000000".to_vec())
    );
    assert_eq!(
        value(&store, merged, b"key-000010")?,
        Some(b"branch-000010".to_vec())
    );
    assert!(total_leaves > 1);
    assert!(store.leaf_get_count() < total_leaves);
    Ok(())
}
