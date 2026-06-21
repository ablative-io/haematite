use super::{BranchError, BranchHandle, DEFAULT_SHARD_ID, ShardId};
use crate::store::{MemoryStore, NodeStore};
use crate::tree::{Cursor, Hash, LeafNode, Node, TreeError, insert};
use crate::wal::LookupResult;
use std::cell::Cell;
use std::convert::Infallible;
use std::fmt::Debug;
use std::sync::Arc;

fn hash(byte: u8) -> Hash {
    Hash::from_bytes([byte; 32])
}

fn empty_root(store: &mut MemoryStore) -> Result<Hash, TreeError> {
    let leaf = LeafNode::new(Vec::new())?;
    Ok(store.put(&Node::Leaf(leaf)))
}

fn build_tree(store: &mut MemoryStore, entries: &[(&[u8], &[u8])]) -> Result<Hash, TreeError> {
    let mut root = empty_root(store)?;
    for (key, value) in entries {
        root = insert(store, root, key, value)?;
    }
    Ok(root)
}

fn assert_debug_clone<T: Debug + Clone>() {}

fn buffer_is_empty(branch: &BranchHandle, shard_id: ShardId) -> Result<bool, BranchError> {
    let buffer = branch
        .shard_buffer(shard_id)
        .ok_or(BranchError::UnknownShard { shard_id })?;
    match buffer.lock() {
        Ok(guard) => Ok(guard.is_empty()),
        Err(poisoned) => {
            drop(poisoned.into_inner());
            Err(BranchError::BufferPoisoned { shard_id })
        }
    }
}

fn buffer_lookup(
    branch: &BranchHandle,
    shard_id: ShardId,
    key: &[u8],
) -> Result<LookupResult, BranchError> {
    let buffer = branch
        .shard_buffer(shard_id)
        .ok_or(BranchError::UnknownShard { shard_id })?;
    match buffer.lock() {
        Ok(guard) => Ok(guard.get(key)),
        Err(poisoned) => {
            drop(poisoned.into_inner());
            Err(BranchError::BufferPoisoned { shard_id })
        }
    }
}

#[test]
fn branch_handle_records_fork_point_current_root_and_empty_buffer() -> Result<(), BranchError> {
    let root = hash(1);
    let branch = BranchHandle::new(root);

    assert_eq!(branch.fork_point(), root);
    assert_eq!(branch.current_root(), root);
    assert_eq!(branch.primary_shard(), DEFAULT_SHARD_ID);
    assert!(buffer_is_empty(&branch, DEFAULT_SHARD_ID)?);
    assert_debug_clone::<BranchHandle>();
    Ok(())
}

#[test]
fn branch_handle_clone_shares_the_same_branch_buffer() -> Result<(), BranchError> {
    let branch = BranchHandle::new(hash(2));
    let clone = branch.clone();

    branch.put(DEFAULT_SHARD_ID, b"key", b"branch")?;

    assert_eq!(
        buffer_lookup(&clone, DEFAULT_SHARD_ID, b"key")?,
        LookupResult::BufferedValue(b"branch".to_vec())
    );
    Ok(())
}

#[test]
fn branch_put_does_not_affect_parent_tree() -> Result<(), BranchError> {
    let mut store = MemoryStore::new();
    let root = build_tree(&mut store, &[(b"parent", b"value")])?;
    let branch = BranchHandle::new(root);

    branch.put(DEFAULT_SHARD_ID, b"branch", b"only")?;

    assert_eq!(Cursor::new(&store, root).get(b"branch")?, None);
    assert_eq!(
        branch.get(DEFAULT_SHARD_ID, &store, b"branch")?,
        Some(b"only".to_vec())
    );
    Ok(())
}

#[test]
fn branch_does_not_observe_parent_writes_after_fork() -> Result<(), BranchError> {
    let mut store = MemoryStore::new();
    let root = build_tree(&mut store, &[(b"a", b"1")])?;
    let branch = BranchHandle::new(root);

    let parent_root = insert(&mut store, root, b"b", b"2")?;
    assert_eq!(
        Cursor::new(&store, parent_root).get(b"b")?,
        Some(b"2".to_vec())
    );

    assert_eq!(branch.get(DEFAULT_SHARD_ID, &store, b"b")?, None);
    Ok(())
}

#[test]
fn branch_get_checks_buffer_before_shared_tree() -> Result<(), BranchError> {
    let mut store = MemoryStore::new();
    let root = build_tree(&mut store, &[(b"key", b"tree"), (b"other", b"tree")])?;
    let branch = BranchHandle::new(root);

    branch.put(DEFAULT_SHARD_ID, b"key", b"buffer")?;

    assert_eq!(
        branch.get(DEFAULT_SHARD_ID, &store, b"key")?,
        Some(b"buffer".to_vec())
    );
    assert_eq!(
        branch.get(DEFAULT_SHARD_ID, &store, b"other")?,
        Some(b"tree".to_vec())
    );
    Ok(())
}

#[test]
fn branch_delete_shadows_shared_tree_value() -> Result<(), BranchError> {
    let mut store = MemoryStore::new();
    let root = build_tree(&mut store, &[(b"key", b"tree")])?;
    let branch = BranchHandle::new(root);

    branch.delete(DEFAULT_SHARD_ID, b"key")?;

    assert_eq!(branch.get(DEFAULT_SHARD_ID, &store, b"key")?, None);
    Ok(())
}

#[test]
fn per_shard_roots_and_buffers_are_independent() -> Result<(), BranchError> {
    let shard_three_root = hash(3);
    let shard_five_root = hash(5);
    let branch = BranchHandle::from_shard_roots([(3, shard_three_root), (5, shard_five_root)])?;

    assert_eq!(branch.shard_count(), 2);
    assert_eq!(branch.primary_shard(), 3);
    assert_eq!(branch.shard_fork_point(3), Some(shard_three_root));
    assert_eq!(branch.shard_fork_point(5), Some(shard_five_root));
    assert_ne!(
        Arc::as_ptr(
            branch
                .shard_buffer(3)
                .ok_or(BranchError::UnknownShard { shard_id: 3 })?
        ),
        Arc::as_ptr(
            branch
                .shard_buffer(5)
                .ok_or(BranchError::UnknownShard { shard_id: 5 })?
        )
    );

    branch.put(3, b"same-key", b"shard-three")?;

    assert_eq!(
        buffer_lookup(&branch, 3, b"same-key")?,
        LookupResult::BufferedValue(b"shard-three".to_vec())
    );
    assert_eq!(
        buffer_lookup(&branch, 5, b"same-key")?,
        LookupResult::NotBuffered
    );
    Ok(())
}

#[test]
fn branch_routes_gets_to_the_requested_shard() -> Result<(), BranchError> {
    let mut store = MemoryStore::new();
    let shard_three_root = build_tree(&mut store, &[(b"key", b"three")])?;
    let shard_five_root = build_tree(&mut store, &[(b"key", b"five")])?;
    let branch = BranchHandle::from_shard_roots([(3, shard_three_root), (5, shard_five_root)])?;

    branch.put(3, b"buffered", b"three-buffer")?;
    branch.put(5, b"buffered", b"five-buffer")?;

    assert_eq!(branch.get(3, &store, b"key")?, Some(b"three".to_vec()));
    assert_eq!(branch.get(5, &store, b"key")?, Some(b"five".to_vec()));
    assert_eq!(
        branch.get(3, &store, b"buffered")?,
        Some(b"three-buffer".to_vec())
    );
    assert_eq!(
        branch.get(5, &store, b"buffered")?,
        Some(b"five-buffer".to_vec())
    );
    Ok(())
}

#[test]
fn empty_and_duplicate_shard_construction_errors() {
    assert!(matches!(
        BranchHandle::from_shard_roots([]),
        Err(BranchError::NoShards)
    ));
    assert!(matches!(
        BranchHandle::from_shard_roots([(7, hash(7)), (7, hash(8))]),
        Err(BranchError::DuplicateShard { shard_id: 7 })
    ));
}

#[derive(Debug)]
struct CountingStore {
    root: Hash,
    node: Node,
    gets: Cell<usize>,
}

impl CountingStore {
    fn new(node: Node) -> Self {
        let root = node.hash();
        Self {
            root,
            node,
            gets: Cell::new(0),
        }
    }
}

impl NodeStore for CountingStore {
    type Error = Infallible;

    fn get(&self, hash: &Hash) -> Result<Option<Node>, Self::Error> {
        self.gets.set(self.gets.get().saturating_add(1));
        if *hash == self.root {
            Ok(Some(self.node.clone()))
        } else {
            Ok(None)
        }
    }

    fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
        Ok(node.hash())
    }
}

#[test]
fn branch_buffer_hit_does_not_read_tree_nodes() -> Result<(), TreeError> {
    let leaf = LeafNode::new(vec![(b"key".to_vec(), b"tree".to_vec())])?;
    let store = CountingStore::new(Node::Leaf(leaf));
    let branch = BranchHandle::new(store.root);

    branch
        .put(DEFAULT_SHARD_ID, b"key", b"buffer")
        .map_err(branch_error_to_tree_error)?;
    assert_eq!(
        branch
            .get(DEFAULT_SHARD_ID, &store, b"key")
            .map_err(branch_error_to_tree_error)?,
        Some(b"buffer".to_vec())
    );
    assert_eq!(store.gets.get(), 0);
    Ok(())
}

fn branch_error_to_tree_error(error: BranchError) -> TreeError {
    match error {
        BranchError::Tree(error) => error,
        BranchError::NoShards
        | BranchError::DuplicateShard { .. }
        | BranchError::UnknownShard { .. }
        | BranchError::BufferPoisoned { .. } => TreeError::InvalidNode,
    }
}
