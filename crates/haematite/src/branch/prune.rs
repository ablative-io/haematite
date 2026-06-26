// BRANCH-004: Snapshot pruning and unreachable node reclamation.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use crate::store::{DeleteNode, NodeStore};
use crate::tree::{Hash, Node};

use super::registry::BranchRegistry;
use super::snapshot::{SnapshotError, SnapshotRegistry};

/// Summary of one caller-initiated pruning run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PruneReport {
    /// Number of content-addressed tree nodes deleted from the store.
    pub node_count: usize,
    /// Total logical bytes reclaimed, measured as each deleted node's
    /// serialised size before deletion.
    pub bytes_reclaimed: usize,
}

/// Errors raised while removing a snapshot and reclaiming unreferenced nodes.
#[derive(Debug)]
pub enum PruneError {
    /// No named snapshot exists for the requested prune target.
    UnknownSnapshot { name: String },
    /// The snapshot registry could not persist or load the requested mutation.
    SnapshotRegistry(SnapshotError),
    /// A root or child hash referenced by the graph walk was absent from the store.
    MissingNode { hash: Hash },
    /// The node store returned an error while reading a referenced hash.
    StoreRead { hash: Hash },
    /// The node store returned an error while deleting an unreferenced hash.
    NodeDelete { hash: Hash },
}

impl fmt::Display for PruneError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownSnapshot { name } => write!(formatter, "unknown snapshot: {name}"),
            Self::SnapshotRegistry(error) => write!(formatter, "snapshot registry error: {error}"),
            Self::MissingNode { hash } => write!(formatter, "missing tree node {hash}"),
            Self::StoreRead { hash } => write!(formatter, "failed to read tree node {hash}"),
            Self::NodeDelete { hash } => write!(formatter, "failed to delete tree node {hash}"),
        }
    }
}

impl std::error::Error for PruneError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SnapshotRegistry(error) => Some(error),
            Self::UnknownSnapshot { .. }
            | Self::MissingNode { .. }
            | Self::StoreRead { .. }
            | Self::NodeDelete { .. } => None,
        }
    }
}

impl From<SnapshotError> for PruneError {
    fn from(error: SnapshotError) -> Self {
        Self::SnapshotRegistry(error)
    }
}

/// Removes `name` from the snapshot registry and reclaims tree nodes that are
/// reachable from the removed snapshot root and from no live root.
///
/// Live roots are the union of active branch roots and the roots of the named
/// snapshots that remain after removal. The commit log is intentionally not
/// accepted by this API, so pruning cannot mutate its append-only contents.
///
/// The reclaim set is computed *before* the snapshot is removed from the
/// registry: a graph-walk failure (a missing or unreadable node) therefore
/// leaves the snapshot in place and the whole operation retryable, rather than
/// dropping the registry entry and orphaning its nodes. The snapshot is removed
/// only once a complete deletion set has been collected; node deletion follows.
pub fn prune<S>(
    store: &S,
    branches: &BranchRegistry,
    snapshots: &mut SnapshotRegistry,
    name: &str,
) -> Result<PruneReport, PruneError>
where
    S: NodeStore + DeleteNode + ?Sized,
{
    // Resolve the target root without removing the snapshot yet, so a failed
    // walk below is fully recoverable (the snapshot is still named).
    let removed_root = snapshots
        .get(name)
        .ok_or_else(|| PruneError::UnknownSnapshot {
            name: name.to_owned(),
        })?;

    let removed_reachable = collect_reachable(store, [removed_root])?;
    let live_roots = live_roots_excluding(branches, snapshots, name);
    let live_reachable = collect_reachable(store, live_roots)?;

    let unreferenced = unreferenced_nodes(removed_reachable, &live_reachable);
    let report = PruneReport {
        node_count: unreferenced.len(),
        bytes_reclaimed: unreferenced.iter().map(|(_hash, bytes)| *bytes).sum(),
    };

    // Both walks succeeded: commit the registry removal, then delete the
    // now-confirmed unreferenced nodes. `&mut SnapshotRegistry` gives exclusive
    // access, so the entry cannot have vanished since the `get` above.
    snapshots
        .remove(name)?
        .ok_or_else(|| PruneError::UnknownSnapshot {
            name: name.to_owned(),
        })?;

    for (hash, _bytes) in unreferenced {
        store
            .delete(&hash)
            .map_err(|_error| PruneError::NodeDelete { hash })?;
    }

    Ok(report)
}

/// Live roots that pruning `excluded` must preserve: every active branch root
/// registered in `branches`, unioned with the roots of every named snapshot
/// other than `excluded` (the snapshot being pruned, which is excluded by name
/// since it has not been removed from the registry yet).
///
/// Contract: `prune` protects exactly the nodes reachable from this set, so the
/// [`BranchRegistry`] must reflect every live branch root. Today a branch's root
/// never advances past its registered fork point — mutations accumulate in a
/// per-branch WAL buffer and no commit path moves `current_root` (which equals
/// `fork_point` for the life of the handle) — so registering fork points covers
/// all live branch roots. If a future commit operation advances `current_root`,
/// that advanced root MUST be registered here (and the superseded one
/// deregistered), otherwise pruning a snapshot could reclaim nodes still
/// reachable from the live branch.
fn live_roots_excluding(
    branches: &BranchRegistry,
    snapshots: &SnapshotRegistry,
    excluded: &str,
) -> HashSet<Hash> {
    let mut roots = branches.live_roots();
    roots.extend(
        snapshots
            .list_snapshots()
            .into_iter()
            .filter(|(name, _root_hash, _timestamp)| name != excluded)
            .map(|(_name, root_hash, _timestamp)| root_hash),
    );
    roots
}

fn collect_reachable<S, I>(store: &S, roots: I) -> Result<HashMap<Hash, usize>, PruneError>
where
    S: NodeStore + ?Sized,
    I: IntoIterator<Item = Hash>,
{
    let mut reachable = HashMap::new();
    let mut stack: Vec<Hash> = roots.into_iter().collect();

    while let Some(hash) = stack.pop() {
        if reachable.contains_key(&hash) {
            continue;
        }

        let node = load_node(store, hash)?;
        let serialised_len = node.serialise().len();
        if let Node::Internal(internal) = &*node {
            stack.extend(
                internal
                    .children()
                    .iter()
                    .map(|(_lower_bound, child_hash)| *child_hash),
            );
        }
        reachable.insert(hash, serialised_len);
    }

    Ok(reachable)
}

fn load_node<S>(store: &S, hash: Hash) -> Result<Arc<Node>, PruneError>
where
    S: NodeStore + ?Sized,
{
    store
        .get(&hash)
        .map_err(|_error| PruneError::StoreRead { hash })?
        .ok_or(PruneError::MissingNode { hash })
}

fn unreferenced_nodes(
    removed_reachable: HashMap<Hash, usize>,
    live_reachable: &HashMap<Hash, usize>,
) -> Vec<(Hash, usize)> {
    removed_reachable
        .into_iter()
        .filter(|(hash, _bytes)| !live_reachable.contains_key(hash))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{PruneError, PruneReport, prune};
    use crate::branch::{BranchRegistry, CommitLog, SnapshotError, SnapshotRegistry};
    use crate::store::MemoryStore;
    use crate::tree::{Hash, InternalNode, LeafNode, Node, NodeError};

    #[derive(Debug)]
    enum TestError {
        Node(NodeError),
        Snapshot(SnapshotError),
        Prune(PruneError),
    }

    impl std::fmt::Display for TestError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Node(error) => write!(formatter, "node error: {error}"),
                Self::Snapshot(error) => write!(formatter, "snapshot error: {error}"),
                Self::Prune(error) => write!(formatter, "prune error: {error}"),
            }
        }
    }

    impl std::error::Error for TestError {}

    impl From<NodeError> for TestError {
        fn from(error: NodeError) -> Self {
            Self::Node(error)
        }
    }

    impl From<SnapshotError> for TestError {
        fn from(error: SnapshotError) -> Self {
            Self::Snapshot(error)
        }
    }

    impl From<PruneError> for TestError {
        fn from(error: PruneError) -> Self {
            Self::Prune(error)
        }
    }

    fn leaf(key: &[u8], value: &[u8]) -> Result<Node, NodeError> {
        LeafNode::new(vec![(key.to_vec(), value.to_vec())]).map(Node::Leaf)
    }

    fn internal(children: Vec<(&[u8], Hash)>) -> Result<Node, NodeError> {
        InternalNode::new(
            children
                .into_iter()
                .map(|(key, hash)| (key.to_vec(), hash))
                .collect(),
        )
        .map(Node::Internal)
    }

    #[test]
    fn prune_removes_snapshot_registry_entry_without_touching_commit_log() -> Result<(), TestError>
    {
        let mut store = MemoryStore::new();
        let root_node = leaf(b"root", b"value")?;
        let root_hash = store.put(&root_node);
        let expected_bytes = root_node.serialise().len();
        let branches = BranchRegistry::new();
        let mut snapshots = SnapshotRegistry::new();
        snapshots.name_at("old", root_hash, 10)?;
        let mut log = CommitLog::new();
        log.append(root_hash, 20)?;

        let report = prune(&store, &branches, &mut snapshots, "old")?;

        assert_eq!(snapshots.get("old"), None);
        assert!(log.list().iter().any(|entry| entry.root_hash == root_hash));
        assert_eq!(
            report,
            PruneReport {
                node_count: 1,
                bytes_reclaimed: expected_bytes,
            }
        );
        Ok(())
    }

    #[test]
    fn prune_unknown_snapshot_returns_error() {
        let store = MemoryStore::new();
        let branches = BranchRegistry::new();
        let mut snapshots = SnapshotRegistry::new();

        let result = prune(&store, &branches, &mut snapshots, "missing");

        assert!(matches!(
            result,
            Err(PruneError::UnknownSnapshot { name }) if name == "missing"
        ));
    }

    #[test]
    fn prune_deletes_only_nodes_unreachable_from_live_roots() -> Result<(), TestError> {
        let mut store = MemoryStore::new();
        let shared = leaf(b"shared", b"kept by every root")?;
        let pruned_only = leaf(b"pruned", b"delete me")?;
        let branch_only = leaf(b"branch", b"active branch keeps me")?;
        let snapshot_only = leaf(b"snapshot", b"named snapshot keeps me")?;

        let shared_hash = store.put(&shared);
        let pruned_only_hash = store.put(&pruned_only);
        let branch_only_hash = store.put(&branch_only);
        let snapshot_only_hash = store.put(&snapshot_only);

        let pruned_root = internal(vec![(b"a", shared_hash), (b"b", pruned_only_hash)])?;
        let branch_root = internal(vec![(b"a", shared_hash), (b"c", branch_only_hash)])?;
        let snapshot_root = internal(vec![(b"a", shared_hash), (b"d", snapshot_only_hash)])?;
        let pruned_root_hash = store.put(&pruned_root);
        let branch_root_hash = store.put(&branch_root);
        let snapshot_root_hash = store.put(&snapshot_root);

        let branches = BranchRegistry::new();
        branches.register(branch_root_hash);
        let mut snapshots = SnapshotRegistry::new();
        snapshots.name_at("old", pruned_root_hash, 10)?;
        snapshots.name_at("keep", snapshot_root_hash, 20)?;

        let expected_bytes = pruned_root.serialise().len() + pruned_only.serialise().len();
        let report = prune(&store, &branches, &mut snapshots, "old")?;

        assert_eq!(
            report,
            PruneReport {
                node_count: 2,
                bytes_reclaimed: expected_bytes,
            }
        );
        assert_eq!(snapshots.get("old"), None);
        assert_eq!(snapshots.get("keep"), Some(snapshot_root_hash));
        assert_eq!(store.get(&pruned_root_hash), None);
        assert_eq!(store.get(&pruned_only_hash), None);
        assert_eq!(store.get(&shared_hash), Some(std::sync::Arc::new(shared)));
        assert_eq!(
            store.get(&branch_root_hash),
            Some(std::sync::Arc::new(branch_root))
        );
        assert_eq!(
            store.get(&branch_only_hash),
            Some(std::sync::Arc::new(branch_only))
        );
        assert_eq!(
            store.get(&snapshot_root_hash),
            Some(std::sync::Arc::new(snapshot_root))
        );
        assert_eq!(
            store.get(&snapshot_only_hash),
            Some(std::sync::Arc::new(snapshot_only))
        );
        Ok(())
    }

    #[test]
    fn prune_retains_snapshot_when_a_referenced_node_is_missing() -> Result<(), TestError> {
        // A graph-walk failure must leave the snapshot named and the operation
        // retryable: the reclaim set is collected BEFORE the registry entry is
        // removed. Falsifiable — removing the snapshot before the walk (the
        // prior ordering) would drop "broken" even though prune returns Err,
        // permanently orphaning its nodes.
        let mut store = MemoryStore::new();
        let present = leaf(b"present", b"here")?;
        let present_hash = store.put(&present);
        let missing_child = Hash::from_bytes([0xEE; 32]);
        let root_node = internal(vec![(b"a", present_hash), (b"z", missing_child)])?;
        let root_hash = store.put(&root_node);
        let branches = BranchRegistry::new();
        let mut snapshots = SnapshotRegistry::new();
        snapshots.name_at("broken", root_hash, 10)?;

        let result = prune(&store, &branches, &mut snapshots, "broken");

        assert!(matches!(
            result,
            Err(PruneError::MissingNode { hash }) if hash == missing_child
        ));
        // Still named (retryable) and nothing it referenced was deleted.
        assert_eq!(snapshots.get("broken"), Some(root_hash));
        assert_eq!(store.get(&root_hash), Some(std::sync::Arc::new(root_node)));
        assert_eq!(store.get(&present_hash), Some(std::sync::Arc::new(present)));
        Ok(())
    }

    #[test]
    fn prune_last_snapshot_reclaims_all_its_nodes() -> Result<(), TestError> {
        // Pruning the only snapshot with no active branches: the live set is
        // empty, so every node reachable from the pruned root is reclaimed.
        let mut store = MemoryStore::new();
        let child_a = leaf(b"a", b"1")?;
        let child_b = leaf(b"b", b"2")?;
        let a_hash = store.put(&child_a);
        let b_hash = store.put(&child_b);
        let root_node = internal(vec![(b"a", a_hash), (b"b", b_hash)])?;
        let root_hash = store.put(&root_node);
        let branches = BranchRegistry::new();
        let mut snapshots = SnapshotRegistry::new();
        snapshots.name_at("solo", root_hash, 10)?;

        let expected_bytes =
            root_node.serialise().len() + child_a.serialise().len() + child_b.serialise().len();
        let report = prune(&store, &branches, &mut snapshots, "solo")?;

        assert_eq!(
            report,
            PruneReport {
                node_count: 3,
                bytes_reclaimed: expected_bytes,
            }
        );
        assert_eq!(snapshots.get("solo"), None);
        assert_eq!(store.get(&root_hash), None);
        assert_eq!(store.get(&a_hash), None);
        assert_eq!(store.get(&b_hash), None);
        Ok(())
    }
}
