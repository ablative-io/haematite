use crate::store::NodeStore;

use super::boundary::BoundaryDetector;
use super::cursor::{TreeError, load_node};
use super::node::{Hash, InternalNode, LeafNode, Node};

type Entry = (Vec<u8>, Vec<u8>);
type ChildRef = (Vec<u8>, Hash);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Mutation {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

pub fn insert<S, K, V>(store: &mut S, root_hash: Hash, key: K, value: V) -> Result<Hash, TreeError>
where
    S: NodeStore + ?Sized,
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
{
    let mutations = [(key.as_ref().to_vec(), Some(value.as_ref().to_vec()))];
    batch_mutate(store, root_hash, mutations.as_slice())
}

pub fn delete<S, K>(store: &mut S, root_hash: Hash, key: K) -> Result<Hash, TreeError>
where
    S: NodeStore + ?Sized,
    K: AsRef<[u8]>,
{
    let mutations = [(key.as_ref().to_vec(), None)];
    batch_mutate(store, root_hash, mutations.as_slice())
}

/// Apply a batch of put/delete mutations and return the new root hash.
///
/// HISTORY-INDEPENDENCE: the resulting root hash is a pure function of the final
/// key->value SET, never of the order of the operations that produced it. This is
/// achieved by re-deriving every node that could change purely from content-defined
/// boundaries (see [`super::boundary::BoundaryDetector`]) — the same way a fresh
/// full build of the same set would lay out every level — rather than from the
/// transient node structure created by earlier operations.
pub fn batch_mutate<S: NodeStore + ?Sized>(
    store: &mut S,
    root_hash: Hash,
    mutations: &[(Vec<u8>, Option<Vec<u8>>)],
) -> Result<Hash, TreeError> {
    if mutations.is_empty() {
        return Ok(root_hash);
    }

    let normalised = normalise_mutations(mutations);
    if normalised.is_empty() {
        return Ok(root_hash);
    }

    // Collect every existing leaf in key order (cheap: only leaf *refs* are read,
    // not their contents, except the leaves the mutation actually touches).
    let leaves = collect_leaf_refs(store, root_hash)?;

    let window = affected_window(leaves.as_slice(), normalised.as_slice())?;
    let Some(window) = window else {
        // No leaf is affected and no insert occurs (e.g. deleting absent keys).
        return Ok(root_hash);
    };

    let rebuilt = rebuild_window(store, &leaves, &window, normalised.as_slice())?;

    finish_root(store, rebuilt)
}

fn normalise_mutations(mutations: &[(Vec<u8>, Option<Vec<u8>>)]) -> Vec<Mutation> {
    let mut normalised: Vec<Mutation> = Vec::new();

    for (key, value) in mutations {
        if let Some(last) = normalised.last_mut().filter(|last| last.key == *key) {
            last.value.clone_from(value);
        } else {
            normalised.push(Mutation {
                key: key.clone(),
                value: value.clone(),
            });
        }
    }

    normalised
}

/// Walk the tree left-to-right collecting every leaf reference (separator key +
/// hash) in key order.
///
/// The tree is height-balanced (every leaf is at the same depth), so the height
/// is found by descending the leftmost spine once. Enumeration then loads only
/// INTERNAL nodes: at the level whose children are leaves, the leaf refs are taken
/// straight from the parent's child entries WITHOUT fetching the leaves. Leaf
/// contents are therefore only read for the few leaves a mutation actually
/// touches, keeping mutation sparse.
fn collect_leaf_refs<S: NodeStore + ?Sized>(
    store: &S,
    root_hash: Hash,
) -> Result<Vec<ChildRef>, TreeError> {
    let height = tree_height(store, root_hash)?;
    if height == 0 {
        // The root is a leaf; its separator is unknown without loading it, but the
        // leftmost leaf is never chosen by separator comparison, so an empty
        // separator is safe.
        return Ok(vec![(Vec::new(), root_hash)]);
    }

    let mut leaves = Vec::new();
    collect_leaf_refs_inner(store, root_hash, height, &mut leaves)?;
    Ok(leaves)
}

/// Number of internal levels above the leaves (0 when the root is a leaf).
fn tree_height<S: NodeStore + ?Sized>(store: &S, root_hash: Hash) -> Result<usize, TreeError> {
    let mut height = 0;
    let mut hash = root_hash;
    loop {
        match &*load_node(store, hash)? {
            Node::Leaf(_leaf) => return Ok(height),
            Node::Internal(internal) => {
                let Some((_separator, child_hash)) = internal.children().first() else {
                    return Err(TreeError::InvalidNode);
                };
                height = height.saturating_add(1);
                hash = *child_hash;
            }
        }
    }
}

/// Collect leaf refs from the subtree at `hash`, which sits `levels_above_leaves`
/// internal levels above the leaves. When that count is 1, the node's children
/// are leaves and are taken without loading them.
fn collect_leaf_refs_inner<S: NodeStore + ?Sized>(
    store: &S,
    hash: Hash,
    levels_above_leaves: usize,
    leaves: &mut Vec<ChildRef>,
) -> Result<(), TreeError> {
    let node = load_node(store, hash)?;
    let Node::Internal(internal) = &*node else {
        return Err(TreeError::InvalidNode);
    };

    if levels_above_leaves <= 1 {
        leaves.extend_from_slice(internal.children());
        return Ok(());
    }

    for (_separator, child_hash) in internal.children() {
        collect_leaf_refs_inner(store, *child_hash, levels_above_leaves - 1, leaves)?;
    }
    Ok(())
}

/// Half-open `[start, end)` range of leaf indices that the mutation may change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Window {
    start: usize,
    end: usize,
}

/// Determine the contiguous run of existing leaves whose entries can change.
///
/// A leaf only ever ends on a content-defined boundary key (or is the rightmost
/// leaf), so the leaves to the LEFT of the first mutated key are unaffected — the
/// boundary that closes the leaf containing the first mutated key is itself stable.
/// The run therefore starts at the leaf into whose key range the smallest mutated
/// key falls. Rightward spillover (an open trailing chunk after re-chunking) is
/// handled later in [`rebuild_window`].
fn affected_window(
    leaves: &[ChildRef],
    mutations: &[Mutation],
) -> Result<Option<Window>, TreeError> {
    let Some(min_key) = mutations.iter().map(|m| m.key.as_slice()).min() else {
        return Ok(None);
    };
    let Some(max_key) = mutations.iter().map(|m| m.key.as_slice()).max() else {
        return Ok(None);
    };

    if leaves.is_empty() {
        // Only possible for a corrupt tree; the empty tree always has one leaf.
        return Err(TreeError::InvalidNode);
    }

    let start = leaf_index_for_key(leaves, min_key);
    // `end` is the leaf after the one containing `max_key`; spillover may extend
    // it further during the rebuild.
    let end = leaf_index_for_key(leaves, max_key)
        .saturating_add(1)
        .min(leaves.len());

    Ok(Some(Window { start, end }))
}

/// Index of the leaf whose key range contains `key` (the last leaf whose
/// separator is `<= key`, or leaf 0 if `key` precedes all separators).
fn leaf_index_for_key(leaves: &[ChildRef], key: &[u8]) -> usize {
    let mut index = 0;
    for (position, (separator, _hash)) in leaves.iter().enumerate() {
        if separator.as_slice() <= key {
            index = position;
        } else {
            break;
        }
    }
    index
}

/// Re-derive the leaves of the affected window from their entries (with mutations
/// applied) using content-defined boundaries, extending rightward while the final
/// rebuilt leaf is "open" (does not end on a boundary) so that an unterminated
/// chunk merges with the following leaf — exactly as a fresh full build would.
///
/// Returns the complete ordered leaf list: unchanged leaves to the left, the
/// freshly built leaves, and unchanged leaves to the right.
fn rebuild_window<S: NodeStore + ?Sized>(
    store: &mut S,
    leaves: &[ChildRef],
    window: &Window,
    mutations: &[Mutation],
) -> Result<Vec<ChildRef>, TreeError> {
    let detector = active_detector();

    let mut entries = load_entries(store, &leaves[window.start..window.end])?;
    apply_mutations(&mut entries, mutations)?;

    // The right edge of the window must land on a boundary key (or the end of the
    // tree). Pull in successive leaves until the last surviving entry is a
    // boundary key, so no open trailing chunk is left dangling.
    let mut consumed_end = window.end;
    while last_entry_is_open(&entries, detector) && consumed_end < leaves.len() {
        let next = load_entries(store, std::slice::from_ref(&leaves[consumed_end]))?;
        entries.extend(next);
        consumed_end = consumed_end.saturating_add(1);
    }

    let new_leaves = store_leaf_replacements(store, entries)?;

    let mut result = Vec::with_capacity(window.start + new_leaves.len() + leaves.len());
    result.extend_from_slice(&leaves[..window.start]);
    result.extend(new_leaves);
    result.extend_from_slice(&leaves[consumed_end..]);
    Ok(result)
}

/// True when the entry list is non-empty and its last key is NOT a boundary key,
/// meaning a re-chunk would leave an unterminated trailing chunk that must absorb
/// the next leaf.
fn last_entry_is_open(entries: &[Entry], detector: BoundaryDetector) -> bool {
    entries
        .last()
        .is_some_and(|(key, _value)| !detector.is_boundary(key.as_slice()))
}

fn load_entries<S: NodeStore + ?Sized>(
    store: &S,
    leaves: &[ChildRef],
) -> Result<Vec<Entry>, TreeError> {
    let mut entries = Vec::new();
    for (_separator, hash) in leaves {
        match &*load_node(store, *hash)? {
            Node::Leaf(leaf) => entries.extend_from_slice(leaf.entries()),
            Node::Internal(_internal) => return Err(TreeError::InvalidNode),
        }
    }
    Ok(entries)
}

fn apply_mutations(entries: &mut Vec<Entry>, mutations: &[Mutation]) -> Result<(), TreeError> {
    for mutation in mutations {
        let search =
            entries.binary_search_by(|(key, _value)| key.as_slice().cmp(mutation.key.as_slice()));
        match (&mutation.value, search) {
            (Some(value), Ok(index)) => {
                let Some((_key, stored_value)) = entries.get_mut(index) else {
                    return Err(TreeError::InvalidNode);
                };
                value.clone_into(stored_value);
            }
            (Some(value), Err(index)) => {
                if index > entries.len() {
                    return Err(TreeError::InvalidNode);
                }
                entries.insert(index, (mutation.key.clone(), value.clone()));
            }
            (None, Ok(index)) => {
                if index >= entries.len() {
                    return Err(TreeError::InvalidNode);
                }
                entries.remove(index);
            }
            (None, Err(_index)) => {}
        }
    }

    Ok(())
}

fn store_leaf_replacements<S: NodeStore + ?Sized>(
    store: &mut S,
    entries: Vec<Entry>,
) -> Result<Vec<ChildRef>, TreeError> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let mut replacements = Vec::new();
    for chunk in split_after_boundaries(entries, |(key, _value)| key.as_slice()) {
        replacements.push(store_leaf(store, chunk)?);
    }
    Ok(replacements)
}

/// Build exactly one internal level from a flat list of child refs by grouping
/// them with the SAME content-defined boundary detector used for leaves. Every
/// boundary-delimited chunk becomes exactly one internal node — including a
/// single-child chunk — so that all children end up at the same depth and the
/// tree stays height-balanced (which the sparse leaf enumeration relies on).
///
/// A whole input of length 1 is passed through unwrapped: that is the spine
/// reducing to a single subtree, not a new level.
///
/// The grouping is a pure function of which separator keys are boundary keys, so
/// it is independent of the operation order that produced `children`.
fn store_internal_replacements<S: NodeStore + ?Sized>(
    store: &mut S,
    children: Vec<ChildRef>,
) -> Result<Vec<ChildRef>, TreeError> {
    match children.len() {
        0 => Ok(Vec::new()),
        1 => Ok(children),
        _ => {
            let mut replacements = Vec::new();
            for chunk in split_after_boundaries(children, |(key, _hash)| key.as_slice()) {
                replacements.push(store_internal(store, chunk)?);
            }
            Ok(replacements)
        }
    }
}

// Test-only seam: lets property tests drive the REAL mutate code path with a
// small `target_size` so that realistic key counts produce genuinely
// multi-leaf trees. Compiled out entirely in non-test builds, so production
// behaviour is identical to `BoundaryDetector::default()`.
#[cfg(test)]
thread_local! {
    static TEST_TARGET_SIZE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(crate) fn set_test_target_size(target_size: Option<usize>) {
    TEST_TARGET_SIZE.with(|cell| cell.set(target_size));
}

#[cfg(test)]
fn active_detector() -> BoundaryDetector {
    TEST_TARGET_SIZE.with(|cell| {
        cell.get()
            .map_or_else(BoundaryDetector::default, BoundaryDetector::new)
    })
}

#[cfg(not(test))]
fn active_detector() -> BoundaryDetector {
    BoundaryDetector::default()
}

fn split_after_boundaries<T, F>(items: Vec<T>, key_of: F) -> Vec<Vec<T>>
where
    F: Fn(&T) -> &[u8],
{
    let detector = active_detector();
    let mut chunks = Vec::new();
    let mut current = Vec::new();

    for item in items {
        let is_boundary = detector.is_boundary(key_of(&item));
        current.push(item);
        if is_boundary {
            chunks.push(current);
            current = Vec::new();
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }

    chunks
}

fn store_leaf<S: NodeStore + ?Sized>(
    store: &mut S,
    entries: Vec<Entry>,
) -> Result<ChildRef, TreeError> {
    let separator = first_entry_key(entries.as_slice())?;
    let leaf = LeafNode::new(entries)?;
    let hash = store
        .put(&Node::Leaf(leaf))
        .map_err(|_| TreeError::InvalidNode)?;
    Ok((separator, hash))
}

fn store_internal<S: NodeStore + ?Sized>(
    store: &mut S,
    children: Vec<ChildRef>,
) -> Result<ChildRef, TreeError> {
    let separator = first_child_key(children.as_slice())?;
    let internal = InternalNode::new(children)?;
    let hash = store
        .put(&Node::Internal(internal))
        .map_err(|_| TreeError::InvalidNode)?;
    Ok((separator, hash))
}

/// Build the new root from the complete ordered leaf list.
///
/// An empty list collapses to the canonical empty leaf. Otherwise the spine is
/// rebuilt level by level from the leaf refs via [`build_spine`], so the root
/// hash depends only on the leaf set — not on how it was produced.
fn finish_root<S: NodeStore + ?Sized>(
    store: &mut S,
    leaves: Vec<ChildRef>,
) -> Result<Hash, TreeError> {
    if leaves.is_empty() {
        return store_empty_leaf(store);
    }

    let (_separator, hash) = build_spine(store, leaves)?;
    Ok(hash)
}

/// Collapse a flat list of child refs into a single root by repeatedly building
/// one content-defined internal level at a time (see [`store_internal_replacements`])
/// until a single node remains.
///
/// This is the SAME boundary-grouping used at the leaf level, so the spine is
/// identical to the spine a fresh full build of the same key->value set would
/// produce — making the root hash history-independent at every level.
///
/// Termination: each pass either shrinks the list (some chunk had >1 element) or,
/// in the degenerate case where every separator is a boundary key (so each chunk
/// is a single element and the level cannot shrink), collapses the whole list
/// into one internal node. Both outcomes are pure functions of the key set.
fn build_spine<S: NodeStore + ?Sized>(
    store: &mut S,
    mut children: Vec<ChildRef>,
) -> Result<ChildRef, TreeError> {
    loop {
        match children.len() {
            0 => return Err(TreeError::InvalidNode),
            1 => return children.into_iter().next().ok_or(TreeError::InvalidNode),
            len => {
                let next = store_internal_replacements(store, children)?;
                if next.len() < len {
                    children = next;
                } else {
                    return store_internal(store, next);
                }
            }
        }
    }
}

fn store_empty_leaf<S: NodeStore + ?Sized>(store: &mut S) -> Result<Hash, TreeError> {
    let leaf = LeafNode::new(Vec::new())?;
    store
        .put(&Node::Leaf(leaf))
        .map_err(|_| TreeError::InvalidNode)
}

fn first_entry_key(entries: &[Entry]) -> Result<Vec<u8>, TreeError> {
    entries
        .first()
        .map(|(key, _value)| key.clone())
        .ok_or(TreeError::InvalidNode)
}

fn first_child_key(children: &[ChildRef]) -> Result<Vec<u8>, TreeError> {
    children
        .first()
        .map(|(key, _hash)| key.clone())
        .ok_or(TreeError::InvalidNode)
}
