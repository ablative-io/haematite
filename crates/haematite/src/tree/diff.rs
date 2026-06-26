use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use crate::store::NodeStore;

use super::node::{Hash, InternalNode, LeafNode, Node};

type Entry = (Vec<u8>, Vec<u8>);
type ChildRef = (Vec<u8>, Hash);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffEntry {
    Added {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Removed {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Modified {
        key: Vec<u8>,
        old_value: Vec<u8>,
        new_value: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffError {
    MissingNode(Hash),
    StoreRead,
    InvalidNode,
}

impl fmt::Display for DiffError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNode(hash) => write!(formatter, "missing tree node {hash}"),
            Self::StoreRead => write!(formatter, "failed to read tree node"),
            Self::InvalidNode => write!(formatter, "invalid tree node"),
        }
    }
}

impl std::error::Error for DiffError {}

pub fn diff<S: NodeStore + ?Sized>(
    store: &S,
    root_a: &Hash,
    root_b: &Hash,
) -> Result<Vec<DiffEntry>, DiffError> {
    if root_a == root_b {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    diff_node_range(store, *root_a, *root_b, None, None, &mut entries)?;
    Ok(entries)
}

fn diff_node_range<S: NodeStore + ?Sized>(
    store: &S,
    hash_a: Hash,
    hash_b: Hash,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    output: &mut Vec<DiffEntry>,
) -> Result<(), DiffError> {
    if !range_may_contain_keys(lower, upper) || hash_a == hash_b {
        return Ok(());
    }

    let node_a = load_diff_node(store, hash_a)?;
    let node_b = load_diff_node(store, hash_b)?;

    match (&*node_a, &*node_b) {
        (Node::Leaf(leaf_a), Node::Leaf(leaf_b)) => {
            diff_leaf_entries_range(leaf_a.entries(), leaf_b.entries(), lower, upper, output);
            Ok(())
        }
        (Node::Internal(internal_a), Node::Internal(internal_b)) => diff_internal_nodes(
            store,
            internal_a.children(),
            internal_b.children(),
            lower,
            upper,
            output,
        ),
        (Node::Leaf(leaf_a), Node::Internal(internal_b)) => {
            diff_leaf_against_internal(store, leaf_a, internal_b, lower, upper, output)
        }
        (Node::Internal(internal_a), Node::Leaf(leaf_b)) => {
            diff_internal_against_leaf(store, internal_a, leaf_b, lower, upper, output)
        }
    }
}

fn diff_internal_nodes<S: NodeStore + ?Sized>(
    store: &S,
    children_a: &[ChildRef],
    children_b: &[ChildRef],
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    output: &mut Vec<DiffEntry>,
) -> Result<(), DiffError> {
    let mut index_a = child_index_for_lower(children_a, lower)?;
    let mut index_b = child_index_for_lower(children_b, lower)?;
    let mut current = lower.map(<[u8]>::to_vec);

    loop {
        let (_, child_hash_a) = children_a.get(index_a).ok_or(DiffError::InvalidNode)?;
        let (_, child_hash_b) = children_b.get(index_b).ok_or(DiffError::InvalidNode)?;
        let end_a = child_end(children_a, index_a);
        let end_b = child_end(children_b, index_b);
        let next = min_bound(min_bound(end_a, end_b), upper);

        diff_node_range(
            store,
            *child_hash_a,
            *child_hash_b,
            current.as_deref(),
            next,
            output,
        )?;

        if next.is_none() || bounds_equal(next, upper) {
            return Ok(());
        }

        let advanced_a = bounds_equal(end_a, next);
        let advanced_b = bounds_equal(end_b, next);
        if advanced_a {
            index_a = index_a.saturating_add(1);
        }
        if advanced_b {
            index_b = index_b.saturating_add(1);
        }
        if !(advanced_a || advanced_b) {
            return Err(DiffError::InvalidNode);
        }
        current = next.map(<[u8]>::to_vec);
    }
}

fn diff_leaf_against_internal<S: NodeStore + ?Sized>(
    store: &S,
    leaf_a: &LeafNode,
    internal_b: &InternalNode,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    output: &mut Vec<DiffEntry>,
) -> Result<(), DiffError> {
    let mut entries_b = Vec::new();
    collect_internal_entries(store, internal_b.children(), lower, upper, &mut entries_b)?;
    diff_leaf_entries_range(leaf_a.entries(), entries_b.as_slice(), lower, upper, output);
    Ok(())
}

fn diff_internal_against_leaf<S: NodeStore + ?Sized>(
    store: &S,
    internal_a: &InternalNode,
    leaf_b: &LeafNode,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    output: &mut Vec<DiffEntry>,
) -> Result<(), DiffError> {
    let mut entries_a = Vec::new();
    collect_internal_entries(store, internal_a.children(), lower, upper, &mut entries_a)?;
    diff_leaf_entries_range(entries_a.as_slice(), leaf_b.entries(), lower, upper, output);
    Ok(())
}

fn collect_entries_range<S: NodeStore + ?Sized>(
    store: &S,
    hash: Hash,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    output: &mut Vec<Entry>,
) -> Result<(), DiffError> {
    if !range_may_contain_keys(lower, upper) {
        return Ok(());
    }

    match &*load_diff_node(store, hash)? {
        Node::Leaf(leaf) => {
            let start = lower_bound_entries(leaf.entries(), lower);
            let end = upper_bound_entries(leaf.entries(), upper);
            output.extend(
                leaf.entries()
                    .iter()
                    .skip(start)
                    .take(end.saturating_sub(start))
                    .cloned(),
            );
            Ok(())
        }
        Node::Internal(internal) => {
            collect_internal_entries(store, internal.children(), lower, upper, output)
        }
    }
}

fn collect_internal_entries<S: NodeStore + ?Sized>(
    store: &S,
    children: &[ChildRef],
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    output: &mut Vec<Entry>,
) -> Result<(), DiffError> {
    if children.is_empty() {
        return Err(DiffError::InvalidNode);
    }

    for (index, (separator, child_hash)) in children.iter().enumerate() {
        if upper.is_some_and(|bound| separator.as_slice() >= bound) {
            break;
        }
        let end = child_end(children, index);
        if end.is_some_and(|bound| lower.is_some_and(|lower| bound <= lower)) {
            continue;
        }
        collect_entries_range(store, *child_hash, lower, min_bound(end, upper), output)?;
    }

    Ok(())
}

fn diff_leaf_entries_range(
    entries_a: &[Entry],
    entries_b: &[Entry],
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    output: &mut Vec<DiffEntry>,
) {
    let start_a = lower_bound_entries(entries_a, lower);
    let end_a = upper_bound_entries(entries_a, upper);
    let start_b = lower_bound_entries(entries_b, lower);
    let end_b = upper_bound_entries(entries_b, upper);

    diff_leaf_entries(
        entries_a
            .iter()
            .skip(start_a)
            .take(end_a.saturating_sub(start_a)),
        entries_b
            .iter()
            .skip(start_b)
            .take(end_b.saturating_sub(start_b)),
        output,
    );
}

fn diff_leaf_entries<'a, A, B>(entries_a: A, entries_b: B, output: &mut Vec<DiffEntry>)
where
    A: IntoIterator<Item = &'a Entry>,
    B: IntoIterator<Item = &'a Entry>,
{
    let mut iter_a = entries_a.into_iter().peekable();
    let mut iter_b = entries_b.into_iter().peekable();

    loop {
        match (iter_a.peek(), iter_b.peek()) {
            (Some((key_a, value_a)), Some((key_b, value_b))) => {
                match key_a.as_slice().cmp(key_b.as_slice()) {
                    Ordering::Less => {
                        output.push(DiffEntry::Removed {
                            key: key_a.clone(),
                            value: value_a.clone(),
                        });
                        iter_a.next();
                    }
                    Ordering::Greater => {
                        output.push(DiffEntry::Added {
                            key: key_b.clone(),
                            value: value_b.clone(),
                        });
                        iter_b.next();
                    }
                    Ordering::Equal => {
                        if value_a != value_b {
                            output.push(DiffEntry::Modified {
                                key: key_a.clone(),
                                old_value: value_a.clone(),
                                new_value: value_b.clone(),
                            });
                        }
                        iter_a.next();
                        iter_b.next();
                    }
                }
            }
            (Some((key, value)), None) => {
                output.push(DiffEntry::Removed {
                    key: key.clone(),
                    value: value.clone(),
                });
                iter_a.next();
            }
            (None, Some((key, value))) => {
                output.push(DiffEntry::Added {
                    key: key.clone(),
                    value: value.clone(),
                });
                iter_b.next();
            }
            (None, None) => return,
        }
    }
}

fn load_diff_node<S: NodeStore + ?Sized>(store: &S, hash: Hash) -> Result<Arc<Node>, DiffError> {
    store
        .get(&hash)
        .map_err(|_error| DiffError::StoreRead)?
        .ok_or(DiffError::MissingNode(hash))
}

fn child_index_for_lower(children: &[ChildRef], lower: Option<&[u8]>) -> Result<usize, DiffError> {
    if children.is_empty() {
        return Err(DiffError::InvalidNode);
    }

    Ok(lower.map_or(0, |key| {
        children
            .partition_point(|(separator, _hash)| separator.as_slice() <= key)
            .saturating_sub(1)
    }))
}

fn child_end(children: &[ChildRef], index: usize) -> Option<&[u8]> {
    children
        .get(index.saturating_add(1))
        .map(|(separator, _hash)| separator.as_slice())
}

fn lower_bound_entries(entries: &[Entry], key: Option<&[u8]>) -> usize {
    key.map_or(0, |key| {
        entries.partition_point(|(entry_key, _value)| entry_key.as_slice() < key)
    })
}

fn upper_bound_entries(entries: &[Entry], key: Option<&[u8]>) -> usize {
    key.map_or(entries.len(), |key| {
        entries.partition_point(|(entry_key, _value)| entry_key.as_slice() < key)
    })
}

fn min_bound<'a>(left: Option<&'a [u8]>, right: Option<&'a [u8]>) -> Option<&'a [u8]> {
    match (left, right) {
        (Some(left), Some(right)) if right < left => Some(right),
        (Some(left), Some(_right)) => Some(left),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn bounds_equal(left: Option<&[u8]>, right: Option<&[u8]>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    }
}

fn range_may_contain_keys(lower: Option<&[u8]>, upper: Option<&[u8]>) -> bool {
    match (lower, upper) {
        (Some(lower), Some(upper)) => lower < upper,
        (None, Some(upper)) => !upper.is_empty(),
        (Some(_) | None, None) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::DiffEntry;

    #[test]
    fn diff_entry_variants_are_constructible_debuggable_and_comparable() {
        let added = DiffEntry::Added {
            key: b"a".to_vec(),
            value: b"one".to_vec(),
        };
        let removed = DiffEntry::Removed {
            key: b"b".to_vec(),
            value: b"two".to_vec(),
        };
        let modified = DiffEntry::Modified {
            key: b"c".to_vec(),
            old_value: b"old".to_vec(),
            new_value: b"new".to_vec(),
        };

        assert_eq!(
            added,
            DiffEntry::Added {
                key: b"a".to_vec(),
                value: b"one".to_vec(),
            }
        );
        assert!(format!("{added:?}").contains("Added"));
        assert!(format!("{removed:?}").contains("Removed"));
        assert!(format!("{modified:?}").contains("Modified"));
    }
}
