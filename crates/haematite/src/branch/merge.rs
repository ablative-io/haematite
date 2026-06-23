use std::fmt;

use crate::store::NodeStore;
use crate::tree::{Hash, Node, TreeError, batch_mutate};

use super::conflict::{ConflictError, ConflictInput, ConflictPolicy};

type Entry = (Vec<u8>, Vec<u8>);
type ChildRef = (Vec<u8>, Hash);
type MergeMutation = (Vec<u8>, Option<Vec<u8>>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeError {
    MissingNode { hash: Hash },
    StoreRead { hash: Hash },
    InvalidNode,
    UnresolvedConflict { key: Vec<u8> },
    Unimplemented { feature: &'static str },
}

impl fmt::Display for MergeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNode { hash } => write!(formatter, "missing tree node {hash}"),
            Self::StoreRead { hash } => write!(formatter, "failed to read tree node {hash}"),
            Self::InvalidNode => write!(formatter, "invalid tree node"),
            Self::UnresolvedConflict { key } => write!(
                formatter,
                "conflict on key {} is unresolved",
                String::from_utf8_lossy(key)
            ),
            Self::Unimplemented { feature } => write!(formatter, "{feature} is not implemented"),
        }
    }
}

impl std::error::Error for MergeError {}

impl From<TreeError> for MergeError {
    fn from(error: TreeError) -> Self {
        match error {
            TreeError::MissingNode { hash } => Self::MissingNode { hash },
            TreeError::InvalidNode => Self::InvalidNode,
        }
    }
}

impl From<ConflictError> for MergeError {
    fn from(error: ConflictError) -> Self {
        match error {
            ConflictError::Unimplemented { feature } => Self::Unimplemented { feature },
            ConflictError::Unresolved { key } => Self::UnresolvedConflict { key },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeConflict {
    pub key: Vec<u8>,
    pub ancestor_value: Option<Vec<u8>>,
    pub parent_value: Option<Vec<u8>>,
    pub branch_value: Option<Vec<u8>>,
    pub resolved_value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeReport {
    pub merged_root: Hash,
    pub conflicts: Vec<MergeConflict>,
}

pub fn merge<S: NodeStore + ?Sized>(
    store: &mut S,
    parent_root: Hash,
    branch_root: Hash,
    ancestor_root: Hash,
    policy: &ConflictPolicy,
) -> Result<Hash, MergeError> {
    merge_with_report(store, parent_root, branch_root, ancestor_root, policy)
        .map(|report| report.merged_root)
}

pub fn merge_with_report<S: NodeStore + ?Sized>(
    store: &mut S,
    parent_root: Hash,
    branch_root: Hash,
    ancestor_root: Hash,
    policy: &ConflictPolicy,
) -> Result<MergeReport, MergeError> {
    if parent_root == branch_root || branch_root == ancestor_root {
        return Ok(MergeReport {
            merged_root: parent_root,
            conflicts: Vec::new(),
        });
    }

    let hashes = NodeHashes {
        ancestor: ancestor_root,
        parent: parent_root,
        branch: branch_root,
    };
    let mut walk = MergeWalk::new(store, policy);
    walk.merge_node_range(hashes, KeyRange::unbounded())?;
    let (mutations, conflicts) = walk.into_parts();

    let merged_root = if mutations.is_empty() {
        parent_root
    } else {
        batch_mutate(store, parent_root, mutations.as_slice()).map_err(MergeError::from)?
    };
    Ok(MergeReport {
        merged_root,
        conflicts,
    })
}

#[derive(Debug, Clone, Copy)]
struct NodeHashes {
    ancestor: Hash,
    parent: Hash,
    branch: Hash,
}

impl NodeHashes {
    fn merge_is_noop(self) -> bool {
        self.parent == self.branch || self.branch == self.ancestor
    }
}

#[derive(Debug, Clone, Copy)]
struct KeyRange<'a> {
    lower: Option<&'a [u8]>,
    upper: Option<&'a [u8]>,
}

impl KeyRange<'_> {
    const fn unbounded() -> Self {
        Self {
            lower: None,
            upper: None,
        }
    }

    fn may_contain_keys(self) -> bool {
        match (self.lower, self.upper) {
            (Some(lower), Some(upper)) => lower < upper,
            (None, Some(upper)) => !upper.is_empty(),
            (Some(_) | None, None) => true,
        }
    }
}

#[derive(Debug)]
struct MergeWalk<'a, S: NodeStore + ?Sized> {
    store: &'a mut S,
    policy: &'a ConflictPolicy,
    mutations: Vec<MergeMutation>,
    conflicts: Vec<MergeConflict>,
}

impl<'a, S: NodeStore + ?Sized> MergeWalk<'a, S> {
    const fn new(store: &'a mut S, policy: &'a ConflictPolicy) -> Self {
        Self {
            store,
            policy,
            mutations: Vec::new(),
            conflicts: Vec::new(),
        }
    }

    fn into_parts(self) -> (Vec<MergeMutation>, Vec<MergeConflict>) {
        (self.mutations, self.conflicts)
    }

    fn merge_node_range(
        &mut self,
        hashes: NodeHashes,
        range: KeyRange<'_>,
    ) -> Result<(), MergeError> {
        if !range.may_contain_keys() || hashes.merge_is_noop() {
            return Ok(());
        }

        let ancestor_node = self.load_node(hashes.ancestor)?;
        let parent_node = if hashes.parent == hashes.ancestor {
            ancestor_node.clone()
        } else {
            self.load_node(hashes.parent)?
        };
        let branch_node = if hashes.branch == hashes.ancestor {
            ancestor_node.clone()
        } else if hashes.branch == hashes.parent {
            parent_node.clone()
        } else {
            self.load_node(hashes.branch)?
        };

        match (&ancestor_node, &parent_node, &branch_node) {
            (Node::Leaf(ancestor), Node::Leaf(parent), Node::Leaf(branch)) => self
                .merge_leaf_entries_range(
                    EntryTriple {
                        ancestor: ancestor.entries(),
                        parent: parent.entries(),
                        branch: branch.entries(),
                    },
                    range,
                ),
            _ => self.merge_child_ranges(
                hashes,
                NodeTriple {
                    ancestor: &ancestor_node,
                    parent: &parent_node,
                    branch: &branch_node,
                },
                range,
            ),
        }
    }

    fn load_node(&self, hash: Hash) -> Result<Node, MergeError> {
        self.store
            .get(&hash)
            .map_err(|_error| MergeError::StoreRead { hash })?
            .ok_or(MergeError::MissingNode { hash })
    }

    fn merge_child_ranges(
        &mut self,
        hashes: NodeHashes,
        nodes: NodeTriple<'_>,
        range: KeyRange<'_>,
    ) -> Result<(), MergeError> {
        let mut ancestor = RangeSource::new(hashes.ancestor, nodes.ancestor, range.lower)?;
        let mut parent = RangeSource::new(hashes.parent, nodes.parent, range.lower)?;
        let mut branch = RangeSource::new(hashes.branch, nodes.branch, range.lower)?;
        let mut current = range.lower.map(<[u8]>::to_vec);

        loop {
            let child_hashes = NodeHashes {
                ancestor: ancestor.current_hash()?,
                parent: parent.current_hash()?,
                branch: branch.current_hash()?,
            };
            let next = min_bound_owned([
                ancestor.current_end(),
                parent.current_end(),
                branch.current_end(),
                range.upper,
            ]);

            self.merge_node_range(
                child_hashes,
                KeyRange {
                    lower: current.as_deref(),
                    upper: next.as_deref(),
                },
            )?;

            if next.is_none() || bounds_equal(next.as_deref(), range.upper) {
                return Ok(());
            }

            let advanced = ancestor.advance_if_at(next.as_deref())
                | parent.advance_if_at(next.as_deref())
                | branch.advance_if_at(next.as_deref());
            if !advanced {
                return Err(MergeError::InvalidNode);
            }
            current = next;
        }
    }

    fn merge_leaf_entries_range(
        &mut self,
        entries: EntryTriple<'_>,
        range: KeyRange<'_>,
    ) -> Result<(), MergeError> {
        let mut ancestor = EntryCursor::new(entries.ancestor, range);
        let mut parent = EntryCursor::new(entries.parent, range);
        let mut branch = EntryCursor::new(entries.branch, range);

        while let Some(key) = min_bound_owned([
            ancestor.peek_key(),
            parent.peek_key(),
            branch.peek_key(),
            None,
        ]) {
            let values = ValueTriple {
                ancestor: ancestor.take_value_for(key.as_slice()),
                parent: parent.take_value_for(key.as_slice()),
                branch: branch.take_value_for(key.as_slice()),
            };
            self.resolve_leaf_key(key.as_slice(), values)?;
        }

        Ok(())
    }

    fn resolve_leaf_key(&mut self, key: &[u8], values: ValueTriple<'_>) -> Result<(), MergeError> {
        let parent_changed = values.parent != values.ancestor;
        let branch_changed = values.branch != values.ancestor;

        match (parent_changed, branch_changed) {
            (false | true, false) => Ok(()),
            (false, true) => {
                self.push_mutation_if_changed(
                    key,
                    values.parent,
                    optional_value_to_vec(values.branch),
                );
                Ok(())
            }
            (true, true) if values.parent == values.branch => Ok(()),
            (true, true) => {
                let ancestor_value = optional_value_to_vec(values.ancestor);
                let parent_value = optional_value_to_vec(values.parent);
                let branch_value = optional_value_to_vec(values.branch);
                let conflict = ConflictInput::new(
                    key.to_vec(),
                    ancestor_value.clone(),
                    parent_value.clone(),
                    branch_value.clone(),
                );
                let resolved = self.policy.resolve(&conflict)?;
                self.conflicts.push(MergeConflict {
                    key: key.to_vec(),
                    ancestor_value,
                    parent_value,
                    branch_value,
                    resolved_value: resolved.clone(),
                });
                self.push_mutation_if_changed(key, values.parent, resolved);
                Ok(())
            }
        }
    }

    fn push_mutation_if_changed(
        &mut self,
        key: &[u8],
        parent_value: Option<&[u8]>,
        resolved_value: Option<Vec<u8>>,
    ) {
        if resolved_value.as_deref() != parent_value {
            self.mutations.push((key.to_vec(), resolved_value));
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct NodeTriple<'a> {
    ancestor: &'a Node,
    parent: &'a Node,
    branch: &'a Node,
}

#[derive(Debug)]
struct RangeSource<'a> {
    hash: Hash,
    children: Option<&'a [ChildRef]>,
    index: usize,
}

impl<'a> RangeSource<'a> {
    fn new(hash: Hash, node: &'a Node, lower: Option<&[u8]>) -> Result<Self, MergeError> {
        match node {
            Node::Leaf(_) => Ok(Self {
                hash,
                children: None,
                index: 0,
            }),
            Node::Internal(internal) => Ok(Self {
                hash,
                children: Some(internal.children()),
                index: child_index_for_lower(internal.children(), lower)?,
            }),
        }
    }

    fn current_hash(&self) -> Result<Hash, MergeError> {
        self.children.map_or(Ok(self.hash), |children| {
            children
                .get(self.index)
                .map(|(_separator, hash)| *hash)
                .ok_or(MergeError::InvalidNode)
        })
    }

    fn current_end(&self) -> Option<&'a [u8]> {
        self.children
            .and_then(|children| child_end(children, self.index))
    }

    fn advance_if_at(&mut self, bound: Option<&[u8]>) -> bool {
        if self.children.is_some() && bounds_equal(self.current_end(), bound) {
            self.index = self.index.saturating_add(1);
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct EntryTriple<'a> {
    ancestor: &'a [Entry],
    parent: &'a [Entry],
    branch: &'a [Entry],
}

#[derive(Debug)]
struct EntryCursor<'a> {
    entries: &'a [Entry],
    index: usize,
    end: usize,
}

impl<'a> EntryCursor<'a> {
    fn new(entries: &'a [Entry], range: KeyRange<'_>) -> Self {
        Self {
            entries,
            index: lower_bound_entries(entries, range.lower),
            end: upper_bound_entries(entries, range.upper),
        }
    }

    fn peek_key(&self) -> Option<&'a [u8]> {
        if self.index < self.end {
            self.entries
                .get(self.index)
                .map(|(key, _value)| key.as_slice())
        } else {
            None
        }
    }

    fn take_value_for(&mut self, key: &[u8]) -> Option<&'a [u8]> {
        if self.peek_key() == Some(key) {
            let value = self
                .entries
                .get(self.index)
                .map(|(_key, value)| value.as_slice());
            self.index = self.index.saturating_add(1);
            value
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ValueTriple<'a> {
    ancestor: Option<&'a [u8]>,
    parent: Option<&'a [u8]>,
    branch: Option<&'a [u8]>,
}

fn optional_value_to_vec(value: Option<&[u8]>) -> Option<Vec<u8>> {
    value.map(<[u8]>::to_vec)
}

fn child_index_for_lower(children: &[ChildRef], lower: Option<&[u8]>) -> Result<usize, MergeError> {
    if children.is_empty() {
        return Err(MergeError::InvalidNode);
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

fn min_bound_owned<const N: usize>(bounds: [Option<&[u8]>; N]) -> Option<Vec<u8>> {
    bounds.into_iter().flatten().min().map(<[u8]>::to_vec)
}

fn bounds_equal(left: Option<&[u8]>, right: Option<&[u8]>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    }
}

#[cfg(test)]
#[path = "merge_tests.rs"]
mod tests;
