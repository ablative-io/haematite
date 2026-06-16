// CORE-003: Structural diff between two trees by root hash comparison
use std::collections::VecDeque;
use std::fmt;

use crate::store::NodeStore;

use super::node::{Hash, InternalNode, Node};

type Entry = (Vec<u8>, Vec<u8>);
type LeafRun = VecDeque<Entry>;

/// A single difference between two trees, reported in ascending key order.
///
/// Diff entries carry keys and values only — never internal hashes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffEntry {
    /// A key present only in tree B.
    Added { key: Vec<u8>, value: Vec<u8> },
    /// A key present only in tree A.
    Removed { key: Vec<u8>, value: Vec<u8> },
    /// A key present in both trees with differing values.
    Modified {
        key: Vec<u8>,
        old_value: Vec<u8>,
        new_value: Vec<u8>,
    },
}

/// Errors that can arise while diffing two trees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffError {
    /// A hash reachable from one of the roots was absent from the store.
    MissingNode(Hash),
    /// A node decoded into a shape the diff could not interpret.
    InvalidNode,
}

impl fmt::Display for DiffError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNode(hash) => write!(formatter, "missing tree node {hash}"),
            Self::InvalidNode => write!(formatter, "invalid tree node"),
        }
    }
}

impl std::error::Error for DiffError {}

/// Walk both trees in key order, yielding only the entries that differ.
///
/// Equal roots short-circuit without touching the store. Otherwise the walk
/// descends only where the trees disagree: any subtree whose hash matches the
/// opposing tree at the same position is skipped without a `store.get`, so cost
/// is proportional to the number of differences, not the tree size (P4).
pub fn diff<S: NodeStore>(
    store: &S,
    root_a: &Hash,
    root_b: &Hash,
) -> Result<Vec<DiffEntry>, DiffError> {
    if root_a == root_b {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    let mut side_a = VecDeque::from([Span::root(*root_a)]);
    let mut side_b = VecDeque::from([Span::root(*root_b)]);

    loop {
        match (side_a.front().is_some(), side_b.front().is_some()) {
            (false, false) => break,
            (false, true) => emit_one(store, &mut side_b, &mut out, Polarity::Added)?,
            (true, false) => emit_one(store, &mut side_a, &mut out, Polarity::Removed)?,
            (true, true) => step(store, &mut side_a, &mut side_b, &mut out)?,
        }
    }

    Ok(out)
}

/// A pending region of one tree, ordered ascending by `lower`. `upper` is the
/// exclusive key bound from the parent layout (`None` = unbounded). A `Subtree`
/// is unloaded and still skippable by hash; `Entries` is a materialised leaf run
/// loaded only because its hash already diverged from the other side.
#[derive(Debug)]
enum Span {
    Subtree {
        lower: Vec<u8>,
        upper: Option<Vec<u8>>,
        hash: Hash,
    },
    Entries {
        entries: LeafRun,
        upper: Option<Vec<u8>>,
    },
}

impl Span {
    const fn root(hash: Hash) -> Self {
        Self::Subtree {
            lower: Vec::new(),
            upper: None,
            hash,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Polarity {
    Added,
    Removed,
}

impl Polarity {
    const fn entry(self, key: Vec<u8>, value: Vec<u8>) -> DiffEntry {
        match self {
            Self::Added => DiffEntry::Added { key, value },
            Self::Removed => DiffEntry::Removed { key, value },
        }
    }
}

/// Front-span summary copied out so the queues can be mutated afterwards.
struct Front {
    hash: Option<Hash>,
    lower: Vec<u8>,
    upper: Option<Vec<u8>>,
}

fn front_info(queue: &VecDeque<Span>) -> Result<Front, DiffError> {
    match queue.front() {
        Some(Span::Subtree { lower, upper, hash }) => Ok(Front {
            hash: Some(*hash),
            lower: lower.clone(),
            upper: upper.clone(),
        }),
        Some(Span::Entries { entries, upper }) => {
            let (lower, _value) = entries.front().ok_or(DiffError::InvalidNode)?;
            Ok(Front {
                hash: None,
                lower: lower.clone(),
                upper: upper.clone(),
            })
        }
        None => Err(DiffError::InvalidNode),
    }
}

/// Advance the walk by one comparison when both sides have a pending span.
fn step<S: NodeStore>(
    store: &S,
    side_a: &mut VecDeque<Span>,
    side_b: &mut VecDeque<Span>,
    out: &mut Vec<DiffEntry>,
) -> Result<(), DiffError> {
    let front_a = front_info(side_a)?;
    let front_b = front_info(side_b)?;

    // P4: identical subtree hashes mean identical content — skip without loading.
    if front_a.hash.is_some() && front_a.hash == front_b.hash {
        side_a.pop_front();
        side_b.pop_front();
        return Ok(());
    }

    // Disjoint ranges: one side lies wholly below the other, so its keys are
    // present on one side only.
    if upper_at_or_below(front_a.upper.as_deref(), &front_b.lower) {
        return emit_one(store, side_a, out, Polarity::Removed);
    }
    if upper_at_or_below(front_b.upper.as_deref(), &front_a.lower) {
        return emit_one(store, side_b, out, Polarity::Added);
    }

    // Overlapping ranges with differing hashes: bring both sides to comparable
    // granularity, then merge leaf runs directly.
    match (front_a.hash, front_b.hash) {
        (None, None) => merge_entries(side_a, side_b, out),
        (Some(hash_a), None) => apply_node(side_a, load(store, hash_a)?, front_a.upper),
        (None, Some(hash_b)) => apply_node(side_b, load(store, hash_b)?, front_b.upper),
        (Some(hash_a), Some(hash_b)) => {
            let node_a = load(store, hash_a)?;
            let node_b = load(store, hash_b)?;
            apply_node(side_a, node_a, front_a.upper)?;
            apply_node(side_b, node_b, front_b.upper)
        }
    }
}

/// Replace the front `Subtree` with the loaded node: leaves materialise into an
/// `Entries` run, internals expand into one `Subtree` per child.
fn apply_node(
    queue: &mut VecDeque<Span>,
    node: Node,
    upper: Option<Vec<u8>>,
) -> Result<(), DiffError> {
    queue.pop_front();
    match node {
        Node::Leaf(leaf) => {
            let entries: VecDeque<_> = leaf.entries().to_vec().into_iter().collect();
            if !entries.is_empty() {
                queue.push_front(Span::Entries { entries, upper });
            }
        }
        Node::Internal(internal) => {
            let spans = child_spans(&internal, upper.as_deref())?;
            for span in spans.into_iter().rev() {
                queue.push_front(span);
            }
        }
    }
    Ok(())
}

/// Expand an internal node into ascending child spans. Each child's exclusive
/// upper bound is the next sibling's separator, with the last child inheriting
/// the parent's bound.
fn child_spans(
    internal: &InternalNode,
    parent_upper: Option<&[u8]>,
) -> Result<Vec<Span>, DiffError> {
    let children = internal.children();
    if children.is_empty() {
        return Err(DiffError::InvalidNode);
    }

    let mut spans = Vec::with_capacity(children.len());
    for (index, (separator, hash)) in children.iter().enumerate() {
        let upper = match children.get(index + 1) {
            Some((next_separator, _next_hash)) => Some(next_separator.clone()),
            None => parent_upper.map(<[u8]>::to_vec),
        };
        spans.push(Span::Subtree {
            lower: separator.clone(),
            upper,
            hash: *hash,
        });
    }
    Ok(spans)
}

/// Two-pointer merge of two materialised leaf runs within their overlapping
/// range. Entries that extend past the shorter run's bound are re-queued so the
/// opposing side's next span can claim them.
fn merge_entries(
    side_a: &mut VecDeque<Span>,
    side_b: &mut VecDeque<Span>,
    out: &mut Vec<DiffEntry>,
) -> Result<(), DiffError> {
    let (mut entries_a, upper_a) = take_entries(side_a)?;
    let (mut entries_b, upper_b) = take_entries(side_b)?;

    loop {
        match (entries_a.front(), entries_b.front()) {
            (Some((key_a, _)), Some((key_b, _))) => match key_a.cmp(key_b) {
                std::cmp::Ordering::Less => emit_front(&mut entries_a, out, Polarity::Removed),
                std::cmp::Ordering::Greater => emit_front(&mut entries_b, out, Polarity::Added),
                std::cmp::Ordering::Equal => {
                    let (key, old_value) = pop_front(&mut entries_a)?;
                    let (_key, new_value) = pop_front(&mut entries_b)?;
                    if old_value != new_value {
                        out.push(DiffEntry::Modified {
                            key,
                            old_value,
                            new_value,
                        });
                    }
                }
            },
            (Some((key_a, _)), None) => {
                if !below_upper(upper_b.as_deref(), key_a) {
                    break;
                }
                emit_front(&mut entries_a, out, Polarity::Removed);
            }
            (None, Some((key_b, _))) => {
                if !below_upper(upper_a.as_deref(), key_b) {
                    break;
                }
                emit_front(&mut entries_b, out, Polarity::Added);
            }
            (None, None) => break,
        }
    }

    if !entries_a.is_empty() {
        side_a.push_front(Span::Entries {
            entries: entries_a,
            upper: upper_a,
        });
    }
    if !entries_b.is_empty() {
        side_b.push_front(Span::Entries {
            entries: entries_b,
            upper: upper_b,
        });
    }
    Ok(())
}

/// Emit every key beneath the front span as `polarity`, expanding internal
/// nodes lazily so only differing subtrees are loaded.
fn emit_one<S: NodeStore>(
    store: &S,
    queue: &mut VecDeque<Span>,
    out: &mut Vec<DiffEntry>,
    polarity: Polarity,
) -> Result<(), DiffError> {
    match queue.pop_front() {
        Some(Span::Entries { mut entries, .. }) => {
            while let Some((key, value)) = entries.pop_front() {
                out.push(polarity.entry(key, value));
            }
            Ok(())
        }
        Some(Span::Subtree { upper, hash, .. }) => match load(store, hash)? {
            Node::Leaf(leaf) => {
                for (key, value) in leaf.entries() {
                    out.push(polarity.entry(key.clone(), value.clone()));
                }
                Ok(())
            }
            Node::Internal(internal) => {
                let spans = child_spans(&internal, upper.as_deref())?;
                for span in spans.into_iter().rev() {
                    queue.push_front(span);
                }
                Ok(())
            }
        },
        None => Ok(()),
    }
}

fn take_entries(queue: &mut VecDeque<Span>) -> Result<(LeafRun, Option<Vec<u8>>), DiffError> {
    match queue.pop_front() {
        Some(Span::Entries { entries, upper }) => Ok((entries, upper)),
        _ => Err(DiffError::InvalidNode),
    }
}

fn emit_front(entries: &mut LeafRun, out: &mut Vec<DiffEntry>, polarity: Polarity) {
    if let Some((key, value)) = entries.pop_front() {
        out.push(polarity.entry(key, value));
    }
}

fn pop_front(entries: &mut LeafRun) -> Result<Entry, DiffError> {
    entries.pop_front().ok_or(DiffError::InvalidNode)
}

/// `true` when `upper` is a finite bound at or below `key` — i.e. the span ends
/// before `key` begins.
fn upper_at_or_below(upper: Option<&[u8]>, key: &[u8]) -> bool {
    matches!(upper, Some(bound) if bound <= key)
}

/// `true` when `key` falls below the exclusive `upper` bound (unbounded counts).
fn below_upper(upper: Option<&[u8]>, key: &[u8]) -> bool {
    upper.is_none_or(|bound| key < bound)
}

fn load<S: NodeStore>(store: &S, hash: Hash) -> Result<Node, DiffError> {
    store
        .get(&hash)
        .map_err(|_error| DiffError::MissingNode(hash))?
        .ok_or(DiffError::MissingNode(hash))
}

#[cfg(test)]
#[path = "diff_tests.rs"]
mod tests;
