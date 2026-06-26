//! Ancestor-free 2-way union + per-key max-stamp merge (AA-3-4c, §2.4).
//!
//! This is the algorithmic heart of step-3 handoff reconciliation. When a new
//! owner is elected it must adopt a committed baseline that loses NO committed
//! write across its promise majority. Per-key follower lag forks the committed
//! state under a single owner (§2.4), so the promise-majority roots can be
//! INCOMPARABLE — adopting any single root silently drops a committed write.
//! The fix is a union: keep every key, and where a key is on both sides keep the
//! entry with the higher causal commit stamp (the CAS-chain tip).
//!
//! WHY NOT `branch::merge_with_report`. That is a THREE-WAY merge: it takes a
//! common `ancestor_root`, its resolver fires ONLY on mutual divergence from that
//! ancestor, and it sees only value bytes (not the stamp envelope). It
//! structurally cannot host an ancestor-free union keyed on `(epoch, seq)`. This
//! module therefore builds its OWN resolver while reusing only the lower-level
//! prolly-tree primitives:
//!
//! - leaf/entry iteration over a committed root ([`collect_stored_entries`], a
//!   recursive walk over [`Node::Leaf`]/[`Node::Internal`] via [`NodeStore::get`],
//!   the same shape as `tree::mutate::collect_leaf_refs_inner` but loading leaves
//!   so the RAW STORED bytes — tombstones included — are visible, NOT
//!   read-filtered);
//! - the tree builder the commit path uses ([`batch_mutate`] over an empty-leaf
//!   baseline, exactly as `ShardActor::apply_durable` builds a commit at
//!   `shard/actor.rs:327`).
//!
//! ORDER-INDEPENDENCE. `max` over the total order `(epoch, seq)` is a
//! commutative, associative, idempotent semilattice join, so merging over >2
//! promisers in any order yields the IDENTICAL root hash. The merged tree is also
//! built history-independently by `batch_mutate`, so the root is a pure function
//! of the winning key->entry set. (This is the property haematite's prolly history
//! was once bitten by; the property tests below pin it.)

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::store::NodeStore;
use crate::sync::ballot::Stamp;
use crate::tree::{Hash, LeafNode, Node, TreeError, batch_mutate};
use crate::ttl::entry::{StampedEntry, TtlDecodeError};

/// A stored `(key, raw-bytes)` pair as it sits in a leaf.
type StoredEntry = (Vec<u8>, Vec<u8>);

/// Errors raised by the ancestor-free union merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandoffMergeError {
    /// A node referenced by a root was absent from the store.
    MissingNode { hash: Hash },
    /// The store failed to read a node.
    StoreRead { hash: Hash },
    /// A node was structurally invalid (e.g. an internal node with no children).
    InvalidNode,
    /// A stored entry's bytes did not decode as a stamped envelope (every
    /// committed write is stamped from 3-4a onward, so an un-stamped entry in a
    /// committed tree is a corruption, not a legacy value).
    UndecodableEntry { key: Vec<u8> },
    /// INVARIANT VIOLATION (R-LE/R-SEQ): two committed entries for one key carry
    /// the SAME `(epoch, seq)` stamp but DIFFERENT bytes. Stamps are globally
    /// unique, so this must be impossible; we fail loud rather than silently pick
    /// one. (Identical bytes at an equal stamp are fine — that is the same write
    /// replicated, and `max` is idempotent.)
    DuplicateStamp { key: Vec<u8>, stamp: Stamp },
}

impl fmt::Display for HandoffMergeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNode { hash } => write!(formatter, "missing tree node {hash}"),
            Self::StoreRead { hash } => write!(formatter, "failed to read tree node {hash}"),
            Self::InvalidNode => formatter.write_str("invalid tree node"),
            Self::UndecodableEntry { key } => write!(
                formatter,
                "committed entry for key {} is not a stamped envelope",
                String::from_utf8_lossy(key)
            ),
            Self::DuplicateStamp { key, stamp } => write!(
                formatter,
                "key {} has two distinct committed values at the same stamp {stamp:?} \
                 (stamp uniqueness invariant violated)",
                String::from_utf8_lossy(key)
            ),
        }
    }
}

impl std::error::Error for HandoffMergeError {}

impl From<TreeError> for HandoffMergeError {
    fn from(error: TreeError) -> Self {
        match error {
            TreeError::MissingNode { hash } => Self::MissingNode { hash },
            TreeError::InvalidNode => Self::InvalidNode,
        }
    }
}

impl From<TtlDecodeError> for HandoffMergeError {
    fn from(_error: TtlDecodeError) -> Self {
        // A truncated/undecodable stamped envelope in a committed tree is
        // corruption; the key is attached by the caller below.
        Self::InvalidNode
    }
}

/// Merge two committed roots by ancestor-free union + per-key max-stamp (§2.4).
///
/// `None` denotes an absent (empty) committed state on that side. The result is:
/// - `None` when BOTH sides are absent (nothing to adopt);
/// - `Some(root)` of a freshly-built tree otherwise, whose key set is the UNION of
///   both sides and whose per-key entry is the one with the maximum `(epoch, seq)`
///   stamp — tombstones included, a tombstone with the higher stamp WINS and is
///   WRITTEN into the merged tree (R-TOMB: it persists; it is not dropped just
///   because it reads as absent).
///
/// "Present on one side only" is decided by whether the key is STORED in that
/// tree at all — a tombstone IS stored, so a tombstone-vs-never-written key keeps
/// the tombstone (the delete is not resurrected). The read-time visibility filter
/// is deliberately NOT applied here.
pub fn merge_committed_union<S: NodeStore + ?Sized>(
    root_a: Option<Hash>,
    root_b: Option<Hash>,
    store: &mut S,
) -> Result<Option<Hash>, HandoffMergeError> {
    if root_a.is_none() && root_b.is_none() {
        return Ok(None);
    }

    // Winning stored bytes per key, built by folding each side's raw entries
    // through the max-stamp join. A BTreeMap keeps keys sorted for a deterministic
    // build, though `batch_mutate` is history-independent regardless.
    let mut winners: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for root in [root_a, root_b].into_iter().flatten() {
        merge_root_into(store, root, &mut winners)?;
    }

    // Build the merged tree the way the commit path does: an empty-leaf baseline
    // mutated with the winning key->stored-bytes set.
    let empty_root = store_empty_leaf(store)?;
    let mutations: Vec<(Vec<u8>, Option<Vec<u8>>)> = winners
        .into_iter()
        .map(|(key, bytes)| (key, Some(bytes)))
        .collect();
    let merged = batch_mutate(store, empty_root, mutations.as_slice())?;
    Ok(Some(merged))
}

/// Fold every stored entry reachable from `root` into `winners` by max-stamp.
fn merge_root_into<S: NodeStore + ?Sized>(
    store: &S,
    root: Hash,
    winners: &mut BTreeMap<Vec<u8>, Vec<u8>>,
) -> Result<(), HandoffMergeError> {
    for (key, bytes) in collect_stored_entries(store, root)? {
        let stamp = decode_stamp(&key, &bytes)?;
        match winners.get(&key) {
            None => {
                winners.insert(key, bytes);
            }
            Some(existing) => {
                let existing_stamp = decode_stamp(&key, existing)?;
                if stamp > existing_stamp {
                    // The chain descendant wins (value OR tombstone).
                    winners.insert(key, bytes);
                } else if stamp == existing_stamp && bytes != *existing {
                    // Globally-unique stamps make this impossible; fail loud.
                    return Err(HandoffMergeError::DuplicateStamp { key, stamp });
                }
                // stamp < existing, or equal-and-identical: keep the incumbent
                // (idempotent max).
            }
        }
    }
    Ok(())
}

/// Decode the causal commit stamp from a stored, stamped entry.
fn decode_stamp(key: &[u8], bytes: &[u8]) -> Result<Stamp, HandoffMergeError> {
    let entry = StampedEntry::decode(bytes)
        .map_err(|_error| HandoffMergeError::UndecodableEntry { key: key.to_vec() })?
        .ok_or_else(|| HandoffMergeError::UndecodableEntry { key: key.to_vec() })?;
    Ok(entry.stamp().clone())
}

/// Collect EVERY stored `(key, raw-bytes)` entry reachable from `root`, in no
/// particular order, by a recursive walk over the prolly tree. Tombstones are
/// returned as their raw stamped bytes (NOT read-filtered) so the merge sees them.
fn collect_stored_entries<S: NodeStore + ?Sized>(
    store: &S,
    root: Hash,
) -> Result<Vec<StoredEntry>, HandoffMergeError> {
    let mut entries = Vec::new();
    collect_into(store, root, &mut entries)?;
    Ok(entries)
}

fn collect_into<S: NodeStore + ?Sized>(
    store: &S,
    hash: Hash,
    out: &mut Vec<StoredEntry>,
) -> Result<(), HandoffMergeError> {
    match &*load_node(store, hash)? {
        Node::Leaf(leaf) => {
            out.extend(leaf.entries().iter().cloned());
            Ok(())
        }
        Node::Internal(internal) => {
            if internal.children().is_empty() {
                return Err(HandoffMergeError::InvalidNode);
            }
            for (_separator, child_hash) in internal.children() {
                collect_into(store, *child_hash, out)?;
            }
            Ok(())
        }
    }
}

fn load_node<S: NodeStore + ?Sized>(store: &S, hash: Hash) -> Result<Arc<Node>, HandoffMergeError> {
    store
        .get(&hash)
        .map_err(|_error| HandoffMergeError::StoreRead { hash })?
        .ok_or(HandoffMergeError::MissingNode { hash })
}

fn store_empty_leaf<S: NodeStore + ?Sized>(store: &mut S) -> Result<Hash, HandoffMergeError> {
    let leaf = LeafNode::new(Vec::new()).map_err(|_error| HandoffMergeError::InvalidNode)?;
    store
        .put(&Node::Leaf(leaf))
        .map_err(|_error| HandoffMergeError::InvalidNode)
}

#[cfg(test)]
#[path = "handoff_merge_tests.rs"]
mod tests;
