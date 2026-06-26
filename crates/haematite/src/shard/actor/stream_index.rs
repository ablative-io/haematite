//! PERF-002: actor-local stream sequence secondary index.
//!
//! The shard actor owns this ordered map directly. It is rebuilt from committed
//! roots for recovery / root adoption, and updated from the committed write batch
//! only after the durable WAL commit succeeds.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::shard::actor::decode_sequence_key;
use crate::store::NodeStore;
use crate::tree::{Hash, Node, TreeError};
use crate::ttl::filter::{Visibility, visible_value};
use crate::wal::{Mutation, WalBuffer, WalError};

use super::handle::{ShardError, StreamSeq};

/// Ordered map from decoded stream key to the next sequence number.
pub(super) type LiveStreamIndex = BTreeMap<Vec<u8>, u64>;

/// Ordered map of live-but-invalid sequence metadata entries.
pub(super) type SequenceIndexErrors = BTreeMap<Vec<u8>, String>;

/// A rebuilt secondary-index snapshot.
#[derive(Debug)]
pub(super) struct StreamIndexSnapshot {
    pub(super) live: LiveStreamIndex,
    pub(super) errors: SequenceIndexErrors,
}

/// Return an ordered scan response from the actor-owned index.
pub(super) fn scan_index(
    index: &LiveStreamIndex,
    errors: &SequenceIndexErrors,
) -> Result<Vec<StreamSeq>, ShardError> {
    if let Some(message) = errors.values().next() {
        return Err(ShardError::Wal(WalError::TreeError(message.clone())));
    }
    Ok(index.iter().map(|(key, seq)| (key.clone(), *seq)).collect())
}

/// Rebuild the live-stream index from a committed root.
pub(super) fn rebuild<S>(
    store: &S,
    committed_root: Option<Hash>,
) -> Result<StreamIndexSnapshot, ShardError>
where
    S: NodeStore + ?Sized,
{
    let mut sequence_entries = BTreeMap::new();
    if let Some(root) = committed_root {
        collect_sequence_entries(store, root, &mut sequence_entries)?;
    }
    Ok(sequence_entries_to_snapshot(sequence_entries))
}

/// Build the pre-PERF-002 full-walk scan view: committed tree plus live buffer.
///
/// This remains available for tests/oracles. Production enumeration uses
/// [`scan_index`] against the actor-owned secondary index.
#[cfg(test)]
pub(super) fn full_walk_with_buffer<S>(
    store: &S,
    committed_root: Option<Hash>,
    buffer: &WalBuffer,
) -> Result<Vec<StreamSeq>, ShardError>
where
    S: NodeStore + ?Sized,
{
    let mut sequence_entries = BTreeMap::new();
    if let Some(root) = committed_root {
        collect_sequence_entries(store, root, &mut sequence_entries)?;
    }
    apply_buffer_to_entries(buffer, &mut sequence_entries);
    let snapshot = sequence_entries_to_snapshot(sequence_entries);
    scan_index(&snapshot.live, &snapshot.errors)
}

/// Apply a just-durably-committed WAL buffer to the in-memory index.
pub(super) fn apply_committed_buffer(
    index: &mut LiveStreamIndex,
    errors: &mut SequenceIndexErrors,
    buffer: &WalBuffer,
) {
    for mutation in buffer {
        match mutation {
            Mutation::Put { key, value } => apply_sequence_put(index, errors, key, value),
            Mutation::Delete { key } => apply_sequence_delete(index, errors, key),
        }
    }
}

fn apply_sequence_put(
    index: &mut LiveStreamIndex,
    errors: &mut SequenceIndexErrors,
    key: &[u8],
    value: &[u8],
) {
    let Some(stream_key) = decode_sequence_key(key) else {
        return;
    };
    match visible_value(value) {
        Ok(Visibility::Live(logical)) => match decode_seq_value(&logical) {
            Ok(next_seq) => {
                errors.remove(stream_key);
                index.insert(stream_key.to_vec(), next_seq);
            }
            Err(message) => {
                index.remove(stream_key);
                errors.insert(stream_key.to_vec(), message);
            }
        },
        Ok(Visibility::Expired) => {
            index.remove(stream_key);
            errors.remove(stream_key);
        }
        Err(error) => {
            index.remove(stream_key);
            errors.insert(stream_key.to_vec(), error.to_string());
        }
    }
}

fn apply_sequence_delete(
    index: &mut LiveStreamIndex,
    errors: &mut SequenceIndexErrors,
    key: &[u8],
) {
    if let Some(stream_key) = decode_sequence_key(key) {
        index.remove(stream_key);
        errors.remove(stream_key);
    }
}

#[cfg(test)]
fn apply_buffer_to_entries(buffer: &WalBuffer, entries: &mut BTreeMap<Vec<u8>, Vec<u8>>) {
    for mutation in buffer {
        match mutation {
            Mutation::Put { key, value } if decode_sequence_key(key).is_some() => {
                entries.insert(key.clone(), value.clone());
            }
            Mutation::Delete { key } if decode_sequence_key(key).is_some() => {
                entries.remove(key);
            }
            Mutation::Put { .. } | Mutation::Delete { .. } => {}
        }
    }
}

fn sequence_entries_to_snapshot(entries: BTreeMap<Vec<u8>, Vec<u8>>) -> StreamIndexSnapshot {
    let mut snapshot = StreamIndexSnapshot {
        live: LiveStreamIndex::new(),
        errors: SequenceIndexErrors::new(),
    };
    for (key, value) in entries {
        apply_sequence_put(&mut snapshot.live, &mut snapshot.errors, &key, &value);
    }
    snapshot
}

/// Walk the committed tree rooted at `root`, inserting only sequence metadata.
fn collect_sequence_entries<S>(
    store: &S,
    root: Hash,
    out: &mut BTreeMap<Vec<u8>, Vec<u8>>,
) -> Result<(), ShardError>
where
    S: NodeStore + ?Sized,
{
    let mut stack = vec![root];
    while let Some(hash) = stack.pop() {
        match &*load_node(store, hash)? {
            Node::Leaf(leaf) => {
                for (key, value) in leaf.entries() {
                    if decode_sequence_key(key).is_some() {
                        out.insert(key.clone(), value.clone());
                    }
                }
            }
            Node::Internal(internal) => {
                for (_separator, child) in internal.children() {
                    stack.push(*child);
                }
            }
        }
    }
    Ok(())
}

fn load_node<S>(store: &S, hash: Hash) -> Result<Arc<Node>, ShardError>
where
    S: NodeStore + ?Sized,
{
    store
        .get(&hash)
        .map_err(|error| ShardError::Wal(WalError::TreeError(error.to_string())))?
        .ok_or(ShardError::Tree(TreeError::MissingNode { hash }))
}

fn decode_seq_value(value: &[u8]) -> Result<u64, String> {
    value
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| "invalid sequence metadata".to_owned())
}
