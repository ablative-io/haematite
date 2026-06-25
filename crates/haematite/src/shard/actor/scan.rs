//! API-001: full-shard sequence scan used by the `EventStore` `scan` predicate.
//!
//! [`scan_sequences`] walks a shard's entire keyspace — the committed tree
//! merged with the live write buffer — and decodes every stream's
//! sequence-metadata key into a `(stream_key, next_seq)` pair. It is the
//! O(total entries) traversal that backs the cross-shard scan; there is no
//! secondary index (out of scope per the brief).

use std::collections::BTreeMap;

use crate::shard::actor::decode_sequence_key;
use crate::store::NodeStore;
use crate::tree::{Hash, Node, TreeError};
use crate::ttl::filter::{Visibility, visible_value};
use crate::wal::{Mutation, WalBuffer, WalError};

use super::handle::{ShardError, StreamSeq};

/// Walk `committed_root` (if any) merged with `buffer` and decode every
/// stream's sequence-metadata key.
///
/// The buffer shadows the tree: a buffered put overrides a committed value and
/// a buffered delete removes the key, so the result matches the view a
/// `get`/`range` against the same shard would return.
pub(super) fn scan_sequences<S>(
    store: &S,
    committed_root: Option<Hash>,
    buffer: &WalBuffer,
) -> Result<Vec<StreamSeq>, ShardError>
where
    S: NodeStore + ?Sized,
{
    // Only sequence-metadata keys are ever needed here, so filter to them DURING
    // collection rather than materialising the shard's entire keyspace (every event
    // payload + KV value) and filtering at the end. Without a secondary index the
    // tree walk still visits every node, but the merged map and its clones become
    // O(streams) instead of O(total entries) — no event payload is ever cloned.
    let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    if let Some(root) = committed_root {
        collect_sequence_entries(store, root, &mut merged)?;
    }
    let buffered = buffer.iter();
    for mutation in buffered {
        match mutation {
            Mutation::Put { key, value } if decode_sequence_key(key).is_some() => {
                merged.insert(key.clone(), value.clone());
            }
            Mutation::Delete { key } if decode_sequence_key(key).is_some() => {
                merged.remove(key);
            }
            // Non-sequence keys (event payloads, KV records) never affect the
            // sequence enumeration; skip without cloning.
            Mutation::Put { .. } | Mutation::Delete { .. } => {}
        }
    }
    let mut streams = Vec::new();
    for (key, value) in merged {
        if let Some(stream_key) = decode_sequence_key(&key) {
            // Every committed write is stamped (AA-3-4a) and may carry a TTL
            // envelope, so the raw tree value here is NOT the bare sequence number
            // — it is the same stamped/enveloped value `get` decodes. Resolve it to
            // the LOGICAL bytes the read path sees before decoding the sequence. A
            // tombstoned or expired counter reads as absent (the stream no longer
            // exists), exactly as `read_stream_next_seq` would see it, so skip it.
            match visible_value(&value)
                .map_err(|error| ShardError::Wal(WalError::TreeError(error.to_string())))?
            {
                Visibility::Live(logical) => {
                    let next_seq = decode_seq_value(&logical)?;
                    streams.push((stream_key.to_vec(), next_seq));
                }
                Visibility::Expired => {}
            }
        }
    }
    Ok(streams)
}

/// Walk the committed tree rooted at `root`, inserting only the
/// sequence-metadata leaf entries into `out`. Uses an explicit stack to bound
/// recursion depth on deep trees. Event payloads and other KV values are never
/// cloned — only keys that decode as sequence keys are retained.
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
        match load_node(store, hash)? {
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

/// Load a node by hash, mapping a missing node or store error to a
/// [`ShardError`].
fn load_node<S>(store: &S, hash: Hash) -> Result<Node, ShardError>
where
    S: NodeStore + ?Sized,
{
    store
        .get(&hash)
        .map_err(|error| ShardError::Wal(WalError::TreeError(error.to_string())))?
        .ok_or(ShardError::Tree(TreeError::MissingNode { hash }))
}

/// Decode a stored eight-byte big-endian sequence value.
fn decode_seq_value(value: &[u8]) -> Result<u64, ShardError> {
    value
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| ShardError::Wal(WalError::TreeError("invalid sequence metadata".to_owned())))
}
