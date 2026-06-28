//! Source-side missing-node discovery for one shard.
//!
//! Pure tree-walk logic over the content-addressed store and a
//! [`TargetNodeReader`]; no native dependency, so it compiles on wasm.

use std::collections::BTreeSet;

use crate::ids::ShardId;
use crate::store::NodeStore;
use crate::sync_codec::error::SyncError;
use crate::sync_codec::message::root::{SyncStats, plan_sync};
use crate::sync_codec::message::transfer::{MissingNodes, NodeTransfer};
use crate::sync_codec::target::TargetNodeReader;
use crate::tree::{Hash, Node};

/// Discover the source nodes missing from the target for one shard.
///
/// The result is ordered children-before-parents so a crash during pull never
/// leaves a newly visible source root without the descendants already written.
pub fn find_missing_nodes<S, T>(
    source_store: &S,
    target_store: &T,
    shard_id: ShardId,
    source_root: Option<Hash>,
    target_root: Option<Hash>,
) -> Result<MissingNodes, SyncError>
where
    S: NodeStore + ?Sized,
    T: TargetNodeReader + ?Sized,
{
    let plan = plan_sync(shard_id, source_root, target_root);
    let mut stats = plan.stats;
    let mut transfers = Vec::new();

    if !plan.requires_tree_walk() {
        return Ok(MissingNodes {
            shard_id,
            source_root,
            target_root,
            decision: plan.exchange.decision,
            transfers,
            stats,
        });
    }

    if let Some(source_hash) = source_root {
        let mut visited = BTreeSet::new();
        collect_missing_node(
            source_store,
            target_store,
            source_hash,
            target_root,
            &mut transfers,
            &mut visited,
            &mut stats,
        )?;
    }

    stats.nodes_transferred = transfers.len();
    stats.bytes_transferred = transfers.iter().map(NodeTransfer::byte_len).sum();

    Ok(MissingNodes {
        shard_id,
        source_root,
        target_root,
        decision: plan.exchange.decision,
        transfers,
        stats,
    })
}

fn collect_missing_node<S, T>(
    source_store: &S,
    target_store: &T,
    source_hash: Hash,
    target_hash: Option<Hash>,
    transfers: &mut Vec<NodeTransfer>,
    visited: &mut BTreeSet<Hash>,
    stats: &mut SyncStats,
) -> Result<(), SyncError>
where
    S: NodeStore + ?Sized,
    T: TargetNodeReader + ?Sized,
{
    if target_hash == Some(source_hash) {
        stats.matching_subtrees_skipped = stats.matching_subtrees_skipped.saturating_add(1);
        return Ok(());
    }

    if !visited.insert(source_hash) {
        return Ok(());
    }

    stats.target_nodes_checked = stats.target_nodes_checked.saturating_add(1);
    if target_store.read_target_node(source_hash)?.is_some() {
        stats.existing_subtrees_skipped = stats.existing_subtrees_skipped.saturating_add(1);
        return Ok(());
    }

    stats.source_nodes_read = stats.source_nodes_read.saturating_add(1);
    let source_node = source_store
        .get(&source_hash)
        .map_err(|_error| SyncError::SourceStoreRead { hash: source_hash })?
        .ok_or(SyncError::MissingSourceNode { hash: source_hash })?;
    let actual_hash = source_node.hash();
    if actual_hash != source_hash {
        return Err(SyncError::HashMismatch {
            expected: source_hash,
            actual: actual_hash,
        });
    }

    let target_node = match target_hash {
        Some(hash) => {
            stats.target_nodes_checked = stats.target_nodes_checked.saturating_add(1);
            target_store.read_target_node(hash)?
        }
        None => None,
    };

    if let Node::Internal(internal) = &*source_node {
        for (separator, child_hash) in internal.children() {
            let target_child_hash = target_node
                .as_ref()
                .and_then(|node| node.child_hash(separator.as_slice()));
            collect_missing_node(
                source_store,
                target_store,
                *child_hash,
                target_child_hash,
                transfers,
                visited,
                stats,
            )?;
        }
    }

    let transfer = NodeTransfer::from_parts(source_hash, Node::clone(&source_node))?;
    stats.record_transfer_bytes(transfer.byte_len());
    transfers.push(transfer);
    Ok(())
}
