use crate::branch::ShardId;
use crate::store::NodeStore;
use crate::tree::Hash;

use super::protocol::{PullRequest, PushResponse, SyncError, SyncStats};
use super::push::build_push_response_for_shard;

/// Result of a target-initiated pull after applying all received nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullResult {
    pub shard_id: ShardId,
    pub source_root: Option<Hash>,
    pub target_root_before: Option<Hash>,
    pub stats: SyncStats,
}

/// Build the target's initial pull request for exactly one shard.
#[must_use]
pub const fn create_pull_request(shard_id: ShardId, target_root: Option<Hash>) -> PullRequest {
    PullRequest::new(shard_id, target_root)
}

/// Apply a complete source response to the target's content-addressed store.
///
/// Nodes are validated and written independently. If a store error or hash
/// mismatch occurs, already-written nodes remain valid and the operation can be
/// retried safely because content-addressed `put` is idempotent.
pub fn apply_push_response<T>(
    target_store: &mut T,
    request: PullRequest,
    response: PushResponse,
) -> Result<PullResult, SyncError>
where
    T: NodeStore + ?Sized,
{
    if response.shard_id != request.shard_id {
        return Err(SyncError::ShardMismatch {
            expected: request.shard_id,
            actual: response.shard_id,
        });
    }
    if response.target_root != request.target_root {
        return Err(SyncError::TargetRootMismatch {
            expected: request.target_root,
            actual: response.target_root,
        });
    }

    let mut stats = response.stats.without_transfer_counts();
    for transfer in response.transfers {
        apply_transfer(target_store, &transfer)?;
        stats.record_transfer_bytes(transfer.byte_len());
    }

    Ok(PullResult {
        shard_id: request.shard_id,
        source_root: response.source_root,
        target_root_before: request.target_root,
        stats,
    })
}

/// Apply at most `limit` transfers from a source response.
///
/// This helper models a network partition or crash after a partial receive while
/// preserving already-applied content-addressed nodes for retry.
pub fn apply_push_response_prefix<T>(
    target_store: &mut T,
    request: PullRequest,
    response: &PushResponse,
    limit: usize,
) -> Result<PullResult, SyncError>
where
    T: NodeStore + ?Sized,
{
    if response.shard_id != request.shard_id {
        return Err(SyncError::ShardMismatch {
            expected: request.shard_id,
            actual: response.shard_id,
        });
    }
    if response.target_root != request.target_root {
        return Err(SyncError::TargetRootMismatch {
            expected: request.target_root,
            actual: response.target_root,
        });
    }

    let mut stats = response.stats.without_transfer_counts();
    for transfer in response.transfers.iter().take(limit) {
        apply_transfer(target_store, transfer)?;
        stats.record_transfer_bytes(transfer.byte_len());
    }

    Ok(PullResult {
        shard_id: request.shard_id,
        source_root: response.source_root,
        target_root_before: request.target_root,
        stats,
    })
}

/// Pull missing nodes for a single shard from a source store into the target
/// store, using the same missing-node response logic a remote source would use.
pub fn pull_from_source<S, T>(
    source_store: &S,
    target_store: &mut T,
    shard_id: ShardId,
    source_root: Option<Hash>,
    target_root: Option<Hash>,
) -> Result<PullResult, SyncError>
where
    S: NodeStore + ?Sized,
    T: NodeStore + ?Sized,
{
    let request = create_pull_request(shard_id, target_root);
    let response = build_push_response_for_shard(
        source_store,
        target_store,
        shard_id,
        source_root,
        request.target_root,
    )?;
    apply_push_response(target_store, request, response)
}

fn apply_transfer<T>(
    target_store: &mut T,
    transfer: &super::protocol::NodeTransfer,
) -> Result<(), SyncError>
where
    T: NodeStore + ?Sized,
{
    let actual = transfer.node.hash();
    if actual != transfer.hash {
        return Err(SyncError::HashMismatch {
            expected: transfer.hash,
            actual,
        });
    }
    let stored_hash =
        target_store
            .put(&transfer.node)
            .map_err(|_error| SyncError::TargetStoreWrite {
                hash: transfer.hash,
            })?;
    if stored_hash != transfer.hash {
        return Err(SyncError::HashMismatch {
            expected: transfer.hash,
            actual: stored_hash,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;
    use crate::tree::{InternalNode, LeafNode, Node};

    fn leaf(key: &[u8], value: &[u8]) -> Result<Node, Box<dyn std::error::Error>> {
        Ok(Node::Leaf(LeafNode::new(vec![(
            key.to_vec(),
            value.to_vec(),
        )])?))
    }

    fn two_leaf_tree(
        left: &Node,
        right: &Node,
        store: &mut MemoryStore,
    ) -> Result<(Hash, Hash, Hash), Box<dyn std::error::Error>> {
        let left_hash = store.put(left);
        let right_hash = store.put(right);
        let root = Node::Internal(InternalNode::new(vec![
            (b"".to_vec(), left_hash),
            (b"z".to_vec(), right_hash),
        ])?);
        let root_hash = store.put(&root);
        Ok((root_hash, left_hash, right_hash))
    }

    #[test]
    fn target_initiated_pull_applies_received_nodes() -> Result<(), Box<dyn std::error::Error>> {
        let source_left = leaf(b"a", b"source-left")?;
        let source_right = leaf(b"z", b"source-right")?;
        let target_left = leaf(b"a", b"target-left")?;

        let mut source = MemoryStore::new();
        let (source_root, source_left_hash, source_right_hash) =
            two_leaf_tree(&source_left, &source_right, &mut source)?;

        let mut target = MemoryStore::new();
        let target_root = target.put(&target_left);

        let result = pull_from_source(
            &source,
            &mut target,
            5,
            Some(source_root),
            Some(target_root),
        )?;

        assert_eq!(result.shard_id, 5);
        assert_eq!(result.source_root, Some(source_root));
        assert!(target.get(&source_root).is_some());
        assert!(target.get(&source_left_hash).is_some());
        assert!(target.get(&source_right_hash).is_some());
        assert!(target.get(&target_root).is_some());
        assert_eq!(result.stats.nodes_transferred, 3);
        Ok(())
    }

    #[test]
    fn identical_roots_pull_transfers_zero_nodes() -> Result<(), Box<dyn std::error::Error>> {
        let root_node = leaf(b"a", b"one")?;
        let mut source = MemoryStore::new();
        let root = source.put(&root_node);
        let mut target = MemoryStore::new();
        target.put(&root_node);

        let result = pull_from_source(&source, &mut target, 5, Some(root), Some(root))?;

        assert_eq!(result.stats.nodes_transferred, 0);
        assert_eq!(result.source_root, Some(root));
        Ok(())
    }

    #[test]
    fn partial_apply_is_safe_and_retry_skips_already_received_nodes()
    -> Result<(), Box<dyn std::error::Error>> {
        let shared = leaf(b"a", b"shared")?;
        let source_right = leaf(b"z", b"source-right")?;
        let target_right = leaf(b"z", b"target-right")?;

        let mut source = MemoryStore::new();
        let (source_root, _shared_hash, source_right_hash) =
            two_leaf_tree(&shared, &source_right, &mut source)?;

        let mut target = MemoryStore::new();
        let (target_root, _target_shared_hash, target_right_hash) =
            two_leaf_tree(&shared, &target_right, &mut target)?;

        let request = create_pull_request(5, Some(target_root));
        let response = build_push_response_for_shard(
            &source,
            &target,
            5,
            Some(source_root),
            request.target_root,
        )?;
        assert_eq!(response.stats.nodes_transferred, 2);

        let partial = apply_push_response_prefix(&mut target, request, &response, 1)?;
        assert_eq!(partial.stats.nodes_transferred, 1);
        assert!(target.get(&source_right_hash).is_some());
        assert!(target.get(&source_root).is_none());
        assert!(target.get(&target_root).is_some());
        assert!(target.get(&target_right_hash).is_some());

        let retry = pull_from_source(
            &source,
            &mut target,
            5,
            Some(source_root),
            Some(target_root),
        )?;

        assert_eq!(retry.stats.nodes_transferred, 1);
        assert!(target.get(&source_root).is_some());
        assert!(target.get(&source_right_hash).is_some());
        assert!(target.get(&target_root).is_some());
        assert!(target.get(&target_right_hash).is_some());
        Ok(())
    }
}
