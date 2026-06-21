use crate::branch::ShardId;
use crate::store::NodeStore;
use crate::tree::Hash;

use super::protocol::{
    PullRequest, PushResponse, RootExchangeResponse, SyncError, TargetNodeReader,
    find_missing_nodes,
};

/// Build the source-side response for a target-initiated pull request.
///
/// This function is side-effect-free for the source store: it only reads the
/// content-addressed nodes needed to determine and send the target's delta.
/// `target_nodes` may be a local target store or a remote reader backed by
/// beamr `TargetNodeRequest`/`TargetNodeResponse` messages.
pub fn build_push_response<S, T>(
    source_store: &S,
    target_nodes: &T,
    request: PullRequest,
    source_root: Option<Hash>,
) -> Result<PushResponse, SyncError>
where
    S: NodeStore + ?Sized,
    T: TargetNodeReader + ?Sized,
{
    build_push_response_for_shard(
        source_store,
        target_nodes,
        request.shard_id,
        source_root,
        request.target_root,
    )
}

/// Build a source-side push response for one shard/root pair.
///
/// The source only needs a target-node reader; it does not own or mutate target state.
pub fn build_push_response_for_shard<S, T>(
    source_store: &S,
    target_nodes: &T,
    shard_id: ShardId,
    source_root: Option<Hash>,
    target_root: Option<Hash>,
) -> Result<PushResponse, SyncError>
where
    S: NodeStore + ?Sized,
    T: TargetNodeReader + ?Sized,
{
    let missing = find_missing_nodes(
        source_store,
        target_nodes,
        shard_id,
        source_root,
        target_root,
    )?;
    Ok(PushResponse::new(
        shard_id,
        missing.source_root,
        missing.target_root,
        missing.transfers,
        missing.stats,
    ))
}

/// Respond to the root-exchange portion of a pull request without walking trees.
#[must_use]
pub fn exchange_roots_for_pull(
    request: PullRequest,
    source_root: Option<Hash>,
) -> RootExchangeResponse {
    RootExchangeResponse::from_request(&request.root_exchange_request(), source_root)
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

    #[test]
    fn push_response_has_no_nodes_when_roots_match() -> Result<(), Box<dyn std::error::Error>> {
        let mut source = MemoryStore::new();
        let root_node = leaf(b"a", b"one")?;
        let root = source.put(&root_node);
        let mut target = MemoryStore::new();
        target.put(&root_node);

        let response = build_push_response(
            &source,
            &target,
            PullRequest::new(5, Some(root)),
            Some(root),
        )?;

        assert_eq!(response.shard_id, 5);
        assert!(response.transfers.is_empty());
        assert_eq!(response.stats.nodes_transferred, 0);
        Ok(())
    }

    #[test]
    fn push_response_sends_only_nodes_missing_from_target() -> Result<(), Box<dyn std::error::Error>>
    {
        let shared = leaf(b"a", b"shared")?;
        let source_only = leaf(b"z", b"source")?;
        let target_only = leaf(b"z", b"target")?;

        let mut source = MemoryStore::new();
        let mut target = MemoryStore::new();
        let shared_hash = source.put(&shared);
        target.put(&shared);
        let source_only_hash = source.put(&source_only);
        let target_only_hash = target.put(&target_only);

        let source_root_node = Node::Internal(InternalNode::new(vec![
            (b"".to_vec(), shared_hash),
            (b"z".to_vec(), source_only_hash),
        ])?);
        let target_root_node = Node::Internal(InternalNode::new(vec![
            (b"".to_vec(), shared_hash),
            (b"z".to_vec(), target_only_hash),
        ])?);
        let source_root = source.put(&source_root_node);
        let target_root = target.put(&target_root_node);

        let response = build_push_response(
            &source,
            &target,
            PullRequest::new(5, Some(target_root)),
            Some(source_root),
        )?;
        let transferred_hashes: Vec<_> = response
            .transfers
            .iter()
            .map(|transfer| transfer.hash)
            .collect();

        assert_eq!(transferred_hashes, vec![source_only_hash, source_root]);
        assert!(!transferred_hashes.contains(&shared_hash));
        assert_eq!(response.stats.nodes_transferred, 2);
        Ok(())
    }
}
