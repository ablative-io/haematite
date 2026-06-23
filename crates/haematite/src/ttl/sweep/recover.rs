// API-003: read-only store + WAL recovery used by the sweep pass.

use std::collections::BTreeMap;

use crate::store::{DiskStore, NodeStore};
use crate::tree::{Hash, Node, TreeError};
use crate::wal::{WalBuffer, WalRecovery};

use super::SweepError;

/// Recover a read-only view of a shard's committed root and uncommitted WAL
/// buffer, without taking the shard actor's write path.
pub(super) fn recover_view(
    store_dir: &std::path::Path,
    wal_path: &std::path::Path,
) -> Result<(DiskStore, Option<Hash>, WalBuffer), SweepError> {
    let store = DiskStore::new(store_dir).map_err(|error| SweepError::Store(error.to_string()))?;
    let recovered = WalRecovery::recover_path(wal_path, &store)
        .map_err(|error| SweepError::Store(error.to_string()))?;
    let root = recovered.committed_root();
    Ok((store, root, recovered.into_buffer()))
}

/// Walk the committed prolly tree under `root`, collecting every `(key, value)`
/// into `out` in no particular order.
pub(super) fn collect_tree<S>(
    store: &S,
    root: Hash,
    out: &mut BTreeMap<Vec<u8>, Vec<u8>>,
) -> Result<(), SweepError>
where
    S: NodeStore + ?Sized,
{
    let mut stack = vec![root];
    while let Some(hash) = stack.pop() {
        match load_node(store, hash)? {
            Node::Leaf(leaf) => {
                for (key, value) in leaf.entries() {
                    out.insert(key.clone(), value.clone());
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

fn load_node<S>(store: &S, hash: Hash) -> Result<Node, SweepError>
where
    S: NodeStore + ?Sized,
{
    store
        .get(&hash)
        .map_err(|error| SweepError::Store(error.to_string()))?
        .ok_or_else(|| SweepError::Store(TreeError::MissingNode { hash }.to_string()))
}
