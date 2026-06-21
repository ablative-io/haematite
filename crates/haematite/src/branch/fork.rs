use crate::tree::Hash;

use super::handle::{BranchError, BranchHandle, ShardId};
use super::registry::BranchRegistry;

/// Fork a single-shard database or shard at its current committed root hash.
///
/// The operation records `root` as both the fork point and the current branch
/// root, then allocates one empty branch-local WAL buffer. It accepts no node
/// store and therefore cannot read or copy tree nodes; work is constant with
/// respect to the number of database entries.
#[must_use]
pub fn fork(root: Hash) -> BranchHandle {
    BranchHandle::new(root)
}

/// Fork a single-shard database or shard and register its live root.
///
/// The root remains registered until the last clone of the returned
/// [`BranchHandle`] is dropped.
#[must_use]
pub fn fork_registered(root: Hash, registry: &BranchRegistry) -> BranchHandle {
    let branch = fork(root);
    let guard = registry.register_roots(branch.fork_points());
    branch.with_registry_guard(guard)
}

/// Fork a logical database with one independent root per shard.
///
/// Each supplied `(shard_id, root)` records that shard's fork point and allocates
/// a distinct empty WAL buffer. The function coordinates shard state inside one
/// [`BranchHandle`] but performs no cross-shard locking or tree access.
pub fn fork_shards<I>(roots: I) -> Result<BranchHandle, BranchError>
where
    I: IntoIterator<Item = (ShardId, Hash)>,
{
    BranchHandle::from_shard_roots(roots)
}

/// Fork multiple shards and register every shard root as live.
///
/// Registering all shard roots keeps future pruning conservative: any tree root
/// referenced by any live branch shard remains visible through
/// [`BranchRegistry::live_roots`](super::BranchRegistry::live_roots).
pub fn fork_shards_registered<I>(
    roots: I,
    registry: &BranchRegistry,
) -> Result<BranchHandle, BranchError>
where
    I: IntoIterator<Item = (ShardId, Hash)>,
{
    let branch = fork_shards(roots)?;
    let guard = registry.register_roots(branch.fork_points());
    Ok(branch.with_registry_guard(guard))
}

#[cfg(test)]
mod tests {
    use super::{fork, fork_registered, fork_shards, fork_shards_registered};
    use crate::branch::{BranchError, BranchRegistry, ShardId};
    use crate::tree::Hash;

    fn hash(byte: u8) -> Hash {
        Hash::from_bytes([byte; 32])
    }

    fn shard_buffer_is_empty(
        branch: &crate::branch::BranchHandle,
        shard_id: ShardId,
    ) -> Result<bool, BranchError> {
        let Some(buffer) = branch.shard_buffer(shard_id) else {
            return Err(BranchError::UnknownShard { shard_id });
        };

        match buffer.lock() {
            Ok(guard) => Ok(guard.is_empty()),
            Err(poisoned) => {
                drop(poisoned.into_inner());
                Err(BranchError::BufferPoisoned { shard_id })
            }
        }
    }

    #[test]
    fn fork_records_current_root_as_fork_point_and_current_root() {
        let root = hash(1);

        let branch = fork(root);

        assert_eq!(branch.fork_point(), root);
        assert_eq!(branch.current_root(), root);
    }

    #[test]
    fn fork_allocates_one_empty_buffer_for_single_shard() -> Result<(), BranchError> {
        let branch = fork(hash(2));

        assert_eq!(branch.shard_count(), 1);
        assert!(shard_buffer_is_empty(&branch, branch.primary_shard())?);
        Ok(())
    }

    #[test]
    fn fork_shards_allocates_one_empty_buffer_per_shard() -> Result<(), BranchError> {
        let branch = fork_shards([(3, hash(3)), (5, hash(5)), (8, hash(8))])?;

        assert_eq!(branch.shard_count(), 3);
        for shard_id in [3, 5, 8] {
            assert!(shard_buffer_is_empty(&branch, shard_id)?);
        }
        Ok(())
    }

    #[test]
    fn fork_shards_preserves_each_independent_root() -> Result<(), BranchError> {
        let shard_three_root = hash(3);
        let shard_five_root = hash(5);

        let branch = fork_shards([(3, shard_three_root), (5, shard_five_root)])?;

        assert_eq!(branch.shard_fork_point(3), Some(shard_three_root));
        assert_eq!(branch.shard_current_root(3), Some(shard_three_root));
        assert_eq!(branch.shard_fork_point(5), Some(shard_five_root));
        assert_eq!(branch.shard_current_root(5), Some(shard_five_root));
        Ok(())
    }

    #[test]
    fn fork_single_shard_has_no_tree_store_to_read_or_copy() {
        let roots_for_large_and_small_trees_are_both_just_hashes = [hash(9), hash(10)];

        let first = fork(roots_for_large_and_small_trees_are_both_just_hashes[0]);
        let second = fork(roots_for_large_and_small_trees_are_both_just_hashes[1]);

        assert_eq!(first.shard_count(), 1);
        assert_eq!(second.shard_count(), 1);
    }

    #[test]
    fn fork_registered_registers_root_until_drop() {
        let registry = BranchRegistry::new();
        let root = hash(11);

        let branch = fork_registered(root, &registry);
        assert!(registry.live_roots().contains(&root));

        drop(branch);
        assert!(!registry.live_roots().contains(&root));
    }

    #[test]
    fn fork_shards_registered_registers_all_shard_roots() -> Result<(), BranchError> {
        let registry = BranchRegistry::new();
        let shard_three_root = hash(12);
        let shard_five_root = hash(13);

        let branch =
            fork_shards_registered([(3, shard_three_root), (5, shard_five_root)], &registry)?;
        let live = registry.live_roots();
        assert!(live.contains(&shard_three_root));
        assert!(live.contains(&shard_five_root));

        drop(branch);
        assert!(registry.live_roots().is_empty());
        Ok(())
    }
}
