use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::tree::Hash;

/// Thread-safe registry of roots referenced by live branch handles.
///
/// The public view is a set of live root hashes for the pruner. Internally the
/// registry keeps reference counts so two live branches at the same root do not
/// let one dropped handle deregister a root still used by the other branch.
#[derive(Clone, Debug, Default)]
pub struct BranchRegistry {
    counts: Arc<Mutex<HashMap<Hash, usize>>>,
}

impl BranchRegistry {
    /// Create an empty active-branch registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one active branch root.
    pub fn register(&self, root: Hash) {
        self.lock_counts()
            .entry(root)
            .and_modify(|count| *count = count.saturating_add(1))
            .or_insert(1);
    }

    /// Deregister one active branch root reference.
    ///
    /// Missing roots are ignored so drop paths remain idempotent from the
    /// registry's perspective.
    pub fn deregister(&self, root: Hash) {
        let mut counts = self.lock_counts();
        if let Some(count) = counts.get_mut(&root) {
            if *count > 1 {
                *count -= 1;
            } else {
                counts.remove(&root);
            }
        }
    }

    /// Return every root currently referenced by at least one live branch.
    #[must_use]
    pub fn live_roots(&self) -> HashSet<Hash> {
        self.lock_counts().keys().copied().collect()
    }

    pub(crate) fn register_roots<I>(&self, roots: I) -> BranchRegistryGuard
    where
        I: IntoIterator<Item = Hash>,
    {
        let roots: Vec<Hash> = roots.into_iter().collect();
        for root in roots.iter().copied() {
            self.register(root);
        }

        BranchRegistryGuard {
            registry: self.clone(),
            roots,
        }
    }

    fn lock_counts(&self) -> MutexGuard<'_, HashMap<Hash, usize>> {
        match self.counts.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

/// Drop guard that keeps registry roots live until the last handle clone is gone.
#[derive(Debug)]
pub(crate) struct BranchRegistryGuard {
    registry: BranchRegistry,
    roots: Vec<Hash>,
}

impl Drop for BranchRegistryGuard {
    fn drop(&mut self) {
        for root in self.roots.iter().copied() {
            self.registry.deregister(root);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BranchRegistry;
    use crate::branch::{fork_registered, fork_shards_registered};
    use crate::tree::Hash;

    fn hash(byte: u8) -> Hash {
        Hash::from_bytes([byte; 32])
    }

    #[test]
    fn new_registry_has_no_live_roots() {
        let registry = BranchRegistry::new();

        assert!(registry.live_roots().is_empty());
    }

    #[test]
    fn registered_fork_root_is_live_until_handle_drops() {
        let registry = BranchRegistry::new();
        let root = hash(1);

        {
            let branch = fork_registered(root, &registry);
            assert_eq!(branch.fork_point(), root);
            assert!(registry.live_roots().contains(&root));
        }

        assert!(!registry.live_roots().contains(&root));
    }

    #[test]
    fn cloned_handle_keeps_registered_root_live() {
        let registry = BranchRegistry::new();
        let root = hash(2);
        let branch = fork_registered(root, &registry);
        let clone = branch.clone();

        drop(branch);
        assert!(registry.live_roots().contains(&root));

        drop(clone);
        assert!(!registry.live_roots().contains(&root));
    }

    #[test]
    fn duplicate_roots_are_reference_counted() {
        let registry = BranchRegistry::new();
        let root = hash(3);
        let first = fork_registered(root, &registry);
        let second = fork_registered(root, &registry);

        drop(first);
        assert!(registry.live_roots().contains(&root));

        drop(second);
        assert!(!registry.live_roots().contains(&root));
    }

    #[test]
    fn registered_multi_shard_fork_tracks_every_shard_root()
    -> Result<(), crate::branch::BranchError> {
        let registry = BranchRegistry::new();
        let shard_three = hash(4);
        let shard_five = hash(5);

        {
            let branch = fork_shards_registered([(3, shard_three), (5, shard_five)], &registry)?;
            assert_eq!(branch.shard_count(), 2);
            let live = registry.live_roots();
            assert!(live.contains(&shard_three));
            assert!(live.contains(&shard_five));
        }

        assert!(registry.live_roots().is_empty());
        Ok(())
    }
}
