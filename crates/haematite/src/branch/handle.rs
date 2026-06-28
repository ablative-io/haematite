use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::store::NodeStore;
use crate::tree::{Cursor, Hash, TreeError};
use crate::wal::{LookupResult, WalBuffer};

use super::registry::BranchRegistryGuard;

/// Shard identifier used by the branch handle.
///
/// Re-exported from the platform-neutral [`crate::ids`] module so the wasm sync
/// codec and the native branch layer share one identical type.
pub use crate::ids::ShardId;

/// The shard used by the single-shard fork helpers.
pub const DEFAULT_SHARD_ID: ShardId = 0;

/// Shared mutable WAL buffer owned by one branch shard.
///
/// Cloned [`BranchHandle`] values clone this reference, so they keep writing to
/// the same branch-local buffer rather than copying buffered mutations.
pub type BranchWalBuffer = Arc<Mutex<WalBuffer>>;

/// Errors raised by branch-handle construction and branch-local operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchError {
    /// A multi-shard branch was requested without any shard roots.
    NoShards,
    /// A shard id appeared more than once while constructing a branch.
    DuplicateShard { shard_id: ShardId },
    /// A branch operation targeted a shard not present in the handle.
    UnknownShard { shard_id: ShardId },
    /// A branch WAL buffer mutex was poisoned by a panicking holder.
    BufferPoisoned { shard_id: ShardId },
    /// Reading the shared content-addressed tree failed.
    Tree(TreeError),
}

impl fmt::Display for BranchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoShards => write!(formatter, "branch requires at least one shard"),
            Self::DuplicateShard { shard_id } => write!(
                formatter,
                "branch shard {shard_id} was provided more than once"
            ),
            Self::UnknownShard { shard_id } => {
                write!(formatter, "branch shard {shard_id} is not present")
            }
            Self::BufferPoisoned { shard_id } => {
                write!(formatter, "branch shard {shard_id} WAL buffer is poisoned")
            }
            Self::Tree(error) => write!(formatter, "branch tree read error: {error}"),
        }
    }
}

impl std::error::Error for BranchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Tree(error) => Some(error),
            Self::NoShards
            | Self::DuplicateShard { .. }
            | Self::UnknownShard { .. }
            | Self::BufferPoisoned { .. } => None,
        }
    }
}

impl From<TreeError> for BranchError {
    fn from(error: TreeError) -> Self {
        Self::Tree(error)
    }
}

/// Handle to a forked branch.
///
/// A handle stores only root hashes and branch-local WAL buffers. It never owns
/// or copies tree nodes; reads fall through to the caller-provided shared
/// content-addressed [`NodeStore`] at the branch's recorded root hash.
#[derive(Debug, Clone)]
pub struct BranchHandle {
    primary_shard: ShardId,
    primary: BranchShard,
    additional_shards: BTreeMap<ShardId, BranchShard>,
    registry_guard: Option<Arc<BranchRegistryGuard>>,
}

impl BranchHandle {
    /// Create a single-shard branch at `root`.
    ///
    /// The branch's current root is initially equal to its fork-point root and
    /// its WAL buffer is empty.
    #[must_use]
    pub fn new(root: Hash) -> Self {
        Self {
            primary_shard: DEFAULT_SHARD_ID,
            primary: BranchShard::new(root),
            additional_shards: BTreeMap::new(),
            registry_guard: None,
        }
    }

    /// Create a per-shard branch from committed shard roots.
    ///
    /// This records each shard root and allocates one distinct empty WAL buffer
    /// per shard. No tree store is accepted here, so construction cannot read or
    /// copy tree nodes.
    pub fn from_shard_roots<I>(roots: I) -> Result<Self, BranchError>
    where
        I: IntoIterator<Item = (ShardId, Hash)>,
    {
        let mut iter = roots.into_iter();
        let Some((primary_shard, primary_root)) = iter.next() else {
            return Err(BranchError::NoShards);
        };

        let mut additional_shards = BTreeMap::new();
        for (shard_id, root) in iter {
            if shard_id == primary_shard
                || additional_shards
                    .insert(shard_id, BranchShard::new(root))
                    .is_some()
            {
                return Err(BranchError::DuplicateShard { shard_id });
            }
        }

        Ok(Self {
            primary_shard,
            primary: BranchShard::new(primary_root),
            additional_shards,
            registry_guard: None,
        })
    }

    /// The fork-point root hash for the primary shard.
    #[must_use]
    pub const fn fork_point(&self) -> Hash {
        self.primary.fork_point
    }

    /// The current root hash for the primary shard.
    #[must_use]
    pub const fn current_root(&self) -> Hash {
        self.primary.current_root
    }

    /// The shard id used by [`fork_point`](Self::fork_point) and
    /// [`current_root`](Self::current_root).
    #[must_use]
    pub const fn primary_shard(&self) -> ShardId {
        self.primary_shard
    }

    /// Number of shards coordinated by this branch handle.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.additional_shards.len().saturating_add(1)
    }

    /// Return true when this handle contains `shard_id`.
    #[must_use]
    pub fn contains_shard(&self, shard_id: ShardId) -> bool {
        self.shard_state(shard_id).is_some()
    }

    /// Return the fork-point root hash for a specific shard.
    #[must_use]
    pub fn shard_fork_point(&self, shard_id: ShardId) -> Option<Hash> {
        self.shard_state(shard_id).map(|state| state.fork_point)
    }

    /// Return the current root hash for a specific shard.
    #[must_use]
    pub fn shard_current_root(&self, shard_id: ShardId) -> Option<Hash> {
        self.shard_state(shard_id).map(|state| state.current_root)
    }

    /// Return a reference to the branch-local WAL buffer for `shard_id`.
    #[must_use]
    pub fn shard_buffer(&self, shard_id: ShardId) -> Option<&BranchWalBuffer> {
        self.shard_state(shard_id).map(|state| &state.buffer)
    }

    /// Iterate the shard ids present in this handle.
    pub fn shard_ids(&self) -> impl Iterator<Item = ShardId> + '_ {
        std::iter::once(self.primary_shard).chain(self.additional_shards.keys().copied())
    }

    /// Buffer a branch-local put on `shard_id`.
    ///
    /// The mutation is appended only to the branch's shard WAL buffer. It is not
    /// propagated to any parent WAL buffer or tree.
    pub fn put<K, V>(&self, shard_id: ShardId, key: K, value: V) -> Result<(), BranchError>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut buffer = self.lock_buffer(shard_id)?;
        buffer.put(key, value);
        drop(buffer);
        Ok(())
    }

    /// Buffer a branch-local delete on `shard_id`.
    ///
    /// A buffered delete shadows any value stored in the shared tree at the
    /// branch root.
    pub fn delete<K>(&self, shard_id: ShardId, key: K) -> Result<(), BranchError>
    where
        K: AsRef<[u8]>,
    {
        let mut buffer = self.lock_buffer(shard_id)?;
        buffer.delete(key);
        drop(buffer);
        Ok(())
    }

    /// Read a key through the branch view for `shard_id`.
    ///
    /// The branch WAL buffer is checked first. Only keys that are not buffered
    /// fall through to the shared content-addressed tree at the branch's current
    /// shard root, so parent writes after the fork are not visible here.
    pub fn get<S>(
        &self,
        shard_id: ShardId,
        store: &S,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, BranchError>
    where
        S: NodeStore + ?Sized,
    {
        let lookup = {
            let buffer = self.lock_buffer(shard_id)?;
            buffer.get(key)
        };

        match lookup {
            LookupResult::BufferedValue(value) => Ok(Some(value)),
            LookupResult::BufferedDelete => Ok(None),
            LookupResult::NotBuffered => {
                let root = self
                    .shard_current_root(shard_id)
                    .ok_or(BranchError::UnknownShard { shard_id })?;
                Cursor::new(store, root).get(key).map_err(BranchError::from)
            }
        }
    }

    pub(crate) fn fork_points(&self) -> impl Iterator<Item = Hash> + '_ {
        std::iter::once(self.primary.fork_point).chain(
            self.additional_shards
                .values()
                .map(|state| state.fork_point),
        )
    }

    pub(crate) fn with_registry_guard(mut self, guard: BranchRegistryGuard) -> Self {
        self.registry_guard = Some(Arc::new(guard));
        self
    }

    fn shard_state(&self, shard_id: ShardId) -> Option<&BranchShard> {
        if shard_id == self.primary_shard {
            Some(&self.primary)
        } else {
            self.additional_shards.get(&shard_id)
        }
    }

    fn lock_buffer(&self, shard_id: ShardId) -> Result<MutexGuard<'_, WalBuffer>, BranchError> {
        let buffer = self
            .shard_buffer(shard_id)
            .ok_or(BranchError::UnknownShard { shard_id })?;
        match buffer.lock() {
            Ok(guard) => Ok(guard),
            Err(poisoned) => {
                drop(poisoned.into_inner());
                Err(BranchError::BufferPoisoned { shard_id })
            }
        }
    }
}

#[derive(Debug, Clone)]
struct BranchShard {
    fork_point: Hash,
    current_root: Hash,
    buffer: BranchWalBuffer,
}

impl BranchShard {
    fn new(root: Hash) -> Self {
        Self {
            fork_point: root,
            current_root: root,
            buffer: Arc::new(Mutex::new(WalBuffer::new())),
        }
    }
}

#[cfg(test)]
#[path = "handle_tests.rs"]
mod tests;
