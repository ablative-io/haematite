//! Sync-time three-way merge for per-shard root reconciliation.
//!
//! Sync transfers missing content-addressed nodes before this module runs. Once a
//! target shard has the target, source, and common-base roots locally available,
//! [`merge_synced_roots`] reuses the branch merge engine to perform a structural
//! three-way merge. The target root is treated as the merge parent and the source
//! root as the branch, so clean source-only changes are applied to the target and
//! true divergent per-key writes are routed through the configured branch conflict
//! policy.

use std::fmt;

use crate::branch::ShardId;
use crate::branch::conflict::ConflictPolicy;
use crate::branch::merge::{MergeConflict, MergeError, merge_with_report};
use crate::store::NodeStore;
use crate::tree::Hash;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncMergeError {
    MissingNode { hash: Hash },
    StoreRead { hash: Hash },
    InvalidNode,
    UnresolvedConflict { key: Vec<u8> },
    Unimplemented { feature: &'static str },
}

impl fmt::Display for SyncMergeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNode { hash } => write!(formatter, "missing tree node {hash}"),
            Self::StoreRead { hash } => write!(formatter, "failed to read tree node {hash}"),
            Self::InvalidNode => formatter.write_str("invalid tree node"),
            Self::UnresolvedConflict { key } => write!(
                formatter,
                "conflict on key {} is unresolved",
                String::from_utf8_lossy(key)
            ),
            Self::Unimplemented { feature } => write!(formatter, "{feature} is not implemented"),
        }
    }
}

impl std::error::Error for SyncMergeError {}

impl From<MergeError> for SyncMergeError {
    fn from(error: MergeError) -> Self {
        match error {
            MergeError::MissingNode { hash } => Self::MissingNode { hash },
            MergeError::StoreRead { hash } => Self::StoreRead { hash },
            MergeError::InvalidNode => Self::InvalidNode,
            MergeError::UnresolvedConflict { key } => Self::UnresolvedConflict { key },
            MergeError::Unimplemented { feature } => Self::Unimplemented { feature },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncMergeRoots {
    pub target_root: Hash,
    pub source_root: Hash,
    pub base_root: Hash,
}

impl SyncMergeRoots {
    pub const fn new(target_root: Hash, source_root: Hash, base_root: Hash) -> Self {
        Self {
            target_root,
            source_root,
            base_root,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncMergeResult {
    pub shard_id: ShardId,
    pub merged_root: Hash,
    pub divergences: Vec<MergeConflict>,
}

impl SyncMergeResult {
    pub const fn divergence_count(&self) -> usize {
        self.divergences.len()
    }

    pub const fn has_divergences(&self) -> bool {
        !self.divergences.is_empty()
    }
}

pub fn merge_synced_roots<S: NodeStore + ?Sized>(
    store: &mut S,
    shard_id: ShardId,
    roots: SyncMergeRoots,
    policy: &ConflictPolicy,
) -> Result<SyncMergeResult, SyncMergeError> {
    let report = merge_with_report(
        store,
        roots.target_root,
        roots.source_root,
        roots.base_root,
        policy,
    )?;

    Ok(SyncMergeResult {
        shard_id,
        merged_root: report.merged_root,
        divergences: report.conflicts,
    })
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::branch::conflict::ConflictPolicy;
    use crate::store::MemoryStore;
    use crate::tree::{Cursor, Hash, LeafNode, Node, batch_mutate};

    use super::{SyncMergeError, SyncMergeRoots, merge_synced_roots};

    static CUSTOM_CALLS: AtomicUsize = AtomicUsize::new(0);

    fn custom_counting_resolution(
        key: &[u8],
        ancestor_value: Option<&[u8]>,
        parent_value: Option<&[u8]>,
        branch_value: Option<&[u8]>,
    ) -> Option<Vec<u8>> {
        CUSTOM_CALLS.fetch_add(1, Ordering::SeqCst);
        if key.is_empty()
            && ancestor_value.is_none()
            && parent_value.is_none()
            && branch_value.is_none()
        {
            None
        } else {
            Some(b"custom".to_vec())
        }
    }

    fn custom_argument_resolution(
        key: &[u8],
        ancestor_value: Option<&[u8]>,
        target_value: Option<&[u8]>,
        source_value: Option<&[u8]>,
    ) -> Option<Vec<u8>> {
        if key.is_empty() {
            return None;
        }

        let mut resolved = Vec::new();
        resolved.extend_from_slice(key);
        resolved.push(b'|');
        resolved.extend_from_slice(ancestor_value.unwrap_or(b"none"));
        resolved.push(b'|');
        resolved.extend_from_slice(target_value.unwrap_or(b"none"));
        resolved.push(b'|');
        resolved.extend_from_slice(source_value.unwrap_or(b"none"));
        Some(resolved)
    }

    fn custom_delete_resolution(
        key: &[u8],
        ancestor_value: Option<&[u8]>,
        target_value: Option<&[u8]>,
        source_value: Option<&[u8]>,
    ) -> Option<Vec<u8>> {
        if key == b"delete-me"
            || (ancestor_value.is_none() && target_value.is_none() && source_value.is_none())
        {
            None
        } else {
            Some(b"kept".to_vec())
        }
    }

    fn empty_root(store: &mut MemoryStore) -> Result<Hash, Box<dyn Error>> {
        let leaf = Node::Leaf(LeafNode::new(Vec::new())?);
        Ok(store.put(&leaf))
    }

    fn build_root(
        store: &mut MemoryStore,
        mutations: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<Hash, Box<dyn Error>> {
        let root = empty_root(store)?;
        Ok(batch_mutate(store, root, mutations)?)
    }

    fn value(
        store: &MemoryStore,
        root: Hash,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
        Ok(Cursor::new(store, root).get(key)?)
    }

    fn put_mutation(key: &[u8], value: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
        (key.to_vec(), Some(value.to_vec()))
    }

    fn delete_mutation(key: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
        (key.to_vec(), None)
    }

    #[test]
    fn divergent_writes_to_same_key_are_detected_and_lww_uses_source_value()
    -> Result<(), Box<dyn Error>> {
        let mut store = MemoryStore::new();
        let base = build_root(&mut store, &[put_mutation(b"k", b"base")])?;
        let target = batch_mutate(&mut store, base, &[put_mutation(b"k", b"target")])?;
        let source = batch_mutate(&mut store, base, &[put_mutation(b"k", b"source")])?;

        let result = merge_synced_roots(
            &mut store,
            5,
            SyncMergeRoots::new(target, source, base),
            &ConflictPolicy::Lww,
        )?;

        assert_eq!(result.shard_id, 5);
        assert_eq!(result.divergence_count(), 1);
        assert!(result.has_divergences());
        assert_eq!(result.divergences[0].key, b"k".to_vec());
        assert_eq!(result.divergences[0].ancestor_value, Some(b"base".to_vec()));
        assert_eq!(result.divergences[0].parent_value, Some(b"target".to_vec()));
        assert_eq!(result.divergences[0].branch_value, Some(b"source".to_vec()));
        assert_eq!(
            result.divergences[0].resolved_value,
            Some(b"source".to_vec())
        );
        assert_eq!(
            value(&store, result.merged_root, b"k")?,
            Some(b"source".to_vec())
        );
        Ok(())
    }

    #[test]
    fn received_source_only_writes_do_not_trigger_divergence_or_policy()
    -> Result<(), Box<dyn Error>> {
        CUSTOM_CALLS.store(0, Ordering::SeqCst);
        let mut store = MemoryStore::new();
        let base = build_root(&mut store, &[put_mutation(b"k", b"base")])?;
        let target = base;
        let source = batch_mutate(&mut store, base, &[put_mutation(b"k", b"source")])?;

        let result = merge_synced_roots(
            &mut store,
            0,
            SyncMergeRoots::new(target, source, base),
            &ConflictPolicy::Custom(custom_counting_resolution),
        )?;

        assert_eq!(result.divergence_count(), 0);
        assert_eq!(CUSTOM_CALLS.load(Ordering::SeqCst), 0);
        assert_eq!(
            value(&store, result.merged_root, b"k")?,
            Some(b"source".to_vec())
        );
        Ok(())
    }

    #[test]
    fn target_only_writes_do_not_trigger_divergence_or_policy() -> Result<(), Box<dyn Error>> {
        CUSTOM_CALLS.store(0, Ordering::SeqCst);
        let mut store = MemoryStore::new();
        let base = build_root(&mut store, &[put_mutation(b"k", b"base")])?;
        let target = batch_mutate(&mut store, base, &[put_mutation(b"k", b"target")])?;
        let source = base;

        let result = merge_synced_roots(
            &mut store,
            0,
            SyncMergeRoots::new(target, source, base),
            &ConflictPolicy::Custom(custom_counting_resolution),
        )?;

        assert_eq!(result.divergence_count(), 0);
        assert_eq!(CUSTOM_CALLS.load(Ordering::SeqCst), 0);
        assert_eq!(result.merged_root, target);
        assert_eq!(
            value(&store, result.merged_root, b"k")?,
            Some(b"target".to_vec())
        );
        Ok(())
    }

    #[test]
    fn divergence_detection_is_per_key_with_clean_keys_propagated() -> Result<(), Box<dyn Error>> {
        let mut store = MemoryStore::new();
        let base = build_root(
            &mut store,
            &[
                put_mutation(b"conflict", b"base"),
                put_mutation(b"target-only", b"base"),
            ],
        )?;
        let target = batch_mutate(
            &mut store,
            base,
            &[
                put_mutation(b"conflict", b"target"),
                put_mutation(b"target-only", b"target"),
            ],
        )?;
        let source = batch_mutate(
            &mut store,
            base,
            &[
                put_mutation(b"conflict", b"source"),
                put_mutation(b"source-only", b"source"),
            ],
        )?;

        let result = merge_synced_roots(
            &mut store,
            2,
            SyncMergeRoots::new(target, source, base),
            &ConflictPolicy::Lww,
        )?;

        assert_eq!(result.divergence_count(), 1);
        assert_eq!(result.divergences[0].key, b"conflict".to_vec());
        assert_eq!(
            value(&store, result.merged_root, b"conflict")?,
            Some(b"source".to_vec())
        );
        assert_eq!(
            value(&store, result.merged_root, b"target-only")?,
            Some(b"target".to_vec())
        );
        assert_eq!(
            value(&store, result.merged_root, b"source-only")?,
            Some(b"source".to_vec())
        );
        Ok(())
    }

    #[test]
    fn custom_policy_receives_base_target_and_source_values() -> Result<(), Box<dyn Error>> {
        let mut store = MemoryStore::new();
        let base = build_root(&mut store, &[put_mutation(b"k", b"base")])?;
        let target = batch_mutate(&mut store, base, &[delete_mutation(b"k")])?;
        let source = batch_mutate(&mut store, base, &[put_mutation(b"k", b"source")])?;

        let result = merge_synced_roots(
            &mut store,
            0,
            SyncMergeRoots::new(target, source, base),
            &ConflictPolicy::Custom(custom_argument_resolution),
        )?;

        assert_eq!(result.divergence_count(), 1);
        assert_eq!(
            value(&store, result.merged_root, b"k")?,
            Some(b"k|base|none|source".to_vec())
        );
        assert_eq!(
            result.divergences[0].resolved_value,
            Some(b"k|base|none|source".to_vec())
        );
        Ok(())
    }

    #[test]
    fn custom_policy_returning_none_deletes_target_key() -> Result<(), Box<dyn Error>> {
        let mut store = MemoryStore::new();
        let base = build_root(&mut store, &[put_mutation(b"delete-me", b"base")])?;
        let target = batch_mutate(&mut store, base, &[put_mutation(b"delete-me", b"target")])?;
        let source = batch_mutate(&mut store, base, &[put_mutation(b"delete-me", b"source")])?;

        let result = merge_synced_roots(
            &mut store,
            0,
            SyncMergeRoots::new(target, source, base),
            &ConflictPolicy::Custom(custom_delete_resolution),
        )?;

        assert_eq!(result.divergence_count(), 1);
        assert_eq!(result.divergences[0].resolved_value, None);
        assert_eq!(value(&store, result.merged_root, b"delete-me")?, None);
        Ok(())
    }

    #[test]
    fn vector_clock_conflict_is_surfaced_without_fallback() -> Result<(), Box<dyn Error>> {
        let mut store = MemoryStore::new();
        let base = build_root(&mut store, &[put_mutation(b"k", b"base")])?;
        let target = batch_mutate(&mut store, base, &[put_mutation(b"k", b"target")])?;
        let source = batch_mutate(&mut store, base, &[put_mutation(b"k", b"source")])?;

        let result = merge_synced_roots(
            &mut store,
            0,
            SyncMergeRoots::new(target, source, base),
            &ConflictPolicy::VectorClock,
        );

        assert_eq!(
            result,
            Err(SyncMergeError::Unimplemented {
                feature: "vector-clock conflict resolution"
            })
        );
        Ok(())
    }
}
