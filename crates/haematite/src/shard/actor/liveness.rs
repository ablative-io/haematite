//! PERF-001: first-match liveness probes for shard range scans.
//!
//! The public range command still materialises every visible entry and returns a
//! `Done` sentinel. This helper is for internal callers that only need to know
//! whether at least one live entry exists in `[from, to)`: it merges the committed
//! tree iterator with the sorted WAL buffer, applies the same visibility filter as
//! `range`, and stops at the first live candidate.

use std::cmp::Ordering;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;

use crate::store::NodeStore;
use crate::tree::{Cursor, Hash, RangeIter};
use crate::ttl::filter::{Visibility, visible_value};
use crate::wal::{Mutation, WalBuffer, WalError};

use super::handle::{ShardCommandKind, ShardError, ShardHandle};

type TreeEntry = (Vec<u8>, Vec<u8>);

impl ShardHandle {
    /// Return whether `[from, to)` contains at least one live merged entry.
    ///
    /// This is an internal optimisation for liveness checks: unlike [`Self::range`]
    /// it does not materialise the full range or emit a `Done` sentinel.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::range`].
    pub(crate) fn has_live_in_range(
        &self,
        from: Vec<u8>,
        to: Vec<u8>,
        timeout: Duration,
    ) -> Result<bool, ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::HasLiveInRange(from, to, reply))?;
        recv_bool(&response, self.pid(), timeout)
    }
}

fn recv_bool(
    response: &mpsc::Receiver<Result<bool, ShardError>>,
    pid: u64,
    timeout: Duration,
) -> Result<bool, ShardError> {
    response
        .recv_timeout(timeout)
        .map_err(|error| match error {
            RecvTimeoutError::Timeout => ShardError::ReplyTimeout { pid },
            RecvTimeoutError::Disconnected => ShardError::ReplyDisconnected { pid },
        })?
}

/// Return whether the merged tree+buffer view of `[from, to)` contains a live
/// value, stopping immediately at the first visible entry.
pub(super) fn has_live_in_range<S>(
    store: &S,
    committed_root: Option<Hash>,
    buffer: &WalBuffer,
    from: &[u8],
    to: &[u8],
) -> Result<bool, ShardError>
where
    S: NodeStore + ?Sized,
{
    if to <= from {
        return Ok(false);
    }

    let cursor = committed_root.map(|root| Cursor::new(store, root));
    let mut tree_iter = cursor.as_ref().map(|cursor| cursor.range(from, to));
    let mut tree_entry = next_tree_entry(&mut tree_iter)?;
    let mut buffer_entries = buffer
        .iter()
        .filter(|mutation| in_range(mutation, from, to))
        .peekable();

    loop {
        let source = next_source(tree_entry.as_ref(), buffer_entries.peek().copied());
        match source {
            Some(NextSource::Tree) => {
                if tree_entry_is_live(tree_entry.as_ref())? {
                    return Ok(true);
                }
                tree_entry = next_tree_entry(&mut tree_iter)?;
            }
            Some(NextSource::Buffer) => {
                if peeked_mutation_is_live(buffer_entries.peek().copied())? {
                    return Ok(true);
                }
                let _ = buffer_entries.next();
            }
            Some(NextSource::Both) => {
                if peeked_mutation_is_live(buffer_entries.peek().copied())? {
                    return Ok(true);
                }
                tree_entry = next_tree_entry(&mut tree_iter)?;
                let _ = buffer_entries.next();
            }
            None => return Ok(false),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NextSource {
    Tree,
    Buffer,
    /// Tree and buffer have the same key; the buffer shadows the tree.
    Both,
}

fn next_source(
    tree_entry: Option<&TreeEntry>,
    buffer_mutation: Option<&Mutation>,
) -> Option<NextSource> {
    match (tree_entry, buffer_mutation) {
        (Some((tree_key, _)), Some(mutation)) => match tree_key.as_slice().cmp(mutation.key()) {
            Ordering::Less => Some(NextSource::Tree),
            Ordering::Equal => Some(NextSource::Both),
            Ordering::Greater => Some(NextSource::Buffer),
        },
        (Some(_), None) => Some(NextSource::Tree),
        (None, Some(_)) => Some(NextSource::Buffer),
        (None, None) => None,
    }
}

fn next_tree_entry<S>(
    tree_iter: &mut Option<RangeIter<'_, S>>,
) -> Result<Option<TreeEntry>, ShardError>
where
    S: NodeStore + ?Sized,
{
    tree_iter.as_mut().map_or(Ok(None), |iter| {
        iter.next().transpose().map_err(ShardError::from)
    })
}

fn tree_entry_is_live(tree_entry: Option<&TreeEntry>) -> Result<bool, ShardError> {
    match tree_entry {
        Some((_, value)) => value_is_live(value),
        None => Ok(false),
    }
}

fn peeked_mutation_is_live(mutation: Option<&Mutation>) -> Result<bool, ShardError> {
    match mutation {
        Some(Mutation::Put { value, .. }) => value_is_live(value),
        Some(Mutation::Delete { .. }) | None => Ok(false),
    }
}

fn value_is_live(value: &[u8]) -> Result<bool, ShardError> {
    match visible_value(value)
        .map_err(|error| ShardError::Wal(WalError::TreeError(error.to_string())))?
    {
        Visibility::Live(_) => Ok(true),
        Visibility::Expired => Ok(false),
    }
}

fn in_range(mutation: &Mutation, from: &[u8], to: &[u8]) -> bool {
    let key = mutation.key();
    from <= key && key < to
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::store::MemoryStore;
    use crate::tree::{Hash, LeafNode, Node};
    use crate::ttl::entry::{TtlEntry, encode_stamped_tombstone};
    use crate::wal::WalBuffer;

    use super::has_live_in_range;

    fn leaf_root(
        store: &mut MemoryStore,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Hash, Box<dyn Error>> {
        let leaf = LeafNode::new(entries)?;
        Ok(store.put(&Node::Leaf(leaf)))
    }

    fn malformed_stamped_value() -> Vec<u8> {
        b"HMSTMP01".to_vec()
    }

    #[test]
    fn stops_before_later_malformed_tree_value_after_first_live() -> Result<(), Box<dyn Error>> {
        let mut store = MemoryStore::new();
        let root = leaf_root(
            &mut store,
            vec![
                (b"a".to_vec(), b"live".to_vec()),
                (b"b".to_vec(), malformed_stamped_value()),
            ],
        )?;
        let buffer = WalBuffer::new();

        assert!(has_live_in_range(&store, Some(root), &buffer, b"a", b"z")?);
        Ok(())
    }

    #[test]
    fn buffered_put_shadows_malformed_tree_value() -> Result<(), Box<dyn Error>> {
        let mut store = MemoryStore::new();
        let root = leaf_root(&mut store, vec![(b"a".to_vec(), malformed_stamped_value())])?;
        let mut buffer = WalBuffer::new();
        buffer.put(b"a", b"buffer-live");

        assert!(has_live_in_range(&store, Some(root), &buffer, b"a", b"b")?);
        Ok(())
    }

    #[test]
    fn buffered_delete_shadows_committed_live_value() -> Result<(), Box<dyn Error>> {
        let mut store = MemoryStore::new();
        let root = leaf_root(&mut store, vec![(b"a".to_vec(), b"committed".to_vec())])?;
        let mut buffer = WalBuffer::new();
        buffer.delete(b"a");

        assert!(!has_live_in_range(&store, Some(root), &buffer, b"a", b"b")?);
        Ok(())
    }

    #[test]
    fn expired_values_and_tombstones_are_not_live() -> Result<(), Box<dyn Error>> {
        let mut store = MemoryStore::new();
        let tombstone = encode_stamped_tombstone(crate::sync::ballot::Stamp::bottom());
        let expired = TtlEntry::expiring(b"stale".to_vec(), 0).encode();
        let root = leaf_root(
            &mut store,
            vec![(b"a".to_vec(), expired), (b"b".to_vec(), tombstone)],
        )?;
        let buffer = WalBuffer::new();

        assert!(!has_live_in_range(&store, Some(root), &buffer, b"a", b"z")?);
        Ok(())
    }
}
