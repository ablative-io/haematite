//! BRANCH-003: Read-only time-travel checkout.
//!
//! [`checkout`] opens a [`ReadOnlyView`] at a historical root hash (R3). The
//! view reads directly from the content-addressed node store via a [`Cursor`],
//! with no WAL replay (CN6), and supports `get` (R3) and `range` (R4). Writes
//! — `put`, `delete`, `commit` — are rejected with [`CheckoutError::ReadOnly`].
//!
//! CN6 is enforced structurally: a [`ReadOnlyView`] holds only a [`Cursor`] over
//! a raw [`NodeStore`], with no reference to a WAL buffer or database head.
//! Callers must pass the underlying durable node store directly — never a
//! WAL-overlay or write-buffer wrapper — or the view would observe uncommitted
//! state. (When CORE-005 lands a real WAL buffer, add an integration test here
//! asserting a post-checkout WAL write is invisible to the view.)

use std::fmt;

use crate::store::NodeStore;
use crate::tree::{Cursor, Hash, RangeIter, TreeError};

/// Errors raised by a read-only checkout view.
#[derive(Debug)]
pub enum CheckoutError {
    /// A write operation was attempted on a read-only view.
    ReadOnly,
    /// Traversing the historical tree failed.
    Tree(TreeError),
}

impl fmt::Display for CheckoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadOnly => write!(f, "checkout view is read-only"),
            Self::Tree(error) => write!(f, "checkout traversal error: {error}"),
        }
    }
}

impl std::error::Error for CheckoutError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Tree(error) => Some(error),
            Self::ReadOnly => None,
        }
    }
}

impl From<TreeError> for CheckoutError {
    fn from(error: TreeError) -> Self {
        Self::Tree(error)
    }
}

/// A read-only view of the tree at a specific historical root hash.
///
/// Construct one with [`checkout`]. Reads traverse the content-addressed node
/// store directly, so the view reflects exactly the state at `root_hash` and is
/// unaffected by any later writes. The view holds no write buffer; `put`,
/// `delete`, and `commit` always fail.
#[derive(Debug)]
pub struct ReadOnlyView<'a, S: NodeStore + ?Sized> {
    cursor: Cursor<'a, S>,
}

impl<S: NodeStore + ?Sized> ReadOnlyView<'_, S> {
    /// The root hash this view was checked out at.
    pub const fn root_hash(&self) -> Hash {
        self.cursor.root_hash()
    }

    /// Reads the value bound to `key` in the historical tree, or `None` if absent.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, CheckoutError> {
        self.cursor.get(key).map_err(CheckoutError::from)
    }

    /// Iterates the historical entries with keys in `[from, to)`, in sorted order.
    pub fn range(&self, from: &[u8], to: &[u8]) -> RangeIter<'_, S> {
        self.cursor.range(from, to)
    }

    /// Always fails: writes are not permitted on a read-only view.
    pub const fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), CheckoutError> {
        Err(CheckoutError::ReadOnly)
    }

    /// Always fails: deletes are not permitted on a read-only view.
    pub const fn delete(&self, _key: &[u8]) -> Result<(), CheckoutError> {
        Err(CheckoutError::ReadOnly)
    }

    /// Always fails: commits are not permitted on a read-only view.
    pub const fn commit(&self) -> Result<(), CheckoutError> {
        Err(CheckoutError::ReadOnly)
    }
}

/// Opens a read-only view at `root_hash`, reading directly from `store`.
///
/// No WAL replay occurs (CN6): the returned view traverses the content-addressed
/// node store at the given root hash.
///
/// This call does not validate that `root_hash` is present in `store`. If it is
/// absent, the view is still returned, but the first [`ReadOnlyView::get`] or
/// [`ReadOnlyView::range`] call reports
/// [`CheckoutError::Tree(TreeError::MissingNode)`](TreeError::MissingNode). A
/// missing root is therefore surfaced as an error, never as an empty tree.
pub const fn checkout<S: NodeStore + ?Sized>(store: &S, root_hash: Hash) -> ReadOnlyView<'_, S> {
    ReadOnlyView {
        cursor: Cursor::new(store, root_hash),
    }
}

#[cfg(test)]
mod tests {
    use super::{CheckoutError, checkout};
    use crate::store::MemoryStore;
    use crate::tree::{Hash, InternalNode, LeafNode, Node, TreeError, insert};

    type TestResult = Result<(), TreeError>;
    type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

    fn empty_root(store: &mut MemoryStore) -> Result<Hash, TreeError> {
        let leaf = LeafNode::new(Vec::new())?;
        Ok(store.put(&Node::Leaf(leaf)))
    }

    fn build_tree(store: &mut MemoryStore, entries: &[(&[u8], &[u8])]) -> Result<Hash, TreeError> {
        let mut root = empty_root(store)?;
        for (key, value) in entries {
            root = insert(store, root, key, value)?;
        }
        Ok(root)
    }

    fn collect_range(
        view: &super::ReadOnlyView<'_, MemoryStore>,
        from: &[u8],
        to: &[u8],
    ) -> Result<Pairs, TreeError> {
        view.range(from, to).collect()
    }

    #[test]
    fn checkout_get_reads_historical_state() -> Result<(), CheckoutError> {
        let mut store = MemoryStore::new();
        let root = build_tree(&mut store, &[(b"a", b"1"), (b"b", b"2")])?;

        let view = checkout(&store, root);
        assert_eq!(view.root_hash(), root);
        assert_eq!(view.get(b"a")?, Some(b"1".to_vec()));
        assert_eq!(view.get(b"b")?, Some(b"2".to_vec()));
        assert_eq!(view.get(b"missing")?, None);
        Ok(())
    }

    #[test]
    fn checkout_does_not_observe_writes_after_the_checkout_point() -> Result<(), CheckoutError> {
        let mut store = MemoryStore::new();
        let root = build_tree(&mut store, &[(b"a", b"1")])?;

        // Check out the historical root, then write more data to the store.
        let later = insert(&mut store, root, b"b", b"2")?;
        assert_ne!(later, root);

        let view = checkout(&store, root);
        // The view reflects only the state at `root`; `b` was added afterwards.
        assert_eq!(view.get(b"a")?, Some(b"1".to_vec()));
        assert_eq!(view.get(b"b")?, None);
        Ok(())
    }

    #[test]
    fn checkout_range_returns_sorted_historical_entries() -> TestResult {
        let mut store = MemoryStore::new();
        let root = build_tree(
            &mut store,
            &[(b"a", b"1"), (b"c", b"3"), (b"b", b"2"), (b"d", b"4")],
        )?;

        let view = checkout(&store, root);
        assert_eq!(
            collect_range(&view, b"a", b"d")?,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
            ]
        );
        Ok(())
    }

    #[test]
    fn checkout_range_excludes_entries_written_after_checkout() -> TestResult {
        let mut store = MemoryStore::new();
        let root = build_tree(&mut store, &[(b"a", b"1"), (b"b", b"2")])?;
        let _later = insert(&mut store, root, b"c", b"3")?;

        let view = checkout(&store, root);
        assert_eq!(
            collect_range(&view, b"a", b"z")?,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
            ]
        );
        Ok(())
    }

    /// Builds a two-leaf tree by hand so range traversal must cross a leaf
    /// boundary (the public `insert` path only splits after thousands of keys).
    fn two_leaf_tree(store: &mut MemoryStore) -> Result<Hash, TreeError> {
        let left = LeafNode::new(vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ])?;
        let right = LeafNode::new(vec![
            (b"m".to_vec(), b"3".to_vec()),
            (b"n".to_vec(), b"4".to_vec()),
        ])?;
        let left_hash = store.put(&Node::Leaf(left));
        let right_hash = store.put(&Node::Leaf(right));
        let root = InternalNode::new(vec![
            (b"a".to_vec(), left_hash),
            (b"m".to_vec(), right_hash),
        ])?;
        Ok(store.put(&Node::Internal(root)))
    }

    #[test]
    fn checkout_range_spans_multiple_leaves() -> TestResult {
        let mut store = MemoryStore::new();
        let root = two_leaf_tree(&mut store)?;

        let view = checkout(&store, root);
        // A range from the first leaf into the second must descend, exhaust the
        // left leaf, pop back to the internal node, and advance to the right.
        assert_eq!(
            collect_range(&view, b"a", b"z")?,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"m".to_vec(), b"3".to_vec()),
                (b"n".to_vec(), b"4".to_vec()),
            ]
        );
        Ok(())
    }

    #[test]
    fn checkout_with_missing_root_errors_on_read() {
        let store = MemoryStore::new();
        let absent = Hash::from_bytes([0xff; 32]);

        // The view is constructed, but the absent root surfaces as an error on
        // the first read — never as an empty tree.
        let view = checkout(&store, absent);
        assert!(matches!(
            view.get(b"any"),
            Err(CheckoutError::Tree(TreeError::MissingNode { .. }))
        ));
        // `range` shares the same node loader, so it too errors rather than
        // yielding an empty iterator over a missing root.
        assert!(matches!(
            view.range(b"a", b"z").next(),
            Some(Err(TreeError::MissingNode { .. }))
        ));
    }

    #[test]
    fn checkout_rejects_writes() -> TestResult {
        let mut store = MemoryStore::new();
        let root = build_tree(&mut store, &[(b"a", b"1")])?;

        let view = checkout(&store, root);
        assert!(matches!(view.put(b"x", b"y"), Err(CheckoutError::ReadOnly)));
        assert!(matches!(view.delete(b"a"), Err(CheckoutError::ReadOnly)));
        assert!(matches!(view.commit(), Err(CheckoutError::ReadOnly)));
        Ok(())
    }
}
