// CORE-005: In-memory WAL buffer with sorted mutation log

use std::collections::BTreeMap;
use std::collections::btree_map::Values;
use std::fmt;
use std::io;

use crate::store::NodeStore;
use crate::tree::{Hash, batch_mutate};

/// A single buffered write against the store.
///
/// The buffer records only the intent — a value to store (`Put`) or a key to
/// remove (`Delete`). Sequence numbers, versions, and timestamps belong to the
/// shard actor layer (CORE-007), not here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Mutation {
    /// The key this mutation applies to, regardless of variant.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }
}

/// Outcome of a buffer lookup that shadows the tree (CN6).
///
/// A `Put` shadows any tree value, a `Delete` shadows it with absence, and
/// `NotBuffered` tells the caller it must consult the tree itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LookupResult {
    BufferedValue(Vec<u8>),
    BufferedDelete,
    NotBuffered,
}

pub(crate) const RECOVERY_UNKNOWN_TAG_EXPECTED: u32 = 0xffff_fffe;
pub(crate) const RECOVERY_MALFORMED_PAYLOAD_EXPECTED: u32 = 0xffff_fffd;

/// Errors raised by the WAL buffer and the durable WAL writer.
#[derive(Debug)]
pub enum WalError {
    /// File I/O failure from the durable writer.
    Io(io::Error),
    /// A CRC32 frame checksum did not match on read (used by recovery).
    ChecksumMismatch { expected: u32, actual: u32 },
    /// The prolly tree rejected the batch flush during `commit`.
    TreeError(String),
    /// A WAL frame or entry used an unknown tag byte.
    InvalidTag { found: u8 },
    /// Encoded bytes ended before the declared field or frame was complete.
    Truncated,
    /// Encoded bytes remained after a complete field, frame, or entry.
    TrailingBytes { trailing: usize },
    /// An encoded length cannot fit on this platform or overflowed an offset.
    LengthOverflow,
    /// A batched fsync policy must use a non-zero interval.
    InvalidFsyncPolicy { interval: usize },
}

impl fmt::Display for WalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "wal i/o error: {error}"),
            Self::ChecksumMismatch {
                expected: RECOVERY_UNKNOWN_TAG_EXPECTED,
                actual,
            } => write!(formatter, "wal corruption: unknown tag {actual:#04x}"),
            Self::ChecksumMismatch {
                expected: RECOVERY_MALFORMED_PAYLOAD_EXPECTED,
                ..
            } => write!(formatter, "wal corruption: malformed payload fields"),
            Self::ChecksumMismatch { expected, actual } => write!(
                formatter,
                "wal checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
            ),
            Self::TreeError(message) => write!(formatter, "wal tree error: {message}"),
            Self::InvalidTag { found } => write!(formatter, "invalid wal tag: {found:#04x}"),
            Self::Truncated => write!(formatter, "wal bytes ended before the frame was complete"),
            Self::TrailingBytes { trailing } => {
                write!(formatter, "wal bytes contain {trailing} trailing bytes")
            }
            Self::LengthOverflow => {
                write!(formatter, "encoded wal length cannot fit on this platform")
            }
            Self::InvalidFsyncPolicy { interval } => write!(
                formatter,
                "invalid wal fsync policy: batched interval must be greater than zero, got {interval}"
            ),
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::ChecksumMismatch { .. }
            | Self::TreeError(_)
            | Self::InvalidTag { .. }
            | Self::Truncated
            | Self::TrailingBytes { .. }
            | Self::LengthOverflow
            | Self::InvalidFsyncPolicy { .. } => None,
        }
    }
}

impl From<io::Error> for WalError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// In-memory, append-amortising WAL buffer (ADR-003).
///
/// Mutations accumulate in a `BTreeMap` keyed by key, so the latest write for a
/// key always wins and iteration is in ascending key order. A `commit` flushes
/// the whole buffer to the prolly tree as a single batch — exactly one
/// path-to-root rewrite per flush (CN8), not one per buffered write.
#[derive(Debug, Default)]
pub struct WalBuffer {
    mutations: BTreeMap<Vec<u8>, Mutation>,
}

impl WalBuffer {
    /// Create an empty buffer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Buffer a `Put`, overwriting any prior mutation for the same key.
    ///
    /// # Durability
    ///
    /// This is an in-memory operation only. The "durably append before it enters
    /// the buffer" invariant (R3/C11) is **not** enforced here — the type system
    /// cannot stop a caller from buffering without first writing the matching
    /// [`WalEntry`] via [`DurableWal::append`] (or using
    /// [`DurableWal::append_mutation`]). The caller is responsible for making
    /// that durable append *before* calling `put`. The unbypassable combined
    /// append-then-buffer cycle is introduced by the shard actor; until then
    /// this ordering is caller discipline.
    ///
    /// [`DurableWal::append`]: super::durable::DurableWal::append
    /// [`DurableWal::append_mutation`]: super::durable::DurableWal::append_mutation
    /// [`WalEntry`]: super::entry::WalEntry
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) {
        let key = key.as_ref().to_vec();
        self.mutations.insert(
            key.clone(),
            Mutation::Put {
                key,
                value: value.as_ref().to_vec(),
            },
        );
    }

    /// Buffer a `Delete`, overwriting any prior mutation for the same key.
    ///
    /// # Durability
    ///
    /// Like [`put`](Self::put), this is in-memory only. The caller must call
    /// [`DurableWal::append`] with the corresponding delete [`WalEntry`] (or
    /// [`DurableWal::append_mutation`]) *before* calling `delete`; the ordering
    /// is caller discipline until the shard actor introduces the combined
    /// append-then-buffer acknowledgement cycle.
    ///
    /// [`DurableWal::append`]: super::durable::DurableWal::append
    /// [`DurableWal::append_mutation`]: super::durable::DurableWal::append_mutation
    /// [`WalEntry`]: super::entry::WalEntry
    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref().to_vec();
        self.mutations.insert(key.clone(), Mutation::Delete { key });
    }

    /// Look up a key in the buffer without touching the tree (CN6).
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> LookupResult {
        match self.mutations.get(key.as_ref()) {
            Some(Mutation::Put { value, .. }) => LookupResult::BufferedValue(value.clone()),
            Some(Mutation::Delete { .. }) => LookupResult::BufferedDelete,
            None => LookupResult::NotBuffered,
        }
    }

    /// Number of distinct keys currently buffered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.mutations.len()
    }

    /// Whether the buffer holds no mutations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }

    /// Iterate buffered mutations in ascending key order.
    pub fn iter(&self) -> Values<'_, Vec<u8>, Mutation> {
        self.mutations.values()
    }

    /// Flush every buffered mutation to the prolly tree as a single batch and
    /// return the new root hash (C24, C25).
    ///
    /// On success the buffer is cleared. If the tree rejects the batch the
    /// buffer is left intact so the caller can retry (R5). An empty buffer is a
    /// no-op that returns `tree_root` unchanged.
    pub fn commit<S>(&mut self, tree_root: Hash, store: &mut S) -> Result<Hash, WalError>
    where
        S: NodeStore + ?Sized,
    {
        let batch: Vec<(Vec<u8>, Option<Vec<u8>>)> = self
            .mutations
            .values()
            .map(|mutation| match mutation {
                Mutation::Put { key, value } => (key.clone(), Some(value.clone())),
                Mutation::Delete { key } => (key.clone(), None),
            })
            .collect();

        let new_root = batch_mutate(store, tree_root, batch.as_slice())
            .map_err(|error| WalError::TreeError(error.to_string()))?;

        self.mutations.clear();
        Ok(new_root)
    }
}

impl<'a> IntoIterator for &'a WalBuffer {
    type Item = &'a Mutation;
    type IntoIter = Values<'a, Vec<u8>, Mutation>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::{LookupResult, Mutation, WalBuffer, WalError};
    use crate::store::NodeStore;
    use crate::tree::{Hash, LeafNode, Node, NodeError, batch_mutate};
    use std::cell::Cell;
    use std::convert::Infallible;

    /// A `NodeStore` that counts `put` calls so a test can prove `commit`
    /// performs exactly one batch flush rather than N individual mutations.
    #[derive(Debug, Default)]
    struct CountingStore {
        nodes: std::collections::HashMap<Hash, Vec<u8>>,
        puts: Cell<usize>,
    }

    impl NodeStore for CountingStore {
        type Error = Infallible;

        fn get(&self, hash: &Hash) -> Result<Option<Node>, Self::Error> {
            Ok(self
                .nodes
                .get(hash)
                .and_then(|bytes| Node::deserialise(bytes).ok()))
        }

        fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
            self.puts.set(self.puts.get() + 1);
            let hash = node.hash();
            self.nodes.insert(hash, node.serialise());
            Ok(hash)
        }
    }

    impl CountingStore {
        fn put_count(&self) -> usize {
            self.puts.get()
        }
    }

    /// Store a node, collapsing the `Infallible` error without `unwrap`.
    fn store_node(store: &mut CountingStore, node: &Node) -> Hash {
        match store.put(node) {
            Ok(hash) => hash,
            Err(infallible) => match infallible {},
        }
    }

    fn empty_root(store: &mut CountingStore) -> Result<Hash, NodeError> {
        let leaf = Node::Leaf(LeafNode::new(Vec::new())?);
        Ok(store_node(store, &leaf))
    }

    #[test]
    fn new_buffer_is_empty() {
        let buffer = WalBuffer::new();
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn put_overwrites_prior_mutation_for_same_key() {
        let mut buffer = WalBuffer::new();
        buffer.put(b"a", b"1");
        buffer.put(b"a", b"2");
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.get(b"a"), LookupResult::BufferedValue(b"2".to_vec()));
    }

    #[test]
    fn delete_overwrites_prior_put_for_same_key() {
        let mut buffer = WalBuffer::new();
        buffer.put(b"a", b"1");
        buffer.delete(b"a");
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.get(b"a"), LookupResult::BufferedDelete);
    }

    #[test]
    fn put_overwrites_prior_delete_for_same_key() {
        let mut buffer = WalBuffer::new();
        buffer.delete(b"a");
        buffer.put(b"a", b"v");
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.get(b"a"), LookupResult::BufferedValue(b"v".to_vec()));
    }

    #[test]
    fn get_shadows_tree_per_variant() {
        let mut buffer = WalBuffer::new();
        assert_eq!(buffer.get(b"key"), LookupResult::NotBuffered);
        buffer.put(b"key", b"val");
        assert_eq!(
            buffer.get(b"key"),
            LookupResult::BufferedValue(b"val".to_vec())
        );
        buffer.put(b"key", b"v2");
        assert_eq!(
            buffer.get(b"key"),
            LookupResult::BufferedValue(b"v2".to_vec())
        );
        buffer.delete(b"key");
        assert_eq!(buffer.get(b"key"), LookupResult::BufferedDelete);
    }

    #[test]
    fn iteration_is_ascending_key_order() {
        let mut buffer = WalBuffer::new();
        buffer.put(b"c", b"3");
        buffer.put(b"a", b"1");
        buffer.put(b"b", b"2");
        let keys: Vec<&[u8]> = buffer.iter().map(Mutation::key).collect();
        assert_eq!(
            keys,
            vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
        );
    }

    #[test]
    fn mutation_clone_equals_original() {
        let original = Mutation::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        assert_eq!(original.clone(), original);
    }

    #[test]
    fn commit_clears_buffer_and_returns_new_root() -> Result<(), NodeError> {
        let mut store = CountingStore::default();
        let root = empty_root(&mut store)?;
        let mut buffer = WalBuffer::new();
        for index in 0..50u32 {
            buffer.put(format!("key-{index:04}"), format!("value-{index}"));
        }
        let new_root = buffer
            .commit(root, &mut store)
            .map_err(|_| NodeError::Truncated)?;
        assert!(buffer.is_empty());
        assert_ne!(new_root, root);
        Ok(())
    }

    #[test]
    fn commit_triggers_exactly_one_batch_not_n_puts() -> Result<(), NodeError> {
        // Reference: a single batch_mutate of the same 50 keys on a fresh store.
        let mut reference = CountingStore::default();
        let ref_root = empty_root(&mut reference)?;
        let baseline = reference.put_count();
        let batch: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..50u32)
            .map(|index| {
                (
                    format!("key-{index:04}").into_bytes(),
                    Some(format!("value-{index}").into_bytes()),
                )
            })
            .collect();
        let expected_root = batch_mutate(&mut reference, ref_root, batch.as_slice())
            .map_err(|_| NodeError::Truncated)?;
        let batch_puts = reference.put_count() - baseline;

        // Subject: commit must produce the same root with the same node-put cost.
        let mut store = CountingStore::default();
        let root = empty_root(&mut store)?;
        let commit_baseline = store.put_count();
        let mut buffer = WalBuffer::new();
        for index in 0..50u32 {
            buffer.put(format!("key-{index:04}"), format!("value-{index}"));
        }
        let new_root = buffer
            .commit(root, &mut store)
            .map_err(|_| NodeError::Truncated)?;
        let commit_puts = store.put_count() - commit_baseline;

        assert_eq!(new_root, expected_root);
        assert_eq!(commit_puts, batch_puts);
        // One batch over 50 keys writes far fewer than 50 nodes — proves amortisation.
        assert!(commit_puts < 50);
        Ok(())
    }

    #[test]
    fn commit_on_empty_buffer_returns_root_unchanged() -> Result<(), NodeError> {
        let mut store = CountingStore::default();
        let root = empty_root(&mut store)?;
        let before = store.put_count();
        let mut buffer = WalBuffer::new();
        let result = buffer
            .commit(root, &mut store)
            .map_err(|_| NodeError::Truncated)?;
        assert_eq!(result, root);
        assert_eq!(store.put_count(), before);
        Ok(())
    }

    #[test]
    fn commit_failure_retains_buffer() {
        // A store whose `get` always returns absence makes the tree report a
        // missing root, forcing commit to fail mid-flush.
        #[derive(Debug)]
        struct MissingRootStore;

        #[derive(Debug)]
        struct NeverHappens;
        impl std::fmt::Display for NeverHappens {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "never happens")
            }
        }
        impl std::error::Error for NeverHappens {}

        impl NodeStore for MissingRootStore {
            type Error = NeverHappens;
            fn get(&self, hash: &Hash) -> Result<Option<Node>, Self::Error> {
                debug_assert_eq!(hash.as_bytes().len(), 32);
                Ok(None)
            }
            fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
                Ok(node.hash())
            }
        }

        let mut store = MissingRootStore;
        let root = Hash::from_bytes([0; 32]);
        let mut buffer = WalBuffer::new();
        for index in 0..50u32 {
            buffer.put(format!("key-{index:04}"), b"v");
        }
        let result = buffer.commit(root, &mut store);
        assert!(matches!(result, Err(WalError::TreeError(_))));
        assert_eq!(buffer.len(), 50);
    }

    #[test]
    fn wal_error_display_names_both_checksums() {
        let error = WalError::ChecksumMismatch {
            expected: 0xDEAD,
            actual: 0xBEEF,
        };
        let rendered = error.to_string();
        assert!(rendered.contains("dead"));
        assert!(rendered.contains("beef"));
    }
}
