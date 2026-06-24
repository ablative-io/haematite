use std::fmt;

pub const HASH_SIZE: usize = 32;

const LEAF_TAG: u8 = 0x00;
const INTERNAL_TAG: u8 = 0x01;
const U64_SIZE: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, std::hash::Hash)]
pub struct Hash([u8; HASH_SIZE]);

impl Hash {
    pub const fn from_bytes(bytes: [u8; HASH_SIZE]) -> Self {
        Self(bytes)
    }

    /// `blake3` digest of arbitrary bytes.
    ///
    /// Used by the active-active receiver apply (2a-4) to hash a key's current
    /// value for the CAS-precondition compare. This is a content hash of the
    /// *value bytes*, distinct from a node's structural [`LeafNode::hash`].
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    pub const fn as_bytes(&self) -> &[u8; HASH_SIZE] {
        &self.0
    }

    pub const fn into_bytes(self) -> [u8; HASH_SIZE] {
        self.0
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeError {
    UnsortedKeys,
    DuplicateKey,
    InvalidTag { found: u8 },
    Truncated,
    TrailingBytes { trailing: usize },
    LengthOverflow,
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsortedKeys => write!(f, "keys must be sorted"),
            Self::DuplicateKey => write!(f, "duplicate key"),
            Self::InvalidTag { found } => write!(f, "invalid node tag: {found:#04x}"),
            Self::Truncated => write!(f, "node bytes ended before the value was complete"),
            Self::TrailingBytes { trailing } => {
                write!(f, "node bytes contain {trailing} trailing bytes")
            }
            Self::LengthOverflow => write!(f, "encoded length cannot fit on this platform"),
        }
    }
}

impl std::error::Error for NodeError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafNode {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl LeafNode {
    pub fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<Self, NodeError> {
        validate_sorted_unique(&entries)?;
        Ok(Self { entries })
    }

    pub fn entries(&self) -> &[(Vec<u8>, Vec<u8>)] {
        &self.entries
    }

    pub fn serialise(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(LEAF_TAG);
        append_len(&mut bytes, self.entries.len());
        for (key, value) in &self.entries {
            append_len_prefixed_bytes(&mut bytes, key);
            append_len_prefixed_bytes(&mut bytes, value);
        }
        bytes
    }

    pub fn deserialise(bytes: &[u8]) -> Result<Self, NodeError> {
        let mut cursor = ByteCursor::new(bytes);
        cursor.read_expected_tag(LEAF_TAG)?;
        let entry_count = cursor.read_len()?;
        let mut entries = Vec::new();
        for _ in 0..entry_count {
            let key = cursor.read_len_prefixed_bytes()?;
            let value = cursor.read_len_prefixed_bytes()?;
            entries.push((key, value));
        }
        cursor.finish()?;
        Self::new(entries)
    }

    pub fn hash(&self) -> Hash {
        hash_serialised(&self.serialise())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalNode {
    children: Vec<(Vec<u8>, Hash)>,
}

impl InternalNode {
    pub fn new(children: Vec<(Vec<u8>, Hash)>) -> Result<Self, NodeError> {
        validate_sorted_unique(&children)?;
        Ok(Self { children })
    }

    pub fn children(&self) -> &[(Vec<u8>, Hash)] {
        &self.children
    }

    pub fn serialise(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(INTERNAL_TAG);
        append_len(&mut bytes, self.children.len());
        for (key, child_hash) in &self.children {
            append_len_prefixed_bytes(&mut bytes, key);
            bytes.extend_from_slice(child_hash.as_bytes());
        }
        bytes
    }

    pub fn deserialise(bytes: &[u8]) -> Result<Self, NodeError> {
        let mut cursor = ByteCursor::new(bytes);
        cursor.read_expected_tag(INTERNAL_TAG)?;
        let child_count = cursor.read_len()?;
        let mut children = Vec::new();
        for _ in 0..child_count {
            let key = cursor.read_len_prefixed_bytes()?;
            let child_hash = Hash::from_bytes(cursor.read_hash_bytes()?);
            children.push((key, child_hash));
        }
        cursor.finish()?;
        Self::new(children)
    }

    pub fn hash(&self) -> Hash {
        hash_serialised(&self.serialise())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    Leaf(LeafNode),
    Internal(InternalNode),
}

impl Node {
    pub fn serialise(&self) -> Vec<u8> {
        match self {
            Self::Leaf(leaf) => leaf.serialise(),
            Self::Internal(internal) => internal.serialise(),
        }
    }

    pub fn deserialise(bytes: &[u8]) -> Result<Self, NodeError> {
        match bytes.first().copied() {
            Some(LEAF_TAG) => LeafNode::deserialise(bytes).map(Self::Leaf),
            Some(INTERNAL_TAG) => InternalNode::deserialise(bytes).map(Self::Internal),
            Some(found) => Err(NodeError::InvalidTag { found }),
            None => Err(NodeError::Truncated),
        }
    }

    pub fn hash(&self) -> Hash {
        match self {
            Self::Leaf(leaf) => leaf.hash(),
            Self::Internal(internal) => internal.hash(),
        }
    }
}

fn validate_sorted_unique<T>(entries: &[(Vec<u8>, T)]) -> Result<(), NodeError> {
    let Some((first, rest)) = entries.split_first() else {
        return Ok(());
    };

    let mut previous_key = first.0.as_slice();
    for (key, _) in rest {
        match previous_key.cmp(key.as_slice()) {
            std::cmp::Ordering::Less => previous_key = key.as_slice(),
            std::cmp::Ordering::Equal => return Err(NodeError::DuplicateKey),
            std::cmp::Ordering::Greater => return Err(NodeError::UnsortedKeys),
        }
    }

    Ok(())
}

fn append_len(bytes: &mut Vec<u8>, len: usize) {
    bytes.extend_from_slice(&(len as u64).to_le_bytes());
}

fn append_len_prefixed_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    append_len(output, bytes.len());
    output.extend_from_slice(bytes);
}

fn hash_serialised(serialised: &[u8]) -> Hash {
    Hash::from_bytes(*blake3::hash(serialised).as_bytes())
}

#[derive(Debug)]
struct ByteCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ByteCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_expected_tag(&mut self, expected: u8) -> Result<(), NodeError> {
        let found = self.read_u8()?;
        if found == expected {
            Ok(())
        } else {
            Err(NodeError::InvalidTag { found })
        }
    }

    fn read_u8(&mut self) -> Result<u8, NodeError> {
        let bytes = self.read_exact(1)?;
        let [value] = bytes else {
            return Err(NodeError::Truncated);
        };
        Ok(*value)
    }

    fn read_len(&mut self) -> Result<usize, NodeError> {
        let bytes = self.read_exact(U64_SIZE)?;
        let len_bytes: [u8; U64_SIZE] = bytes.try_into().map_err(|_error| NodeError::Truncated)?;
        let len = u64::from_le_bytes(len_bytes);
        usize::try_from(len).map_err(|_error| NodeError::LengthOverflow)
    }

    fn read_len_prefixed_bytes(&mut self) -> Result<Vec<u8>, NodeError> {
        let len = self.read_len()?;
        self.read_exact(len).map(<[u8]>::to_vec)
    }

    fn read_hash_bytes(&mut self) -> Result<[u8; HASH_SIZE], NodeError> {
        self.read_exact(HASH_SIZE)
            .and_then(|bytes| bytes.try_into().map_err(|_error| NodeError::Truncated))
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], NodeError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(NodeError::LengthOverflow)?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(NodeError::Truncated)?;
        self.offset = end;
        Ok(bytes)
    }

    const fn finish(&self) -> Result<(), NodeError> {
        let trailing = self.bytes.len().saturating_sub(self.offset);
        if trailing == 0 {
            Ok(())
        } else {
            Err(NodeError::TrailingBytes { trailing })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Hash, InternalNode, LeafNode, Node, NodeError};

    fn leaf_entries() -> Vec<(Vec<u8>, Vec<u8>)> {
        vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"b".to_vec(), b"two".to_vec()),
        ]
    }

    #[test]
    fn leaf_accepts_sorted_entries() -> Result<(), NodeError> {
        let entries = leaf_entries();
        let leaf = LeafNode::new(entries.clone())?;
        assert_eq!(leaf.entries(), entries.as_slice());
        Ok(())
    }

    #[test]
    fn leaf_rejects_unsorted_entries() {
        let entries = vec![
            (b"b".to_vec(), b"two".to_vec()),
            (b"a".to_vec(), b"one".to_vec()),
        ];
        assert!(matches!(
            LeafNode::new(entries),
            Err(NodeError::UnsortedKeys)
        ));
    }

    #[test]
    fn leaf_rejects_duplicate_entries() {
        let entries = vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"a".to_vec(), b"two".to_vec()),
        ];
        assert!(matches!(
            LeafNode::new(entries),
            Err(NodeError::DuplicateKey)
        ));
    }

    #[test]
    fn leaf_round_trips_through_serialisation() -> Result<(), NodeError> {
        let leaf = LeafNode::new(leaf_entries())?;
        let first = leaf.serialise();
        let second = leaf.serialise();
        assert_eq!(first, second);
        assert_eq!(LeafNode::deserialise(&first)?, leaf);
        Ok(())
    }

    #[test]
    fn internal_accepts_sorted_children() -> Result<(), NodeError> {
        let children = vec![
            (b"a".to_vec(), Hash::from_bytes([1; 32])),
            (b"b".to_vec(), Hash::from_bytes([2; 32])),
        ];
        let internal = InternalNode::new(children.clone())?;
        assert_eq!(internal.children(), children.as_slice());
        Ok(())
    }

    #[test]
    fn internal_rejects_unsorted_children() {
        let children = vec![
            (b"b".to_vec(), Hash::from_bytes([2; 32])),
            (b"a".to_vec(), Hash::from_bytes([1; 32])),
        ];
        assert!(matches!(
            InternalNode::new(children),
            Err(NodeError::UnsortedKeys)
        ));
    }

    #[test]
    fn internal_rejects_duplicate_children() {
        let children = vec![
            (b"a".to_vec(), Hash::from_bytes([1; 32])),
            (b"a".to_vec(), Hash::from_bytes([2; 32])),
        ];
        assert!(matches!(
            InternalNode::new(children),
            Err(NodeError::DuplicateKey)
        ));
    }

    #[test]
    fn internal_round_trips_through_serialisation() -> Result<(), NodeError> {
        let internal = InternalNode::new(vec![
            (b"a".to_vec(), Hash::from_bytes([1; 32])),
            (b"b".to_vec(), Hash::from_bytes([2; 32])),
        ])?;
        let first = internal.serialise();
        let second = internal.serialise();
        assert_eq!(first, second);
        assert_eq!(InternalNode::deserialise(&first)?, internal);
        Ok(())
    }

    #[test]
    fn hash_uses_blake3_of_serialised_bytes() -> Result<(), NodeError> {
        let leaf = LeafNode::new(leaf_entries())?;
        let expected = Hash::from_bytes(*blake3::hash(&leaf.serialise()).as_bytes());
        assert_eq!(leaf.hash(), expected);
        assert_eq!(format!("{}", leaf.hash()).len(), 64);
        Ok(())
    }

    #[test]
    fn internal_hash_uses_blake3_of_serialised_bytes() -> Result<(), NodeError> {
        let internal = InternalNode::new(vec![
            (b"a".to_vec(), Hash::from_bytes([1; 32])),
            (b"b".to_vec(), Hash::from_bytes([2; 32])),
        ])?;
        let expected = Hash::from_bytes(*blake3::hash(&internal.serialise()).as_bytes());

        assert_eq!(internal.hash(), expected);
        Ok(())
    }

    #[test]
    fn hash_display_is_lowercase_hex() {
        let hash = Hash::from_bytes([0xab; 32]);

        assert_eq!(hash.to_string(), "ab".repeat(32));
    }

    #[test]
    fn equal_nodes_have_equal_hashes_and_different_nodes_do_not() -> Result<(), NodeError> {
        let first = LeafNode::new(leaf_entries())?;
        let second = LeafNode::new(leaf_entries())?;
        let third = LeafNode::new(vec![(b"a".to_vec(), b"changed".to_vec())])?;

        assert_eq!(first.hash(), second.hash());
        assert_ne!(first.hash(), third.hash());
        Ok(())
    }

    #[test]
    fn node_enum_delegates_and_round_trips() -> Result<(), NodeError> {
        let leaf = LeafNode::new(leaf_entries())?;
        let node = Node::Leaf(leaf.clone());
        assert_eq!(node.hash(), leaf.hash());
        assert_eq!(Node::deserialise(&node.serialise())?, node);

        let internal = InternalNode::new(vec![(b"a".to_vec(), leaf.hash())])?;
        let node = Node::Internal(internal.clone());
        assert_eq!(node.hash(), internal.hash());
        assert_eq!(Node::deserialise(&node.serialise())?, node);
        Ok(())
    }
}
