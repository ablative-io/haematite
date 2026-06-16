use std::num::NonZeroUsize;

use lru::LruCache as InnerLruCache;

use crate::tree::{Hash, Node};

#[derive(Debug)]
pub struct LruCache {
    nodes: InnerLruCache<Hash, Node>,
}

impl LruCache {
    pub fn new(max_entries: usize) -> Result<Self, CacheError> {
        let capacity = NonZeroUsize::new(max_entries).ok_or(CacheError::InvalidCapacity)?;
        Ok(Self {
            nodes: InnerLruCache::new(capacity),
        })
    }

    pub fn get(&mut self, hash: &Hash) -> Option<Node> {
        self.nodes.get(hash).cloned()
    }

    pub fn put(&mut self, hash: Hash, node: Node) {
        self.nodes.put(hash, node);
    }

    pub fn remove(&mut self, hash: &Hash) -> Option<Node> {
        self.nodes.pop(hash)
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.nodes.cap().get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheError {
    InvalidCapacity,
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCapacity => write!(f, "cache capacity must be greater than zero"),
        }
    }
}

impl std::error::Error for CacheError {}

#[cfg(test)]
mod tests {
    use super::{CacheError, LruCache};
    use crate::tree::{Hash, LeafNode, Node, NodeError};

    fn node(key: u8, value: &[u8]) -> Result<Node, NodeError> {
        LeafNode::new(vec![(vec![key], value.to_vec())]).map(Node::Leaf)
    }

    #[test]
    fn new_rejects_zero_capacity() {
        let result = LruCache::new(0);
        assert!(matches!(result, Err(CacheError::InvalidCapacity)));
    }

    #[test]
    fn new_uses_configured_capacity() -> Result<(), CacheError> {
        let cache = LruCache::new(3)?;
        assert_eq!(cache.capacity(), 3);
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
        Ok(())
    }

    #[test]
    fn inserting_beyond_capacity_evicts_lru_entry() -> Result<(), Box<dyn std::error::Error>> {
        let mut cache = LruCache::new(2)?;
        let first = node(b'a', b"one")?;
        let second = node(b'b', b"two")?;
        let third = node(b'c', b"three")?;
        let first_hash = first.hash();
        let second_hash = second.hash();
        let third_hash = third.hash();

        cache.put(first_hash, first);
        cache.put(second_hash, second);
        assert_eq!(cache.get(&first_hash), Some(node(b'a', b"one")?));
        cache.put(third_hash, third.clone());

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(&second_hash), None);
        assert_eq!(cache.get(&first_hash), Some(node(b'a', b"one")?));
        assert_eq!(cache.get(&third_hash), Some(third));
        Ok(())
    }

    #[test]
    fn remove_evicts_entry_by_hash() -> Result<(), Box<dyn std::error::Error>> {
        let mut cache = LruCache::new(1)?;
        let cached = node(b'a', b"one")?;
        let hash = cached.hash();

        cache.put(hash, cached.clone());
        assert_eq!(cache.remove(&hash), Some(cached));
        assert_eq!(cache.get(&hash), None);
        assert_eq!(cache.remove(&Hash::from_bytes([7; 32])), None);
        Ok(())
    }
}
