use std::collections::HashMap;
use std::fmt::Debug;

use crate::tree::{Hash, Node};

pub trait NodeStore: Debug {
    fn get(&self, hash: &Hash) -> Option<Node>;

    fn put(&mut self, node: &Node) -> Hash;
}

#[derive(Debug, Default, Clone)]
pub struct MemoryStore {
    nodes: HashMap<Hash, Vec<u8>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl NodeStore for MemoryStore {
    fn get(&self, hash: &Hash) -> Option<Node> {
        self.nodes
            .get(hash)
            .and_then(|serialised| Node::deserialise(serialised).ok())
    }

    fn put(&mut self, node: &Node) -> Hash {
        let hash = node.hash();
        self.nodes.insert(hash, node.serialise());
        hash
    }
}

#[cfg(test)]
mod tests {
    use super::{MemoryStore, NodeStore};
    use crate::tree::{Hash, LeafNode, Node, NodeError};

    fn node() -> Result<Node, NodeError> {
        LeafNode::new(vec![(b"a".to_vec(), b"one".to_vec())]).map(Node::Leaf)
    }

    #[test]
    fn put_stores_node_under_content_hash() -> Result<(), NodeError> {
        let node = node()?;
        let mut store = MemoryStore::new();
        let hash = store.put(&node);

        assert_eq!(hash, node.hash());
        assert_eq!(store.get(&hash), Some(node));
        Ok(())
    }

    #[test]
    fn get_returns_none_for_unknown_hash() {
        let store = MemoryStore::new();
        assert_eq!(store.get(&Hash::from_bytes([7; 32])), None);
    }
}
