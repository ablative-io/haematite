use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Debug;

use crate::tree::{Hash, Node};

use super::DeleteNode;

pub trait NodeStore: Debug {
    type Error: std::error::Error;

    fn get(&self, hash: &Hash) -> Result<Option<Node>, Self::Error>;

    fn put(&mut self, node: &Node) -> Result<Hash, Self::Error>;
}

#[derive(Debug, Default, Clone)]
pub struct MemoryStore {
    nodes: RefCell<HashMap<Hash, Vec<u8>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, hash: &Hash) -> Option<Node> {
        self.nodes
            .borrow()
            .get(hash)
            .and_then(|serialised| Node::deserialise(serialised).ok())
    }

    pub fn put(&mut self, node: &Node) -> Hash {
        let hash = node.hash();
        self.nodes.borrow_mut().insert(hash, node.serialise());
        hash
    }

    pub fn delete(&self, hash: &Hash) {
        self.nodes.borrow_mut().remove(hash);
    }
}

impl NodeStore for MemoryStore {
    type Error = Infallible;

    fn get(&self, hash: &Hash) -> Result<Option<Node>, Self::Error> {
        Ok(Self::get(self, hash))
    }

    fn put(&mut self, node: &Node) -> Result<Hash, Self::Error> {
        Ok(Self::put(self, node))
    }
}

impl DeleteNode for MemoryStore {
    type Error = Infallible;

    fn delete(&self, hash: &Hash) -> Result<(), Self::Error> {
        Self::delete(self, hash);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryStore;
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

    #[test]
    fn delete_removes_stored_node() -> Result<(), NodeError> {
        let node = node()?;
        let mut store = MemoryStore::new();
        let hash = store.put(&node);

        store.delete(&hash);

        assert_eq!(store.get(&hash), None);
        Ok(())
    }
}
