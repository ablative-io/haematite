use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use crate::store::NodeStore;

use super::node::{Hash, Node, NodeError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeError {
    MissingNode { hash: Hash },
    InvalidNode,
}

impl fmt::Display for TreeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNode { hash } => write!(formatter, "missing tree node {hash}"),
            Self::InvalidNode => write!(formatter, "invalid tree node"),
        }
    }
}

impl std::error::Error for TreeError {}

impl From<NodeError> for TreeError {
    fn from(_: NodeError) -> Self {
        Self::InvalidNode
    }
}

#[derive(Debug)]
pub struct Cursor<'a, S: NodeStore + ?Sized> {
    store: &'a S,
    root_hash: Hash,
}

impl<'a, S: NodeStore + ?Sized> Cursor<'a, S> {
    pub const fn new(store: &'a S, root_hash: Hash) -> Self {
        Self { store, root_hash }
    }

    pub const fn root_hash(&self) -> Hash {
        self.root_hash
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, TreeError> {
        let mut next_hash = self.root_hash;

        loop {
            match &*load_node(self.store, next_hash)? {
                Node::Leaf(leaf) => return leaf_value(leaf, key),
                Node::Internal(internal) => {
                    let children = internal.children();
                    let index = child_index_for_key(children, key)?;
                    let Some((_separator, child_hash)) = children.get(index) else {
                        return Err(TreeError::InvalidNode);
                    };
                    next_hash = *child_hash;
                }
            }
        }
    }

    pub fn range(&self, from: &[u8], to: &[u8]) -> RangeIter<'_, S> {
        RangeIter::new(self.store, self.root_hash, from, to)
    }
}

#[derive(Debug)]
pub struct RangeIter<'a, S: NodeStore + ?Sized> {
    store: &'a S,
    root_hash: Hash,
    from: Vec<u8>,
    to: Vec<u8>,
    stack: Vec<RangeFrame>,
    leaf: Option<Arc<Node>>,
    leaf_index: usize,
    started: bool,
    finished: bool,
}

impl<'a, S: NodeStore + ?Sized> RangeIter<'a, S> {
    fn new(store: &'a S, root_hash: Hash, from: &[u8], to: &[u8]) -> Self {
        Self {
            store,
            root_hash,
            from: from.to_vec(),
            to: to.to_vec(),
            stack: Vec::new(),
            leaf: None,
            leaf_index: 0,
            started: false,
            finished: to <= from,
        }
    }

    fn start(&mut self) -> Result<(), TreeError> {
        self.started = true;
        self.descend_for_key(self.root_hash)
    }

    fn descend_for_key(&mut self, hash: Hash) -> Result<(), TreeError> {
        let mut next_hash = hash;

        loop {
            let node = load_node(self.store, next_hash)?;
            match &*node {
                Node::Leaf(leaf) => {
                    self.leaf_index = lower_bound(leaf.entries(), self.from.as_slice());
                    self.leaf = Some(Arc::clone(&node));
                    return Ok(());
                }
                Node::Internal(internal) => {
                    let children = internal.children();
                    let index = child_index_for_key(children, self.from.as_slice())?;
                    let Some((_separator, child_hash)) = children.get(index) else {
                        return Err(TreeError::InvalidNode);
                    };
                    next_hash = *child_hash;
                    self.stack.push(RangeFrame {
                        node: Arc::clone(&node),
                        next_index: index.saturating_add(1),
                    });
                }
            }
        }
    }

    fn descend_leftmost(&mut self, hash: Hash) -> Result<(), TreeError> {
        let mut next_hash = hash;

        loop {
            let node = load_node(self.store, next_hash)?;
            match &*node {
                Node::Leaf(_leaf) => {
                    self.leaf_index = 0;
                    self.leaf = Some(Arc::clone(&node));
                    return Ok(());
                }
                Node::Internal(internal) => {
                    let Some((_separator, child_hash)) = internal.children().first() else {
                        return Err(TreeError::InvalidNode);
                    };
                    next_hash = *child_hash;
                    self.stack.push(RangeFrame {
                        node: Arc::clone(&node),
                        next_index: 1,
                    });
                }
            }
        }
    }

    fn advance_to_next_leaf(&mut self) -> Result<(), TreeError> {
        while let Some(mut frame) = self.stack.pop() {
            let children = frame_children(&frame.node)?;
            if frame.next_index < children.len() {
                let Some((separator, child_hash)) = children.get(frame.next_index) else {
                    return Err(TreeError::InvalidNode);
                };
                let separator_at_or_past_end = separator.as_slice() >= self.to.as_slice();
                let child_hash = *child_hash;

                frame.next_index = frame.next_index.saturating_add(1);
                self.stack.push(frame);

                if separator_at_or_past_end {
                    self.finished = true;
                    return Ok(());
                }

                return self.descend_leftmost(child_hash);
            }
        }

        self.finished = true;
        Ok(())
    }
}

impl<S: NodeStore + ?Sized> Iterator for RangeIter<'_, S> {
    type Item = Result<(Vec<u8>, Vec<u8>), TreeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        if !self.started {
            match self.start() {
                Ok(()) => {}
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            }
        }

        loop {
            if let Some(node) = self.leaf.clone()
                && let Node::Leaf(leaf) = &*node
            {
                let entries = leaf.entries();
                while let Some((key, value)) = entries.get(self.leaf_index) {
                    self.leaf_index = self.leaf_index.saturating_add(1);

                    if key.as_slice() < self.from.as_slice() {
                        continue;
                    }

                    if key.as_slice() >= self.to.as_slice() {
                        self.finished = true;
                        return None;
                    }

                    return Some(Ok((key.clone(), value.clone())));
                }
            }

            if let Err(error) = self.advance_to_next_leaf() {
                self.finished = true;
                return Some(Err(error));
            }

            if self.finished {
                return None;
            }
        }
    }
}

#[derive(Debug, Clone)]
struct RangeFrame {
    node: Arc<Node>,
    next_index: usize,
}

fn frame_children(node: &Node) -> Result<&[(Vec<u8>, Hash)], TreeError> {
    match node {
        Node::Internal(internal) => Ok(internal.children()),
        Node::Leaf(_leaf) => Err(TreeError::InvalidNode),
    }
}

pub(crate) fn load_node<S: NodeStore + ?Sized>(
    store: &S,
    hash: Hash,
) -> Result<Arc<Node>, TreeError> {
    store
        .get(&hash)
        .map_err(|_| TreeError::MissingNode { hash })?
        .ok_or(TreeError::MissingNode { hash })
}

pub(crate) fn child_index_for_key(
    children: &[(Vec<u8>, Hash)],
    key: &[u8],
) -> Result<usize, TreeError> {
    if children.is_empty() {
        return Err(TreeError::InvalidNode);
    }

    Ok(children
        .partition_point(|(separator, _hash)| separator.as_slice() <= key)
        .saturating_sub(1))
}

fn leaf_value(leaf: &super::node::LeafNode, key: &[u8]) -> Result<Option<Vec<u8>>, TreeError> {
    match leaf
        .entries()
        .binary_search_by(|(entry_key, _value)| entry_key.as_slice().cmp(key))
    {
        Ok(index) => leaf
            .entries()
            .get(index)
            .map(|(_key, value)| Some(value.clone()))
            .ok_or(TreeError::InvalidNode),
        Err(_index) => Ok(None),
    }
}

fn lower_bound(entries: &[(Vec<u8>, Vec<u8>)], key: &[u8]) -> usize {
    entries.partition_point(|(entry_key, _value)| {
        matches!(entry_key.as_slice().cmp(key), Ordering::Less)
    })
}
