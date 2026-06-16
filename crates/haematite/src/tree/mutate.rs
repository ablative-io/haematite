use crate::store::NodeStore;

use super::boundary::BoundaryDetector;
use super::cursor::{TreeError, child_index_for_key, load_node};
use super::node::{Hash, InternalNode, LeafNode, Node};

type Entry = (Vec<u8>, Vec<u8>);
type ChildRef = (Vec<u8>, Hash);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Mutation {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Rewrite {
    Unchanged,
    Replaced(Vec<ChildRef>),
}

pub fn insert<S, K, V>(store: &mut S, root_hash: Hash, key: K, value: V) -> Result<Hash, TreeError>
where
    S: NodeStore + ?Sized,
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
{
    let mutations = [(key.as_ref().to_vec(), Some(value.as_ref().to_vec()))];
    batch_mutate(store, root_hash, mutations.as_slice())
}

pub fn delete<S, K>(store: &mut S, root_hash: Hash, key: K) -> Result<Hash, TreeError>
where
    S: NodeStore + ?Sized,
    K: AsRef<[u8]>,
{
    let mutations = [(key.as_ref().to_vec(), None)];
    batch_mutate(store, root_hash, mutations.as_slice())
}

pub fn batch_mutate<S: NodeStore + ?Sized>(
    store: &mut S,
    root_hash: Hash,
    mutations: &[(Vec<u8>, Option<Vec<u8>>)],
) -> Result<Hash, TreeError> {
    if mutations.is_empty() {
        return Ok(root_hash);
    }

    let normalised = normalise_mutations(mutations);
    if normalised.is_empty() {
        return Ok(root_hash);
    }

    let rewrite = rewrite_node(store, root_hash, normalised.as_slice(), true)?;
    finish_root(store, root_hash, rewrite)
}

fn normalise_mutations(mutations: &[(Vec<u8>, Option<Vec<u8>>)]) -> Vec<Mutation> {
    let mut normalised: Vec<Mutation> = Vec::new();

    for (key, value) in mutations {
        if let Some(last) = normalised.last_mut().filter(|last| last.key == *key) {
            last.value.clone_from(value);
        } else {
            normalised.push(Mutation {
                key: key.clone(),
                value: value.clone(),
            });
        }
    }

    normalised
}

fn rewrite_node<S: NodeStore + ?Sized>(
    store: &mut S,
    hash: Hash,
    mutations: &[Mutation],
    is_root: bool,
) -> Result<Rewrite, TreeError> {
    if mutations.is_empty() {
        return Ok(Rewrite::Unchanged);
    }

    match load_node(store, hash)? {
        Node::Leaf(leaf) => rewrite_leaf(store, leaf.entries(), mutations),
        Node::Internal(internal) => {
            rewrite_internal(store, internal.children(), mutations, is_root)
        }
    }
}

fn rewrite_leaf<S: NodeStore + ?Sized>(
    store: &mut S,
    entries: &[Entry],
    mutations: &[Mutation],
) -> Result<Rewrite, TreeError> {
    let (entries, changed) = apply_leaf_mutations(entries, mutations)?;
    if !changed {
        return Ok(Rewrite::Unchanged);
    }

    store_leaf_replacements(store, entries).map(Rewrite::Replaced)
}

fn apply_leaf_mutations(
    entries: &[Entry],
    mutations: &[Mutation],
) -> Result<(Vec<Entry>, bool), TreeError> {
    let mut rewritten = entries.to_vec();
    let mut changed = false;

    for mutation in mutations {
        let search =
            rewritten.binary_search_by(|(key, _value)| key.as_slice().cmp(mutation.key.as_slice()));
        match (&mutation.value, search) {
            (Some(value), Ok(index)) => {
                let Some((_key, stored_value)) = rewritten.get_mut(index) else {
                    return Err(TreeError::InvalidNode);
                };
                if stored_value != value {
                    value.clone_into(stored_value);
                    changed = true;
                }
            }
            (Some(value), Err(index)) => {
                if index > rewritten.len() {
                    return Err(TreeError::InvalidNode);
                }
                rewritten.insert(index, (mutation.key.clone(), value.clone()));
                changed = true;
            }
            (None, Ok(index)) => {
                if index >= rewritten.len() {
                    return Err(TreeError::InvalidNode);
                }
                rewritten.remove(index);
                changed = true;
            }
            (None, Err(_index)) => {}
        }
    }

    Ok((rewritten, changed))
}

fn rewrite_internal<S: NodeStore + ?Sized>(
    store: &mut S,
    children: &[ChildRef],
    mutations: &[Mutation],
    is_root: bool,
) -> Result<Rewrite, TreeError> {
    if children.is_empty() {
        return Err(TreeError::InvalidNode);
    }

    let groups = partition_mutations(children, mutations)?;
    let mut rewritten = Vec::new();
    let mut changed = false;

    for ((separator, child_hash), group) in children.iter().zip(groups.iter()) {
        if group.is_empty() {
            rewritten.push((separator.clone(), *child_hash));
        } else {
            match rewrite_node(store, *child_hash, group.as_slice(), false)? {
                Rewrite::Unchanged => rewritten.push((separator.clone(), *child_hash)),
                Rewrite::Replaced(replacements) => {
                    changed = true;
                    rewritten.extend(replacements);
                }
            }
        }
    }

    if changed {
        store_internal_replacements(store, rewritten, is_root).map(Rewrite::Replaced)
    } else {
        Ok(Rewrite::Unchanged)
    }
}

fn partition_mutations(
    children: &[ChildRef],
    mutations: &[Mutation],
) -> Result<Vec<Vec<Mutation>>, TreeError> {
    let mut groups = vec![Vec::new(); children.len()];

    for mutation in mutations {
        let index = child_index_for_key(children, mutation.key.as_slice())?;
        let Some(group) = groups.get_mut(index) else {
            return Err(TreeError::InvalidNode);
        };
        group.push(mutation.clone());
    }

    Ok(groups)
}

fn store_leaf_replacements<S: NodeStore + ?Sized>(
    store: &mut S,
    entries: Vec<Entry>,
) -> Result<Vec<ChildRef>, TreeError> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let mut replacements = Vec::new();
    for chunk in split_after_boundaries(entries, |(key, _value)| key.as_slice()) {
        replacements.push(store_leaf(store, chunk)?);
    }
    Ok(replacements)
}

fn store_internal_replacements<S: NodeStore + ?Sized>(
    store: &mut S,
    children: Vec<ChildRef>,
    is_root: bool,
) -> Result<Vec<ChildRef>, TreeError> {
    match children.len() {
        0 => Ok(Vec::new()),
        1 => Ok(children),
        _ if is_root => Ok(children),
        _ => {
            let mut replacements = Vec::new();
            for chunk in split_after_boundaries(children, |(key, _hash)| key.as_slice()) {
                replacements.push(store_internal_subtree(store, chunk)?);
            }
            Ok(replacements)
        }
    }
}

fn split_after_boundaries<T, F>(items: Vec<T>, key_of: F) -> Vec<Vec<T>>
where
    F: Fn(&T) -> &[u8],
{
    let detector = BoundaryDetector::default();
    let mut chunks = Vec::new();
    let mut current = Vec::new();

    for item in items {
        let is_boundary = detector.is_boundary(key_of(&item));
        current.push(item);
        if is_boundary {
            chunks.push(current);
            current = Vec::new();
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }

    chunks
}

fn store_leaf<S: NodeStore + ?Sized>(
    store: &mut S,
    entries: Vec<Entry>,
) -> Result<ChildRef, TreeError> {
    let separator = first_entry_key(entries.as_slice())?;
    let leaf = LeafNode::new(entries)?;
    let hash = store
        .put(&Node::Leaf(leaf))
        .map_err(|_| TreeError::InvalidNode)?;
    Ok((separator, hash))
}

fn store_internal<S: NodeStore + ?Sized>(
    store: &mut S,
    children: Vec<ChildRef>,
) -> Result<ChildRef, TreeError> {
    let separator = first_child_key(children.as_slice())?;
    let internal = InternalNode::new(children)?;
    let hash = store
        .put(&Node::Internal(internal))
        .map_err(|_| TreeError::InvalidNode)?;
    Ok((separator, hash))
}

fn finish_root<S: NodeStore + ?Sized>(
    store: &mut S,
    root_hash: Hash,
    rewrite: Rewrite,
) -> Result<Hash, TreeError> {
    let Rewrite::Replaced(replacements) = rewrite else {
        return Ok(root_hash);
    };

    match replacements.len() {
        0 => store_empty_leaf(store),
        1 => replacements
            .first()
            .map(|(_separator, hash)| *hash)
            .ok_or(TreeError::InvalidNode),
        _ => store_root_internal(store, replacements),
    }
}

fn store_root_internal<S: NodeStore + ?Sized>(
    store: &mut S,
    children: Vec<ChildRef>,
) -> Result<Hash, TreeError> {
    let children = root_children(store, children)?;
    let internal = InternalNode::new(children)?;
    store
        .put(&Node::Internal(internal))
        .map_err(|_| TreeError::InvalidNode)
}

fn root_children<S: NodeStore + ?Sized>(
    store: &mut S,
    children: Vec<ChildRef>,
) -> Result<Vec<ChildRef>, TreeError> {
    if children.len() <= 2 {
        return Ok(children);
    }

    let original_children = children;
    let mut children = flatten_child_refs(store, original_children.as_slice())?;
    if children.len() <= 2 {
        return Ok(children);
    }

    let split_index = children.len() / 2;
    let right_children = children.split_off(split_index);
    let left = reuse_or_store_subtree(store, original_children.as_slice(), children.as_slice())?;
    let right = reuse_or_store_subtree(
        store,
        original_children.as_slice(),
        right_children.as_slice(),
    )?;
    Ok(vec![left, right])
}

fn flatten_child_refs<S: NodeStore + ?Sized>(
    store: &S,
    children: &[ChildRef],
) -> Result<Vec<ChildRef>, TreeError> {
    let mut flattened = Vec::new();
    for (separator, hash) in children {
        match load_node(store, *hash)? {
            Node::Leaf(_leaf) => flattened.push((separator.clone(), *hash)),
            Node::Internal(internal) => {
                flattened.extend(flatten_child_refs(store, internal.children())?);
            }
        }
    }
    Ok(flattened)
}

fn reuse_or_store_subtree<S: NodeStore + ?Sized>(
    store: &mut S,
    original_children: &[ChildRef],
    target_children: &[ChildRef],
) -> Result<ChildRef, TreeError> {
    if let Some(child) = matching_existing_child(store, original_children, target_children)? {
        return Ok(child);
    }

    store_internal_subtree(store, target_children.to_vec())
}

fn matching_existing_child<S: NodeStore + ?Sized>(
    store: &S,
    original_children: &[ChildRef],
    target_children: &[ChildRef],
) -> Result<Option<ChildRef>, TreeError> {
    for child in original_children {
        let child_slice = std::slice::from_ref(child);
        if flatten_child_refs(store, child_slice)? == target_children {
            return Ok(Some(child.clone()));
        }
    }

    Ok(None)
}

fn store_internal_subtree<S: NodeStore + ?Sized>(
    store: &mut S,
    children: Vec<ChildRef>,
) -> Result<ChildRef, TreeError> {
    match children.len() {
        0 => Err(TreeError::InvalidNode),
        1 => children.into_iter().next().ok_or(TreeError::InvalidNode),
        _ => store_internal(store, children),
    }
}

fn store_empty_leaf<S: NodeStore + ?Sized>(store: &mut S) -> Result<Hash, TreeError> {
    let leaf = LeafNode::new(Vec::new())?;
    store
        .put(&Node::Leaf(leaf))
        .map_err(|_| TreeError::InvalidNode)
}

fn first_entry_key(entries: &[Entry]) -> Result<Vec<u8>, TreeError> {
    entries
        .first()
        .map(|(key, _value)| key.clone())
        .ok_or(TreeError::InvalidNode)
}

fn first_child_key(children: &[ChildRef]) -> Result<Vec<u8>, TreeError> {
    children
        .first()
        .map(|(key, _hash)| key.clone())
        .ok_or(TreeError::InvalidNode)
}
