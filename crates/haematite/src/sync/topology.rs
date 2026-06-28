//! Sync topology configuration and convergence properties.
//!
//! A topology is explicit: there is intentionally no [`Default`] implementation
//! because distributed database creation must choose full-mesh, ring, or a
//! caller-supplied custom graph. The pair list is a stable representation of the
//! sync relationships. For convergence and scheduler partner derivation those
//! relationships are treated as undirected: either endpoint can schedule a pull
//! from the other endpoint, while the sync protocol remains responsible for the
//! concrete source/target direction of any one pull.
//!
//! Convergence bounds assume one successful sync round runs every scheduled
//! pair for every shard, merge is monotonic/idempotent, and failed rounds are
//! retried. Under those assumptions full-mesh converges in one round, ring
//! converges in at most `N - 1` rounds for `N` nodes, and custom topology
//! converges if and only if its pair graph is connected.

use std::collections::BTreeSet;
use std::fmt;

/// Identifier for a distributed haematite node.
///
/// Re-exported from the platform-neutral [`crate::sync_codec::ids`] module so the
/// wasm codec and the native topology layer share one identical type. This keeps
/// `crate::sync::topology::SyncNodeId` and `crate::sync::SyncNodeId` stable.
pub use crate::sync_codec::ids::SyncNodeId;

/// One topology relationship between two distributed nodes.
///
/// The field names are kept as `source`/`target` for API clarity and stable
/// serialization of custom pairs, but the topology layer treats the pair as an
/// undirected relationship for partner derivation and convergence analysis.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct SyncPair {
    pub source: SyncNodeId,
    pub target: SyncNodeId,
}

impl SyncPair {
    pub fn new(source: impl Into<SyncNodeId>, target: impl Into<SyncNodeId>) -> Self {
        Self {
            source: source.into(),
            target: target.into(),
        }
    }

    pub fn is_self_pair(&self) -> bool {
        self.source == self.target
    }

    pub fn other_endpoint(&self, node: &SyncNodeId) -> Option<&SyncNodeId> {
        if &self.source == node {
            Some(&self.target)
        } else if &self.target == node {
            Some(&self.source)
        } else {
            None
        }
    }
}

/// Explicit sync topology selected at distributed database creation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum SyncTopology {
    /// Every node has a direct sync relationship with every other node.
    FullMesh,
    /// Nodes are linked to their successor in the supplied node order, including
    /// the final node wrapping back to the first.
    Ring,
    /// Caller-supplied topology relationships.
    Custom(Vec<SyncPair>),
}

impl SyncTopology {
    /// Derive the stable topology pair list for the supplied nodes.
    ///
    /// Full-mesh emits unordered combinations (`i < j` in the node list), so 4
    /// nodes produce 6 pairs. Ring emits one successor pair per node, so 4 nodes
    /// produce 4 pairs. Custom returns the explicit list after endpoint
    /// validation.
    pub fn sync_pairs(&self, nodes: &[SyncNodeId]) -> Result<Vec<SyncPair>, TopologyError> {
        let known_nodes = validate_nodes(nodes)?;
        match self {
            Self::FullMesh => Ok(full_mesh_pairs(nodes)),
            Self::Ring => Ok(ring_pairs(nodes)),
            Self::Custom(pairs) => {
                validate_pairs(pairs, &known_nodes)?;
                Ok(pairs.clone())
            }
        }
    }

    /// Return the nodes this local node should pull from for one scheduler tick.
    ///
    /// Each topology pair is interpreted as a relationship, so a local node uses
    /// the opposite endpoint as a partner whether it appears in `source` or
    /// `target`.
    pub fn partners_for(
        &self,
        local_node: &SyncNodeId,
        nodes: &[SyncNodeId],
    ) -> Result<Vec<SyncNodeId>, TopologyError> {
        let known_nodes = validate_nodes(nodes)?;
        if !known_nodes.contains(local_node) {
            return Err(TopologyError::LocalNodeNotInTopology {
                node: local_node.clone(),
            });
        }

        let mut partners = Vec::new();
        for pair in self.sync_pairs(nodes)? {
            match pair.other_endpoint(local_node) {
                Some(other) if !partners.contains(other) => partners.push(other.clone()),
                Some(_) | None => {}
            }
        }
        Ok(partners)
    }

    /// Documented convergence properties for this topology over `nodes`.
    pub fn convergence_properties(
        &self,
        nodes: &[SyncNodeId],
    ) -> Result<ConvergenceProperties, TopologyError> {
        let pairs = self.sync_pairs(nodes)?;
        let node_count = nodes.len();
        let connected = graph_connected(nodes, &pairs);
        let rounds = match self {
            Self::FullMesh => Some(usize::from(node_count > 1)),
            Self::Ring => Some(node_count.saturating_sub(1)),
            Self::Custom(_) => None,
        };

        Ok(ConvergenceProperties { connected, rounds })
    }
}

/// Summary of topology convergence under successful repeated sync rounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConvergenceProperties {
    /// Whether the topology graph can propagate all writes to all nodes.
    pub connected: bool,
    /// Upper-bound sync rounds to convergence when the topology has a fixed
    /// documented bound. Custom topologies depend on graph shape, so this is
    /// `None` even when connected.
    pub rounds: Option<usize>,
}

impl ConvergenceProperties {
    pub const fn converges(self) -> bool {
        self.connected
    }

    pub const fn rounds_to_converge(self) -> Option<usize> {
        self.rounds
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopologyError {
    DuplicateNode { node: SyncNodeId },
    UnknownEndpoint { node: SyncNodeId },
    SelfPair { node: SyncNodeId },
    LocalNodeNotInTopology { node: SyncNodeId },
}

impl fmt::Display for TopologyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateNode { node } => {
                write!(formatter, "sync topology contains duplicate node `{node}`")
            }
            Self::UnknownEndpoint { node } => {
                write!(formatter, "sync pair references unknown node `{node}`")
            }
            Self::SelfPair { node } => {
                write!(formatter, "sync pair for node `{node}` points to itself")
            }
            Self::LocalNodeNotInTopology { node } => {
                write!(
                    formatter,
                    "local sync node `{node}` is not in the topology node list"
                )
            }
        }
    }
}

impl std::error::Error for TopologyError {}

fn validate_nodes(nodes: &[SyncNodeId]) -> Result<BTreeSet<SyncNodeId>, TopologyError> {
    let mut known_nodes = BTreeSet::new();
    for node in nodes {
        if !known_nodes.insert(node.clone()) {
            return Err(TopologyError::DuplicateNode { node: node.clone() });
        }
    }
    Ok(known_nodes)
}

fn validate_pairs(
    pairs: &[SyncPair],
    known_nodes: &BTreeSet<SyncNodeId>,
) -> Result<(), TopologyError> {
    for pair in pairs {
        if pair.is_self_pair() {
            return Err(TopologyError::SelfPair {
                node: pair.source.clone(),
            });
        }
        if !known_nodes.contains(&pair.source) {
            return Err(TopologyError::UnknownEndpoint {
                node: pair.source.clone(),
            });
        }
        if !known_nodes.contains(&pair.target) {
            return Err(TopologyError::UnknownEndpoint {
                node: pair.target.clone(),
            });
        }
    }
    Ok(())
}

fn full_mesh_pairs(nodes: &[SyncNodeId]) -> Vec<SyncPair> {
    let mut pairs =
        Vec::with_capacity(nodes.len().saturating_mul(nodes.len().saturating_sub(1)) / 2);
    for (source_index, source) in nodes.iter().enumerate() {
        for target in nodes.iter().skip(source_index + 1) {
            pairs.push(SyncPair::new(source.clone(), target.clone()));
        }
    }
    pairs
}

fn ring_pairs(nodes: &[SyncNodeId]) -> Vec<SyncPair> {
    if nodes.len() < 2 {
        return Vec::new();
    }

    let mut pairs = Vec::with_capacity(nodes.len());
    for (index, source) in nodes.iter().enumerate() {
        let target_index = if index + 1 == nodes.len() {
            0
        } else {
            index + 1
        };
        pairs.push(SyncPair::new(source.clone(), nodes[target_index].clone()));
    }
    pairs
}

fn graph_connected(nodes: &[SyncNodeId], pairs: &[SyncPair]) -> bool {
    let Some(first) = nodes.first() else {
        return true;
    };
    if nodes.len() == 1 {
        return true;
    }

    let mut visited = BTreeSet::new();
    let mut stack = vec![first.clone()];
    visited.insert(first.clone());

    while let Some(node) = stack.pop() {
        for pair in pairs {
            match pair.other_endpoint(&node) {
                Some(other) if visited.insert(other.clone()) => stack.push(other.clone()),
                Some(_) | None => {}
            }
        }
    }

    visited.len() == nodes.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(count: usize) -> Vec<SyncNodeId> {
        (0..count).map(SyncNodeId::from).collect()
    }

    #[test]
    fn full_mesh_with_four_nodes_produces_all_six_combinations() -> Result<(), TopologyError> {
        let nodes = nodes(4);
        let pairs = SyncTopology::FullMesh.sync_pairs(&nodes)?;

        assert_eq!(pairs.len(), 6);
        assert_eq!(
            pairs,
            vec![
                SyncPair::new(0, 1),
                SyncPair::new(0, 2),
                SyncPair::new(0, 3),
                SyncPair::new(1, 2),
                SyncPair::new(1, 3),
                SyncPair::new(2, 3),
            ]
        );
        Ok(())
    }

    #[test]
    fn ring_with_four_nodes_produces_successor_pairs() -> Result<(), TopologyError> {
        let nodes = nodes(4);
        let pairs = SyncTopology::Ring.sync_pairs(&nodes)?;

        assert_eq!(pairs.len(), 4);
        assert_eq!(
            pairs,
            vec![
                SyncPair::new(0, 1),
                SyncPair::new(1, 2),
                SyncPair::new(2, 3),
                SyncPair::new(3, 0),
            ]
        );
        Ok(())
    }

    #[test]
    fn custom_topology_returns_explicit_validated_pairs() -> Result<(), TopologyError> {
        let nodes = vec!["a".into(), "b".into(), "c".into()];
        let explicit = vec![SyncPair::new("a", "c"), SyncPair::new("b", "c")];
        let topology = SyncTopology::Custom(explicit.clone());

        assert_eq!(topology.sync_pairs(&nodes)?, explicit);
        Ok(())
    }

    #[test]
    fn custom_topology_rejects_unknown_endpoints() {
        let nodes = vec!["a".into(), "b".into()];
        let topology = SyncTopology::Custom(vec![SyncPair::new("a", "c")]);

        assert!(matches!(
            topology.sync_pairs(&nodes),
            Err(TopologyError::UnknownEndpoint { node }) if node == SyncNodeId::from("c")
        ));
    }

    #[test]
    fn partners_are_derived_from_pairs_involving_local_node() -> Result<(), TopologyError> {
        let nodes = nodes(4);
        let partners = SyncTopology::Ring.partners_for(&SyncNodeId::from(0), &nodes)?;

        assert_eq!(partners, vec![SyncNodeId::from(1), SyncNodeId::from(3)]);
        Ok(())
    }

    #[test]
    fn full_mesh_converges_in_one_round() -> Result<(), TopologyError> {
        let properties = SyncTopology::FullMesh.convergence_properties(&nodes(4))?;

        assert!(properties.converges());
        assert_eq!(properties.rounds_to_converge(), Some(1));
        Ok(())
    }

    #[test]
    fn ring_converges_in_at_most_n_minus_one_rounds() -> Result<(), TopologyError> {
        let properties = SyncTopology::Ring.convergence_properties(&nodes(5))?;

        assert!(properties.converges());
        assert_eq!(properties.rounds_to_converge(), Some(4));
        Ok(())
    }

    #[test]
    fn custom_converges_if_and_only_if_pair_graph_is_connected() -> Result<(), TopologyError> {
        let nodes = vec!["a".into(), "b".into(), "c".into()];
        let connected =
            SyncTopology::Custom(vec![SyncPair::new("a", "b"), SyncPair::new("b", "c")])
                .convergence_properties(&nodes)?;
        let disconnected =
            SyncTopology::Custom(vec![SyncPair::new("a", "b")]).convergence_properties(&nodes)?;

        assert!(connected.converges());
        assert_eq!(connected.rounds_to_converge(), None);
        assert!(!disconnected.converges());
        assert_eq!(disconnected.rounds_to_converge(), None);
        Ok(())
    }
}
