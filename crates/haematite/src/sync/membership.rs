//! Membership binding for quorum-on-write (active-active 2a-2).
//!
//! This module computes the two membership inputs a Strong CAS write needs from
//! the FULL static cluster membership ([`DistributedDatabaseConfig::nodes`]) and a
//! live reachability view (beamr's `connected_nodes()`):
//!
//! * `total_nodes` — the quorum DENOMINATOR, which is ALWAYS the full membership
//!   count, NEVER the reachable subset. This is the load-bearing Q3 invariant from
//!   `tests/spike_quorum.rs`: sizing quorum from the reachable subset lets a
//!   minority partition trivially self-quorum (the split-brain bug). Liveness must
//!   never shrink the denominator.
//! * `send_targets` — the reachable peers (excluding the local node) to send
//!   `WriteProposal`s to. Liveness affects THIS and only this (design Fix E): a
//!   transient blip changes who we send to, never whether the majority can win.
//!
//! This is the binding ONLY: it is not wired to any live send/apply path (that is
//! 2a-3/2a-4). It is exercised with synthetic reachability sets in unit tests.

use std::collections::BTreeSet;

use crate::db::DistributedDatabaseConfig;
use crate::sync::topology::SyncNodeId;

/// The membership inputs for one Strong CAS write.
///
/// Produced by [`resolve_membership`]. `total_nodes` is the quorum denominator
/// (full membership); `send_targets` is the reachable peer set to propose to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteMembership {
    /// Quorum denominator = FULL membership count (`config.nodes.len()`). Never the
    /// reachable subset.
    pub total_nodes: usize,
    /// Reachable peers (excluding the local node) to send proposals to. Liveness
    /// affects this set ONLY.
    pub send_targets: Vec<SyncNodeId>,
}

/// Resolve `(total_nodes, send_targets)` from the full cluster config and a live
/// reachability view.
///
/// `reachable` is the set of node names currently connected (in production, the
/// beamr `connected_nodes()` atoms mapped back to their `SyncNodeId` names). It is
/// intersected with the configured membership and the local node is excluded, so:
///
/// * an unknown/extra name in `reachable` can never inflate `send_targets`;
/// * the local node is never a send target (it self-acks);
/// * `total_nodes` is computed from `config.nodes` and is INDEPENDENT of
///   `reachable` — a fully partitioned node still reports the full denominator and
///   is therefore fenced rather than able to self-quorum.
///
/// `send_targets` is returned in the configured `nodes` order with duplicates in
/// the configured list collapsed, so the result is deterministic.
#[must_use]
pub fn resolve_membership(
    config: &DistributedDatabaseConfig,
    reachable: &BTreeSet<SyncNodeId>,
) -> WriteMembership {
    let total_nodes = config.nodes.len();

    let mut emitted = BTreeSet::new();
    let mut send_targets = Vec::new();
    for node in &config.nodes {
        if node == &config.local_node {
            continue;
        }
        if reachable.contains(node) && emitted.insert(node.clone()) {
            send_targets.push(node.clone());
        }
    }

    WriteMembership {
        total_nodes,
        send_targets,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::consistency::{ConsistencyError, StrongConsistency, wait_for_quorum};
    use std::time::Duration;

    fn config(local: &str, nodes: &[&str]) -> DistributedDatabaseConfig {
        DistributedDatabaseConfig {
            local_node: SyncNodeId::from(local),
            nodes: nodes.iter().map(|name| SyncNodeId::from(*name)).collect(),
            topology: None,
            sync_interval: 1,
        }
    }

    fn reachable(names: &[&str]) -> BTreeSet<SyncNodeId> {
        names.iter().map(|name| SyncNodeId::from(*name)).collect()
    }

    #[test]
    fn total_nodes_is_full_membership_not_reachable_subset() {
        // 3-node cluster, only the local node reachable: denominator MUST stay 3.
        let config = config("a", &["a", "b", "c"]);
        let membership = resolve_membership(&config, &reachable(&["a"]));

        assert_eq!(membership.total_nodes, 3, "denominator is full membership");
        assert!(
            membership.send_targets.is_empty(),
            "no reachable peers to propose to"
        );
    }

    #[test]
    fn send_targets_are_reachable_peers_excluding_local() {
        let config = config("a", &["a", "b", "c"]);
        let membership = resolve_membership(&config, &reachable(&["a", "b", "c"]));

        assert_eq!(membership.total_nodes, 3);
        assert_eq!(
            membership.send_targets,
            vec![SyncNodeId::from("b"), SyncNodeId::from("c")],
            "local node is never a send target; peers in config order"
        );
    }

    #[test]
    fn unknown_reachable_names_cannot_inflate_send_targets() {
        let config = config("a", &["a", "b"]);
        // `z` is reachable but not in the configured membership.
        let membership = resolve_membership(&config, &reachable(&["b", "z"]));

        assert_eq!(membership.total_nodes, 2);
        assert_eq!(membership.send_targets, vec![SyncNodeId::from("b")]);
    }

    #[test]
    fn minority_denominator_fences_against_self_quorum_q3() {
        // Re-assert Q3 against the REAL binding: a minority partition (only the
        // local node reachable) sizes quorum from FULL membership (3 → quorum 2),
        // so its lone local ack cannot self-quorum. Sizing from the reachable
        // subset (1 → quorum 1) would let it "win" — the bug this prevents.
        let config = config("c", &["a", "b", "c"]);
        let membership = resolve_membership(&config, &reachable(&["c"]));
        assert_eq!(membership.total_nodes, 3);

        let strong = StrongConsistency::new(membership.total_nodes, Duration::from_millis(5));
        // Local ack only, no remote acks: fenced via timeout (no liveness input).
        let outcome = wait_for_quorum::<SyncNodeId, _>(strong, std::iter::empty());
        assert!(
            matches!(
                outcome,
                Err(ConsistencyError::QuorumTimeout { .. }
                    | ConsistencyError::QuorumUnavailable { .. })
            ),
            "minority must be fenced, got {outcome:?}"
        );
    }
}
