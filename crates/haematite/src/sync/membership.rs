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
use crate::sync::cluster_members::ClusterMembers;
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
///
/// This is the STATIC-CONFIG path (no durable record). It is retained
/// byte-for-byte and delegates to [`resolve_membership_with_record`] with `None`,
/// so every existing caller and test is unchanged. CSOT-1 introduces the durable
/// override via [`resolve_membership_with_record`].
#[must_use]
pub fn resolve_membership(
    config: &DistributedDatabaseConfig,
    reachable: &BTreeSet<SyncNodeId>,
) -> WriteMembership {
    resolve_membership_with_record(config, None, reachable)
}

/// Resolve `(total_nodes, send_targets)` with an OPTIONAL durable `cluster/members`
/// record taking PRECEDENCE over static config (CSOT-1, task #146).
///
/// Denominator precedence, exactly:
///
/// * `record = Some(r)` ⇒ `total_nodes = r.denominator()` — the durable record
///   WINS. This is the load-bearing #146 cutover: membership becomes durable state.
/// * `record = None` ⇒ `total_nodes = config.nodes.len()` — the fallback is
///   BYTE-IDENTICAL to the pre-CSOT-1 behaviour, so single-node and static-config
///   deployments are unaffected until a record exists.
///
/// `send_targets` is UNCHANGED by the record in CSOT-1: it is still the reachable
/// subset of `config.nodes` (excluding the local node), in configured order with
/// duplicates collapsed. Deriving send targets from the durable record is a later
/// phase (it needs the discovery/address fields CSOT-1 deliberately omits); wiring
/// it now would change send behaviour, which CSOT-1 forbids. The denominator is the
/// only quorum-load-bearing value, and it is the only thing the record overrides.
#[must_use]
pub fn resolve_membership_with_record(
    config: &DistributedDatabaseConfig,
    record: Option<&ClusterMembers>,
    reachable: &BTreeSet<SyncNodeId>,
) -> WriteMembership {
    let total_nodes = record.map_or_else(|| config.nodes.len(), ClusterMembers::denominator);

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

    // --- CSOT-1: durable record precedence (task #146) ---------------------

    use crate::sync::cluster_members::{ClusterMember, ClusterMembers, MemberStatus};

    fn record(cn: &str, epoch: u64, members: &[&str]) -> ClusterMembers {
        ClusterMembers {
            cluster_identity: cn.to_owned(),
            config_epoch: epoch,
            members: members
                .iter()
                .map(|name| ClusterMember::active(SyncNodeId::from(*name)))
                .collect(),
        }
    }

    #[test]
    fn no_record_denominator_is_byte_identical_to_static_config() {
        // GATE (b): with NO durable record, the resolved membership is IDENTICAL to
        // the historical static-config result for the same inputs.
        let config = config("a", &["a", "b", "c"]);
        let reach = reachable(&["a", "b", "c"]);

        let static_only = resolve_membership(&config, &reach);
        let explicit_none = resolve_membership_with_record(&config, None, &reach);

        assert_eq!(
            static_only, explicit_none,
            "None path == static config path"
        );
        assert_eq!(
            static_only.total_nodes, 3,
            "denominator = config.nodes.len()"
        );
    }

    #[test]
    fn present_record_denominator_wins_over_config_nodes_len() {
        // GATE (a): with a record present, the per-write denominator equals the
        // RECORD's value, NOT config.nodes.len(). Here config says 3 but the durable
        // record names 5 members, so quorum must size against 5.
        let config = config("a", &["a", "b", "c"]);
        let rec = record("prod", 4, &["a", "b", "c", "d", "e"]);

        let membership =
            resolve_membership_with_record(&config, Some(&rec), &reachable(&["a", "b", "c"]));

        assert_eq!(
            membership.total_nodes, 5,
            "record denominator (5) wins over config.nodes.len() (3)"
        );
        assert_ne!(
            membership.total_nodes,
            config.nodes.len(),
            "must NOT fall back to static config when a record exists"
        );
    }

    #[test]
    fn single_node_genesis_record_yields_denominator_one()
    -> Result<(), crate::sync::cluster_members::ClusterMembersError> {
        // GATE (c) unit half: a lone genesis record produces a denominator of 1
        // (self-quorum), regardless of a larger static config.
        let config = config("solo", &["solo", "ghost-b", "ghost-c"]);
        let genesis = ClusterMembers::genesis("cluster-solo", SyncNodeId::from("solo"))?;

        let membership =
            resolve_membership_with_record(&config, Some(&genesis), &reachable(&["solo"]));

        assert_eq!(membership.total_nodes, 1, "lone genesis denominator = 1");
        assert_eq!(
            quorum_size(membership.total_nodes),
            Ok(1),
            "self-quorum: one node satisfies its own quorum"
        );
        Ok(())
    }

    #[test]
    fn record_does_not_change_send_targets_in_csot1() {
        // Inertness: the record overrides the DENOMINATOR only; send_targets stay
        // the reachable subset of config.nodes in CSOT-1.
        let config = config("a", &["a", "b", "c"]);
        let rec = record("prod", 1, &["a", "b", "c", "d"]);

        let with_record =
            resolve_membership_with_record(&config, Some(&rec), &reachable(&["a", "b", "c"]));

        assert_eq!(
            with_record.send_targets,
            vec![SyncNodeId::from("b"), SyncNodeId::from("c")],
            "send_targets unchanged by the record"
        );
    }

    #[test]
    fn down_and_joining_members_still_count_in_csot1_denominator() {
        // CSOT-1 is total: every stored member counts, so the denominator equals
        // the static count exactly. (Status-aware exclusion is a CSOT-2 concern.)
        let config = config("a", &["a", "b", "c"]);
        let rec = ClusterMembers {
            cluster_identity: "prod".to_owned(),
            config_epoch: 2,
            members: vec![
                ClusterMember::active(SyncNodeId::from("a")),
                ClusterMember {
                    node_id: SyncNodeId::from("b"),
                    status: MemberStatus::Joining,
                },
                ClusterMember {
                    node_id: SyncNodeId::from("c"),
                    status: MemberStatus::Down,
                },
            ],
        };

        let membership = resolve_membership_with_record(&config, Some(&rec), &reachable(&["a"]));
        assert_eq!(membership.total_nodes, 3, "all statuses count in CSOT-1");
    }

    use crate::sync::consistency::quorum_size;
}
