//! CSOT-1: the durable `cluster/members` record — the quorum DENOMINATOR as
//! durable haematite state (task #146, phase 1: inert + safe).
//!
//! This is the *substrate* the #146 design calls the easy, inert part: a durable,
//! versioned member-set record with a cluster identity (`cn`) and a monotonic
//! config epoch. It defines the record SHAPE, its byte (de)serialisation, and a
//! genesis constructor. It deliberately does NOT implement discovery, formation,
//! or membership deltas (later phases CSOT-2..CSOT-5).
//!
//! The load-bearing wiring is [`super::membership::resolve_membership_with_record`]:
//! when a durable record is present its `denominator()` WINS over static
//! `config.nodes`; when absent, behaviour falls back to `config.nodes.len()` and is
//! byte-identical to the pre-CSOT-1 path. See [`super::membership`].
//!
//! Placement: the reserved key [`CLUSTER_MEMBERS_KEY`] routes like any key through
//! `handle_for`, i.e. to the DETERMINISTIC shard for that key — the same shard on
//! every node given the cluster-uniform `shard_count`, so a cross-node reader finds
//! the record under the same key with no directory lookup. (It is NOT literally
//! shard 0; a well-known fixed index would be a CSOT-2 refinement if ever needed.)
//! The persistence round-trip
//! ([`crate::db::Database::write_genesis_cluster_members`] /
//! [`crate::db::Database::read_cluster_members`]) uses the existing durable
//! append/read primitive — no new storage engine.

use crate::sync::topology::SyncNodeId;

/// The reserved store key under which the single `cluster/members` record lives.
///
/// A byte key in the reserved `\0` namespace so it can never collide with a
/// user-supplied event-stream or KV key.
pub const CLUSTER_MEMBERS_KEY: &[u8] = b"\0cluster/members";

/// Lifecycle status of a single member in the cluster.
///
/// CSOT-1 only ever mints [`MemberStatus::Active`] members (genesis). The other
/// variants exist so the record shape already supports the CSOT-2 delta protocol
/// (`Joining`/`Leaving`/`Down`) without a later schema break — they are inert here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MemberStatus {
    /// Not yet caught up; must not inflate the write denominator (CSOT-2).
    Joining,
    /// A full voting member — counts toward the quorum denominator.
    Active,
    /// Gracefully draining; still counted until the final remove delta (CSOT-2).
    Leaving,
    /// Observed down, pending an agreed eviction delta (CSOT-3).
    Down,
}

impl MemberStatus {
    /// Whether a member in this status counts toward the quorum DENOMINATOR.
    ///
    /// CSOT-1 keeps this total: every stored member counts, matching the
    /// static-config denominator exactly (`config.nodes.len()`). The distinction
    /// (e.g. a `Joining` member not yet counting) is a CSOT-2 concern; encoding it
    /// now would change behaviour, which CSOT-1 forbids.
    #[must_use]
    pub const fn counts_toward_denominator(self) -> bool {
        matches!(
            self,
            Self::Joining | Self::Active | Self::Leaving | Self::Down
        )
    }
}

/// One member of the cluster: its identity plus lifecycle status.
///
/// `replication_addr` / `grpc_forward_addr` from the #146 design are DISCOVERY
/// facts (row 2), deliberately NOT modelled here: CSOT-1 is the denominator
/// substrate only. They arrive with CSOT-4 (seed-only bootstrap), keeping this
/// record purely about the quorum member set.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClusterMember {
    /// The member's stable node identity (matches `DistributedDatabaseConfig`).
    pub node_id: SyncNodeId,
    /// Lifecycle status.
    pub status: MemberStatus,
}

impl ClusterMember {
    /// An `Active` member with the given id.
    #[must_use]
    pub const fn active(node_id: SyncNodeId) -> Self {
        Self {
            node_id,
            status: MemberStatus::Active,
        }
    }
}

/// Errors that can arise decoding or validating a durable `cluster/members` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterMembersError {
    /// Serialising the owned, validated record to JSON failed. Not expected in
    /// practice; kept distinct from `Decode` so an encode fault is never mislabelled.
    Encode(String),
    /// The stored bytes were not valid record JSON.
    Decode(String),
    /// The record named zero members: a denominator of 0 is never valid (it would
    /// make `quorum_size` return `InvalidNodeCount`). A live cluster is at least 1.
    EmptyMemberSet,
    /// The same `node_id` appeared more than once in the member list.
    DuplicateMember(String),
    /// The cluster identity (`cn`) was the empty string.
    EmptyClusterIdentity,
}

impl std::fmt::Display for ClusterMembersError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encode(reason) => write!(formatter, "cluster/members encode failed: {reason}"),
            Self::Decode(reason) => write!(formatter, "cluster/members decode failed: {reason}"),
            Self::EmptyMemberSet => formatter.write_str("cluster/members has an empty member set"),
            Self::DuplicateMember(node) => {
                write!(formatter, "cluster/members has duplicate member: {node}")
            }
            Self::EmptyClusterIdentity => {
                formatter.write_str("cluster/members has an empty cluster identity (cn)")
            }
        }
    }
}

impl std::error::Error for ClusterMembersError {}

/// The authoritative, durable member set — the quorum DENOMINATOR as state.
///
/// Value shape (per #146 §2.1): an ordered, versioned member list with a cluster
/// identity (`cn`) and a monotonic config epoch. The epoch increments on every
/// committed membership delta (CSOT-2+); at genesis it is `0`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClusterMembers {
    /// Cluster identity — a stable name for THIS cluster, so a record from a
    /// different cluster can never be mistaken for ours. Design field `cn`.
    #[serde(rename = "cn")]
    pub cluster_identity: String,
    /// Monotonic CONFIG epoch (distinct from a per-shard `owner_epoch`). Increments
    /// on every committed membership delta; `0` at genesis.
    pub config_epoch: u64,
    /// The ordered member list. Order is preserved as written (deterministic).
    pub members: Vec<ClusterMember>,
}

impl ClusterMembers {
    /// Construct the single-node GENESIS record: a lone `Active` member at config
    /// epoch 0. This is the denominator-1, self-quorum record a fresh lone node
    /// writes for itself (#146 §4.2 step 2) — already safe today.
    ///
    /// Returns [`ClusterMembersError::EmptyClusterIdentity`] if `cluster_identity`
    /// is empty.
    pub fn genesis(
        cluster_identity: impl Into<String>,
        node_id: SyncNodeId,
    ) -> Result<Self, ClusterMembersError> {
        let record = Self {
            cluster_identity: cluster_identity.into(),
            config_epoch: 0,
            members: vec![ClusterMember::active(node_id)],
        };
        record.validate()?;
        Ok(record)
    }

    /// The quorum DENOMINATOR this record contributes — the number of members that
    /// count toward quorum. In CSOT-1 every stored member counts, so this equals
    /// `members.len()` and matches the static-config denominator exactly.
    #[must_use]
    pub fn denominator(&self) -> usize {
        self.members
            .iter()
            .filter(|member| member.status.counts_toward_denominator())
            .count()
    }

    /// Validate the record's invariants (non-empty identity, non-empty deduped
    /// member set). Called on construct and after decode so a malformed durable
    /// record can never silently become a bad denominator.
    pub fn validate(&self) -> Result<(), ClusterMembersError> {
        if self.cluster_identity.is_empty() {
            return Err(ClusterMembersError::EmptyClusterIdentity);
        }
        if self.members.is_empty() {
            return Err(ClusterMembersError::EmptyMemberSet);
        }
        let mut seen = std::collections::BTreeSet::new();
        for member in &self.members {
            if !seen.insert(member.node_id.clone()) {
                return Err(ClusterMembersError::DuplicateMember(
                    member.node_id.as_str().to_owned(),
                ));
            }
        }
        Ok(())
    }

    /// Serialise the record to durable bytes (canonical JSON).
    ///
    /// Returns [`ClusterMembersError::Encode`] only if serialisation fails, which
    /// for this owned, already-validated struct is not expected in practice.
    pub fn encode(&self) -> Result<Vec<u8>, ClusterMembersError> {
        serde_json::to_vec(self).map_err(|error| ClusterMembersError::Encode(error.to_string()))
    }

    /// Decode durable bytes back into a validated record.
    ///
    /// Both the JSON parse and the [`Self::validate`] invariants are enforced, so a
    /// truncated or malformed record is rejected rather than yielding a bad
    /// denominator.
    pub fn decode(bytes: &[u8]) -> Result<Self, ClusterMembersError> {
        let record: Self = serde_json::from_slice(bytes)
            .map_err(|error| ClusterMembersError::Decode(error.to_string()))?;
        record.validate()?;
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    fn node(name: &str) -> SyncNodeId {
        SyncNodeId::from(name)
    }

    #[test]
    fn genesis_is_single_active_member_at_epoch_zero() -> Result<(), Box<dyn Error>> {
        let record = ClusterMembers::genesis("cluster-x", node("a"))?;
        assert_eq!(record.config_epoch, 0);
        assert_eq!(record.denominator(), 1, "lone node self-quorum denominator");
        assert_eq!(record.members, vec![ClusterMember::active(node("a"))]);
        assert_eq!(record.cluster_identity, "cluster-x");
        Ok(())
    }

    #[test]
    fn genesis_rejects_empty_cluster_identity() {
        assert_eq!(
            ClusterMembers::genesis("", node("a")),
            Err(ClusterMembersError::EmptyClusterIdentity)
        );
    }

    #[test]
    fn encode_decode_round_trips_a_multi_member_record() -> Result<(), Box<dyn Error>> {
        let record = ClusterMembers {
            cluster_identity: "prod".to_owned(),
            config_epoch: 7,
            members: vec![
                ClusterMember::active(node("a")),
                ClusterMember {
                    node_id: node("b"),
                    status: MemberStatus::Joining,
                },
                ClusterMember {
                    node_id: node("c"),
                    status: MemberStatus::Down,
                },
            ],
        };
        let bytes = record.encode()?;
        let decoded = ClusterMembers::decode(&bytes)?;
        assert_eq!(decoded, record, "byte round-trip is lossless");
        assert_eq!(decoded.denominator(), 3, "all statuses count in CSOT-1");
        Ok(())
    }

    #[test]
    fn decode_rejects_garbage_bytes() {
        let error = ClusterMembers::decode(b"not json");
        assert!(matches!(error, Err(ClusterMembersError::Decode(_))));
    }

    #[test]
    fn decode_rejects_empty_member_set() -> Result<(), Box<dyn Error>> {
        let bytes = serde_json::to_vec(&serde_json::json!({
            "cn": "prod",
            "config_epoch": 0,
            "members": [],
        }))?;
        assert_eq!(
            ClusterMembers::decode(&bytes),
            Err(ClusterMembersError::EmptyMemberSet)
        );
        Ok(())
    }

    #[test]
    fn validate_rejects_duplicate_members() {
        let record = ClusterMembers {
            cluster_identity: "prod".to_owned(),
            config_epoch: 0,
            members: vec![
                ClusterMember::active(node("dup")),
                ClusterMember::active(node("dup")),
            ],
        };
        assert!(matches!(
            record.validate(),
            Err(ClusterMembersError::DuplicateMember(node)) if node == "dup"
        ));
    }

    #[test]
    fn cluster_identity_serialises_as_cn() -> Result<(), Box<dyn Error>> {
        let record = ClusterMembers::genesis("ident", node("a"))?;
        let json: serde_json::Value = serde_json::from_slice(&record.encode()?)?;
        assert_eq!(
            json.get("cn").and_then(serde_json::Value::as_str),
            Some("ident")
        );
        Ok(())
    }
}
