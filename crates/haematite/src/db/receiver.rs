//! Receiver-side conditional-durable-apply-then-ack (active-active 2a-4).
//!
//! This is the HARDEST, most correctness-critical half of quorum-on-write: a
//! replica must apply an incoming [`WriteProposal`] **conditionally** (a CAS on
//! the current value hash) and **durably** (fsynced to stable storage) BEFORE it
//! returns a [`WriteAck`] of [`AckOutcome::Applied`]. If it acked a non-durable
//! page-cache write, a crash would lose data the writer already counted toward its
//! quorum — silently breaking the whole guarantee.
//!
//! The apply logic lives on [`Database`] (which owns the shard store/router); the
//! [`DistributionEndpoint`](crate::sync::endpoint::DistributionEndpoint) stays
//! transport-only. The durability is provided by routing the apply through the
//! owning shard's `apply_durable` command, which performs the CAS read, the
//! `put`, and a tree `commit` in ONE actor slice — and `commit` fsyncs the tree
//! nodes to the `DiskStore` and writes a fsynced committed-root marker into the
//! WAL. So the `Ok` returned by the shard attests durability, and the `Applied`
//! ack is sequenced strictly AFTER it.

use std::time::Duration;

use crate::api::kv::{KvKey, KvValue};
use crate::db::{Database, DatabaseError};
use crate::shard::actor::ShardError;
use crate::sync::membership::WriteMembership;
use crate::sync::{QuorumOutcome, SyncNodeId};
use crate::sync::protocol::{AckOutcome, RejectReason, WriteAck, WriteProposal};
use crate::tree::Hash;

impl Database {
    /// Coordinate one Strong CAS write to quorum across the cluster AND, on
    /// commit, durably persist the proposer's own committed value LOCALLY.
    ///
    /// This is the proposer-side commit path for active-active quorum-on-write. It
    /// closes the gap that a bare [`propose_write`](crate::sync::DistributionEndpoint::propose_write) leaves:
    /// `propose_write` counts the proposer's local ack toward quorum but does NOT
    /// apply the value on the proposer (it is transport-only). A "committed" write
    /// that is absent on its own writer is a correctness hazard — a later stale CAS
    /// *create* (`expected = None`) would MATCH on the un-applied proposer and
    /// apply, reopening the heal-mid-write split-brain hole.
    ///
    /// Sequencing (apply STRICTLY after quorum success, never before):
    ///
    /// 1. Drive the write to peer-quorum via
    ///    [`propose_write`](crate::sync::DistributionEndpoint::propose_write). Peers
    ///    durably apply + ack; the proposer's local ack is counted as a phantom by
    ///    the tally (no local apply yet).
    /// 2. **On `Ok` (quorum reached):** durably CAS-apply the value locally through
    ///    the SAME owning-shard `apply_durable` path the receiver uses — the same
    ///    `expected` precondition the cluster just agreed on. On local `Applied` →
    ///    return the quorum outcome. On a local CAS mismatch or apply fault → return
    ///    [`DatabaseError::LocalCommitFailed`] (reported, never swallowed).
    /// 3. **On `Err` (Fenced / `QuorumTimeout` / transport):** apply NOTHING locally
    ///    and return the error mapped to [`DatabaseError::ConsistencyError`]. Because
    ///    the local apply happens only on success, a fenced or timed-out write leaves
    ///    no local state to roll back.
    ///
    /// # Safety boundary — sequential vs concurrent (do NOT overclaim)
    ///
    /// This makes the proposer's committed value durable, which closes
    /// **SEQUENTIAL** conflicting-write safety: the heal-mid-write scenario where a
    /// partitioned proposer re-proposes AFTER the majority has fully committed is
    /// correctly fenced (the majority nodes already hold the value, so the stale
    /// CAS create is rejected). It does **NOT** by itself close the
    /// **CONCURRENT-proposer** window: between a proposer reaching peer-quorum
    /// (step 1) and applying locally (step 2), a concurrent conflicting proposer
    /// could still observe the proposer un-applied and win a second quorum. That
    /// window is closed by the step-3 epoch fence / single-owner-per-shard
    /// (`AcquireShard`), under which there is never more than one proposer per key.
    /// The 2a-5 end-to-end test proves only the SEQUENTIAL property.
    ///
    /// # Errors
    /// Returns [`DatabaseError::ConsistencyError`] when the write does not reach
    /// quorum (fenced, timed out, or the transport was unavailable), or
    /// [`DatabaseError::LocalCommitFailed`] when quorum was reached but the local
    /// durable commit failed.
    pub fn replicate_write(
        &self,
        key: KvKey,
        expected: Option<Hash>,
        value: KvValue,
        ttl: Option<Duration>,
        membership: &WriteMembership,
        timeout: Duration,
    ) -> Result<QuorumOutcome<SyncNodeId>, DatabaseError> {
        let endpoint = self.distribution().ok_or_else(|| {
            DatabaseError::Distribution("no distribution endpoint for replicate_write".to_owned())
        })?;

        // Step 1: drive the write to peer-quorum. Clone key/value because the local
        // durable apply in step 2 needs them again.
        let outcome = endpoint
            .propose_write(key.clone(), expected, value.clone(), ttl, membership, timeout)
            .map_err(|error| DatabaseError::ConsistencyError(error.to_string()))?;

        // Step 2: quorum reached — durably persist the proposer's own committed
        // value locally via the SAME conditional-durable apply the receiver runs.
        let handle = self
            .handle_for(&key)
            .map_err(|error| DatabaseError::LocalCommitFailed(error.to_string()))?;
        match handle.apply_durable(key, expected, value, ttl, self.timeout()) {
            Ok(()) => Ok(outcome),
            // Any failure here is surfaced loudly, never swallowed. A local CAS
            // mismatch (`ShardError::CasHashMismatch`) would mean another writer
            // raced this key locally — impossible under single-owner-per-key — so a
            // mismatch signals a violated invariant just as an IO fault signals a
            // storage failure; both are a failed durable local commit.
            Err(error) => Err(DatabaseError::LocalCommitFailed(error.to_string())),
        }
    }

    /// Conditionally + durably apply an inbound [`WriteProposal`] and produce the
    /// [`WriteAck`] to return to the originating writer.
    ///
    /// Logic, exactly (design Fix B + Fix C):
    ///
    /// 1. **CAS compare (Fix C):** the owning shard reads the current value hash
    ///    for `proposal.key` and compares it to `proposal.expected` (`None` =
    ///    expect-absent). On MISMATCH nothing is applied and the ack is
    ///    <code>[AckOutcome::Rejected]([RejectReason::CasMismatch])</code> — a
    ///    vote-against that fences a stale heal-mid-write proposal.
    /// 2. **On MATCH (Fix B):** the shard applies `key=value` (with `ttl`) and
    ///    fsyncs it to stable storage; only AFTER the durable apply returns is the
    ///    ack [`AckOutcome::Applied`].
    /// 3. **On apply error** (IO etc., not a CAS mismatch): the ack is
    ///    <code>[AckOutcome::Rejected]([RejectReason::ApplyError])</code>.
    ///
    /// The returned `WriteAck` echoes `proposal.write_id` UNCHANGED (so the
    /// writer's incarnation gate matches), and carries this node's distribution
    /// name + creation as `acker`/`acker_creation`.
    ///
    /// The CAS read, the apply, and the fsync all execute in a single shard-actor
    /// slice, so there is no interleaving point between the compare and the write.
    ///
    /// Requires [`Database::with_distribution`] to have installed an endpoint
    /// (the ack needs this node's distribution identity).
    #[must_use]
    pub fn apply_write_proposal(&self, proposal: &WriteProposal) -> WriteAck {
        let (acker, acker_creation) = self.acker_identity();
        let outcome = self.apply_proposal_durably(proposal);
        WriteAck {
            write_id: proposal.write_id.clone(),
            acker,
            acker_creation,
            outcome,
        }
    }

    /// Apply an inbound [`WriteProposal`] and SEND the resulting [`WriteAck`] back
    /// to the originating writer over the live distribution transport.
    ///
    /// This is the single-call receiver path the inbound drain drives: it applies
    /// (conditionally + durably) and routes the ack to `proposal.write_id.origin`.
    ///
    /// # Errors
    /// Returns a [`DatabaseError`] only if the ack could not be SENT (no endpoint,
    /// or a transport failure). A CAS mismatch or apply fault is NOT an error here
    /// — it is carried as the ack `outcome` and delivered to the writer as a
    /// vote-against, which is the whole point of the CAS fence.
    pub fn handle_inbound_write(&self, proposal: &WriteProposal) -> Result<(), DatabaseError> {
        let ack = self.apply_write_proposal(proposal);
        let origin = proposal.write_id.origin.as_str().to_owned();
        self.send_sync_message(&origin, &crate::sync::SyncMessage::WriteAck(ack))
    }

    /// Run the conditional-durable apply against the owning shard and classify the
    /// result into an [`AckOutcome`].
    fn apply_proposal_durably(&self, proposal: &WriteProposal) -> AckOutcome {
        let handle = match self.handle_for(&proposal.key) {
            Ok(handle) => handle,
            Err(_error) => return AckOutcome::Rejected(RejectReason::ApplyError),
        };
        match handle.apply_durable(
            proposal.key.clone(),
            proposal.expected,
            proposal.value.clone(),
            proposal.ttl,
            self.timeout(),
        ) {
            Ok(()) => AckOutcome::Applied,
            // A CAS hash mismatch is a vote-against, not a fault: the replica is
            // ahead and applied nothing.
            Err(ShardError::CasHashMismatch { .. }) => {
                AckOutcome::Rejected(RejectReason::CasMismatch)
            }
            // Any other shard error (IO, timeout, unavailable, WAL/tree fault) is a
            // genuine apply error.
            Err(_other) => AckOutcome::Rejected(RejectReason::ApplyError),
        }
    }

    /// This node's distribution identity for an ack: `(acker, acker_creation)`.
    ///
    /// When no endpoint is attached (single-process tests of the apply path), a
    /// placeholder name and creation `0` are used; the durability + CAS semantics
    /// are independent of identity, and the real round-trip test always attaches an
    /// endpoint.
    fn acker_identity(&self) -> (SyncNodeId, u32) {
        self.distribution().map_or_else(
            || (SyncNodeId::new(String::new()), 0),
            |endpoint| {
                (
                    SyncNodeId::new(endpoint.local_name().to_owned()),
                    endpoint.local_creation(),
                )
            },
        )
    }
}

/// Drain inbound [`WriteProposal`]s from the attached endpoint and apply+ack each,
/// until `timeout` elapses with no further proposal.
///
/// This is a simple synchronous responder loop the real two-endpoint test drives
/// on a dedicated thread, replacing the 2a-3 stub acker with the real apply. Every
/// non-`WriteProposal` inbound message is ignored (it belongs to other protocol
/// paths). A drain disconnect (endpoint torn down) ends the loop cleanly.
///
/// # Errors
/// Returns a [`DatabaseError`] only if receiving from the drain fails for a reason
/// other than a plain timeout.
pub fn respond_to_inbound_writes(
    database: &Database,
    timeout: Duration,
) -> Result<(), DatabaseError> {
    use crate::sync::SyncMessage;

    loop {
        match database.recv_sync_message(timeout)? {
            Some(Ok(SyncMessage::WriteProposal(proposal))) => {
                // A send failure here (e.g. the writer already returned) is not
                // fatal to the responder; keep draining.
                drop(database.handle_inbound_write(&proposal));
            }
            // Other inbound messages are not this responder's concern.
            Some(_) => {}
            // Timed out with no proposal: the caller decides whether to loop again.
            None => return Ok(()),
        }
    }
}
