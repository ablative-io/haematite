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

use crate::db::{Database, DatabaseError};
use crate::shard::actor::ShardError;
use crate::sync::SyncNodeId;
use crate::sync::protocol::{AckOutcome, RejectReason, WriteAck, WriteProposal};

impl Database {
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
