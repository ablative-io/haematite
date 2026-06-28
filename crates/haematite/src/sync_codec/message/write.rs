//! Active-active write-replication message types (single-key and batch).

use std::time::Duration;

use crate::ids::{KvKey, KvValue};
use crate::ids::ShardId;
use crate::sync_codec::ballot::{Ballot, Stamp};
use crate::sync_codec::ids::SyncNodeId;
use crate::tree::Hash;

/// Incarnation-safe correlation id for a single active-active write.
///
/// The id embeds the originating node's restart incarnation (`origin_creation`)
/// so that a slow acknowledgement for a *prior* writer incarnation cannot
/// satisfy a *post-restart* write that happened to reuse the same in-memory
/// `counter`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WriteId {
    pub origin: SyncNodeId,
    pub origin_creation: u32,
    pub counter: u64,
}

impl WriteId {
    #[must_use]
    pub fn new(origin: impl Into<SyncNodeId>, origin_creation: u32, counter: u64) -> Self {
        Self {
            origin: origin.into(),
            origin_creation,
            counter,
        }
    }
}

/// Reason a [`WriteProposal`] was rejected by a receiving replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// The CAS precondition did not match the replica's current value hash.
    CasMismatch,
    /// The replica failed to apply the write for a non-CAS reason.
    ApplyError,
    /// The replica fenced the write: its `promised[shard]` ballot strictly
    /// exceeds the proposal's `epoch` (§2.3), so a stale/deposed owner's write
    /// is rejected. Like [`Self::CasMismatch`] this is a *vote-against*, not a
    /// transport fault — it erodes possible-accepts toward a fence/quorum
    /// failure, never a retryable infrastructure error.
    Fenced,
}

impl RejectReason {
    pub(crate) const fn to_wire(self) -> u8 {
        match self {
            Self::CasMismatch => 0,
            Self::ApplyError => 1,
            Self::Fenced => 2,
        }
    }

    pub(crate) const fn from_wire(value: u8) -> Result<Self, crate::sync_codec::error::SyncError> {
        match value {
            0 => Ok(Self::CasMismatch),
            1 => Ok(Self::ApplyError),
            2 => Ok(Self::Fenced),
            _ => Err(crate::sync_codec::error::SyncError::InvalidMessage),
        }
    }
}

/// Outcome of a receiving replica's conditional apply of a [`WriteProposal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    /// The replica conditionally and durably applied the write.
    Applied,
    /// The replica did not apply the write; the reason is a vote-against.
    Rejected(RejectReason),
}

/// Active-active proposal: a CAS-conditioned write replicated to a peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteProposal {
    pub write_id: WriteId,
    /// The explicit owning shard for this write (mirrors
    /// [`BatchWriteProposal::shard_id`]). A bare single-key proposal would route by
    /// hashing its `key`, but a ROUTED stamped write (the durable-timer primitive,
    /// `Database::replicate_write_routed`) co-locates a physical `key` on a
    /// DIFFERENT key's shard, so the shard is named directly here and the receiver
    /// routes by `shard_id` rather than re-deriving it from `key`. For the ordinary
    /// (non-routed) caller this is exactly `shard_for(&key)`, so the on-disk routing
    /// is byte-identical to the pre-`shard_id` behaviour.
    pub shard_id: ShardId,
    pub key: KvKey,
    /// CAS precondition: the prior value hash (`None` means create-if-absent).
    pub expected: Option<Hash>,
    pub value: KvValue,
    pub ttl: Option<Duration>,
    /// The owner epoch this write is stamped with (§2.3). The receiver fences it
    /// (rejects) iff `epoch < promised[shard]`. With no election the stamp is
    /// [`Ballot::bottom`] and every node's `promised` is also bottom, so the
    /// fence is a no-op (2a sequential semantics preserved unchanged).
    pub epoch: Ballot,
    /// The owner-assigned per-(shard, live-epoch) sequence number (AA-3-4a,
    /// R-SEQ). Drawn ONCE by the owner from an atomic counter and carried here so
    /// EVERY replica stores the identical commit stamp `(epoch, seq)` for this
    /// write (§2.4). With no live election the owner stamps `seq = 0` over the
    /// bottom epoch (2a-compat). The receiver stores `(epoch, seq)` verbatim,
    /// never inventing its own.
    pub seq: u64,
    /// AA-3-4b: when `true`, this proposal is a DELETE — the receiver applies a
    /// stamped TOMBSTONE (not the `value`/`ttl`, which are empty) through the same
    /// fence + CAS + stamp path. `expected` is the hash of the value being deleted
    /// (`None` to delete an absent/tombstoned key). A delete is the one fenced,
    /// stamped, replicated write a put is — there is no second delete path.
    pub tombstone: bool,
}

impl WriteProposal {
    /// The commit stamp `(epoch, seq)` this write carries (AA-3-4a). The receiver
    /// stores THIS stamp verbatim so every replica's copy is byte-identical.
    #[must_use]
    pub fn stamp(&self) -> Stamp {
        Stamp::new(self.epoch.clone(), self.seq)
    }
}

/// Acknowledgement of a [`WriteProposal`] from a receiving replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteAck {
    pub write_id: WriteId,
    pub acker: SyncNodeId,
    pub acker_creation: u32,
    pub outcome: AckOutcome,
}

/// One entry of a [`BatchWriteProposal`] (A1b): a single key's CAS-conditioned
/// put within a replicated multi-key append.
///
/// Mirrors the per-key fields of a [`WriteProposal`] — `key`, the CAS
/// precondition `expected` (`None` = create-if-absent), `value`, and `ttl` — but
/// carries NO per-entry stamp or epoch: the WHOLE batch shares ONE `stamp` on the
/// enclosing [`BatchWriteProposal`] (§2.4), so every key in an applied batch lands
/// the IDENTICAL commit stamp in ONE atomic fsync. The wire-codec encodes the
/// entries length-prefixed; the field order matches
/// [`crate::shard::actor::handle::BatchItem`] so the receiver can hand them
/// straight to `apply_durable_batch` without re-ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchWriteEntry {
    pub key: KvKey,
    /// CAS precondition for THIS key (`None` means create-if-absent). Each entry
    /// has its own precondition; any single mismatch rejects the WHOLE batch.
    pub expected: Option<Hash>,
    pub value: KvValue,
    pub ttl: Option<Duration>,
}

/// Active-active BATCH proposal (A1b): the replicated multi-key analogue of a
/// [`WriteProposal`].
///
/// A whole stream-append's entries (event keys + the sequence key) are proposed as
/// ONE all-or-nothing unit applied through `apply_durable_batch`.
///
/// Routing/metadata mirror the single-key path, with the multi-key
/// generalisations:
///
/// * `shard_id` — the explicit owning shard for the WHOLE batch. A
///   [`WriteProposal`] routes by hashing its single `key`; a batch spans many keys
///   that (by construction at the proposer in A1c, exactly as a stream append's
///   keys do) all map to ONE shard, so the shard is named directly here — the same
///   way election messages ([`Prepare`](crate::sync_codec::Prepare)) name their
///   shard rather than re-deriving it. The receiver routes by `shard_id`.
/// * `entries` — the per-key puts, each its own [`BatchWriteEntry`] (with its own
///   CAS `expected`). May be empty (a no-op batch) or large.
/// * `stamp` — ONE shared commit stamp `(epoch, seq)` for the WHOLE batch (§2.4).
///   Its `epoch` is the fence epoch (the receiver rejects the whole batch iff
///   `epoch < promised[shard]`); its `seq` is the owner-assigned per-(shard,epoch)
///   sequence drawn ONCE. Every replica stores the identical stamp on every key.
/// * `write_id` — the SAME incarnation-safe correlation id a [`WriteProposal`]
///   carries, so the batch ack's incarnation gate matches verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchWriteProposal {
    pub write_id: WriteId,
    pub shard_id: ShardId,
    pub entries: Vec<BatchWriteEntry>,
    /// The ONE commit stamp `(epoch, seq)` shared by every entry (§2.4). The
    /// receiver fences the whole batch iff `stamp.epoch < promised[shard]`, and
    /// stores THIS stamp verbatim on every applied key.
    pub stamp: Stamp,
}

impl BatchWriteProposal {
    /// The whole batch's shared commit stamp `(epoch, seq)`.
    #[must_use]
    pub fn stamp(&self) -> Stamp {
        self.stamp.clone()
    }
}

/// Acknowledgement of a [`BatchWriteProposal`] from a receiving replica (A1b).
///
/// Same shape as [`WriteAck`] (echoes `write_id`, carries `acker`/`acker_creation`,
/// and an [`AckOutcome`]) but for a whole batch. Because `apply_durable_batch` is
/// all-or-nothing, the `outcome` is a single verdict for the ENTIRE batch:
/// [`AckOutcome::Applied`] iff EVERY key was durably applied under the shared
/// stamp, otherwise [`AckOutcome::Rejected`] with the reason a fence
/// ([`RejectReason::Fenced`]) or any single CAS mismatch
/// ([`RejectReason::CasMismatch`]) — in which case NOTHING was written. There is no
/// per-entry partial outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchWriteAck {
    pub write_id: WriteId,
    pub acker: SyncNodeId,
    pub acker_creation: u32,
    pub outcome: AckOutcome,
}
