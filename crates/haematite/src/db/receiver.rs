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

use crate::api::event_store::encode_event_value;
use crate::api::kv::{KvKey, KvValue};
use crate::branch::{ShardId, current_timestamp};
use crate::db::helpers::{event_range_start, event_sequence_key, map_shard_error};
use crate::db::{Database, DatabaseError};
use crate::shard::actor::{PromiseState, RecordPromiseOutcome, ShardError};
use crate::sync::ballot::Ballot;
use crate::sync::endpoint::{ElectionError, ElectionOutcome};
use crate::sync::membership::WriteMembership;
use crate::sync::{QuorumOutcome, SyncNodeId};
use crate::sync::protocol::{
    AckOutcome, BatchWriteAck, BatchWriteEntry, BatchWriteProposal, Nack, Prepare, Promise,
    RejectReason, WriteAck, WriteProposal,
};
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

        // R-LE + R-SEQ (AA-3-4a, §2.4): draw the commit stamp `(live_epoch, seq)`
        // from this shard's IN-MEMORY serve-authority. `live_epoch` is set ONLY by
        // a successful `acquire_shard` THIS process lifetime — NOT the disk
        // `owner_epoch` — so a node that recovered `owner_epoch = e'` from disk but
        // did not re-acquire stamps `bottom`, never `e'` (the duplicate-stamp bug
        // R-LE prevents). With no live election the stamp is `(bottom, seq)`: every
        // node's `promised` is also bottom, so `bottom >= bottom` accepts and the
        // fence is a no-op (2a sequential semantics preserved). `seq` is one atomic
        // fetch_add — no TOCTOU (R-SEQ). The SAME stamp goes to the peers (on the
        // WriteProposal) and to the local apply, so every replica stores it
        // identically. We do NOT gate on "am I owner"; the receiver fence is the
        // enforcement, and gating here would break 2a back-compat.
        let shard = self.shard_for(&key);
        let stamp = self.owner_stamps.next_stamp(shard);

        // Step 1: drive the write to peer-quorum. Clone key/value because the local
        // durable apply in step 2 needs them again.
        let outcome = endpoint
            .propose_write_stamped(
                key.clone(),
                expected,
                value.clone(),
                ttl,
                stamp.clone(),
                false,
                membership,
                timeout,
            )
            .map_err(|error| DatabaseError::ConsistencyError(error.to_string()))?;

        // Step 2: quorum reached — durably persist the proposer's own committed
        // value locally via the SAME conditional-durable apply the receiver runs,
        // stamped with the SAME epoch the cluster just accepted.
        let handle = self
            .handle_for(&key)
            .map_err(|error| DatabaseError::LocalCommitFailed(error.to_string()))?;
        // The proposer applies the IDENTICAL stamp it put on the WriteProposal, so
        // its local copy matches every peer's (§2.4).
        match handle.apply_durable(key, expected, value, ttl, stamp, self.timeout()) {
            Ok(()) => Ok(outcome),
            // Any failure here is surfaced loudly, never swallowed. A local CAS
            // mismatch (`ShardError::CasHashMismatch`) would mean another writer
            // raced this key locally — impossible under single-owner-per-key — so a
            // mismatch signals a violated invariant just as an IO fault signals a
            // storage failure; both are a failed durable local commit.
            Err(error) => Err(DatabaseError::LocalCommitFailed(error.to_string())),
        }
    }

    /// Coordinate one Strong CAS DELETE to quorum across the cluster AND, on
    /// commit, durably persist the proposer's own stamped tombstone LOCALLY
    /// (AA-3-4b, §2.4).
    ///
    /// A delete is the SAME fenced + stamped + quorum-replicated write a put is —
    /// there is no second delete path. It mirrors [`Self::replicate_write`] exactly
    /// (stamp from `owner_stamps.next_stamp`, quorum via the SAME proposal flow,
    /// then the proposer's own durable apply with the IDENTICAL stamp) but the
    /// receiver and the proposer store a stamped TOMBSTONE instead of a value:
    ///
    /// * `expected` is the hash of the value being deleted (`None` to delete an
    ///   absent / already-tombstoned key; create-if-absent semantics apply because
    ///   a tombstone reads as `None`).
    /// * A delete from a stale/deposed owner is `Fenced` (rejected) by the receiver
    ///   exactly like a put, surfaced as a quorum failure to the caller.
    ///
    /// # Errors
    /// As [`Self::replicate_write`]: [`DatabaseError::ConsistencyError`] when the
    /// delete does not reach quorum (fenced/timed-out/transport), or
    /// [`DatabaseError::LocalCommitFailed`] when quorum was reached but the local
    /// durable tombstone commit failed.
    pub fn replicate_delete(
        &self,
        key: KvKey,
        expected: Option<Hash>,
        membership: &WriteMembership,
        timeout: Duration,
    ) -> Result<QuorumOutcome<SyncNodeId>, DatabaseError> {
        let endpoint = self.distribution().ok_or_else(|| {
            DatabaseError::Distribution("no distribution endpoint for replicate_delete".to_owned())
        })?;

        // R-LE + R-SEQ: a delete draws the SAME `(live_epoch, seq)` stamp a put
        // does (§2.4) — it is a comparable, mergeable, stamped entry.
        let shard = self.shard_for(&key);
        let stamp = self.owner_stamps.next_stamp(shard);

        // Step 1: drive the tombstone to peer-quorum (empty value/ttl; the
        // `tombstone` flag tells each receiver to store a stamped tombstone).
        let outcome = endpoint
            .propose_write_stamped(
                key.clone(),
                expected,
                Vec::new(),
                None,
                stamp.clone(),
                true,
                membership,
                timeout,
            )
            .map_err(|error| DatabaseError::ConsistencyError(error.to_string()))?;

        // Step 2: quorum reached — durably persist the proposer's own tombstone
        // with the IDENTICAL stamp the cluster accepted.
        let handle = self
            .handle_for(&key)
            .map_err(|error| DatabaseError::LocalCommitFailed(error.to_string()))?;
        match handle.apply_durable_tombstone(key, expected, stamp, self.timeout()) {
            Ok(()) => Ok(outcome),
            Err(error) => Err(DatabaseError::LocalCommitFailed(error.to_string())),
        }
    }

    /// Coordinate ONE replicated, all-or-nothing multi-key STREAM APPEND to quorum
    /// across the cluster AND, on commit, durably apply the proposer's own batch
    /// LOCALLY (A1c — the multi-key analogue of [`Self::replicate_write`]).
    ///
    /// `payloads` are appended in order to `stream_key` starting at `expected_seq`
    /// (0-based, matching [`crate::api::event_store::EventStore::append`]'s
    /// `expected_seq` contract). Returns the stream's new next-seq
    /// (`expected_seq + payloads.len()`) on success.
    ///
    /// Sequencing (apply STRICTLY after quorum, never before), mirroring
    /// [`Self::replicate_write`]:
    ///
    /// a. **Owner-local OCC pre-check.** Read the stream's current next-seq; if it
    ///    is not `expected_seq`, return [`DatabaseError::SequenceConflict`] WITHOUT
    ///    proposing anything (the SAME contract `append` honours). An absent stream
    ///    reads as next-seq `0`.
    /// b. **Build the byte-identical batch entries.** One write-once event put per
    ///    payload — `(event_key, expected = None, encode_event_value(ts, payload))` —
    ///    PLUS the sequence-counter put. The event value uses the SAME
    ///    `encode_event_value` the local `append` path uses, under ONE timestamp, so
    ///    every replica stores the IDENTICAL logical value and the existing
    ///    `EventStore::read` decodes it after replication.
    /// c. **Sequence-counter CAS (the OCC decision).** The counter entry's `expected`
    ///    is `Some(Hash::of(expected_seq.to_be_bytes()))` when `expected_seq > 0`
    ///    (the counter already exists, holding the BE-encoded `expected_seq`), and
    ///    `None` when `expected_seq == 0` (a fresh stream's counter is absent, so a
    ///    create-if-absent CAS is the only one that matches).
    ///
    ///    Why NOT `expected = None` unconditionally (the obvious "lean"): the batch
    ///    CAS in `apply_durable_batch` is over the LOGICAL value hash, and `None`
    ///    means create-if-absent. On every append PAST the first the counter EXISTS,
    ///    so `expected = None` would MISMATCH the present counter and reject EVERY
    ///    valid append — a correctness/liveness bug, not merely an availability one.
    ///
    ///    Why this CAS is safe AND does not erode availability: the epoch fence in
    ///    `apply_durable_batch` (`stamp.epoch < promised` ⇒ reject the whole batch)
    ///    already guarantees only the single live owner can commit, and step (a)
    ///    already guarantees the OWNER's counter equals `expected_seq`. A correctly-
    ///    replicated in-quorum replica only ever applied THIS owner's stamped
    ///    batches, so its counter is also `expected_seq` and `Hash::of(its bytes)`
    ///    matches — it accepts. A replica that legitimately MISSED a prior batch has
    ///    a different counter and SHOULD reject (accepting would silently create an
    ///    event-sequence gap / overwrite). So the counter CAS rejects exactly the
    ///    replicas that must reject; it is the guard against partial-replication
    ///    gaps, complementing (not duplicating) the fence. The event keys are
    ///    write-once, so they carry `expected = None` (create-if-absent) — a re-sent
    ///    identical batch is idempotent on an already-applied event key only if the
    ///    counter also matches, which all-or-nothing enforces.
    /// d. **Draw ONE stamp, drive quorum, then apply locally.** Draw a single
    ///    `(epoch, seq)` stamp from `owner_stamps.next_stamp(shard)`,
    ///    `propose_batch_stamped` it to quorum, and on quorum-success durably apply
    ///    the IDENTICAL batch + stamp locally via `apply_durable_batch`. A quorum
    ///    failure surfaces as [`DatabaseError::ConsistencyError`]; a failed local
    ///    commit as [`DatabaseError::LocalCommitFailed`] — exactly as
    ///    [`Self::replicate_write`].
    ///
    /// # Errors
    /// [`DatabaseError::SequenceConflict`] (stale `expected_seq`, nothing proposed),
    /// [`DatabaseError::ConsistencyError`] (the batch did not reach quorum — fenced,
    /// timed out, or transport unavailable), or [`DatabaseError::LocalCommitFailed`]
    /// (quorum reached but the proposer's own durable batch commit failed).
    // `stream_key`/`payloads` are taken by value to match the owned-argument
    // ergonomics of the `append` family (`EventStore::append`, `Database::append`)
    // and `replicate_write`'s owned `key` — the caller builds owned event payloads.
    // They are addressed by reference internally (the key is reused for every event
    // key + the counter; payloads are encoded in place), so neither is moved.
    #[allow(clippy::needless_pass_by_value)]
    pub fn replicate_append(
        &self,
        stream_key: Vec<u8>,
        payloads: Vec<Vec<u8>>,
        expected_seq: u64,
        membership: &WriteMembership,
        timeout: Duration,
    ) -> Result<u64, DatabaseError> {
        let endpoint = self.distribution().ok_or_else(|| {
            DatabaseError::Distribution("no distribution endpoint for replicate_append".to_owned())
        })?;

        // Step a: OWNER-LOCAL OCC pre-check. An absent stream reads next-seq 0. On a
        // mismatch we return SequenceConflict and propose NOTHING — the SAME contract
        // `append` honours, so a stale caller never half-replicates a batch.
        let actual_seq = self.read_stream_next_seq(&stream_key)?.unwrap_or(0);
        if actual_seq != expected_seq {
            return Err(DatabaseError::SequenceConflict {
                expected: expected_seq,
                actual: actual_seq,
            });
        }

        // An empty batch is a no-op append (matches `append`): next-seq is unchanged
        // and nothing is proposed or committed.
        if payloads.is_empty() {
            return Ok(expected_seq);
        }

        let entry_count = u64::try_from(payloads.len()).map_err(|_| {
            DatabaseError::ConsistencyError("too many append entries".to_owned())
        })?;
        let new_seq = expected_seq.checked_add(entry_count).ok_or_else(|| {
            DatabaseError::ConsistencyError("append sequence overflow".to_owned())
        })?;

        // Step b: build the byte-identical batch. ONE timestamp for the whole batch,
        // exactly as `append_batch_with_ttl` uses one `current_timestamp()`.
        let timestamp = current_timestamp();
        let mut entries = Vec::with_capacity(payloads.len().saturating_add(1));
        for (offset, payload) in payloads.iter().enumerate() {
            // The engine stores the first event at engine-key seq 1 (1-based); the
            // API `expected_seq` is 0-based, so event i lands at engine seq
            // `expected_seq + i + 1` — the SAME mapping the local `append` uses
            // (`actual.checked_add(offset + 1)`).
            let offset = u64::try_from(offset).map_err(|_| {
                DatabaseError::ConsistencyError("too many append entries".to_owned())
            })?;
            let engine_seq = expected_seq
                .checked_add(offset)
                .and_then(|seq| seq.checked_add(1))
                .ok_or_else(|| {
                    DatabaseError::ConsistencyError("append sequence overflow".to_owned())
                })?;
            entries.push(BatchWriteEntry {
                key: event_range_start(&stream_key, engine_seq),
                // Event keys are WRITE-ONCE: create-if-absent.
                expected: None,
                value: encode_event_value(timestamp, payload),
                ttl: None,
            });
        }

        // Step c: the sequence-counter put with the OCC CAS decided above.
        let seq_expected = if expected_seq == 0 {
            // Fresh stream: the counter is absent → create-if-absent.
            None
        } else {
            // Existing stream: the counter holds the BE-encoded `expected_seq`. CAS
            // on its logical value hash so a replica missing a prior batch rejects.
            Some(Hash::of(&expected_seq.to_be_bytes()))
        };
        entries.push(BatchWriteEntry {
            key: event_sequence_key(&stream_key),
            expected: seq_expected,
            value: new_seq.to_be_bytes().to_vec(),
            ttl: None,
        });

        // Step d: draw ONE stamp (R-LE + R-SEQ, as `replicate_write`), drive the
        // WHOLE batch to peer-quorum, then on success durably apply it locally with
        // the IDENTICAL stamp every replica accepted (§2.4).
        let shard = self.shard_for(&stream_key);
        let stamp = self.owner_stamps.next_stamp(shard);

        endpoint
            .propose_batch_stamped(shard, entries.clone(), stamp.clone(), membership, timeout)
            .map_err(|error| DatabaseError::ConsistencyError(error.to_string()))?;

        let handle = self
            .handle_for_shard(shard)
            .map_err(|error| DatabaseError::LocalCommitFailed(error.to_string()))?;
        let items = entries
            .into_iter()
            .map(|entry| (entry.key, entry.expected, entry.value, entry.ttl))
            .collect();
        match handle.apply_durable_batch(items, stamp, self.timeout()) {
            Ok(()) => Ok(new_seq),
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
        // R-SEQ: store the OWNER-ASSIGNED stamp `(epoch, seq)` verbatim — this
        // replica never invents its own seq, so every replica's stored stamp for
        // this write is byte-identical (§2.4 merge precondition). AA-3-4b: a
        // `tombstone` proposal applies a stamped tombstone through the SAME fence +
        // CAS path; otherwise a stamped value.
        let result = if proposal.tombstone {
            handle.apply_durable_tombstone(
                proposal.key.clone(),
                proposal.expected,
                proposal.stamp(),
                self.timeout(),
            )
        } else {
            handle.apply_durable(
                proposal.key.clone(),
                proposal.expected,
                proposal.value.clone(),
                proposal.ttl,
                proposal.stamp(),
                self.timeout(),
            )
        };
        match result {
            Ok(()) => AckOutcome::Applied,
            // A CAS hash mismatch is a vote-against, not a fault: the replica is
            // ahead and applied nothing.
            Err(ShardError::CasHashMismatch { .. }) => {
                AckOutcome::Rejected(RejectReason::CasMismatch)
            }
            // An epoch fence is ALSO a vote-against (§2.3): this replica promised a
            // higher ballot, so the proposer is a stale/deposed owner. It must
            // erode possible-accepts toward a fence/quorum failure exactly like a
            // CAS mismatch — NOT a transport fault — so the deposed owner's write
            // surfaces to its caller as a fence, never a false success.
            Err(ShardError::Fenced { .. }) => AckOutcome::Rejected(RejectReason::Fenced),
            // Any other shard error (IO, timeout, unavailable, WAL/tree fault) is a
            // genuine apply error.
            Err(_other) => AckOutcome::Rejected(RejectReason::ApplyError),
        }
    }

    /// Conditionally + durably apply an inbound [`BatchWriteProposal`] ALL-OR-
    /// NOTHING and produce the [`BatchWriteAck`] to return to the originating writer
    /// (A1b — the receiver half of a replicated multi-key append).
    ///
    /// The batch analogue of [`Self::apply_write_proposal`]: the proposal names ONE
    /// owning `shard_id` (a batch spans many keys that all map to one shard, exactly
    /// as a stream append's keys do), the entries are handed straight to that shard's
    /// `apply_durable_batch`, and the SINGLE verdict is returned. `apply_durable_batch`
    /// is atomic — it fences once, runs EVERY per-key CAS, then commits ALL keys in
    /// one fsync, or writes NOTHING — so:
    ///
    /// * Every key durably applied under the shared `stamp` → [`AckOutcome::Applied`].
    /// * The shard fenced the batch (`stamp.epoch < promised[shard]`) → nothing
    ///   written, ack [`AckOutcome::Rejected`]([`RejectReason::Fenced`]).
    /// * Any single key's CAS precondition mismatched → nothing written, ack
    ///   [`AckOutcome::Rejected`]([`RejectReason::CasMismatch`]).
    /// * Any other apply fault (routing/IO/timeout) → nothing written, ack
    ///   [`AckOutcome::Rejected`]([`RejectReason::ApplyError`]).
    ///
    /// A fence or any CAS mismatch rejects the WHOLE batch; the ack is NEVER a false
    /// accept, because `apply_durable_batch` guarantees nothing was written on either
    /// rejection. The returned ack echoes `proposal.write_id` UNCHANGED and carries
    /// this node's `acker`/`acker_creation`.
    #[must_use]
    pub fn apply_batch_write_proposal(&self, proposal: &BatchWriteProposal) -> BatchWriteAck {
        let (acker, acker_creation) = self.acker_identity();
        let outcome = self.apply_batch_proposal_durably(proposal);
        BatchWriteAck {
            write_id: proposal.write_id.clone(),
            acker,
            acker_creation,
            outcome,
        }
    }

    /// Apply an inbound [`BatchWriteProposal`] and SEND the resulting
    /// [`BatchWriteAck`] back to the originating writer over the live distribution
    /// transport (A1b). The batch analogue of [`Self::handle_inbound_write`].
    ///
    /// # Errors
    /// Returns a [`DatabaseError`] only if the ack could not be SENT (no endpoint or
    /// a transport failure). A fence or CAS mismatch is NOT an error here — it is
    /// carried as the ack `outcome` (a vote-against) and delivered to the writer.
    pub fn handle_inbound_batch_write(
        &self,
        proposal: &BatchWriteProposal,
    ) -> Result<(), DatabaseError> {
        let ack = self.apply_batch_write_proposal(proposal);
        let origin = proposal.write_id.origin.as_str().to_owned();
        self.send_sync_message(&origin, &crate::sync::SyncMessage::BatchWriteAck(ack))
    }

    /// Run the all-or-nothing conditional-durable BATCH apply against the named
    /// owning shard and classify the single result into an [`AckOutcome`] (A1b).
    fn apply_batch_proposal_durably(&self, proposal: &BatchWriteProposal) -> AckOutcome {
        let handle = match self.handle_for_shard(proposal.shard_id) {
            Ok(handle) => handle,
            Err(_error) => return AckOutcome::Rejected(RejectReason::ApplyError),
        };
        // Hand the entries to the shard's atomic multi-key apply in the SAME
        // `(key, expected, value, ttl)` order, under the ONE shared stamp the whole
        // batch carries. This replica never invents its own seq — every key stores
        // the owner-assigned stamp verbatim (§2.4).
        let items = proposal
            .entries
            .iter()
            .map(|entry| {
                (
                    entry.key.clone(),
                    entry.expected,
                    entry.value.clone(),
                    entry.ttl,
                )
            })
            .collect();
        match handle.apply_durable_batch(items, proposal.stamp(), self.timeout()) {
            Ok(()) => AckOutcome::Applied,
            // A single key's CAS hash mismatch rejects the WHOLE batch (nothing
            // written) — a vote-against, not a fault.
            Err(ShardError::CasHashMismatch { .. }) => {
                AckOutcome::Rejected(RejectReason::CasMismatch)
            }
            // An epoch fence rejects the WHOLE batch (nothing written) — a
            // vote-against from a node that promised a higher ballot (§2.3).
            Err(ShardError::Fenced { .. }) => AckOutcome::Rejected(RejectReason::Fenced),
            // Any other shard error (IO, timeout, unavailable, WAL/tree fault) is a
            // genuine apply error; still nothing was written (all-or-nothing).
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

    // =====================================================================
    // AA-3-2: AcquireShard election — acceptor + candidate (Phase 1).
    // =====================================================================

    /// Acceptor side of Phase-1 (§2.2 step 3): respond to an inbound `Prepare`.
    ///
    /// Routes to the owning shard by `prepare.shard_id` and durably records the
    /// promise through the shard handle (`record_promise` fsyncs BEFORE it returns,
    /// §3 value 1). The outcome decides the reply:
    ///
    /// * [`RecordPromiseOutcome::Promised`] (the ballot strictly exceeded the
    ///   persisted `promised`) → send back `Promise{shard_id, ballot,
    ///   accepted_epoch, committed_root}`, where `accepted_epoch` is THIS shard's
    ///   current `owner_epoch` and `committed_root` is its current committed root.
    ///   Both are read in the SAME in-slice snapshot as the (just-advanced) state so
    ///   the new owner can state-sync in 3-4. They are populated correctly, never
    ///   fabricated: a fresh shard reports `None`/`None`.
    /// * [`RecordPromiseOutcome::Rejected { promised }`] → send back
    ///   `Nack{shard_id, promised}` so the candidate learns the higher ballot.
    ///
    /// # Errors
    /// Returns a [`DatabaseError`] only if routing/recording/sending failed (no
    /// endpoint, dead shard, transport fault). A Nack is NOT an error — it is the
    /// correct, expected reply to a losing Prepare.
    pub fn handle_inbound_prepare(&self, prepare: &Prepare) -> Result<(), DatabaseError> {
        let handle = self.handle_for_shard(prepare.shard_id)?;
        let outcome = handle
            .record_promise(prepare.ballot.clone(), self.timeout())
            .map_err(map_shard_error)?;
        let origin = prepare.ballot.node.as_str().to_owned();
        let message = match outcome {
            RecordPromiseOutcome::Promised => {
                // Read the post-promise snapshot in one in-slice command so the
                // accepted_epoch/committed_root we return are consistent with the
                // ballot we just promised. These two fields are for 3-4 handoff
                // state-sync; populated from real shard state, not stubbed.
                let state = handle
                    .read_promise_state(self.timeout())
                    .map_err(map_shard_error)?;
                let (promiser, _) = self.acker_identity();
                crate::sync::SyncMessage::Promise(Promise {
                    shard_id: prepare.shard_id,
                    ballot: prepare.ballot.clone(),
                    promiser,
                    accepted_epoch: state.owner_epoch,
                    committed_root: state.committed_root,
                })
            }
            RecordPromiseOutcome::Rejected { promised } => {
                crate::sync::SyncMessage::Nack(Nack {
                    shard_id: prepare.shard_id,
                    promised,
                })
            }
        };
        self.send_sync_message(&origin, &message)
    }

    /// Candidate side of Phase-1 (§2.2): run `AcquireShard` for `shard_id` and, on
    /// a majority of promises, become the shard's owner under a fresh epoch.
    ///
    /// Steps, in the SAFETY-CRITICAL order (§2.2):
    ///
    /// 1. Read this shard's `(promised, owner_epoch, persisted_max_minted)` in one
    ///    in-slice snapshot, compute the mint floor
    ///    `max(promised.counter, owner_epoch.counter, persisted_max_minted) + 1`,
    ///    and `reserve_minted(floor)` — which FSYNCs `persisted_max_minted` BEFORE
    ///    any Prepare leaves the node (R4). Ballot = `(reserved, local_node_id)`.
    /// 2. Record the candidate's OWN promise locally via `record_promise(ballot)`.
    ///    By construction the ballot strictly exceeds everything persisted, so this
    ///    MUST return `Promised`; it is counted as the first of the quorum.
    /// 3. Send `Prepare` to every reachable peer and (step 4) collect Promise/Nack
    ///    to a strict majority (delegated to
    ///    [`run_prepare_round`](crate::sync::DistributionEndpoint::run_prepare_round)).
    /// 5. On a majority → `record_owner_epoch(ballot)` (FSYNC before serving, §3
    ///    value 2) and return [`ElectionOutcome`].
    /// 6. On a Nack-driven loss / timeout → bounded retry: re-mint strictly above
    ///    the highest ballot seen with randomized backoff, up to a small cap, then
    ///    return a clean [`DatabaseError`]. Retry/backoff is LIVENESS ONLY: every
    ///    attempt re-mints+fsyncs a strictly higher ballot and re-takes its own
    ///    majority, so the unique-ballot / majority invariants are never relaxed.
    ///
    /// The whole call BLOCKS (it parks on the vote receiver), so it must run OUTSIDE
    /// the distribution runtime — the same `ensure_outside_runtime` guard
    /// `propose_write` carries, enforced inside `run_prepare_round`.
    ///
    /// # Errors
    /// Returns [`DatabaseError::ElectionLost`] if a higher ballot was promised
    /// elsewhere on every attempt, [`DatabaseError::ElectionTimeout`] if a majority
    /// never promised in time, or [`DatabaseError::ConsistencyError`] /
    /// [`DatabaseError::ShardError`] on a transport/local fault.
    pub fn acquire_shard(
        &self,
        shard_id: ShardId,
        membership: &WriteMembership,
        timeout: Duration,
    ) -> Result<ElectionOutcome, DatabaseError> {
        const MAX_ATTEMPTS: u32 = 5;

        let endpoint = self
            .distribution()
            .ok_or_else(|| DatabaseError::Distribution("no distribution endpoint".to_owned()))?;
        let local_node = SyncNodeId::new(endpoint.local_name().to_owned());
        let handle = self.handle_for_shard(shard_id)?;

        // The floor a re-mint must strictly exceed, carried across retries: starts
        // unconstrained (0) and is raised to any higher competing ballot a Nack
        // reveals, so each attempt's ballot dominates everything seen so far.
        let mut competing_floor: u64 = 0;

        for attempt in 0..MAX_ATTEMPTS {
            // --- Step 1: mint floor from a fresh in-slice snapshot, then reserve. --
            let state: PromiseState = handle
                .read_promise_state(self.timeout())
                .map_err(map_shard_error)?;
            let local_floor = mint_floor(&state);
            let floor = local_floor.max(competing_floor.saturating_add(1));
            let reserved = handle
                .reserve_minted(floor, self.timeout())
                .map_err(map_shard_error)?;
            let ballot = Ballot::new(reserved, local_node.clone());

            // --- Step 2: record the candidate's OWN promise (must succeed). -------
            match handle
                .record_promise(ballot.clone(), self.timeout())
                .map_err(map_shard_error)?
            {
                RecordPromiseOutcome::Promised => {}
                RecordPromiseOutcome::Rejected { promised } => {
                    // A ballot we just minted strictly above persisted_max_minted and
                    // promised cannot be rejected by our own actor unless another
                    // concurrent local acquire raced ahead. Treat that promised as a
                    // competing floor and retry above it rather than miscount it.
                    competing_floor = competing_floor.max(promised.counter);
                    continue;
                }
            }

            // The self-promise carries this node's accepted_epoch/committed_root for
            // 3-4 handoff (read from the same post-promise snapshot semantics as a
            // peer's Promise). It is counted as the first quorum vote.
            let self_promise = Promise {
                shard_id,
                ballot: ballot.clone(),
                promiser: local_node.clone(),
                accepted_epoch: state.owner_epoch.clone(),
                committed_root: state.committed_root,
            };

            // --- Steps 3-4: Prepare round, collect to a majority. -----------------
            match endpoint.run_prepare_round(shard_id, &ballot, self_promise, membership, timeout) {
                Ok(promises) => {
                    // --- Step 5: WON. Persist owner_epoch (fsync) before serving. --
                    handle
                        .record_owner_epoch(ballot.clone(), self.timeout())
                        .map_err(map_shard_error)?;
                    // R-LE (AA-3-4a, §2.4): set the IN-MEMORY `live_epoch` for this
                    // shard to the won ballot. This is the ONLY writer of
                    // `live_epoch` and the ONLY thing that authorizes serving
                    // (stamping) writes under this epoch. It resets the per-epoch
                    // `seq` to 0. A crash-recovered `owner_epoch` never reaches
                    // here, so it can never re-authorize a stamp without a fresh
                    // (strictly-higher) win.
                    self.owner_stamps.record_won(shard_id, ballot.clone());
                    return Ok(ElectionOutcome { ballot, promises });
                }
                // --- Step 6: lost to a higher ballot — re-mint strictly above it. --
                Err(ElectionError::Lost { highest_seen }) => {
                    competing_floor = competing_floor.max(highest_seen.counter);
                    backoff_before_retry(attempt, &local_node, reserved);
                }
                // Timed out without a higher ballot: retry too (a transient
                // reachability blip), still bounded by MAX_ATTEMPTS.
                Err(ElectionError::Timeout { .. }) => {
                    backoff_before_retry(attempt, &local_node, reserved);
                }
                // A local precondition failed — not a clean election loss; surface it.
                Err(ElectionError::Transport(message)) => {
                    return Err(DatabaseError::ConsistencyError(message));
                }
            }
        }

        // Exhausted retries. Classify the loss from whether a higher ballot was seen.
        if competing_floor > 0 {
            Err(DatabaseError::ElectionLost {
                highest_seen: competing_floor,
            })
        } else {
            Err(DatabaseError::ElectionTimeout {
                attempts: MAX_ATTEMPTS,
            })
        }
    }

    // =====================================================================
    // AA-3-4d: Handoff merge — reconstruct a LOSSLESS committed baseline from
    // the ENTIRE promise majority before serving (§2.4). THE durability gate.
    // =====================================================================

    /// Acquire a shard AND become a live owner: win the Phase-1 election, then
    /// merge the promise majority's committed states into a lossless baseline
    /// before returning (§2.2 + §2.4). This is the ONLY entry point that
    /// authorizes serving.
    ///
    /// Composes [`Self::acquire_shard`] (single-ownership + the Promise majority)
    /// with [`Self::become_live`] (union-merge over ALL promisers). On return the
    /// node is a LIVE owner: every committed write/delete across the majority is
    /// locally present, so a subsequent read/`replicate_write` can never roll one
    /// back (R5). A bare `acquire_shard` leaves the node "elected but not live" — it
    /// MUST NOT serve until `become_live` completes.
    ///
    /// # Errors
    /// Propagates any [`Self::acquire_shard`] election error, or a
    /// [`DatabaseError::Distribution`] / [`DatabaseError::ShardError`] if a
    /// promiser pull or the local merge/apply fails (in which case the node is
    /// elected but NOT live and must not serve — fail-closed).
    pub fn acquire_shard_and_serve(
        &self,
        shard_id: ShardId,
        membership: &WriteMembership,
        timeout: Duration,
    ) -> Result<ElectionOutcome, DatabaseError> {
        let outcome = self.acquire_shard(shard_id, membership, timeout)?;
        self.become_live(shard_id, &outcome, timeout)?;
        Ok(outcome)
    }

    /// Reconstruct a lossless committed baseline for a freshly-elected owner by
    /// MERGING the committed states of EVERY promiser in its majority, then permit
    /// it to serve (§2.4 handoff merge — THE durability gate, R5).
    ///
    /// Steps, exactly:
    ///
    /// 1. For each DISTINCT promiser (by node id) in `outcome.promises` that is NOT
    ///    the local node, pull its full reachable committed node set over the live
    ///    transport (`run_catch_up_round` → the source's `export_reachable`). A
    ///    promiser whose `committed_root` is `None` contributes an empty tree (it
    ///    still gets visited, but its `merge_committed_union(acc, None)` is a no-op).
    /// 2. Hand ALL promiser contributions (their `committed_root` + transfers) to
    ///    the shard actor's `merge_adopt`, which folds
    ///    `merge_committed_union(acc, promiser_root)` over them starting from the
    ///    LOCAL committed root, then durably adopts the merged union. Because the
    ///    merge is a commutative/associative/idempotent max-stamp semilattice join
    ///    and the promise majority intersects every committed-write quorum (§4), the
    ///    adopted baseline dominates EVERY committed write/delete across the majority
    ///    — forks and tombstones included. No single root is "selected"; the union
    ///    is taken over all of them, so no committed write is dropped.
    /// 3. Return `Ok(())` ONLY after the merge is durably adopted. A caller that uses
    ///    [`Self::acquire_shard_and_serve`] therefore cannot serve before the merge.
    ///
    /// `seq` needs no recovery from the merge: `live_epoch` (set by `acquire_shard`)
    /// strictly exceeds every merged write's epoch, so the owner's writes start at
    /// `(live_epoch, 0)` and dominate (R-LE).
    ///
    /// # Errors
    /// Returns [`DatabaseError::Distribution`] if a promiser pull fails (no
    /// endpoint, source unreachable, or no response within `timeout`), or
    /// [`DatabaseError::ShardError`] if the local merge/apply fails. On ANY error the
    /// node is elected but NOT live and must NOT serve (fail-closed): the merge is
    /// never partially adopted, so a pull failure can never serve stale/partial data.
    pub fn become_live(
        &self,
        shard_id: ShardId,
        outcome: &ElectionOutcome,
        timeout: Duration,
    ) -> Result<(), DatabaseError> {
        use std::collections::HashSet;

        let endpoint = self
            .distribution()
            .ok_or_else(|| DatabaseError::Distribution("no distribution endpoint".to_owned()))?;
        let handle = self.handle_for_shard(shard_id)?;
        let local_node = SyncNodeId::new(endpoint.local_name().to_owned());

        // Pull each DISTINCT non-local promiser's full committed tree. We CANNOT
        // pull from ourselves (the local committed root is already the merge's
        // starting accumulator inside `merge_adopt`). De-dup by promiser node id so a
        // promiser that appears twice is pulled once; ALL distinct promisers are
        // folded (no single-root selection — that is what would drop a forked write).
        let mut seen: HashSet<SyncNodeId> = HashSet::new();
        let mut contributions: Vec<(Option<Hash>, Vec<crate::sync::NodeTransfer>)> = Vec::new();
        for promise in &outcome.promises {
            if promise.promiser == local_node {
                continue;
            }
            if !seen.insert(promise.promiser.clone()) {
                continue;
            }
            // Pull the promiser's full reachable committed set. `from_root` is the
            // root it advertised in its Promise; the source answers from its CURRENT
            // committed root and we adopt whatever `source_root` it returns.
            let response = endpoint
                .run_catch_up_round(shard_id, &promise.promiser, promise.committed_root, timeout)
                .map_err(|error| DatabaseError::Distribution(error.to_string()))?;
            contributions.push((response.source_root, response.transfers));
        }

        // Fold merge_committed_union over the LOCAL root + every promiser, durably.
        // A pull failure above returned early WITHOUT adopting anything; the adopt is
        // a single in-slice commit, so serving never sees a partial baseline.
        handle
            .merge_adopt(contributions, self.timeout())
            .map_err(map_shard_error)?;

        Ok(())
    }

    /// Source side of handoff merge (§2.4): answer an inbound
    /// [`crate::sync::ShardSyncRequest`] by exporting this shard's full reachable
    /// committed node set and routing the [`crate::sync::PushResponse`] back to the
    /// requester.
    ///
    /// Routes to the owning shard by `request.shard_id`, reads the reachable set via
    /// `export_reachable` (the existing `find_missing_nodes` pull primitive against
    /// an empty target), and fires the response back fire-and-forget (the source
    /// cannot block on a reply). The requester's blocked `run_catch_up_round`
    /// receives it via the catch-up registry.
    ///
    /// # Errors
    /// Returns a [`DatabaseError`] only if routing/exporting/sending failed.
    pub fn handle_inbound_shard_sync_request(
        &self,
        request: &crate::sync::ShardSyncRequest,
    ) -> Result<(), DatabaseError> {
        use crate::sync::{PushResponse, SyncMessage};

        let handle = self.handle_for_shard(request.shard_id)?;
        let (source_root, transfers) = handle
            .export_reachable(request.shard_id, self.timeout())
            .map_err(map_shard_error)?;

        let response = PushResponse::new(
            request.shard_id,
            source_root,
            // The requester asked for the full set (target_root = None); echo that.
            None,
            transfers,
            crate::sync::SyncStats::default(),
        );

        let endpoint = self
            .distribution()
            .ok_or_else(|| DatabaseError::Distribution("no distribution endpoint".to_owned()))?;
        endpoint
            .send_message_fire_and_forget(&request.requester, &SyncMessage::PushResponse(response))
            .map_err(|error| DatabaseError::Distribution(error.to_string()))
    }
}

/// The §2.2 mint floor: `max(promised.counter, owner_epoch.counter,
/// persisted_max_minted) + 1`. The `+1` makes the next ballot strictly exceed
/// every counter the shard has ever promised, owned, or minted.
fn mint_floor(state: &PromiseState) -> u64 {
    let owner_counter = state.owner_epoch.as_ref().map_or(0, |ballot| ballot.counter);
    state
        .promised
        .counter
        .max(owner_counter)
        .max(state.persisted_max_minted)
        .saturating_add(1)
}

/// Randomized backoff between Prepare attempts (§2.2 step 6 / §4 liveness).
///
/// This is a LIVENESS mitigation ONLY — it never affects which ballots are legal,
/// only the spacing of retries so duelling proposers desynchronise. With no `rand`
/// dependency, jitter is derived from a cheap mix of the wall clock, the local
/// node name, and the just-reserved counter, scaled into a small bounded window
/// that grows with the attempt number.
fn backoff_before_retry(attempt: u32, local_node: &SyncNodeId, reserved: u64) {
    use std::time::{SystemTime, UNIX_EPOCH};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |elapsed| elapsed.as_nanos());
    // Cheap deterministic-per-input mix; not cryptographic, just decorrelating.
    let mut seed = nanos as u64 ^ reserved.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for byte in local_node.as_str().bytes() {
        seed = seed.wrapping_mul(31).wrapping_add(u64::from(byte));
    }
    // Window grows with the attempt: base 2ms, +up to (attempt+1)*8ms of jitter.
    let base_ms = 2_u64;
    let jitter_ceiling_ms = u64::from(attempt.saturating_add(1)).saturating_mul(8);
    let jitter_ms = if jitter_ceiling_ms == 0 {
        0
    } else {
        seed % jitter_ceiling_ms
    };
    std::thread::sleep(Duration::from_millis(base_ms.saturating_add(jitter_ms)));
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
            // A1b receiver: an inbound BatchWriteProposal is the multi-key analogue
            // of a WriteProposal — apply it all-or-nothing through the named shard
            // and reply with the single BatchWriteAck verdict. A send failure is
            // non-fatal; keep draining. (BatchWriteAck itself never lands here — it
            // is a REPLY routed to the proposer.)
            Some(Ok(SyncMessage::BatchWriteProposal(proposal))) => {
                drop(database.handle_inbound_batch_write(&proposal));
            }
            // AA-3-2 acceptor: an inbound Prepare is the election counterpart of a
            // WriteProposal — record the promise and reply Promise/Nack. A send
            // failure (candidate already returned) is non-fatal; keep draining.
            // (Promise/Nack themselves never land here — they are REPLIES, routed to
            // the candidate's election registry, not the generic drain.)
            Some(Ok(SyncMessage::Prepare(prepare))) => {
                drop(database.handle_inbound_prepare(&prepare));
            }
            // AA-3-4d source: an inbound ShardSyncRequest is a handoff-merge REQUEST —
            // the source exports its reachable committed node set and routes the
            // PushResponse back to the requester. A send failure (requester already
            // merged / gone) is non-fatal; keep draining. (PushResponse itself never
            // lands here — it is a REPLY routed to the catch-up registry.)
            Some(Ok(SyncMessage::ShardSyncRequest(request))) => {
                drop(database.handle_inbound_shard_sync_request(&request));
            }
            // Other inbound messages are not this responder's concern.
            Some(_) => {}
            // Timed out with no proposal: the caller decides whether to loop again.
            None => return Ok(()),
        }
    }
}
