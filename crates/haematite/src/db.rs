use std::fmt;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use beamr::scheduler::Scheduler;

use beamr::atom::Atom;

use crate::shard::actor::{ShardError, ShardHandle};
use crate::shard::router::ShardRouter;
use crate::sync::endpoint::{DistributionEndpoint, InboundSync};
use crate::sync::protocol::SyncMessage;
use crate::sync::scheduler::SyncSchedulerHandle;
use crate::tree::Hash;
use crate::ttl::sweep::SweepHandle;

mod config;
mod error;
pub(crate) mod helpers;
mod owner_stamp;
mod receiver;
mod startup;

pub use config::{DatabaseConfig, DistributedDatabaseConfig};
pub use error::DatabaseError;
pub use receiver::respond_to_inbound_writes;

use config::{read_config, validate_database_config};

pub use helpers::run_indexed_parallel;
use helpers::{
    event_range_end, event_range_start, event_sequence_key, has_live_events_on_handle,
    map_shard_error, range_on_handle,
};

const CONFIG_FILE: &str = "config.json";

pub(crate) type DbEntry = (Vec<u8>, Vec<u8>);
pub(crate) type DbRange = Vec<DbEntry>;
pub(crate) type ShardCommitResult = (usize, Result<Hash, ShardError>);

/// Top-level database handle. Callers use this API instead of shard actors.
pub struct Database {
    config: DatabaseConfig,
    scheduler: Arc<Scheduler>,
    router: ShardRouter,
    sweeps: Vec<SweepHandle>,
    sync_schedulers: Vec<SyncSchedulerHandle>,
    /// Live beamr distribution endpoint, present once `with_distribution` runs.
    ///
    /// The active-active "2a-0" substrate: the inbound drain + outbound send
    /// plumbing that lets two live databases exchange `SyncMessage`s. It does not
    /// yet drive the merge/pull protocol (the sync trigger is still a no-op); it
    /// only exposes the transport primitives later increments build on.
    distribution: Option<DistributionEndpoint>,
    /// AA-3-4a R-LE / R-SEQ: per-shard IN-MEMORY serve-authority. `live_epoch` is
    /// set ONLY by a successful `acquire_shard` THIS lifetime (never recovered
    /// from disk), and the atomic `seq` is drawn once per committed write. The
    /// commit stamp `(live_epoch, seq)` is stamped here on the owner and carried
    /// on the `WriteProposal` so every replica stores the identical stamp.
    owner_stamps: owner_stamp::OwnerStamps,
    timeout: Duration,
}

impl fmt::Debug for Database {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Database")
            .field("config", &self.config)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl Database {
    /// Create a new database directory, write its config, and spawn all shards.
    pub fn create(config: DatabaseConfig) -> Result<Self, DatabaseError> {
        validate_database_config(&config)?;
        let data_dir = config.data_dir.clone();
        let should_cleanup = !data_dir.exists();
        fs::create_dir_all(&data_dir).map_err(DatabaseError::DirectoryCreate)?;
        let result = startup::initialise_database(config);
        if result.is_err() && should_cleanup {
            drop(fs::remove_dir_all(&data_dir));
        }
        result
    }

    /// Open an existing database directory and restart its shard actors.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let path = path.as_ref().to_path_buf();
        let mut config = read_config(&path)?;
        config.data_dir = path;
        validate_database_config(&config)?;
        startup::start_database(config, startup::StartupMode::Open)
    }

    /// Return the shard index that owns `key`.
    pub fn shard_for(&self, key: &[u8]) -> usize {
        self.router.shard_for(key)
    }

    /// Atomically append event entries under `key` using optimistic concurrency.
    pub fn append(
        &self,
        key: Vec<u8>,
        entries: Vec<Vec<u8>>,
        expected_seq: u64,
    ) -> Result<u64, DatabaseError> {
        self.append_with_ttl(key, entries, expected_seq, None)
    }

    /// Atomically append event entries with optional TTL metadata.
    pub fn append_with_ttl(
        &self,
        key: Vec<u8>,
        entries: Vec<Vec<u8>>,
        expected_seq: u64,
        ttl: Option<Duration>,
    ) -> Result<u64, DatabaseError> {
        self.validate_ttl_write(ttl)?;
        self.handle_for(&key)?
            .append_with_ttl(key, entries, expected_seq, ttl, self.timeout)
            .map_err(map_shard_error)
    }

    /// Read all appended events for `key` in sequence order.
    pub fn read_events(&self, key: &[u8]) -> Result<Vec<Vec<u8>>, DatabaseError> {
        self.read_events_from(key, 0)
    }

    /// Read appended events for `key` from `from_seq` onward.
    pub fn read_events_from(
        &self,
        key: &[u8],
        from_seq: u64,
    ) -> Result<Vec<Vec<u8>>, DatabaseError> {
        let from = event_range_start(key, from_seq);
        let to = event_range_end(key);
        let entries = range_on_handle(self.handle_for(key)?, &from, &to, self.timeout)?;
        Ok(entries.into_iter().map(|(_, value)| value).collect())
    }

    /// Read appended event entries for `key` from `from_seq` onward as raw
    /// `(encoded_key, value)` pairs, in sequence order.
    ///
    /// Unlike [`Self::read_events_from`], this preserves the encoded tree key so
    /// the caller (the `EventStore`) can decode each event's sequence number from
    /// its key rather than trusting a value-side copy.
    pub fn read_event_entries_from(
        &self,
        key: &[u8],
        from_seq: u64,
    ) -> Result<DbRange, DatabaseError> {
        let from = event_range_start(key, from_seq);
        let to = event_range_end(key);
        range_on_handle(self.handle_for(key)?, &from, &to, self.timeout)
    }

    /// Read the next sequence metadata for an event stream, if the stream exists.
    pub fn read_stream_next_seq(&self, key: &[u8]) -> Result<Option<u64>, DatabaseError> {
        // Route on the STREAM key — `append` writes the sequence metadata into the
        // shard of the stream key, so the read must select the same shard. Routing
        // on `event_sequence_key(key)` (a different hash) would read the wrong
        // shard for `shard_count > 1` and miss the metadata entirely.
        self.handle_for(key)?
            .read_value(event_sequence_key(key), self.timeout)
            .map_err(map_shard_error)
    }

    /// Return true if a stream has at least one non-expired event visible now.
    pub fn stream_has_live_events(&self, key: &[u8]) -> Result<bool, DatabaseError> {
        has_live_events_on_handle(self.handle_for(key)?, key, self.timeout)
    }

    /// Read the scalar `u64` value at `key`, or `None` if it is unset.
    pub fn read_value(&self, key: &[u8]) -> Result<Option<u64>, DatabaseError> {
        self.handle_for(key)?
            .read_value(key.to_vec(), self.timeout)
            .map_err(map_shard_error)
    }

    /// Atomically compare-and-swap the scalar `u64` value at `key`.
    ///
    /// The read-compare-write executes inside the owning shard's single-threaded
    /// actor, so concurrent CAS calls against the same key are serialised and
    /// cannot race. Returns [`DatabaseError::CasMismatch`] if the current value
    /// is not `expected`.
    pub fn cas(&self, key: Vec<u8>, expected: Option<u64>, new: u64) -> Result<(), DatabaseError> {
        self.handle_for(&key)?
            .cas(key, expected, new, self.timeout)
            .map_err(map_shard_error)
    }

    /// Attach a live beamr distribution endpoint to this database.
    ///
    /// This is the active-active "2a-0" substrate: it installs the inbound-drain
    /// and outbound-send plumbing two live databases need to exchange
    /// `SyncMessage`s over a real network. The endpoint owns its own atom table,
    /// connection manager, accept loop, and tokio runtime (see
    /// [`DistributionEndpoint`]).
    ///
    /// It does NOT replace the no-op sync trigger or drive the pull/merge
    /// protocol — those are later increments. The database simply takes ownership
    /// of the endpoint and re-exports its transport primitives
    /// ([`Database::connect_peer`], [`Database::send_sync_message`],
    /// [`Database::recv_sync_message`]).
    #[must_use]
    pub fn with_distribution(mut self, endpoint: DistributionEndpoint) -> Self {
        self.distribution = Some(endpoint);
        self
    }

    /// Borrow the attached distribution endpoint, if any.
    #[must_use]
    pub const fn distribution(&self) -> Option<&DistributionEndpoint> {
        self.distribution.as_ref()
    }

    /// Register `peer_name` at `addr` and dial it over real distribution.
    ///
    /// Requires [`Database::with_distribution`] to have installed an endpoint.
    pub fn connect_peer(
        &self,
        peer_name: &str,
        addr: std::net::SocketAddr,
    ) -> Result<(), DatabaseError> {
        let endpoint = self.require_distribution()?;
        endpoint.add_peer(peer_name, addr);
        endpoint
            .connect(peer_name)
            .map_err(|error| DatabaseError::Distribution(error.to_string()))
    }

    /// Intern `peer_name` into the endpoint's atom table for addressed sends.
    pub fn peer_atom(&self, peer_name: &str) -> Result<Atom, DatabaseError> {
        Ok(self.require_distribution()?.peer_atom(peer_name))
    }

    /// Send `message` to the peer named `peer_name` over the live transport.
    ///
    /// Requires an attached endpoint and an established connection to the peer.
    pub fn send_sync_message(
        &self,
        peer_name: &str,
        message: &SyncMessage,
    ) -> Result<(), DatabaseError> {
        self.require_distribution()?
            .send_to(peer_name, message)
            .map_err(|error| DatabaseError::Distribution(error.to_string()))
    }

    /// Block until an inbound sync message arrives or `timeout` elapses.
    ///
    /// Returns `Ok(Some(_))` with the decoded message (or a decode error from the
    /// wire), `Ok(None)` on timeout, and an error if no endpoint is attached or
    /// the drain has been disconnected.
    pub fn recv_sync_message(
        &self,
        timeout: Duration,
    ) -> Result<Option<InboundSync>, DatabaseError> {
        self.require_distribution()?
            .recv_inbound(timeout)
            .map_err(|error| DatabaseError::Distribution(error.to_string()))
    }

    /// Atoms for all currently active distribution connections.
    pub fn connected_nodes(&self) -> Result<Vec<Atom>, DatabaseError> {
        Ok(self.require_distribution()?.connected_nodes())
    }

    /// Test-support: stop every shard actor so subsequent storage commands fail.
    ///
    /// Used by the 2a-4 receiver tests to force a genuine apply fault (a
    /// disconnected/timed-out shard reply, distinct from a CAS mismatch) and prove
    /// it surfaces as `Rejected(ApplyError)`. Not part of the production API.
    #[doc(hidden)]
    pub fn shutdown_shards_for_test(&self) {
        for handle in self.router.handles_in_order() {
            drop(handle.shutdown(self.timeout));
        }
    }

    /// Test-support: read a shard's current durably-`promised` ballot (AA-3-2).
    ///
    /// Used by the election e2e tests to assert the swing voter's monotonic
    /// `promised` reflects the MAX winner (the single-live-owner invariant). Not
    /// part of the production API.
    #[doc(hidden)]
    #[must_use]
    pub fn promised_ballot_for_test(&self, shard_id: usize) -> Option<crate::sync::Ballot> {
        let handle = self.router.handle_for_shard(shard_id)?;
        handle
            .read_promise_state(self.timeout)
            .ok()
            .map(|state| state.promised)
    }

    /// Test-support: durably advance a shard's `promised` ballot (AA-3-2), the same
    /// way an inbound `Prepare` would, WITHOUT needing a live election/transport.
    ///
    /// Returns `true` iff the ballot strictly exceeded the prior `promised` (so it
    /// was recorded). Used by the A1b receiver fence test to put a shard's
    /// `promised` above an inbound batch's epoch so the batch is fenced. Not part of
    /// the production API.
    #[doc(hidden)]
    pub fn record_promise_for_test(&self, shard_id: usize, ballot: crate::sync::Ballot) -> bool {
        let Some(handle) = self.router.handle_for_shard(shard_id) else {
            return false;
        };
        matches!(
            handle.record_promise(ballot, self.timeout),
            Ok(crate::shard::actor::RecordPromiseOutcome::Promised)
        )
    }

    /// Test-support: decode the committed commit-stamp `(epoch, seq)` a node
    /// stored for `key` (AA-3-4a). Reads the RAW stored envelope (stamp NOT
    /// stripped) and decodes its stamp, so a test can prove every replica stored
    /// the IDENTICAL owner-assigned stamp. Returns `None` if the key is absent or
    /// was stored without a stamp envelope.
    #[doc(hidden)]
    #[must_use]
    pub fn stored_stamp_for_test(&self, key: &[u8]) -> Option<crate::sync::Stamp> {
        let handle = self.router.handle_for(key)?;
        let raw = handle.get_raw(key.to_vec(), self.timeout).ok()??;
        crate::ttl::entry::StampedEntry::decode(&raw)
            .ok()
            .flatten()
            .map(|entry| entry.stamp().clone())
    }

    /// Test-support (AA-3-4b): `Some(true)` if `key` is stored as a STAMPED
    /// TOMBSTONE on this node, `Some(false)` if it is stored as a stamped value,
    /// `None` if absent or stored without a stamp envelope. Reads the RAW stored
    /// envelope so a test can prove a committed delete landed a tombstone (not a
    /// removal) on a peer, and that R-TOMB kept it through a sweep.
    #[doc(hidden)]
    #[must_use]
    pub fn stored_is_tombstone_for_test(&self, key: &[u8]) -> Option<bool> {
        let handle = self.router.handle_for(key)?;
        let raw = handle.get_raw(key.to_vec(), self.timeout).ok()??;
        crate::ttl::entry::StampedEntry::decode(&raw)
            .ok()
            .flatten()
            .map(|entry| entry.is_tombstone())
    }

    /// Test-support: read a shard's IN-MEMORY `live_epoch` (R-LE, AA-3-4a). This
    /// is `Ballot::bottom()` until a successful `acquire_shard` THIS lifetime, and
    /// is NEVER seeded from the disk-recovered `owner_epoch`.
    #[doc(hidden)]
    #[must_use]
    pub fn live_epoch_for_test(&self, shard_id: usize) -> crate::sync::Ballot {
        self.owner_stamps.live_epoch(shard_id)
    }

    /// Test-support: the commit stamp the NEXT write to `shard_id` would draw,
    /// WITHOUT advancing the counter (R-LE / R-SEQ peek). Used by the crash gate
    /// to prove a recovered owner would stamp `bottom`, never the recovered `e'`.
    #[doc(hidden)]
    #[must_use]
    pub fn next_stamp_for_test(&self, shard_id: usize) -> crate::sync::Stamp {
        self.owner_stamps.peek_stamp(shard_id)
    }

    /// Draw the next commit stamp `(live_epoch, seq)` for a write to the shard
    /// owning `key` (R-LE / R-SEQ). One atomic `seq` draw; `bottom` epoch until a
    /// live election this lifetime. Used by the unified stamped write/delete paths.
    pub(crate) fn next_stamp_for_key(&self, key: &[u8]) -> crate::sync::Stamp {
        self.owner_stamps.next_stamp(self.shard_for(key))
    }

    fn require_distribution(&self) -> Result<&DistributionEndpoint, DatabaseError> {
        self.distribution
            .as_ref()
            .ok_or_else(|| DatabaseError::Distribution("no distribution endpoint".into()))
    }

    /// Collect every stream's `(stream_key, next_seq)` pair across all shards.
    ///
    /// This walks each shard in parallel, scanning its full key range for the
    /// per-stream sequence-metadata keys and decoding each one. It is the
    /// O(total entries) traversal that backs the `EventStore` `scan` predicate.
    pub fn scan_sequence_keys(&self) -> Result<Vec<(Vec<u8>, u64)>, DatabaseError> {
        let handles = self.router.handles_in_order().to_vec();
        let timeout = self.timeout;
        let results = run_indexed_parallel(handles, |handle: ShardHandle| {
            handle.scan_sequences(timeout)
        })?;
        let mut streams = Vec::new();
        for (_, result) in results {
            streams.extend(result.map_err(map_shard_error)?);
        }
        Ok(streams)
    }

    /// Collect `(stream_key, next_seq)` pairs from ONLY the named shards.
    ///
    /// The scoped counterpart of [`Self::scan_sequence_keys`]: a node that owns a
    /// subset of shards enumerates only its own streams (e.g. to recover exactly
    /// the workflows whose event streams it serves) without paying for, or
    /// surfacing, streams that live on shards another node owns. Each id must be in
    /// `0..shard_count`; an out-of-range id is [`DatabaseError::InvalidShardCount`].
    pub fn scan_sequence_keys_for_shards(
        &self,
        shard_ids: &[usize],
    ) -> Result<Vec<(Vec<u8>, u64)>, DatabaseError> {
        let all = self.router.handles_in_order();
        let mut handles = Vec::with_capacity(shard_ids.len());
        for &shard_id in shard_ids {
            handles.push(
                all.get(shard_id)
                    .ok_or(DatabaseError::InvalidShardCount)?
                    .clone(),
            );
        }
        let timeout = self.timeout;
        let results = run_indexed_parallel(handles, |handle: ShardHandle| {
            handle.scan_sequences(timeout)
        })?;
        let mut streams = Vec::new();
        for (_, result) in results {
            streams.extend(result.map_err(map_shard_error)?);
        }
        Ok(streams)
    }

    pub const fn shard_count(&self) -> usize {
        self.config.shard_count
    }

    pub(crate) const fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) fn shard_handles_in_order(&self) -> &[ShardHandle] {
        self.router.handles_in_order()
    }

    pub(crate) fn handle_for(&self, key: &[u8]) -> Result<&ShardHandle, DatabaseError> {
        self.router
            .handle_for(key)
            .ok_or(DatabaseError::InvalidShardCount)
    }

    /// Route to a shard by its index (AA-3-2 election routing). A `Prepare`/
    /// `acquire_shard` names the shard directly, so it bypasses key-hash routing.
    pub(crate) fn handle_for_shard(&self, shard_id: usize) -> Result<&ShardHandle, DatabaseError> {
        self.router
            .handle_for_shard(shard_id)
            .ok_or(DatabaseError::InvalidShardCount)
    }

    pub(crate) const fn validate_ttl_write(
        &self,
        ttl: Option<Duration>,
    ) -> Result<(), DatabaseError> {
        if ttl.is_some() && self.config.sweep_interval.is_none() {
            return Err(DatabaseError::MissingSweepInterval);
        }
        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        for handle in &self.sync_schedulers {
            if let Err(error) = handle.shutdown(self.timeout) {
                log::debug!(
                    "database sync scheduler shutdown skipped for supervisor pid {}: {error}",
                    handle.supervisor_pid()
                );
            }
        }
        for handle in &self.sweeps {
            if let Err(error) = handle.shutdown(self.timeout) {
                log::debug!(
                    "database sweep shutdown skipped for supervisor pid {}: {error}",
                    handle.supervisor_pid()
                );
            }
        }
        for handle in self.router.handles_in_order() {
            if let Err(error) = handle.shutdown(self.timeout) {
                log::debug!(
                    "database shard shutdown skipped for pid {}: {error}",
                    handle.pid()
                );
            }
        }
        self.scheduler.shutdown();
    }
}

#[cfg(test)]
#[path = "db_tests.rs"]
mod tests;
