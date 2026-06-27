//! Live beamr distribution endpoint for haematite databases.
//!
//! This is the active-active "2a-0" substrate: the production wiring that lets
//! two live [`Database`](crate::db::Database) instances exchange
//! [`SyncMessage`](crate::sync::SyncMessage)s over a real network. Until this
//! module existed haematite's distribution had never run over a socket — the
//! production sync trigger is a no-op and nothing constructed a beamr
//! `ConnectionManager`.
//!
//! A [`DistributionEndpoint`] bundles and owns everything one node needs to
//! participate in distribution:
//!
//! * an `Arc<AtomTable>` — the single, shared interning table peers are addressed
//!   through (an `Atom` is an index into one specific table, so the sender must
//!   address a peer by the atom for the peer's advertised handshake name interned
//!   in this exact table — this is the load-bearing wiring detail);
//! * a bare [`ConnectionManager`] (NOT `NetKernel`) running the OTP handshake and
//!   read loop;
//! * the [`AcceptHandle`] returned by `listen`, keeping the accept loop alive;
//! * a dedicated multi-thread tokio runtime used to drive the async beamr
//!   transport from haematite's synchronous call paths;
//! * an inbound drain: every decoded `SyncMessage` is pushed into an `mpsc`
//!   channel the owner reads with [`DistributionEndpoint::recv_inbound`].
//!
//! # Runtime drop discipline
//!
//! Dropping a tokio runtime from within an async context panics. beamr's
//! `NetKernel` moves the runtime drop onto a `std::thread` for exactly this
//! reason. [`DistributionEndpoint`] follows the same discipline: it holds the
//! runtime as `Option<Arc<Runtime>>` and its [`Drop`] takes the `Arc` and drops
//! it on a freshly spawned `std::thread`, so the (potentially blocking) runtime
//! shutdown can never run on an async worker — even if the endpoint is dropped
//! inside a `#[tokio::test]`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use beamr::atom::{Atom, AtomTable};
use beamr::distribution::ConnectionManager;
use beamr::distribution::resolver::{NodeResolver, ResolveError, ResolveFuture};
use dashmap::DashMap;
use tokio::runtime::{Builder, Handle, Runtime};

use crate::api::kv::{KvKey, KvValue};
use crate::branch::ShardId;
use crate::sync::SyncNodeId;
use crate::sync::ballot::{Ballot, Stamp};
use crate::sync::consistency::{
    CasVote, ConsistencyError, QuorumOutcome, StrongConsistency, quorum_size,
    wait_for_cas_quorum_from_receiver,
};
use crate::sync::membership::WriteMembership;
use crate::tree::Hash;

use super::protocol::{
    AckOutcome, BatchWriteAck, BatchWriteEntry, BatchWriteProposal, Nack, Prepare, Promise,
    PushResponse, RejectReason, ShardSyncRequest, SyncError, SyncMessage, WriteAck, WriteId,
    WriteProposal, encode_beamr_sync_frame, register_beamr_sync_handler,
    send_sync_message_via_beamr,
};

/// Writer-side correlation registry: in-flight `WriteId` → the channel that the
/// blocked coordinator is tallying votes on.
///
/// Owned by the [`DistributionEndpoint`] so the inbound `WriteAck` handler (which
/// runs on a beamr read-loop task) and the synchronous [`DistributionEndpoint::propose_write`]
/// coordinator share exactly one map. `DashMap` is used (rather than a
/// `Mutex<HashMap<…>>`) because the two access sites run concurrently on
/// different thread classes — the async read loop inserting/looking-up votes
/// while a blocking writer registers/deregisters — and `DashMap` gives sharded,
/// poison-free concurrent access without wrapping every touch in a poisoned-lock
/// recovery dance. The value channel is an `mpsc::Sender<CasVote<SyncNodeId>>`
/// (the blocking primitive `wait_for_cas_quorum_from_receiver` consumes).
type WriteRegistry = Arc<DashMap<WriteId, Sender<CasVote<SyncNodeId>>>>;

/// Election-side correlation registry: an in-flight `AcquireShard` keyed by the
/// shard under election → the channel its blocked coordinator collects votes on.
///
/// Keyed by `ShardId` alone (NOT by ballot): a single endpoint runs at most ONE
/// `acquire_shard` for a given shard at a time (the coordinator blocks), so the
/// shard id uniquely identifies the in-flight election. This is what lets a `Nack`
/// — which carries only the promiser's `promised` ballot, never the candidate's —
/// be routed back to the right coordinator. The coordinator re-checks each
/// `Promise.ballot == my_ballot` itself (a stale Promise for a prior attempt is
/// ignored), so keying by shard id never misattributes a vote. Mirrors
/// [`WriteRegistry`]: a `DashMap` for poison-free concurrent access between the
/// async read loop (routing votes) and the blocking coordinator (registering).
type ElectionRegistry = Arc<DashMap<ShardId, Sender<ElectionVote>>>;

/// Catch-up correlation registry (AA-3-4, §2.4): an in-flight handoff state-sync
/// keyed by the shard being caught up → the channel its blocked coordinator
/// collects the [`PushResponse`] on.
///
/// Keyed by `ShardId` alone, exactly like [`ElectionRegistry`]: a single endpoint
/// runs at most ONE catch-up for a given shard at a time (the coordinator blocks),
/// so the shard id uniquely identifies the in-flight pull, and the `PushResponse`
/// carries its `shard_id` so the inbound router can route it back. A `DashMap` for
/// poison-free concurrent access between the async read loop (routing the response)
/// and the blocking coordinator (registering).
type CatchUpRegistry = Arc<DashMap<ShardId, Sender<PushResponse>>>;

/// One inbound reply to a `Prepare` round, routed to the waiting coordinator.
#[derive(Debug, Clone)]
pub enum ElectionVote {
    /// A node promised the candidate's ballot (carries its accepted epoch +
    /// committed root for handoff state-sync, §2.4).
    Promised(Promise),
    /// A node refused, surfacing the higher ballot it has already `promised`.
    Nacked(Nack),
}

/// The result of a won `AcquireShard` election (§2.2 step 4).
///
/// Carries the won `ballot` (= the owner epoch) and the majority of `Promise`s
/// that elected it. The promises are retained so increment 3-4 can pick the
/// most-advanced `committed_root` among them and state-sync before serving.
#[derive(Debug, Clone)]
pub struct ElectionOutcome {
    /// The ballot the candidate won under; this IS the per-shard owner epoch.
    pub ballot: Ballot,
    /// The collected majority of promises (including the candidate's self-promise,
    /// recorded as a synthetic `Promise` from the local node). At least
    /// `quorum_size(total_nodes)` entries.
    pub promises: Vec<Promise>,
}

/// Why an `AcquireShard` election did not win (§2.2 step 4 loss arms).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionError {
    /// A node Nack'd with a strictly higher `promised` ballot, OR the timeout
    /// elapsed before a majority of promises arrived, on EVERY retry attempt. This
    /// is a clean liveness loss (the safety invariants were never relaxed to get
    /// here); the highest competing ballot seen is surfaced so a caller can choose
    /// to retry above it later.
    Lost { highest_seen: Ballot },
    /// The election could not reach a majority before the deadline and saw no
    /// higher competing ballot (e.g. a minority of nodes was reachable).
    Timeout {
        required: usize,
        promised_votes: usize,
    },
    /// A local precondition failed (no transport, blocking call from inside the
    /// runtime, or a quorum-size computation error). Distinct from a clean
    /// election loss — nothing about the cluster's ballots was learned.
    Transport(String),
}

impl std::fmt::Display for ElectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lost { highest_seen } => write!(
                formatter,
                "election lost: a higher ballot {highest_seen:?} was promised elsewhere"
            ),
            Self::Timeout {
                required,
                promised_votes,
            } => write!(
                formatter,
                "election timed out: required {required} promises, collected {promised_votes}"
            ),
            Self::Transport(message) => write!(formatter, "election transport error: {message}"),
        }
    }
}

impl std::error::Error for ElectionError {}

/// Default authentication cookie shared across a haematite cluster's links.
///
/// Distribution peers must agree on this value or the OTP handshake is rejected.
const DEFAULT_COOKIE: &str = "haematite-distribution-cookie";

/// A mutable name-to-address resolver for distribution peers.
///
/// Listen addresses are only known after a node binds its listener (callers
/// typically bind on `127.0.0.1:0` and discover the OS-assigned port), so the
/// resolver is populated after [`DistributionEndpoint::bind`] returns.
#[derive(Default)]
struct EndpointResolver {
    nodes: Mutex<HashMap<String, SocketAddr>>,
}

impl EndpointResolver {
    fn insert(&self, name: &str, addr: SocketAddr) {
        self.nodes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name.to_owned(), addr);
    }
}

impl NodeResolver for EndpointResolver {
    fn resolve<'a>(&'a self, name: &'a str) -> ResolveFuture<'a> {
        let result = self
            .nodes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)
            .copied()
            .ok_or(ResolveError::NotFound);
        Box::pin(async move { result })
    }
}

/// An inbound sync message together with its decode result.
///
/// The beamr read loop hands the registered control-frame handler the decoded
/// payload, which may be a decode error; both are forwarded so the owner can
/// observe malformed frames rather than silently dropping them.
pub type InboundSync = Result<SyncMessage, SyncError>;

/// Live beamr distribution endpoint owned by one haematite node.
///
/// Construct with [`DistributionEndpoint::bind`], wire peers in with
/// [`DistributionEndpoint::add_peer`] + [`DistributionEndpoint::connect`], send
/// with [`DistributionEndpoint::send`], and drain inbound traffic with
/// [`DistributionEndpoint::recv_inbound`].
pub struct DistributionEndpoint {
    /// Shared atom table — peers are addressed by atoms interned here.
    atom_table: Arc<AtomTable>,
    /// Mutable peer name → address map backing the beamr resolver.
    resolver: Arc<EndpointResolver>,
    /// Bare beamr connection manager (handshake + read loop).
    manager: ConnectionManager,
    /// Keeps the async accept loop alive; dropped/shut down on teardown.
    accept: AcceptGuard,
    /// Dedicated multi-thread runtime driving the async beamr transport.
    ///
    /// Held as `Option<Arc<Runtime>>` so [`Drop`] can move the runtime drop onto
    /// a `std::thread` — dropping a tokio runtime in an async context panics.
    runtime: Option<Arc<Runtime>>,
    /// Receiver end of the inbound-sync drain.
    ///
    /// Wrapped in a `Mutex` so the endpoint is `Sync` (an `mpsc::Receiver` is
    /// `!Sync`); this lets the owning `Database` be shared as `Arc<Database>`
    /// across threads, as the rest of the database API already permits.
    inbound: Mutex<Receiver<InboundSync>>,
    /// The local node's advertised distribution name.
    local_name: String,
    /// Bound listen address (with the OS-assigned port resolved).
    local_addr: SocketAddr,
    /// This endpoint's per-restart OTP incarnation.
    ///
    /// The inbound `WriteAck` router gates acks on
    /// `write_id.origin_creation == local_creation` (design Fix D) so a stale ack
    /// for a *prior* writer incarnation cannot satisfy a post-restart write that
    /// reused the same in-memory `counter`.
    local_creation: u32,
    /// Monotonic source for the `counter` field of locally-originated `WriteId`s.
    write_counter: AtomicU64,
    /// Writer-side correlation registry shared with the inbound `WriteAck` router.
    registry: WriteRegistry,
    /// Election-side correlation registry shared with the inbound `Promise`/`Nack`
    /// router (AA-3-2).
    elections: ElectionRegistry,
    /// Catch-up correlation registry shared with the inbound `PushResponse` router
    /// (AA-3-4 handoff state-sync).
    catch_ups: CatchUpRegistry,
}

impl std::fmt::Debug for DistributionEndpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DistributionEndpoint")
            .field("local_name", &self.local_name)
            .field("local_addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

/// The write attributes one CAS proposal carries.
///
/// Grouped so the [`DistributionEndpoint::propose_write`] /
/// [`DistributionEndpoint::propose_write_stamped`] coordinators take few
/// arguments. These four travel together — key + its CAS precondition + the
/// value to write + an optional TTL — while the stamp tail (epoch / stamp /
/// tombstone) and quorum tail (membership / timeout) stay separate because they
/// are coordinator concerns, not the write's own data.
pub struct ProposeWrite {
    /// The key to write.
    pub key: KvKey,
    /// CAS precondition: the prior value hash (`None` means create-if-absent).
    pub expected: Option<Hash>,
    /// The value to write (empty for a tombstone, where `tombstone` is set on the
    /// coordinator call).
    pub value: KvValue,
    /// Optional time-to-live for the written entry.
    pub ttl: Option<Duration>,
}

impl DistributionEndpoint {
    /// Bind a distribution listener for `local_name` on `listen_addr`.
    ///
    /// Builds a dedicated multi-thread tokio runtime, constructs a bare
    /// [`ConnectionManager`] keyed by a shared [`AtomTable`], starts the async
    /// accept loop, and registers the inbound sync drain. Pass `cookie = None` to
    /// use the default cluster cookie. `local_creation` is the per-restart OTP
    /// incarnation advertised in the handshake.
    ///
    /// Bind on `127.0.0.1:0` to let the OS assign a port and read it back with
    /// [`DistributionEndpoint::local_addr`].
    pub fn bind(
        local_name: impl Into<String>,
        listen_addr: SocketAddr,
        local_creation: u32,
        cookie: Option<&str>,
    ) -> Result<Self, SyncError> {
        ensure_outside_runtime()?;
        let local_name = local_name.into();
        let runtime = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|_error| SyncError::TransportRuntimeUnavailable)?,
        );

        let atom_table = Arc::new(AtomTable::with_common_atoms());
        let resolver = Arc::new(EndpointResolver::default());
        let manager = ConnectionManager::new(
            Arc::clone(&atom_table),
            Arc::clone(&resolver) as Arc<dyn NodeResolver + Send + Sync>,
            cookie.unwrap_or(DEFAULT_COOKIE),
            local_name.clone(),
            local_creation,
        );

        let accept = runtime
            .block_on(manager.listen(listen_addr))
            .map_err(|error: std::io::Error| SyncError::TransportBind(error.to_string()))?;
        let local_addr = accept.local_addr();

        // The endpoint advertises its own listen address so a peer that only
        // knows our name can dial back.
        resolver.insert(&local_name, local_addr);

        let (tx, inbound) = mpsc::channel::<InboundSync>();
        let registry: WriteRegistry = Arc::new(DashMap::new());
        let elections: ElectionRegistry = Arc::new(DashMap::new());
        let catch_ups: CatchUpRegistry = Arc::new(DashMap::new());
        register_inbound_drain(
            &manager,
            tx,
            Arc::clone(&registry),
            Arc::clone(&elections),
            Arc::clone(&catch_ups),
            local_creation,
        );

        Ok(Self {
            atom_table,
            resolver,
            manager,
            accept: AcceptGuard::new(accept),
            runtime: Some(runtime),
            inbound: Mutex::new(inbound),
            local_name,
            local_addr,
            local_creation,
            write_counter: AtomicU64::new(0),
            registry,
            elections,
            catch_ups,
        })
    }

    /// The local node's advertised distribution name.
    #[must_use]
    pub fn local_name(&self) -> &str {
        &self.local_name
    }

    /// The bound listen address (with the OS-assigned port resolved).
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// This endpoint's per-restart OTP incarnation (the `creation` advertised in
    /// the handshake and embedded in locally-originated `WriteId`s).
    #[must_use]
    pub const fn local_creation(&self) -> u32 {
        self.local_creation
    }

    /// Register a peer's name → address mapping so it can be dialed by name.
    ///
    /// Must be called before [`DistributionEndpoint::connect`] for that peer.
    pub fn add_peer(&self, name: &str, addr: SocketAddr) {
        self.resolver.insert(name, addr);
    }

    /// Intern `name` in this endpoint's shared atom table.
    ///
    /// The returned [`Atom`] is the address a peer is sent to via
    /// [`DistributionEndpoint::send`]; it is only valid for this endpoint's table.
    #[must_use]
    pub fn peer_atom(&self, name: &str) -> Atom {
        self.atom_table.intern(name)
    }

    /// Dial `peer_name`, running the OTP handshake, and add the link.
    ///
    /// The peer must already be registered via [`DistributionEndpoint::add_peer`].
    /// On success the connection table is keyed by the name the peer advertises in
    /// the handshake; address that peer through [`DistributionEndpoint::peer_atom`].
    pub fn connect(&self, peer_name: &str) -> Result<(), SyncError> {
        ensure_outside_runtime()?;
        let manager = self.manager.clone();
        let peer_name = peer_name.to_owned();
        self.runtime()?
            .block_on(async move { manager.connect(&peer_name).await })
            .map(drop)
            .map_err(|_error| SyncError::TransportConnectFailed)
    }

    /// Return the node-name atoms for all currently active connections.
    #[must_use]
    pub fn connected_nodes(&self) -> Vec<Atom> {
        self.manager.connected_nodes()
    }

    /// True if there is an active distribution link to `peer_name`.
    #[must_use]
    pub fn is_connected(&self, peer_name: &str) -> bool {
        self.manager
            .get_connection(self.atom_table.intern(peer_name))
            .is_some()
    }

    /// Send `message` to the peer addressed by `remote`.
    ///
    /// `remote` must be an atom obtained from [`DistributionEndpoint::peer_atom`]
    /// for this endpoint. The frame is written by bridging beamr's async
    /// `write_raw` onto the endpoint runtime via `Handle::block_on`.
    ///
    /// # Threading contract
    ///
    /// This call blocks the calling thread until the frame is written, so it must
    /// run on a synchronous (non-async) thread. If invoked from within ANY tokio
    /// runtime context it returns [`SyncError::TransportBlockingFromAsync`] rather
    /// than panicking. Production callers run on haematite's synchronous
    /// shard/database threads, which satisfies this; async callers must instead
    /// drive the send through [`DistributionEndpoint::runtime_handle`].
    /// (`connect` and `bind` carry the same guard.)
    pub fn send(&self, remote: Atom, message: &SyncMessage) -> Result<(), SyncError> {
        ensure_outside_runtime()?;
        let handle = self.runtime()?.handle().clone();
        send_sync_message_via_beamr(&self.manager, remote, message, |connection, frame| {
            handle.block_on(async move {
                connection
                    .write_raw(&frame)
                    .await
                    .map_err(|_error| SyncError::TransportWrite)
            })
        })
    }

    /// Send `message` to the peer named `peer_name`.
    ///
    /// Convenience wrapper over [`DistributionEndpoint::send`] that interns the
    /// peer name in this endpoint's atom table.
    ///
    /// `peer_name` must be the name the peer **advertises in its handshake** (its
    /// own `local_name`), because the connection table is keyed by the advertised
    /// name, not the dial/resolver key. If a peer is dialed under one name but
    /// advertises another, this fails closed with
    /// [`SyncError::TransportConnectionUnavailable`] (never mis-delivers).
    pub fn send_to(&self, peer_name: &str, message: &SyncMessage) -> Result<(), SyncError> {
        self.send(self.atom_table.intern(peer_name), message)
    }

    /// Coordinate one Strong CAS write to quorum across the cluster.
    ///
    /// This is the active-active "2a-3" writer-side coordinator and the sync/async
    /// bridge. It:
    ///
    /// 1. allocates an incarnation-safe [`WriteId`] (`origin = local_name`,
    ///    `origin_creation = local_creation`, `counter` from a monotonic field);
    /// 2. registers `write_id → Sender` in the shared correlation registry so the
    ///    inbound `WriteAck` router can feed votes back;
    /// 3. spawns a [`WriteProposal`] send to each `send_target` onto the endpoint
    ///    runtime (fire-and-forget; a failed send is logged-and-ignored — the tally
    ///    times out or fences. Robust at-least-once retry/backoff is a follow-up;
    ///    the structure leaves room for a retry loop here);
    /// 4. blocks the calling thread on [`wait_for_cas_quorum_from_receiver`]. The
    ///    LOCAL node self-accepts implicitly via the tally's `count_local_ack`; no
    ///    local [`CasVote`] is sent (that would double-count the local ack).
    ///
    /// The `write_id` is ALWAYS deregistered before returning (commit, fence, or
    /// timeout) by a drop-guard, so neither an early return nor a panic can leak a
    /// registry entry; a late ack arriving after deregistration is dropped by the
    /// inbound router's unknown-`write_id` path.
    ///
    /// # Threading contract
    ///
    /// This call BLOCKS the calling thread on the quorum receiver, so it must run
    /// on a synchronous (non-async) thread. If invoked from within ANY tokio
    /// runtime context it returns [`ConsistencyError::TransportUnavailable`] rather
    /// than parking a beamr worker (which could wedge the single-worker runtime
    /// under load).
    pub fn propose_write(
        &self,
        write: ProposeWrite,
        shard_id: ShardId,
        epoch: Ballot,
        membership: &WriteMembership,
        timeout: Duration,
    ) -> Result<QuorumOutcome<SyncNodeId>, ConsistencyError> {
        // The bare-epoch entry point (2a / 3-3 direct callers) stamps `seq = 0`.
        // The owner-driven path (`Database::replicate_write`) uses
        // `propose_write_stamped` with the atomic per-epoch `seq` (R-SEQ). The caller
        // supplies the owning `shard_id` — for a key-routed write that is exactly
        // `shard_for(&key)`, which is what the receiver previously re-derived, so the
        // routing stays byte-identical.
        self.propose_write_stamped(
            write,
            shard_id,
            Stamp::new(epoch, 0),
            false,
            membership,
            timeout,
        )
    }

    /// As [`Self::propose_write`], but carrying the full owner-assigned commit
    /// stamp `(epoch, seq)` (AA-3-4a, R-SEQ) and a `tombstone` flag (AA-3-4b). The
    /// `seq` is placed on the `WriteProposal` so every replica stores the identical
    /// stamp (§2.4); `tombstone` tells the receiver to apply a stamped tombstone
    /// (a replicated delete) through the same fence + CAS path.
    pub fn propose_write_stamped(
        &self,
        write: ProposeWrite,
        shard_id: ShardId,
        stamp: Stamp,
        tombstone: bool,
        membership: &WriteMembership,
        timeout: Duration,
    ) -> Result<QuorumOutcome<SyncNodeId>, ConsistencyError> {
        let ProposeWrite {
            key,
            expected,
            value,
            ttl,
        } = write;
        // The coordinator BLOCKS; it must not run on a runtime worker (it would
        // park a beamr worker and can deadlock the single-worker runtime).
        if Handle::try_current().is_ok() {
            return Err(ConsistencyError::TransportUnavailable);
        }

        let write_id = WriteId {
            origin: SyncNodeId::new(self.local_name.clone()),
            origin_creation: self.local_creation,
            counter: self.write_counter.fetch_add(1, Ordering::Relaxed),
        };

        let (vote_tx, vote_rx) = mpsc::channel::<CasVote<SyncNodeId>>();
        self.registry.insert(write_id.clone(), vote_tx);

        // Deregister on EVERY exit path (commit, fence, timeout, early return, or
        // panic) so the registry can never leak an entry. A late ack that arrives
        // after this guard fires is dropped by the inbound router (unknown id).
        let _guard = RegistryGuard {
            registry: &self.registry,
            write_id: write_id.clone(),
        };

        let handle = self
            .runtime()
            .map_err(|_error| ConsistencyError::TransportUnavailable)?
            .handle()
            .clone();
        let proposal = WriteProposal {
            write_id,
            shard_id,
            key,
            expected,
            value,
            ttl,
            epoch: stamp.epoch,
            seq: stamp.seq,
            tombstone,
        };

        // Encode the proposal frame ONCE on this synchronous thread (a `SyncError`
        // here means the proposal could not be framed at all — fail closed rather
        // than self-quorum on an unsendable write).
        let frame = encode_beamr_sync_frame(&SyncMessage::WriteProposal(proposal))
            .map_err(|_error| ConsistencyError::TransportUnavailable)?;
        let frame = Arc::new(frame);

        // Fire-and-forget a proposal to each reachable send target. We `spawn`
        // onto the endpoint runtime (rather than the sync `block_on` bridge) so the
        // sends run concurrently while this thread proceeds to block on votes.
        // `propose_write` runs OUTSIDE the runtime (guarded above), so `handle.spawn`
        // is the correct cross-thread hand-off onto the runtime. At-least-once is a
        // single attempt this increment; a failed send is logged-and-ignored (the
        // tally times out or fences). Structured so a retry loop slots in here.
        for target in &membership.send_targets {
            let manager = self.manager.clone();
            let remote = self.atom_table.intern(target.as_str());
            let frame = Arc::clone(&frame);
            handle.spawn(async move {
                match manager.get_connection(remote) {
                    Some(connection) => {
                        if let Err(error) = connection.write_raw(frame.as_slice()).await {
                            log::warn!("write proposal send failed: {error}");
                        }
                    }
                    None => log::warn!("write proposal send target unreachable"),
                }
            });
        }

        let strong = StrongConsistency::new(membership.total_nodes, timeout);
        wait_for_cas_quorum_from_receiver(strong, &vote_rx)
    }

    /// Coordinate ONE all-or-nothing multi-key BATCH to quorum across the cluster
    /// (A1c — the batch analogue of [`Self::propose_write_stamped`]).
    ///
    /// Sends ONE [`BatchWriteProposal`] (the WHOLE stream-append's entries under
    /// ONE shared `(epoch, seq)` `stamp` and ONE explicit owning `shard_id`) to each
    /// membership peer, then blocks collecting [`BatchWriteAck`]s and tallies a
    /// strict-majority quorum on EXACTLY the same machinery the single-key path uses:
    ///
    /// * the SAME writer-side correlation registry (`write_id → Sender<CasVote>`),
    ///   because a [`BatchWriteAck`] has the identical `(write_id, acker,
    ///   acker_creation, outcome)` shape as a [`WriteAck`] — the inbound router maps
    ///   its outcome to a [`CasVote`] with [`route_write_ack`], so an ack counts
    ///   toward quorum ONLY when `outcome == Applied`; a [`AckOutcome::Rejected`]
    ///   (a fence or any single-key CAS mismatch) is a vote-AGAINST, and a genuine
    ///   apply fault is a retryable `Fault` — IDENTICAL to the single-key tally;
    /// * the SAME [`StrongConsistency`] tally + [`wait_for_cas_quorum_from_receiver`]
    ///   blocking primitive, so the local node self-accepts as a phantom (no local
    ///   apply yet — the proposer applies in `replicate_append` step d).
    ///
    /// Because the ack shape and the vote mapping are shared, there is NO parallel
    /// tally: this is `propose_write_stamped` over a batch frame.
    ///
    /// # Threading contract
    /// BLOCKS the calling thread on the quorum receiver, so it must run OUTSIDE any
    /// tokio runtime (same guard as [`Self::propose_write_stamped`]); from within a
    /// runtime it returns [`ConsistencyError::TransportUnavailable`].
    ///
    /// # Errors
    /// Returns [`ConsistencyError`] when the batch does not reach quorum (fenced,
    /// timed out, or the transport was unavailable / the frame could not be encoded).
    pub fn propose_batch_stamped(
        &self,
        shard_id: ShardId,
        entries: Vec<BatchWriteEntry>,
        stamp: Stamp,
        membership: &WriteMembership,
        timeout: Duration,
    ) -> Result<QuorumOutcome<SyncNodeId>, ConsistencyError> {
        // The coordinator BLOCKS; it must not run on a runtime worker (it would
        // park a beamr worker and can deadlock the single-worker runtime).
        if Handle::try_current().is_ok() {
            return Err(ConsistencyError::TransportUnavailable);
        }

        let write_id = WriteId {
            origin: SyncNodeId::new(self.local_name.clone()),
            origin_creation: self.local_creation,
            counter: self.write_counter.fetch_add(1, Ordering::Relaxed),
        };

        // The SAME writer-side registry the single-key path uses: a BatchWriteAck
        // echoes this write_id and the inbound router feeds its CasVote back here.
        let (vote_tx, vote_rx) = mpsc::channel::<CasVote<SyncNodeId>>();
        self.registry.insert(write_id.clone(), vote_tx);
        let _guard = RegistryGuard {
            registry: &self.registry,
            write_id: write_id.clone(),
        };

        let handle = self
            .runtime()
            .map_err(|_error| ConsistencyError::TransportUnavailable)?
            .handle()
            .clone();
        let proposal = BatchWriteProposal {
            write_id,
            shard_id,
            entries,
            stamp,
        };

        // Encode the batch frame ONCE on this synchronous thread (a framing failure
        // means the batch is unsendable — fail closed rather than self-quorum on an
        // unsendable write).
        let frame = encode_beamr_sync_frame(&SyncMessage::BatchWriteProposal(proposal))
            .map_err(|_error| ConsistencyError::TransportUnavailable)?;
        let frame = Arc::new(frame);

        // Fire-and-forget the batch proposal to each reachable send target, spawned
        // onto the endpoint runtime exactly like `propose_write_stamped`.
        for target in &membership.send_targets {
            let manager = self.manager.clone();
            let remote = self.atom_table.intern(target.as_str());
            let frame = Arc::clone(&frame);
            handle.spawn(async move {
                match manager.get_connection(remote) {
                    Some(connection) => {
                        if let Err(error) = connection.write_raw(frame.as_slice()).await {
                            log::warn!("batch write proposal send failed: {error}");
                        }
                    }
                    None => log::warn!("batch write proposal send target unreachable"),
                }
            });
        }

        let strong = StrongConsistency::new(membership.total_nodes, timeout);
        wait_for_cas_quorum_from_receiver(strong, &vote_rx)
    }

    /// Run ONE Phase-1 Prepare round for `shard_id` at `ballot` and collect
    /// promises to a strict majority of `membership.total_nodes` (§2.2 steps 2-4).
    ///
    /// This is the transport half of `AcquireShard`: the caller
    /// ([`Database::acquire_shard`](crate::db::Database::acquire_shard)) has ALREADY
    /// minted+fsync'd the ballot and recorded its own local self-promise (§2.2
    /// steps 1-2); it passes that self-promise in as `self_promise`, counted as the
    /// FIRST vote. We then:
    ///
    /// 1. register `shard_id → Sender<ElectionVote>` so the inbound `Promise`/`Nack`
    ///    router feeds replies back (deregistered on EVERY exit by a drop guard);
    /// 2. fire a `Prepare{shard_id, ballot}` at each reachable `send_target`
    ///    (spawned onto the endpoint runtime, fire-and-forget — exactly like
    ///    `propose_write`'s proposal sends);
    /// 3. block collecting votes until a strict majority of promises (including the
    ///    self-promise) is reached → win; or a deadline / higher-ballot loss.
    ///
    /// A `Promise` whose `ballot != ballot` (a stale reply for a different attempt)
    /// is ignored. A `Nack` carrying a strictly higher `promised` records the
    /// highest competing ballot so the caller can re-mint above it on retry. The
    /// required majority is `quorum_size(total_nodes) = total/2 + 1` — the SAME
    /// strict-majority denominator the write path uses, the load-bearing §4
    /// intersection property.
    ///
    /// # Threading contract
    /// BLOCKS the calling thread on the vote receiver, so it must run OUTSIDE any
    /// tokio runtime (same guard as [`Self::propose_write`]); from within a runtime
    /// it returns [`ElectionError::Transport`] rather than parking a worker.
    pub fn run_prepare_round(
        &self,
        shard_id: ShardId,
        ballot: &Ballot,
        self_promise: Promise,
        membership: &WriteMembership,
        timeout: Duration,
    ) -> Result<Vec<Promise>, ElectionError> {
        // The coordinator BLOCKS; it must not run on a runtime worker.
        if Handle::try_current().is_ok() {
            return Err(ElectionError::Transport(
                "acquire_shard blocked from inside the distribution runtime".to_owned(),
            ));
        }

        let required = quorum_size(membership.total_nodes)
            .map_err(|error| ElectionError::Transport(error.to_string()))?;

        let (vote_tx, vote_rx) = mpsc::channel::<ElectionVote>();
        // Only one election per shard per endpoint at a time; replace any stale
        // entry. Deregister on EVERY exit so a late vote after return is dropped.
        self.elections.insert(shard_id, vote_tx);
        let _guard = ElectionGuard {
            elections: &self.elections,
            shard_id,
        };

        let handle = self
            .runtime()
            .map_err(|error| ElectionError::Transport(error.to_string()))?
            .handle()
            .clone();

        // Frame the Prepare ONCE on this synchronous thread; a framing failure
        // means the Prepare is unsendable — fail closed rather than self-elect.
        let frame = encode_beamr_sync_frame(&SyncMessage::Prepare(Prepare {
            shard_id,
            ballot: ballot.clone(),
        }))
        .map_err(|error| ElectionError::Transport(error.to_string()))?;
        let frame = Arc::new(frame);

        // Step 3: fire a Prepare at every reachable peer (fire-and-forget onto the
        // runtime, exactly like propose_write). A failed send is logged-and-ignored;
        // the tally times out or loses if too few promises return.
        for target in &membership.send_targets {
            let manager = self.manager.clone();
            let remote = self.atom_table.intern(target.as_str());
            let frame = Arc::clone(&frame);
            handle.spawn(async move {
                match manager.get_connection(remote) {
                    Some(connection) => {
                        if let Err(error) = connection.write_raw(frame.as_slice()).await {
                            log::warn!("prepare send failed: {error}");
                        }
                    }
                    None => log::warn!("prepare send target unreachable"),
                }
            });
        }

        collect_prepare_votes(ballot, required, self_promise, &vote_rx, timeout)
    }

    /// Run one handoff catch-up round against a single `source` promiser (AA-3-4,
    /// §2.4): send a [`ShardSyncRequest`] naming this node as requester and block
    /// for the source's [`PushResponse`] (the full reachable node set + its
    /// committed root). Mirrors [`Self::run_prepare_round`]'s registry+coordinator
    /// shape: register a per-shard channel, fire the request onto the runtime,
    /// park for the reply.
    ///
    /// Returns the source's `PushResponse` (the caller folds it into the owning
    /// shard's `merge_adopt`). Like the Prepare round, this BLOCKS, so it must run
    /// OUTSIDE the distribution runtime.
    ///
    /// # Errors
    /// Returns [`SyncError`] if called from inside the runtime, if framing the
    /// request fails, or if no response arrives within `timeout`.
    pub fn run_catch_up_round(
        &self,
        shard_id: ShardId,
        source: &SyncNodeId,
        from_root: Option<Hash>,
        timeout: Duration,
    ) -> Result<PushResponse, SyncError> {
        // The coordinator BLOCKS; it must not run on a runtime worker.
        if Handle::try_current().is_ok() {
            return Err(SyncError::TransportBlockingFromAsync);
        }

        let (tx, rx) = mpsc::channel::<PushResponse>();
        // Only one catch-up per shard per endpoint at a time; replace any stale
        // entry. Deregister on EVERY exit so a late response after return is dropped.
        self.catch_ups.insert(shard_id, tx);
        let _guard = CatchUpGuard {
            catch_ups: &self.catch_ups,
            shard_id,
        };

        let handle = self.runtime()?.handle().clone();

        let request = ShardSyncRequest::new(
            shard_id,
            SyncNodeId::new(self.local_name.clone()),
            from_root,
        );
        let frame = encode_beamr_sync_frame(&SyncMessage::ShardSyncRequest(request))?;
        let frame = Arc::new(frame);

        let manager = self.manager.clone();
        let remote = self.atom_table.intern(source.as_str());
        let frame_for_send = Arc::clone(&frame);
        handle.spawn(async move {
            match manager.get_connection(remote) {
                Some(connection) => {
                    if let Err(error) = connection.write_raw(frame_for_send.as_slice()).await {
                        log::warn!("catch-up request send failed: {error}");
                    }
                }
                None => log::warn!("catch-up request source unreachable"),
            }
        });

        match rx.recv_timeout(timeout) {
            Ok(response) => Ok(response),
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                Err(SyncError::TransportDrainDisconnected)
            }
        }
    }

    /// Send a framed [`SyncMessage`] to `target` over the live transport, fire-and-
    /// forget onto the runtime (AA-3-4 source-side reply). Used by the responder to
    /// route a `PushResponse` back to a catch-up requester it cannot block on.
    ///
    /// # Errors
    /// Returns [`SyncError`] if framing fails or the runtime is unavailable.
    pub fn send_message_fire_and_forget(
        &self,
        target: &SyncNodeId,
        message: &SyncMessage,
    ) -> Result<(), SyncError> {
        let frame = encode_beamr_sync_frame(message)?;
        let frame = Arc::new(frame);
        let handle = self.runtime()?.handle().clone();
        let manager = self.manager.clone();
        let remote = self.atom_table.intern(target.as_str());
        handle.spawn(async move {
            match manager.get_connection(remote) {
                Some(connection) => {
                    if let Err(error) = connection.write_raw(frame.as_slice()).await {
                        log::warn!("catch-up response send failed: {error}");
                    }
                }
                None => log::warn!("catch-up response target unreachable"),
            }
        });
        Ok(())
    }

    /// Block until an inbound sync message arrives or `timeout` elapses.
    ///
    /// Returns `Ok(Some(_))` with the decoded message (or a decode error),
    /// `Ok(None)` on timeout, and [`SyncError::TransportDrainDisconnected`] only
    /// if every sender has been dropped (the endpoint is shutting down).
    pub fn recv_inbound(&self, timeout: Duration) -> Result<Option<InboundSync>, SyncError> {
        let inbound = self
            .inbound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match inbound.recv_timeout(timeout) {
            Ok(message) => Ok(Some(message)),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Disconnected) => Err(SyncError::TransportDrainDisconnected),
        }
    }

    /// A clonable handle to the endpoint's runtime for spawning async work.
    ///
    /// Later increments (the writer-side coordinator) spawn proposal sends onto
    /// this handle. The contract: a blocking call (e.g. a Strong write parked on a
    /// quorum receiver) must NOT run on this runtime's worker threads.
    pub fn runtime_handle(&self) -> Result<Handle, SyncError> {
        Ok(self.runtime()?.handle().clone())
    }

    fn runtime(&self) -> Result<&Arc<Runtime>, SyncError> {
        self.runtime
            .as_ref()
            .ok_or(SyncError::TransportRuntimeUnavailable)
    }
}

/// Guard the `block_on` bridges (`bind`/`connect`/`send`) against being called
/// from inside a tokio runtime, where `block_on` panics ("Cannot start a runtime
/// from within a runtime"). Fails safe with a `SyncError` instead of panicking so
/// an async-context caller gets a recoverable error, not a crash.
fn ensure_outside_runtime() -> Result<(), SyncError> {
    if Handle::try_current().is_ok() {
        return Err(SyncError::TransportBlockingFromAsync);
    }
    Ok(())
}

impl Drop for DistributionEndpoint {
    fn drop(&mut self) {
        // Shut the accept loop down first (synchronous notify), then move the
        // runtime drop OFF any async context. Dropping a tokio runtime from an
        // async worker panics; spawning a plain std::thread to own the (last)
        // Arc guarantees the blocking shutdown runs on a non-async thread.
        self.accept.shutdown();
        if let Some(runtime) = self.runtime.take() {
            thread::spawn(move || drop(runtime));
        }
    }
}

/// Holds the [`AcceptHandle`] and shuts the accept loop down on drop.
struct AcceptGuard {
    handle: beamr::distribution::connection::AcceptHandle,
}

impl AcceptGuard {
    const fn new(handle: beamr::distribution::connection::AcceptHandle) -> Self {
        Self { handle }
    }

    fn shutdown(&self) {
        self.handle.shutdown();
    }
}

/// Deregisters a `write_id` from the correlation registry on drop.
///
/// Held by [`DistributionEndpoint::propose_write`] so EVERY exit path — commit,
/// fence, timeout, early return, or panic — removes the entry. This is the
/// "registry leak" mitigation from the design risk register: a registered
/// `write_id` is bounded by the lifetime of the in-flight write.
struct RegistryGuard<'registry> {
    registry: &'registry WriteRegistry,
    write_id: WriteId,
}

impl Drop for RegistryGuard<'_> {
    fn drop(&mut self) {
        self.registry.remove(&self.write_id);
    }
}

/// Deregisters an in-flight election (by shard id) from the election registry on
/// drop. Held by [`DistributionEndpoint::run_prepare_round`] so EVERY exit path —
/// win, loss, timeout, early return, or panic — removes the entry; a late vote
/// arriving after the coordinator returned is then dropped by the inbound router.
struct ElectionGuard<'registry> {
    elections: &'registry ElectionRegistry,
    shard_id: ShardId,
}

impl Drop for ElectionGuard<'_> {
    fn drop(&mut self) {
        self.elections.remove(&self.shard_id);
    }
}

/// Deregisters an in-flight catch-up (by shard id) from the catch-up registry on
/// drop (AA-3-4). Held by [`DistributionEndpoint::run_catch_up_round`] so EVERY
/// exit path — success, timeout, early return, or panic — removes the entry; a
/// late `PushResponse` arriving after the coordinator returned is then dropped.
struct CatchUpGuard<'registry> {
    catch_ups: &'registry CatchUpRegistry,
    shard_id: ShardId,
}

impl Drop for CatchUpGuard<'_> {
    fn drop(&mut self) {
        self.catch_ups.remove(&self.shard_id);
    }
}

/// Tally Prepare-round votes until a strict majority of promises is reached, the
/// deadline elapses, or a higher competing ballot is learned (§2.2 step 4).
///
/// Counting rules, exactly:
/// * `self_promise` is the candidate's own durably-recorded promise (§2.2 step 2),
///   counted as the FIRST promise — it is part of full membership and one of the
///   quorum. So `promises` starts at `[self_promise]` and `granted` at 1.
/// * A `Promise` is counted ONLY if `promise.ballot == ballot` (a stale Promise
///   for a prior attempt is ignored) AND its `promiser` has not already promised
///   (dedup by the GRANTING node id so a duplicate frame cannot double-count). The
///   `ballot` echoes the candidate's ballot, so `promiser` — not `ballot.node` —
///   identifies who voted.
/// * Reaching `required` distinct promises wins immediately.
/// * A `Nack` with `nack.promised > ballot` (or any promise/nack carrying a higher
///   ballot) updates `highest_seen`; it never decreases the promise count, but on
///   timeout it turns the loss into [`ElectionError::Lost`] rather than
///   [`ElectionError::Timeout`], so the caller knows to re-mint strictly above it.
fn collect_prepare_votes(
    ballot: &Ballot,
    required: usize,
    self_promise: Promise,
    receiver: &Receiver<ElectionVote>,
    timeout: Duration,
) -> Result<Vec<Promise>, ElectionError> {
    use std::collections::HashSet;
    use std::time::Instant;

    let mut promised_nodes: HashSet<SyncNodeId> = HashSet::new();
    // Seed the dedup set with the self-promise's PROMISER (the local node), so the
    // candidate's own vote is counted exactly once and a peer that happens to share
    // no id with it is counted separately.
    promised_nodes.insert(self_promise.promiser.clone());
    let mut promises = vec![self_promise];
    let mut highest_seen = ballot.clone();

    if promises.len() >= required {
        return Ok(promises);
    }

    let deadline = Instant::now() + timeout;
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Err(finish_loss(required, promises.len(), ballot, &highest_seen));
        };

        match receiver.recv_timeout(remaining) {
            Ok(ElectionVote::Promised(promise)) => {
                // Ignore a stale Promise for a different (prior) ballot attempt.
                if &promise.ballot != ballot {
                    if promise.ballot > highest_seen {
                        highest_seen = promise.ballot.clone();
                    }
                    continue;
                }
                if promised_nodes.insert(promise.promiser.clone()) {
                    promises.push(promise);
                    if promises.len() >= required {
                        return Ok(promises);
                    }
                }
            }
            Ok(ElectionVote::Nacked(nack)) => {
                if nack.promised > highest_seen {
                    highest_seen = nack.promised;
                }
            }
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                return Err(finish_loss(required, promises.len(), ballot, &highest_seen));
            }
        }
    }
}

/// Classify a Prepare-round loss: if a strictly higher ballot was seen, this is a
/// [`ElectionError::Lost`] (re-mint above `highest_seen`); otherwise a plain
/// [`ElectionError::Timeout`] (too few nodes promised in time).
fn finish_loss(
    required: usize,
    promised_votes: usize,
    own_ballot: &Ballot,
    highest_seen: &Ballot,
) -> ElectionError {
    if highest_seen > own_ballot {
        ElectionError::Lost {
            highest_seen: highest_seen.clone(),
        }
    } else {
        ElectionError::Timeout {
            required,
            promised_votes,
        }
    }
}

/// Register the beamr control-frame handler that drains decoded sync messages.
///
/// Inbound [`SyncMessage::WriteAck`] is ROUTED to the writer-side correlation
/// registry instead of the generic drain (it is a reply to a local in-flight
/// write, not a request to apply). Every other variant flows to the generic
/// drain unchanged.
fn register_inbound_drain(
    manager: &ConnectionManager,
    sender: Sender<InboundSync>,
    registry: WriteRegistry,
    elections: ElectionRegistry,
    catch_ups: CatchUpRegistry,
    local_creation: u32,
) {
    register_beamr_sync_handler(manager, move |decoded| {
        // The read loop runs on a beamr lifecycle task; `Sender::send` is
        // non-blocking and safe to call from there.
        match decoded {
            Ok(SyncMessage::WriteAck(ack)) => route_write_ack(&registry, local_creation, &ack),
            // A1c: a BatchWriteAck is the REPLY to a local in-flight
            // `propose_batch_stamped`. It has the identical (write_id, acker,
            // acker_creation, outcome) shape as a WriteAck, so it routes through the
            // SAME writer-side registry with the SAME incarnation gate + vote mapping
            // (`route_ack_outcome`) — Applied = Accept, fence/CAS-mismatch = Reject,
            // apply-fault = Fault. Without this it would fall to the generic drain and
            // the coordinator's votes would be lost (perpetual timeout).
            Ok(SyncMessage::BatchWriteAck(ack)) => {
                route_batch_write_ack(&registry, local_creation, &ack);
            }
            // Promise/Nack are REPLIES to a local in-flight `acquire_shard`, routed
            // to the election registry (not the generic drain). A `Prepare`, by
            // contrast, is a REQUEST to act as an acceptor — it flows to the generic
            // drain so the responder loop applies it via `handle_inbound_prepare`.
            Ok(SyncMessage::Promise(promise)) => {
                route_election_vote(
                    &elections,
                    promise.shard_id,
                    ElectionVote::Promised(promise),
                );
            }
            Ok(SyncMessage::Nack(nack)) => {
                route_election_vote(&elections, nack.shard_id, ElectionVote::Nacked(nack));
            }
            // A PushResponse is the REPLY to a local in-flight `become_live` catch-up
            // (AA-3-4): route it to the catch-up registry by shard. A
            // ShardSyncRequest, by contrast, is a REQUEST to act as the sync SOURCE —
            // it flows to the generic drain so the responder loop answers it via
            // `handle_inbound_shard_sync_request`.
            Ok(SyncMessage::PushResponse(response)) => {
                route_catch_up_response(&catch_ups, response);
            }
            // Every OTHER variant (Prepare, ShardSyncRequest, sync traffic, decode
            // errors) -> generic drain. A send error means the receiver was dropped.
            other => {
                let _ = sender.send(other);
            }
        }
    });
}

/// Route an inbound election reply (`Promise`/`Nack`) to the coordinator waiting
/// on its shard. An unknown/expired shard key (the election already returned and
/// deregistered) and a send onto a disconnected receiver are both dropped
/// quietly — mirrors [`route_write_ack`]'s unknown-id handling.
fn route_election_vote(elections: &ElectionRegistry, shard_id: ShardId, vote: ElectionVote) {
    let Some(sender) = elections.get(&shard_id) else {
        return;
    };
    let _ = sender.send(vote);
}

/// Route an inbound catch-up reply (`PushResponse`) to the coordinator waiting on
/// its shard (AA-3-4). An unknown/expired shard key (the catch-up already returned
/// and deregistered) and a send onto a disconnected receiver are both dropped
/// quietly — mirrors [`route_election_vote`].
fn route_catch_up_response(catch_ups: &CatchUpRegistry, response: PushResponse) {
    let Some(sender) = catch_ups.get(&response.shard_id) else {
        return;
    };
    let _ = sender.send(response);
}

/// Route an inbound `WriteAck` to the coordinator waiting on its `write_id`.
///
/// Applies the incarnation gate (design Fix D): an ack is dropped unless
/// `write_id.origin_creation == local_creation`, so a stale ack from a prior
/// writer incarnation can never satisfy a reused `write_id`. An unknown/expired
/// `write_id` (already deregistered) and a send onto a disconnected receiver are
/// both dropped quietly — no panic.
fn route_write_ack(registry: &WriteRegistry, local_creation: u32, ack: &WriteAck) {
    route_ack_outcome(
        registry,
        local_creation,
        &ack.write_id,
        &ack.acker,
        ack.outcome,
    );
}

/// Route an inbound `BatchWriteAck` to the coordinator waiting on its `write_id`
/// (A1c). Identical routing to [`route_write_ack`] — same incarnation gate, same
/// registry, same `outcome → CasVote` mapping — because a [`BatchWriteAck`] carries
/// the identical correlation fields. The single all-or-nothing batch verdict counts
/// toward quorum exactly as a single-key verdict does.
fn route_batch_write_ack(registry: &WriteRegistry, local_creation: u32, ack: &BatchWriteAck) {
    route_ack_outcome(
        registry,
        local_creation,
        &ack.write_id,
        &ack.acker,
        ack.outcome,
    );
}

/// Shared routing for a single-key or batch ack: apply the Fix D incarnation gate,
/// look up the in-flight `write_id`, map the [`AckOutcome`] to a [`CasVote`], and
/// feed it to the waiting coordinator. An ack for a prior incarnation, an
/// unknown/expired `write_id`, or a send onto a disconnected receiver are all
/// dropped quietly — no panic.
fn route_ack_outcome(
    registry: &WriteRegistry,
    local_creation: u32,
    write_id: &WriteId,
    acker: &SyncNodeId,
    outcome: AckOutcome,
) {
    // Fix D incarnation gate: discard an ack minted for a prior incarnation of
    // this writer, even if it names a counter we have since reused.
    if write_id.origin_creation != local_creation {
        return;
    }

    // Unknown / already-deregistered write_id -> drop quietly.
    let Some(sender) = registry.get(write_id) else {
        return;
    };

    let vote = match outcome {
        AckOutcome::Applied => CasVote::Accept(acker.clone()),
        // A CAS mismatch AND an epoch fence are both vote-AGAINSTs: the replica
        // refused on purpose (it is ahead / it promised a higher ballot), so each
        // erodes possible-accepts toward ConsistencyError::Fenced. Only a genuine
        // apply fault is a (retryable) transport-style Fault.
        AckOutcome::Rejected(RejectReason::CasMismatch | RejectReason::Fenced) => {
            CasVote::Reject(acker.clone())
        }
        AckOutcome::Rejected(RejectReason::ApplyError) => CasVote::Fault(acker.clone()),
    };

    // Send-on-disconnected (coordinator already returned + dropped the receiver)
    // -> drop quietly.
    let _ = sender.send(vote);
}
