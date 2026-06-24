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
use crate::sync::SyncNodeId;
use crate::sync::consistency::{
    CasVote, ConsistencyError, QuorumOutcome, StrongConsistency, wait_for_cas_quorum_from_receiver,
};
use crate::sync::membership::WriteMembership;
use crate::tree::Hash;

use super::protocol::{
    AckOutcome, RejectReason, SyncError, SyncMessage, WriteAck, WriteId, WriteProposal,
    encode_beamr_sync_frame, register_beamr_sync_handler, send_sync_message_via_beamr,
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
        register_inbound_drain(&manager, tx, Arc::clone(&registry), local_creation);

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
        key: KvKey,
        expected: Option<Hash>,
        value: KvValue,
        ttl: Option<Duration>,
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
            key,
            expected,
            value,
            ttl,
        };

        // Encode the proposal frame ONCE on this synchronous thread (a `SyncError`
        // here means the proposal could not be framed at all — fail closed rather
        // than self-quorum on an unsendable write).
        let frame = encode_beamr_sync_frame(&SyncMessage::WriteProposal(proposal))
            .map_err(|_error| ConsistencyError::TransportUnavailable)?;
        let frame = Arc::new(frame);

        // Fire-and-forget a proposal to each reachable send target. The send runs
        // natively async on the endpoint runtime (NOT the sync `block_on` bridge —
        // we are already inside the runtime here). At-least-once is a single
        // attempt this increment; a failed send is logged-and-ignored (the tally
        // times out or fences). Structured so a retry loop slots in here.
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
    local_creation: u32,
) {
    register_beamr_sync_handler(manager, move |decoded| {
        // The read loop runs on a beamr lifecycle task; `Sender::send` is
        // non-blocking and safe to call from there.
        match decoded {
            Ok(SyncMessage::WriteAck(ack)) => route_write_ack(&registry, local_creation, &ack),
            // Every OTHER variant (and decode errors) -> generic drain. A send
            // error means the receiver was dropped (endpoint torn down).
            other => {
                let _ = sender.send(other);
            }
        }
    });
}

/// Route an inbound `WriteAck` to the coordinator waiting on its `write_id`.
///
/// Applies the incarnation gate (design Fix D): an ack is dropped unless
/// `write_id.origin_creation == local_creation`, so a stale ack from a prior
/// writer incarnation can never satisfy a reused `write_id`. An unknown/expired
/// `write_id` (already deregistered) and a send onto a disconnected receiver are
/// both dropped quietly — no panic.
fn route_write_ack(registry: &WriteRegistry, local_creation: u32, ack: &WriteAck) {
    // Fix D incarnation gate: discard an ack minted for a prior incarnation of
    // this writer, even if it names a counter we have since reused.
    if ack.write_id.origin_creation != local_creation {
        return;
    }

    // Unknown / already-deregistered write_id -> drop quietly.
    let Some(sender) = registry.get(&ack.write_id) else {
        return;
    };

    let vote = match ack.outcome {
        AckOutcome::Applied => CasVote::Accept(ack.acker.clone()),
        AckOutcome::Rejected(RejectReason::CasMismatch) => CasVote::Reject(ack.acker.clone()),
        AckOutcome::Rejected(RejectReason::ApplyError) => CasVote::Fault(ack.acker.clone()),
    };

    // Send-on-disconnected (coordinator already returned + dropped the receiver)
    // -> drop quietly.
    let _ = sender.send(vote);
}
