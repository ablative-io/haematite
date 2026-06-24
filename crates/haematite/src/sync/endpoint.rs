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
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use beamr::atom::{Atom, AtomTable};
use beamr::distribution::ConnectionManager;
use beamr::distribution::resolver::{NodeResolver, ResolveError, ResolveFuture};
use tokio::runtime::{Builder, Handle, Runtime};

use super::protocol::{
    SyncError, SyncMessage, register_beamr_sync_handler, send_sync_message_via_beamr,
};

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
        register_inbound_drain(&manager, tx);

        Ok(Self {
            atom_table,
            resolver,
            manager,
            accept: AcceptGuard::new(accept),
            runtime: Some(runtime),
            inbound: Mutex::new(inbound),
            local_name,
            local_addr,
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
    /// This call blocks the calling thread until the frame is written. It MUST
    /// NOT be invoked from within the endpoint runtime's own worker threads —
    /// `Handle::block_on` panics if called from inside a runtime. Production
    /// callers run on haematite's synchronous shard/database threads, which
    /// satisfies this contract; async callers must instead drive the send through
    /// [`DistributionEndpoint::runtime_handle`].
    pub fn send(&self, remote: Atom, message: &SyncMessage) -> Result<(), SyncError> {
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
    pub fn send_to(&self, peer_name: &str, message: &SyncMessage) -> Result<(), SyncError> {
        self.send(self.atom_table.intern(peer_name), message)
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

/// Register the beamr control-frame handler that drains decoded sync messages.
fn register_inbound_drain(manager: &ConnectionManager, sender: Sender<InboundSync>) {
    register_beamr_sync_handler(manager, move |decoded| {
        // The read loop runs on a beamr lifecycle task; `Sender::send` is
        // non-blocking and safe to call from there. A send error means the
        // receiver was dropped (endpoint torn down) — nothing to do.
        let _ = sender.send(decoded);
    });
}
