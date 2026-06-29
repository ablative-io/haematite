//! SPIKE (NOT a feature to ship) — empirical validation that a haematite
//! `SyncMessage` can round-trip between two REAL beamr 0.9.0 distribution
//! endpoints over loopback TCP.
//!
//! This is the load-bearing unknown for active-active. haematite's sync wire
//! helpers (`crates/haematite/src/sync/protocol/wire.rs`) exist but have NEVER
//! been exercised over a real network: the production sync trigger is a no-op
//! (`db.rs:378`) and nothing in haematite constructs a beamr `ConnectionManager`
//! / `NetKernel`. This spike proves the path end-to-end with NO mocking of the
//! transport — real `TcpListener`s, real OTP handshake, real beamr read loop,
//! real haematite encode/decode.
//!
//! What it characterizes:
//!  * Two REAL beamr `ConnectionManager`s on loopback (distinct ports, port 0).
//!  * A REAL OTP distribution handshake establishing the link (poll
//!    `connected_nodes()` with a timeout — the handshake is async).
//!  * The RECEIVER registers haematite's existing `register_beamr_sync_handler`
//!    (`wire.rs:273`) with a handler that pushes the decoded `SyncMessage` into
//!    an `std::sync::mpsc` channel.
//!  * The SENDER sends a `RootExchangeRequest` (simplest variant) using the
//!    existing `send_root_exchange_request_via_beamr` helper (`wire.rs:170`)
//!    with the sender's `ConnectionManager` + the receiver's node `Atom`.
//!  * Asserts the receiver decodes the `SyncMessage` and it matches what was
//!    sent (round-trip identity on shard_id / target_root).
//!
//! KEY beamr 0.9.0 facts this spike relies on (verified against the resolved
//! dependency source):
//!  * `beamr::distribution::ConnectionManager::new(atom_table, resolver, cookie,
//!    local_node_name, local_creation)` (connection.rs:336). The connection
//!    table is keyed by the name the PEER advertises in the handshake, not the
//!    resolver key (connection.rs:510-554).
//!  * `ConnectionManager::listen(addr) -> AcceptHandle` spawns the async accept
//!    loop (connection.rs:494); `connect(node_name)` runs the outbound handshake
//!    (connection.rs:518).
//!  * The beamr read loop frames exactly like `encode_beamr_sync_frame`: an
//!    8-byte header (`control_len` || `payload_len`, both big-endian u32) then
//!    `control || payload`, dispatched to the registered control-frame handler
//!    as `handler(control, payload)` (connection.rs:587-630). This is byte-for-
//!    byte the frame haematite's `encode_beamr_sync_frame` produces (wire.rs:115).
//!  * Both managers SHARE one `Arc<AtomTable>` so the receiver's advertised node
//!    name interns to the SAME `Atom` the sender hands `get_connection`. This
//!    mirrors beamr's own `tests/distribution_e2e.rs`.
//!
//! Run with output:
//!   cargo test -p haematite --test spike_distribution_transport -- --nocapture --test-threads=1

#![allow(clippy::unwrap_used, clippy::panic, clippy::print_stdout)]
#![allow(
    clippy::expect_used,
    clippy::doc_markdown,
    clippy::redundant_closure_for_method_calls
)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use beamr::atom::{Atom, AtomTable};
use beamr::distribution::ConnectionManager;
use beamr::distribution::resolver::{NodeResolver, ResolveError, ResolveFuture};

use haematite::sync::{
    RootExchangeRequest, SyncError, SyncMessage, register_beamr_sync_handler,
    send_root_exchange_request_via_beamr,
};
use haematite::tree::Hash;

const COOKIE: &str = "haematite-spike-cookie";
const SENDER_NAME: &str = "sender@127.0.0.1";
const RECEIVER_NAME: &str = "receiver@127.0.0.1";

/// A resolver whose name->address map is filled in AFTER each node binds its
/// listener on port 0 (so we know the OS-assigned port). Mirrors the
/// `DynamicResolver` in beamr's own `tests/distribution_e2e.rs`.
#[derive(Default)]
struct DynamicResolver {
    nodes: Mutex<HashMap<String, SocketAddr>>,
}

impl DynamicResolver {
    fn insert(&self, name: &str, addr: SocketAddr) {
        self.nodes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(name.to_owned(), addr);
    }
}

impl NodeResolver for DynamicResolver {
    fn resolve<'a>(&'a self, name: &'a str) -> ResolveFuture<'a> {
        let result = self
            .nodes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(name)
            .copied()
            .ok_or(ResolveError::NotFound);
        Box::pin(async move { result })
    }
}

/// Poll `connected_nodes()` until `node` appears or the deadline elapses. The
/// OTP handshake is async and completes on a beamr lifecycle task, so we cannot
/// assume the link is up the instant `connect()` returns on the dialer side
/// (the accept side registers independently).
async fn wait_for_connection(manager: &ConnectionManager, node: Atom, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if manager.get_connection(node).is_some() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_message_round_trips_over_real_beamr_distribution() {
    // ----- Shared identity infrastructure -----------------------------------
    // ONE atom table shared by both managers. This is what makes the receiver's
    // advertised handshake name intern to the SAME Atom on the sender side, so
    // the sender's `get_connection(receiver_atom)` finds the link.
    let atom_table = Arc::new(AtomTable::with_common_atoms());
    let resolver = Arc::new(DynamicResolver::default());

    // ----- Stand up two REAL beamr endpoints --------------------------------
    let sender_manager = ConnectionManager::new(
        Arc::clone(&atom_table),
        Arc::clone(&resolver) as Arc<dyn NodeResolver + Send + Sync>,
        COOKIE,
        SENDER_NAME,
        1, // local_creation (incarnation)
    );
    let receiver_manager = ConnectionManager::new(
        Arc::clone(&atom_table),
        Arc::clone(&resolver) as Arc<dyn NodeResolver + Send + Sync>,
        COOKIE,
        RECEIVER_NAME,
        1,
    );

    // Bind both accept loops on loopback, OS-assigned ports.
    let sender_listen = sender_manager
        .listen("127.0.0.1:0".parse().expect("listen addr parses"))
        .await
        .expect("sender binds listener");
    let receiver_listen = receiver_manager
        .listen("127.0.0.1:0".parse().expect("listen addr parses"))
        .await
        .expect("receiver binds listener");

    println!(
        "[spike] sender listening on {}, receiver listening on {}",
        sender_listen.local_addr(),
        receiver_listen.local_addr()
    );

    // Now that ports are known, populate the resolver both ways.
    resolver.insert(SENDER_NAME, sender_listen.local_addr());
    resolver.insert(RECEIVER_NAME, receiver_listen.local_addr());

    // ----- RECEIVER: register the REAL haematite sync handler ----------------
    let (tx, rx) = mpsc::channel::<Result<SyncMessage, SyncError>>();
    register_beamr_sync_handler(&receiver_manager, move |decoded| {
        // The beamr read loop invokes this on a lifecycle task once a control
        // frame whose control == SYNC_CONTROL_FRAME arrives.
        let _ = tx.send(decoded);
    });

    // ----- Establish the connection (REAL OTP handshake) --------------------
    // Dial from the sender to the receiver. The returned connection (and the
    // table entry) is keyed by the receiver's advertised handshake name.
    let conn = sender_manager
        .connect(RECEIVER_NAME)
        .await
        .expect("sender completes handshake with receiver");
    println!(
        "[spike] handshake complete; dialed connection peer node atom = {:?}",
        conn.node()
    );

    // The Atom the sender must address: the receiver's advertised name, interned
    // in the SHARED atom table.
    let receiver_atom = atom_table.intern(RECEIVER_NAME);

    assert!(
        wait_for_connection(&sender_manager, receiver_atom, Duration::from_secs(5)).await,
        "sender's connection table never registered the receiver node"
    );
    println!(
        "[spike] sender connected_nodes = {:?}",
        sender_manager.connected_nodes()
    );

    // ----- SENDER: send a RootExchangeRequest via the REAL wire helper -------
    let sent_request = RootExchangeRequest::new(
        7, // shard_id
        Some(Hash::from_bytes([0xab; 32])),
    );

    // The wire helper hands us the established `Arc<DistConnection>` plus the
    // fully-encoded frame and asks us to write it. beamr's `write_raw` is async;
    // we are on a multi_thread runtime, so drive it via block_in_place +
    // block_on without blocking an async worker improperly.
    send_root_exchange_request_via_beamr(
        &sender_manager,
        receiver_atom,
        sent_request,
        |connection, frame| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    connection
                        .write_raw(&frame)
                        .await
                        .map_err(|_io| SyncError::TransportConnectionUnavailable)
                })
            })
        },
    )
    .expect("sender writes the encoded sync frame over the real link");
    println!("[spike] sender wrote RootExchangeRequest frame ({sent_request:?})");

    // ----- Assert round-trip identity on the receiver ------------------------
    let received = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("receiver's control-frame handler decoded a SyncMessage in time")
        .expect("decoded payload is a valid SyncMessage");

    println!("[spike] receiver decoded: {received:?}");

    match received {
        SyncMessage::RootRequest(got) => {
            assert_eq!(got.shard_id, sent_request.shard_id, "shard_id round-trips");
            assert_eq!(
                got.target_root, sent_request.target_root,
                "target_root round-trips"
            );
            assert_eq!(got, sent_request, "full RootExchangeRequest round-trips");
        }
        other => panic!("expected RootRequest, got {other:?}"),
    }

    println!("[spike] PASS: SyncMessage round-tripped over real beamr loopback distribution");

    // ----- Teardown ----------------------------------------------------------
    sender_listen.shutdown();
    receiver_listen.shutdown();
}
