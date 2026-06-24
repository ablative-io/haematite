//! Integration test for the active-active "2a-0" distribution substrate.
//!
//! Stands up TWO real [`Database`] instances, each with a live
//! [`DistributionEndpoint`] bound on `127.0.0.1:0`, connects them over real
//! loopback TCP (real OTP handshake, no transport mocking), and proves a
//! `SyncMessage` sent from node A via the **Database-level API**
//! (`send_sync_message`) is received and decoded on node B's drain
//! (`recv_sync_message`).
//!
//! This is the end-to-end proof for 2a-0: the plumbing (inbound drain + outbound
//! send) works through the Database API against a real network, not just raw
//! `ConnectionManager`s.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::{Duration, Instant};

use haematite::sync::{DistributionEndpoint, RootExchangeRequest, SyncError, SyncMessage};
use haematite::tree::Hash;
use haematite::{Database, DatabaseConfig};

const NODE_A_NAME: &str = "node-a@127.0.0.1";
const NODE_B_NAME: &str = "node-b@127.0.0.1";

/// Build a minimal single-shard, non-distributed database in `dir`.
///
/// 2a-0 only wires the transport substrate; the database's own sync config is
/// irrelevant to the endpoint, so we keep it single-node here and attach the
/// endpoint explicitly.
fn make_database(dir: &std::path::Path) -> Database {
    let config = DatabaseConfig {
        data_dir: dir.to_path_buf(),
        shard_count: 1,
        sweep_interval: None,
        distributed: None,
    };
    Database::create(config).expect("database is created")
}

/// Poll `predicate` until it returns true or `timeout` elapses.
fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if predicate() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn sync_message_round_trips_between_two_live_databases() {
    let dir_a = tempfile::tempdir().expect("tempdir a");
    let dir_b = tempfile::tempdir().expect("tempdir b");

    // ----- Two real databases, each with a live distribution endpoint --------
    let loopback = "127.0.0.1:0".parse().expect("loopback addr parses");
    let endpoint_a =
        DistributionEndpoint::bind(NODE_A_NAME, loopback, 1, None).expect("node A endpoint binds");
    let endpoint_b =
        DistributionEndpoint::bind(NODE_B_NAME, loopback, 1, None).expect("node B endpoint binds");

    let addr_a = endpoint_a.local_addr();
    let addr_b = endpoint_b.local_addr();

    let database_a = make_database(dir_a.path()).with_distribution(endpoint_a);
    let database_b = make_database(dir_b.path()).with_distribution(endpoint_b);

    // ----- Teach each side how to reach the other, then dial A -> B -----------
    // The connect side must know B's address; the accept side (B) must know A's
    // address so its handshake bookkeeping resolves.
    database_b
        .connect_peer(NODE_A_NAME, addr_a)
        .map(drop)
        .ok();
    database_a
        .connect_peer(NODE_B_NAME, addr_b)
        .expect("node A dials node B");

    // The OTP handshake completes asynchronously on a beamr lifecycle task, so
    // poll the Database-level `connected_nodes` view until B's link registers.
    let b_atom = database_a.peer_atom(NODE_B_NAME).expect("intern B name");
    let connected = wait_until(Duration::from_secs(5), || {
        database_a
            .connected_nodes()
            .map(|nodes| nodes.contains(&b_atom))
            .unwrap_or(false)
    });
    assert!(connected, "node A never registered a link to node B");

    // ----- Send a SyncMessage A -> B via the Database-level API --------------
    let sent = RootExchangeRequest::new(7, Some(Hash::from_bytes([0xab; 32])));
    database_a
        .send_sync_message(NODE_B_NAME, &SyncMessage::RootRequest(sent))
        .expect("node A sends the sync message over the live transport");

    // ----- Assert node B drains + decodes the message ------------------------
    let received = database_b
        .recv_sync_message(Duration::from_secs(5))
        .expect("node B recv does not error")
        .expect("node B receives a message before timeout")
        .expect("decoded payload is a valid SyncMessage");

    match received {
        SyncMessage::RootRequest(got) => {
            assert_eq!(got, sent, "RootExchangeRequest round-trips through the Database API");
        }
        other => panic!("expected RootRequest, got {other:?}"),
    }
}

/// The `block_on` bridges (`bind`/`connect`/`send`) must FAIL SAFE — return a
/// `SyncError`, not panic — when invoked from within a tokio runtime context.
/// This is the must-fix from the 2a-0 endpoint review: a `#[tokio::main]`
/// consumer calling into the endpoint from an async task previously got a
/// guaranteed `block_on` panic. `bind` exercises the shared `ensure_outside_runtime`
/// guard that all three entry points share.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn distribution_calls_from_async_context_error_not_panic() {
    let loopback = "127.0.0.1:0".parse().expect("loopback addr parses");
    match DistributionEndpoint::bind(NODE_A_NAME, loopback, 1, None) {
        Err(SyncError::TransportBlockingFromAsync) => {}
        other => panic!("expected TransportBlockingFromAsync from async context, got {other:?}"),
    }
}
