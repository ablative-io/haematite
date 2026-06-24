//! Real two-endpoint apply round-trip for active-active "2a-4".
//!
//! This replaces the 2a-3 STUB responder with the REAL receiver: node A
//! `propose_write`s a Strong CAS write; node B's live [`Database`] drains the
//! inbound `WriteProposal`, applies it **conditionally + durably** through the
//! force-sync shard path, and acks `Applied`; node A reaches quorum AND node B's
//! store actually contains the value (proof the apply ran, not a stub).
//!
//! Real beamr loopback TCP, real OTP handshake, no transport mocking.
//!
//! Tests return `Result` and use `?` (the crate denies `expect`/`unwrap`/`panic`).

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use haematite::db::respond_to_inbound_writes;
use haematite::sync::membership::WriteMembership;
use haematite::sync::{DistributionEndpoint, SyncNodeId};
use haematite::{Database, DatabaseConfig};

type TestResult = Result<(), Box<dyn Error>>;

const NODE_A: &str = "node-a@127.0.0.1";
const NODE_B: &str = "node-b@127.0.0.1";

fn loopback() -> Result<SocketAddr, Box<dyn Error>> {
    Ok("127.0.0.1:0".parse()?)
}

fn config_for(path: &Path) -> DatabaseConfig {
    DatabaseConfig {
        data_dir: path.to_path_buf(),
        shard_count: 1,
        sweep_interval: None,
        distributed: None,
    }
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

/// REAL two-endpoint apply round-trip: A proposes, B's REAL receiver applies
/// durably + acks Applied, A commits AND B's store contains the value.
#[test]
fn real_apply_round_trip_commits_and_stores_value() -> TestResult {
    let dir_b = tempfile::tempdir()?;

    // ----- Node A: a bare endpoint (it only needs to propose) ----------------
    let endpoint_a = DistributionEndpoint::bind(NODE_A, loopback()?, 1, None)?;
    // ----- Node B: a real Database with a live endpoint (the receiver) --------
    let endpoint_b = DistributionEndpoint::bind(NODE_B, loopback()?, 1, None)?;

    let addr_a = endpoint_a.local_addr();
    let addr_b = endpoint_b.local_addr();

    let database_b = Arc::new(
        Database::create(config_for(dir_b.path().join("db").as_path()))?
            .with_distribution(endpoint_b),
    );

    // ----- Wire the link both ways and wait for A -> B to register -----------
    endpoint_a.add_peer(NODE_B, addr_b);
    database_b.connect_peer(NODE_A, addr_a).ok();
    endpoint_a.connect(NODE_B)?;

    let connected = wait_until(Duration::from_secs(5), || endpoint_a.is_connected(NODE_B));
    if !connected {
        return Err("node A never registered a link to node B".into());
    }

    // ----- Node B runs the REAL responder loop on a dedicated thread ---------
    // It drains the inbound WriteProposal, applies it durably, and sends back a
    // real WriteAck — no stub.
    let responder_db = Arc::clone(&database_b);
    let responder = std::thread::spawn(move || {
        // One drain pass with a generous window is enough for this single write.
        drop(respond_to_inbound_writes(
            &responder_db,
            Duration::from_secs(5),
        ));
    });

    // ----- Node A proposes a Strong CAS create (expected = None) -------------
    // total_nodes = 2 -> quorum 2 = A's local ack + B's real Applied ack.
    let membership = WriteMembership {
        total_nodes: 2,
        send_targets: vec![SyncNodeId::from(NODE_B)],
    };
    let key = b"replicated".to_vec();
    let outcome = endpoint_a.propose_write(
        key.clone(),
        None,
        b"value".to_vec(),
        None,
        &membership,
        Duration::from_secs(5),
    )?;

    assert!(outcome.reached(), "quorum reached via real apply: {outcome:?}");
    assert_eq!(outcome.required, 2);
    assert_eq!(outcome.acknowledged, 2, "local ack + B's real WriteAck");
    assert!(
        outcome
            .acknowledged_nodes
            .contains(&SyncNodeId::from(NODE_B)),
        "B is recorded as a real acker"
    );

    responder.join().map_err(|_| "responder thread panicked")?;

    // ----- THE PROOF: B's store actually contains the applied value ----------
    assert_eq!(
        database_b.get(&key)?,
        Some(b"value".to_vec()),
        "node B's store must hold the durably-applied value"
    );
    Ok(())
}
