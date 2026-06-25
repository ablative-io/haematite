//! Integration tests for the active-active "2a-3" writer-side coordinator.
//!
//! These exercise [`DistributionEndpoint::propose_write`] — the writer-side
//! coordinator + sync/async bridge that ties the prior 2a increments into a live
//! cross-node write path. The receiver-side conditional-durable apply is 2a-4 and
//! does NOT exist yet, so a STUB ack-producer stands in: a test thread on node B
//! drains the inbound `WriteProposal` and sends back a `WriteAck` WITHOUT applying
//! anything.
//!
//! Tests return `Result` and use `?` because the crate denies `expect_used`.

#![allow(clippy::panic, clippy::doc_markdown)]

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use haematite::sync::consistency::ConsistencyError;
use haematite::sync::ballot::Ballot;
use haematite::sync::membership::WriteMembership;
use haematite::sync::{
    AckOutcome, DistributionEndpoint, ProposeWrite, SyncMessage, SyncNodeId, WriteAck,
    WriteProposal,
};

type TestResult = Result<(), Box<dyn Error>>;

const NODE_A: &str = "node-a@127.0.0.1";
const NODE_B: &str = "node-b@127.0.0.1";

fn loopback() -> Result<SocketAddr, Box<dyn Error>> {
    Ok("127.0.0.1:0".parse()?)
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

/// Bind A and B, dial A -> B and B -> A, and block until A sees the link to B.
fn connect_pair(
    a_creation: u32,
    b_creation: u32,
) -> Result<(DistributionEndpoint, DistributionEndpoint), Box<dyn Error>> {
    let endpoint_a = DistributionEndpoint::bind(NODE_A, loopback()?, a_creation, None)?;
    let endpoint_b = DistributionEndpoint::bind(NODE_B, loopback()?, b_creation, None)?;

    let addr_a = endpoint_a.local_addr();
    let addr_b = endpoint_b.local_addr();

    // Both sides must be able to resolve each other for handshake bookkeeping.
    endpoint_a.add_peer(NODE_B, addr_b);
    endpoint_b.add_peer(NODE_A, addr_a);

    // B dials A as well so its connection table is keyed by A's advertised name,
    // letting B send acks back to A by name.
    let _ = endpoint_b.connect(NODE_A);
    endpoint_a.connect(NODE_B)?;

    let connected = wait_until(Duration::from_secs(5), || endpoint_a.is_connected(NODE_B));
    if !connected {
        return Err("node A never registered a link to node B".into());
    }
    Ok((endpoint_a, endpoint_b))
}

/// A 2-node membership where `targets` is the reachable peer set to propose to.
fn membership(total_nodes: usize, targets: &[&str]) -> WriteMembership {
    WriteMembership {
        total_nodes,
        send_targets: targets.iter().map(|name| SyncNodeId::from(*name)).collect(),
    }
}

/// Spawn a test-only stub receiver on `endpoint_b`: drain inbound
/// `WriteProposal`s and reply with a `WriteAck` carrying `outcome`. B does NOT
/// apply anything — this stands in for the not-yet-built 2a-4 receiver.
fn spawn_stub_acker(
    endpoint_b: Arc<DistributionEndpoint>,
    acker: SyncNodeId,
    acker_creation: u32,
    outcome: AckOutcome,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        // Drain until the proposal arrives (or give up after a generous window).
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            match endpoint_b.recv_inbound(Duration::from_millis(100)) {
                Ok(Some(Ok(SyncMessage::WriteProposal(proposal)))) => {
                    let WriteProposal { write_id, .. } = proposal;
                    let ack = WriteAck {
                        write_id,
                        acker: acker.clone(),
                        acker_creation,
                        outcome,
                    };
                    let _ = endpoint_b.send_to(NODE_A, &SyncMessage::WriteAck(ack));
                    return;
                }
                Ok(_) => {}
                Err(_) => return,
            }
        }
    })
}

/// (1) REAL two-endpoint quorum: A proposes; B (a stub) replies WriteAck{Applied};
/// A's `propose_write` commits. total_nodes = 2 -> quorum 2 = local ack + B.
#[test]
fn reaches_quorum_via_real_two_endpoint_ack() -> TestResult {
    let (endpoint_a, endpoint_b) = connect_pair(1, 1)?;
    let endpoint_b = Arc::new(endpoint_b);

    let stub = spawn_stub_acker(
        Arc::clone(&endpoint_b),
        SyncNodeId::from(NODE_B),
        1,
        AckOutcome::Applied,
    );

    let membership = membership(2, &[NODE_B]);
    let outcome = endpoint_a.propose_write(
        ProposeWrite {
            key: b"k".to_vec(),
            expected: None,
            value: b"v".to_vec(),
            ttl: None,
        },
        Ballot::bottom(),
        &membership,
        Duration::from_secs(5),
    )?;

    assert!(outcome.reached(), "quorum reached: {outcome:?}");
    assert_eq!(outcome.required, 2);
    assert_eq!(outcome.acknowledged, 2, "local ack + B's WriteAck");
    assert!(
        outcome.acknowledged_nodes.contains(&SyncNodeId::from(NODE_B)),
        "B is recorded as an acker"
    );

    stub.join().map_err(|_| "stub thread panicked")?;
    Ok(())
}

/// (2) Duplicate WriteAck deduped: two Applied acks from the SAME acker count
/// once. With total_nodes = 3 (quorum 2), local ack + one B ack = 2 reaches
/// quorum; a second identical B ack must not push acknowledged past the dedup.
#[test]
fn duplicate_write_ack_deduped() -> TestResult {
    let (endpoint_a, endpoint_b) = connect_pair(1, 1)?;
    let endpoint_b = Arc::new(endpoint_b);

    // Stub that, on one proposal, sends the SAME Applied ack TWICE.
    let stub_b = Arc::clone(&endpoint_b);
    let stub = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            match stub_b.recv_inbound(Duration::from_millis(100)) {
                Ok(Some(Ok(SyncMessage::WriteProposal(proposal)))) => {
                    let ack = WriteAck {
                        write_id: proposal.write_id,
                        acker: SyncNodeId::from(NODE_B),
                        acker_creation: 1,
                        outcome: AckOutcome::Applied,
                    };
                    let _ = stub_b.send_to(NODE_A, &SyncMessage::WriteAck(ack.clone()));
                    let _ = stub_b.send_to(NODE_A, &SyncMessage::WriteAck(ack));
                    return;
                }
                Ok(_) => {}
                Err(_) => return,
            }
        }
    });

    // total_nodes = 3 -> quorum 2. Local ack (1) + B's single distinct ack (1) = 2.
    // The duplicate must NOT count, so a quorum of 2 is exactly right and stable.
    let membership = membership(3, &[NODE_B]);
    let outcome = endpoint_a.propose_write(
        ProposeWrite {
            key: b"k".to_vec(),
            expected: None,
            value: b"v".to_vec(),
            ttl: None,
        },
        Ballot::bottom(),
        &membership,
        Duration::from_secs(5),
    )?;

    assert_eq!(outcome.required, 2);
    assert_eq!(
        outcome.acknowledged, 2,
        "duplicate B ack deduped: local + B counted once each"
    );
    assert_eq!(
        outcome.acknowledged_nodes,
        vec![SyncNodeId::from(NODE_B)],
        "B recorded exactly once"
    );

    stub.join().map_err(|_| "stub thread panicked")?;
    Ok(())
}

/// (3) Late ack after quorum: an ack arriving AFTER `propose_write` returned
/// (write_id deregistered) is dropped with no panic. We commit a write, then have
/// B send a fresh WriteAck for the (now-deregistered) write_id; routing it must
/// be a quiet no-op.
#[test]
fn late_ack_after_quorum_dropped() -> TestResult {
    let (endpoint_a, endpoint_b) = connect_pair(1, 1)?;
    let endpoint_b = Arc::new(endpoint_b);

    // Capture the proposal's write_id so we can replay a late ack for it.
    let stub_b = Arc::clone(&endpoint_b);
    let (id_tx, id_rx) = std::sync::mpsc::channel();
    let stub = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            match stub_b.recv_inbound(Duration::from_millis(100)) {
                Ok(Some(Ok(SyncMessage::WriteProposal(proposal)))) => {
                    let write_id = proposal.write_id;
                    let ack = WriteAck {
                        write_id: write_id.clone(),
                        acker: SyncNodeId::from(NODE_B),
                        acker_creation: 1,
                        outcome: AckOutcome::Applied,
                    };
                    let _ = stub_b.send_to(NODE_A, &SyncMessage::WriteAck(ack));
                    let _ = id_tx.send(write_id);
                    return;
                }
                Ok(_) => {}
                Err(_) => return,
            }
        }
    });

    let membership = membership(2, &[NODE_B]);
    let outcome = endpoint_a.propose_write(
        ProposeWrite {
            key: b"k".to_vec(),
            expected: None,
            value: b"v".to_vec(),
            ttl: None,
        },
        Ballot::bottom(),
        &membership,
        Duration::from_secs(5),
    )?;
    assert!(outcome.reached(), "first write commits");
    stub.join().map_err(|_| "stub thread panicked")?;

    // Now replay a LATE ack for the deregistered write_id. Must not panic.
    let write_id = id_rx.recv_timeout(Duration::from_secs(1))?;
    let late_ack = WriteAck {
        write_id,
        acker: SyncNodeId::from(NODE_B),
        acker_creation: 1,
        outcome: AckOutcome::Applied,
    };
    endpoint_b.send_to(NODE_A, &SyncMessage::WriteAck(late_ack))?;

    // Give the read loop a moment to process the late ack; absence of a panic /
    // hang is the assertion. A subsequent write must still work cleanly.
    std::thread::sleep(Duration::from_millis(200));

    let stub2 = spawn_stub_acker(
        Arc::clone(&endpoint_b),
        SyncNodeId::from(NODE_B),
        1,
        AckOutcome::Applied,
    );
    let outcome2 = endpoint_a.propose_write(
        ProposeWrite {
            key: b"k2".to_vec(),
            expected: None,
            value: b"v2".to_vec(),
            ttl: None,
        },
        Ballot::bottom(),
        &membership,
        Duration::from_secs(5),
    )?;
    assert!(outcome2.reached(), "writer healthy after a dropped late ack");
    stub2.join().map_err(|_| "stub thread panicked")?;
    Ok(())
}

/// (4) Restart-reuse rejected (Fix D): a WriteAck whose `origin_creation` does NOT
/// match the writer's local creation is dropped — it cannot satisfy the in-flight
/// (reused-counter) write. A's endpoint has creation 7; B replies with an ack
/// whose write_id.origin_creation is forced to 6 (a stale prior incarnation). The
/// gate drops it, so the only vote is the local ack -> never reaches quorum of 2
/// -> times out.
#[test]
fn restart_reuse_ack_rejected_fix_d() -> TestResult {
    let (endpoint_a, endpoint_b) = connect_pair(7, 1)?;
    let endpoint_b = Arc::new(endpoint_b);
    assert_eq!(endpoint_a.local_creation(), 7);

    // Stub: take the real proposal but MANGLE the ack's origin_creation to 6.
    let stub_b = Arc::clone(&endpoint_b);
    let stub = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            match stub_b.recv_inbound(Duration::from_millis(100)) {
                Ok(Some(Ok(SyncMessage::WriteProposal(proposal)))) => {
                    let mut write_id = proposal.write_id;
                    // Forge a stale prior incarnation for the SAME counter.
                    write_id.origin_creation = 6;
                    let ack = WriteAck {
                        write_id,
                        acker: SyncNodeId::from(NODE_B),
                        acker_creation: 1,
                        outcome: AckOutcome::Applied,
                    };
                    let _ = stub_b.send_to(NODE_A, &SyncMessage::WriteAck(ack));
                    return;
                }
                Ok(_) => {}
                Err(_) => return,
            }
        }
    });

    // total_nodes = 2 -> quorum 2. The stale-incarnation ack is dropped, so only
    // the local ack counts -> quorum unreachable -> timeout.
    let membership = membership(2, &[NODE_B]);
    let outcome = endpoint_a.propose_write(
        ProposeWrite {
            key: b"k".to_vec(),
            expected: None,
            value: b"v".to_vec(),
            ttl: None,
        },
        Ballot::bottom(),
        &membership,
        Duration::from_millis(800),
    );
    assert!(
        matches!(outcome, Err(ConsistencyError::QuorumTimeout { .. })),
        "stale-incarnation ack must NOT satisfy the reused write_id, got {outcome:?}"
    );

    stub.join().map_err(|_| "stub thread panicked")?;
    Ok(())
}

/// (5) Threading contract: calling `propose_write` from within a tokio runtime
/// returns an error (not a panic). The blocking coordinator must never park a
/// runtime worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn propose_write_from_async_context_errors_not_panics() -> TestResult {
    // `bind` itself guards against async context (even a `spawn_blocking` thread
    // is still associated with the runtime via `Handle::try_current`), so build
    // the endpoint on a PLAIN std thread outside the runtime, then call
    // `propose_write` from this async task to exercise the coordinator's own guard.
    let endpoint = std::thread::spawn(|| {
        let addr: SocketAddr = "127.0.0.1:0".parse().map_err(|error| format!("{error}"))?;
        DistributionEndpoint::bind(NODE_A, addr, 1, None).map_err(|error| format!("{error}"))
    })
    .join()
    .map_err(|_| "bind thread panicked")?
    .map_err(|error| -> Box<dyn Error> { error.into() })?;

    let membership = WriteMembership {
        total_nodes: 1,
        send_targets: Vec::new(),
    };
    let outcome = endpoint.propose_write(
        ProposeWrite {
            key: b"k".to_vec(),
            expected: None,
            value: b"v".to_vec(),
            ttl: None,
        },
        Ballot::bottom(),
        &membership,
        Duration::from_millis(100),
    );
    match outcome {
        Err(ConsistencyError::TransportUnavailable) => Ok(()),
        other => panic!("expected TransportUnavailable from async context, got {other:?}"),
    }
}
