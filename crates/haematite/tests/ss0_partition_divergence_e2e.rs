//! SS-0 — GATING SPIKE: is `replicate_append` run-history SAFE under a REAL
//! network partition with CONCURRENT DIVERGENT writes to the SAME stream?
//!
//! This is the one thing the green active-active failover demo does NOT prove. The
//! showcase (and `replicated_append_e2e.rs` GATE 2/3) KILLS or DEPOSES a node
//! *cleanly* — it never has two nodes BOTH believing they may write the SAME
//! workflow's event stream at the SAME sequence at the SAME time, then heals. An
//! earlier AION-DISTRIBUTION spike (`spike_fencing.rs` E2/E3) showed that the OLD
//! `ConflictPolicy::Lww` / `merge_synced_roots` engine silently LWW-DROPS divergent
//! same-`(stream,seq)` event writes. SS-0 settles whether the CURRENT production
//! path (`replicate_append` quorum + the apply-time epoch fence + `become_live`'s
//! `merge_committed_union`) prevents that divergence — or reproduces the loss.
//!
//! It drives the REAL distributed write path: live `DistributionEndpoint`s over
//! real beamr loopback TCP, the real quorum coordinator (`propose_batch_stamped`),
//! and the real receiver-side conditional-durable-apply-then-ack
//! (`respond_to_inbound_writes` → `apply_durable_batch`). The partition is modelled
//! the same faithful way `three_node_split_brain_e2e.rs` does it: a partitioned node
//! is simply never linked, so it is absent from the other side's
//! `connected_nodes()` and any proposal to it is dropped exactly as across a real
//! partition.
//!
//! VERDICT THIS TEST ENCODES (each gate is an assertion, not a comment):
//!
//! G1 — TWO PARTITIONED WOULD-BE OWNERS CANNOT BOTH COMMIT. With the cluster split
//!   {A,B} | {C}, A (majority) commits `[a1,a2]` at seq 0 to quorum {B}; C (minority,
//!   isolated) attempts the DIVERGENT append `[c1,c2]` at the SAME seq 0 and is
//!   FENCED BY QUORUM (it counts only its own local ack, 1 < 2) — it commits NOTHING
//!   anywhere. There is never a moment where both A's and C's divergent batches are
//!   committed. (This is the run-history analogue of split-brain prevention, on the
//!   STREAM path, not the single-key CAS path.)
//!
//! G2 — HEAL UNIONS, DOES NOT LWW-DROP. After the heal, C is elected the new owner
//!   and `become_live`-merges. The merged history is EXACTLY A's committed `[a1,a2]`
//!   in order with next-seq 2 — A's events are intact (none silently dropped) and C's
//!   never-committed `[c1,c2]` are correctly absent (they never reached quorum). No
//!   event vanishes; the seq counter is not LWW-corrupted.
//!
//! G3 — A STALE OWNER THAT STILL THINKS IT OWNS THE SHARD IS FENCED AT THE STREAM.
//!   After C is elected, A — never told it was deposed — re-attempts a divergent
//!   `replicate_append` at the next seq. The apply-time epoch fence
//!   (`stamp.epoch < promised`) on the intersection quorum rejects it: it fails as a
//!   FENCE (not a timeout, not a commit) and lands on NO node. This is the exact
//!   "two live owners, both write" window the spike warned about — and it is closed.
//!
//! Together G1+G2+G3 prove: divergent committed writes to the same run-history
//! stream are STRUCTURALLY PREVENTED (quorum over FULL membership + the monotonic
//! apply-time fence), and the heal path UNIONS rather than LWW-drops. The
//! `spike_fencing.rs` LWW data-loss finding is stale for the production path.
//!
//! Tests return `Result` and use `?` (the crate denies `expect`/`unwrap`/`panic`).

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use haematite::db::respond_to_inbound_writes;
use haematite::sync::membership::WriteMembership;
use haematite::sync::{DistributionEndpoint, SyncNodeId};
use haematite::{Database, DatabaseConfig, DatabaseError};

type TestResult = Result<(), Box<dyn Error>>;

const NODE_A: &str = "node-a@127.0.0.1";
const NODE_B: &str = "node-b@127.0.0.1";
const NODE_C: &str = "node-c@127.0.0.1";

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// Window for a write/election we EXPECT to succeed.
const OP_TIMEOUT: Duration = Duration::from_secs(5);
/// Window for a write we EXPECT to be fenced by TIMEOUT (the isolated minority):
/// kept short so the partitioned-minority gate does not idle for seconds.
const FENCE_TIMEOUT: Duration = Duration::from_millis(400);

/// Single-shard cluster: the contended stream and all three nodes share shard 0,
/// so ownership of shard 0 IS ownership of the stream — the split-brain we want to
/// stress.
const SHARD: usize = 0;

/// Width of the big-endian timestamp header `encode_event_value` prepends to each
/// stored event value (`timestamp.to_be_bytes() || payload`); stripped on read to
/// recover the raw payload, exactly as the sibling e2e tests do.
const TS_WIDTH: usize = 8;

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

/// One node: a live `Database` with an attached endpoint plus a background responder
/// draining + answering inbound `Prepare`s, `WriteProposal`s, `BatchWriteProposal`s,
/// and `ShardSyncRequest`s for the whole test.
struct Node {
    db: Arc<Database>,
    addr: SocketAddr,
    name: &'static str,
    responder: Option<JoinHandle<()>>,
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl Node {
    fn spawn(name: &'static str, dir: &Path) -> Result<Self, Box<dyn Error>> {
        let endpoint = DistributionEndpoint::bind(name, loopback()?, 1, None)?;
        let addr = endpoint.local_addr();
        let db = Arc::new(
            Database::create(config_for(dir.join("db").as_path()))?.with_distribution(endpoint),
        );

        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let responder_db = Arc::clone(&db);
        let responder_running = Arc::clone(&running);
        let responder = std::thread::spawn(move || {
            // Re-arm the drain in a loop so this node keeps applying inbound
            // proposals for the whole test (each pass returns on a plain timeout).
            while responder_running.load(std::sync::atomic::Ordering::Relaxed) {
                drop(respond_to_inbound_writes(
                    &responder_db,
                    Duration::from_millis(50),
                ));
            }
        });

        Ok(Self {
            db,
            addr,
            name,
            responder: Some(responder),
            running,
        })
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);
        if let Some(handle) = self.responder.take() {
            drop(handle.join());
        }
    }
}

/// Dial `from` -> `to` (one direction) and wait for the link to register on the
/// dialing side. Modelling a partition = simply NOT calling this for the isolated
/// peer until the heal.
fn link(from: &Node, to: &Node) -> TestResult {
    let endpoint = from
        .db
        .distribution()
        .ok_or("dialing node has no endpoint")?;
    endpoint.add_peer(to.name, to.addr);
    endpoint.connect(to.name)?;
    if !wait_until(HANDSHAKE_TIMEOUT, || endpoint.is_connected(to.name)) {
        return Err(format!("{} never registered a link to {}", from.name, to.name).into());
    }
    Ok(())
}

fn link_both(a: &Node, b: &Node) -> TestResult {
    link(a, b)?;
    link(b, a)?;
    Ok(())
}

/// `total_nodes` is ALWAYS the full cluster size (the load-bearing invariant —
/// quorum is over full membership, NEVER the reachable subset, so a minority cannot
/// self-quorum); `send_targets` is the explicit reachable peer set the partition
/// models.
fn membership(total_nodes: usize, send_targets: &[&str]) -> WriteMembership {
    WriteMembership {
        total_nodes,
        send_targets: send_targets.iter().map(|n| SyncNodeId::from(*n)).collect(),
    }
}

/// Read a node's local event stream, stripping the per-event timestamp header to
/// recover the raw payloads in sequence order.
fn read_payloads(node: &Node, stream: &[u8]) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
    let raw = node.db.read_events(stream)?;
    let mut out = Vec::with_capacity(raw.len());
    for value in raw {
        if value.len() < TS_WIDTH {
            return Err(format!("event value shorter than timestamp header: {value:?}").into());
        }
        out.push(value[TS_WIDTH..].to_vec());
    }
    Ok(out)
}

fn next_seq(node: &Node, stream: &[u8]) -> Result<Option<u64>, Box<dyn Error>> {
    Ok(node.db.read_stream_next_seq(stream)?)
}

/// G1 — the isolated minority C CANNOT commit the DIVERGENT batch. C believes it
/// may write the same stream and attempts the conflicting append at the SAME seq 0
/// with NO reachable peers. It counts only its own local ack (1 < quorum 2) → fenced
/// by quorum. `replicate_append` applies locally ONLY on quorum success, so it
/// commits NOTHING — not even on C, and never on the majority {A,B}.
fn assert_g1_minority_cannot_commit_divergent(
    node_a: &Node,
    node_b: &Node,
    node_c: &Node,
    stream: &[u8],
    divergent: &[Vec<u8>],
) -> TestResult {
    let c_divergent = node_c.db.replicate_append(
        stream,
        divergent,
        0,
        // total_nodes = 3 (FULL membership), but NO reachable peers in the partition.
        &membership(3, &[]),
        FENCE_TIMEOUT,
    );
    match &c_divergent {
        Err(DatabaseError::ConsistencyError(_)) => {}
        other => {
            return Err(format!(
                "G1: isolated C's divergent append must FAIL to reach quorum (cannot \
                 self-quorum), got {other:?}"
            )
            .into());
        }
    }
    assert!(
        read_payloads(node_c, stream)?.is_empty(),
        "G1: C's fenced divergent batch must commit NOTHING on C (no quorum, no local apply)"
    );
    for node in [node_a, node_b] {
        let payloads = read_payloads(node, stream)?;
        assert!(
            !payloads.iter().any(|p| divergent.contains(p)),
            "G1: C's divergent events must NOT have reached node {} — only A's committed batch exists",
            node.name
        );
    }
    Ok(())
}

/// G3 — the STALE old owner A is FENCED at the stream path. A was never told it was
/// deposed; it re-attempts a DIVERGENT append at the next seq. C's election raised
/// B's and C's `promised` ballot above A's live epoch, so the apply-time fence
/// (`stamp.epoch < promised`) on the {B,C} intersection rejects A's batch — it fails
/// as a FENCE (not a timeout, not a commit) and lands on NO node. This is precisely
/// the "two live owners both write" window the spike warned about.
fn assert_g3_stale_owner_is_fenced(
    node_a: &Node,
    node_b: &Node,
    node_c: &Node,
    stream: &[u8],
) -> TestResult {
    let a3 = b"A:stale-event-2".to_vec();
    let stale = node_a.db.replicate_append(
        stream,
        std::slice::from_ref(&a3),
        2,
        &membership(3, &[NODE_B, NODE_C]),
        OP_TIMEOUT,
    );
    match &stale {
        Err(DatabaseError::ConsistencyError(message)) => assert!(
            message.contains("fenced"),
            "G3: the stale owner's divergent append must fail as a FENCE, got: {message}"
        ),
        other => {
            return Err(format!(
                "G3: the stale owner's append must be fenced (ConsistencyError), NEVER Ok, \
                 got {other:?}"
            )
            .into());
        }
    }
    for node in [node_a, node_b, node_c] {
        let payloads = read_payloads(node, stream)?;
        assert!(
            !payloads.iter().any(|p| p == &a3),
            "G3: the stale owner's fenced event must NOT be present on node {}",
            node.name
        );
    }
    for node in [node_b, node_c] {
        assert_eq!(
            next_seq(node, stream)?,
            Some(2),
            "G3: node {} seq counter must NOT advance past the true count 2 from the fenced append",
            node.name
        );
    }
    Ok(())
}

// ===========================================================================
// THE SS-0 SCENARIO: concurrent divergent appends to the SAME stream across a
// real partition, then heal — all three gates in one timeline.
// ===========================================================================

/// Timeline:
///  * Cluster {A,B,C}, single shard 0. Stream `k` (on shard 0) is empty everywhere.
///  * PARTITION: {A,B} reachable, {C} isolated (never linked until the heal).
///  * A acquires shard 0 over {B} and `replicate_append`s the DIVERGENT batch
///    `[a1,a2]` at seq 0 → commits on the {A,B} majority (quorum 2).
///  * C, isolated, BELIEVES it may write the same stream: it attempts
///    `replicate_append` of the DIVERGENT batch `[c1,c2]` at the SAME seq 0 with NO
///    reachable peers → FENCED BY QUORUM (only its own local ack, 1 < 2). It commits
///    NOTHING. [G1: two would-be owners cannot both commit.]
///  * HEAL: link C <-> {A,B}. C is elected the new owner over {A,B} and
///    `become_live`-merges. [G2: the merged history is A's `[a1,a2]`, intact and in
///    order, next-seq 2 — UNION, not LWW-drop; C's never-committed batch absent.]
///  * The STALE old owner A (never told it was deposed) re-attempts a divergent
///    append `[a3]` at seq 2. The apply-time epoch fence on the {B,C} intersection
///    rejects it. [G3: a stale live owner is fenced at the stream path, lands on no
///    node.]
#[test]
fn concurrent_divergent_appends_are_fenced_then_union_merged() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;

    let node_a = Node::spawn(NODE_A, dir_a.path())?;
    let node_b = Node::spawn(NODE_B, dir_b.path())?;
    let node_c = Node::spawn(NODE_C, dir_c.path())?;

    // The contended run-history stream. shard_count=1 → it is on shard 0, the shard
    // every node contends to own.
    let stream = b"run/contended".to_vec();
    assert_eq!(
        node_a.db.shard_for(&stream),
        SHARD,
        "single-shard cluster: the contended stream must live on shard 0"
    );

    let a1 = b"A:event-0".to_vec();
    let a2 = b"A:event-1".to_vec();
    let c1 = b"C:event-0".to_vec();
    let c2 = b"C:event-1".to_vec();

    // --- PARTITION {A,B} | {C}: link A<->B only; C stays isolated. -----------
    link_both(&node_a, &node_b)?;

    // --- Majority side {A,B}: A acquires shard 0, then commits its divergent
    // batch at seq 0 to quorum {B}. This is the REAL production path. -----------
    node_a
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B]), OP_TIMEOUT)?;
    let a_next = node_a.db.replicate_append(
        &stream,
        &[a1.clone(), a2.clone()],
        0,
        &membership(3, &[NODE_B]),
        OP_TIMEOUT,
    )?;
    assert_eq!(
        a_next, 2,
        "A's majority append commits and returns next-seq 2"
    );

    // Both A and B durably hold A's batch (A committed locally on quorum success; B's
    // responder applied it) — the winning history lives on the {A,B} majority.
    assert_eq!(
        read_payloads(&node_a, &stream)?,
        vec![a1.clone(), a2.clone()],
        "A must durably hold its own committed batch"
    );
    let b_has_winner = wait_until(
        OP_TIMEOUT,
        || matches!(read_payloads(&node_b, &stream), Ok(ref p) if p == &vec![a1.clone(), a2.clone()]),
    );
    assert!(
        b_has_winner,
        "B must durably hold A's committed batch after quorum"
    );

    // === G1: the isolated minority C CANNOT commit the DIVERGENT batch. =========
    assert_g1_minority_cannot_commit_divergent(&node_a, &node_b, &node_c, &stream, &[c1, c2])?;

    // === HEAL: link C <-> {A,B} (both directions, both peers). ==================
    link_both(&node_c, &node_a)?;
    link_both(&node_c, &node_b)?;

    // === G2: the new owner UNION-merges; A's history is intact, NOT LWW-dropped. =
    // C is elected the new owner over {A,B} and `become_live`-merges every promiser's
    // committed tree (`merge_committed_union`, max-stamp). The merged run history must
    // be EXACTLY A's `[a1,a2]` in order with next-seq 2 — no event silently dropped,
    // the seq counter not LWW-corrupted, and C's never-committed batch absent.
    node_c
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_A, NODE_B]), OP_TIMEOUT)?;
    assert_eq!(
        read_payloads(&node_c, &stream)?,
        vec![a1, a2],
        "G2: the new owner must serve A's full committed history after heal (UNION merge, \
         not LWW-drop)"
    );
    assert_eq!(
        next_seq(&node_c, &stream)?,
        Some(2),
        "G2: the new owner's seq counter must be the true committed count 2 (not LWW-corrupted)"
    );

    // === G3: the STALE old owner A is FENCED at the stream path. ================
    assert_g3_stale_owner_is_fenced(&node_a, &node_b, &node_c, &stream)?;

    Ok(())
}
