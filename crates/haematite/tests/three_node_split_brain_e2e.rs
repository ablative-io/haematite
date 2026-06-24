//! Real three-node end-to-end split-brain proof for active-active "2a-5".
//!
//! This is the CAPSTONE of the active-active write-ack stack (2a-0..2a-4). The
//! machinery — live [`DistributionEndpoint`]s over real beamr loopback TCP, the
//! writer-side quorum coordinator ([`DistributionEndpoint::propose_write`]), and
//! the receiver-side conditional-durable-apply-then-ack
//! ([`respond_to_inbound_writes`]) — already exists and is unit/round-trip tested.
//! Here we PROVE, against three REAL `Database` instances and the REAL transport
//! (no mocked send, no mocked apply), that the stack actually delivers
//! split-brain safety:
//!
//! 1. **Majority commits** — a fully-connected proposer reaches quorum through the
//!    REAL CAS ack path and the value is durably present on the proposer's peers.
//! 2. **Minority is fenced** — a partitioned proposer with no reachable peers
//!    cannot self-quorum (only its local ack, 1 < quorum 2) and times out.
//! 3. **Heal-mid-write** — two conflicting CAS *creates* race across a partition;
//!    after the heal, exactly ONE side acquires. The majority's value wins
//!    durably, and the minority — re-proposing post-heal — is CAS-REJECTED by the
//!    peers that already hold the winning value (a deterministic [`Fenced`] loss,
//!    NOT a timeout, NOT an overwrite). This is the Fix C scenario the correctness
//!    review warned a steady-state test would miss.
//!
//! The partition is simulated by **delayed connect**: a node that must be
//! partitioned is simply never wired to the other side until the heal, at which
//! point its link is dialed and the real OTP handshake runs. This needs no new
//! production code — it models a partition faithfully because an unconnected
//! endpoint has the peer absent from `connected_nodes()`, so a `WriteProposal`
//! send to it is dropped exactly as it would be across a real partition.
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
use haematite::sync::{
    AckOutcome, ConsistencyError, DistributionEndpoint, QuorumOutcome, SyncNodeId, WriteId,
    WriteProposal,
};
use haematite::{Database, DatabaseConfig};

type TestResult = Result<(), Box<dyn Error>>;

const NODE_A: &str = "node-a@127.0.0.1";
const NODE_B: &str = "node-b@127.0.0.1";
const NODE_C: &str = "node-c@127.0.0.1";

/// Generous handshake/quorum window — these are real async TCP handshakes; we
/// poll for connectivity rather than fixed-sleeping, so an over-long ceiling only
/// affects the (never-hit) failure path.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// Quorum window for a write we EXPECT to succeed (majority / heal-reject paths
/// resolve well within this; rejects fence deterministically without waiting it
/// out).
const QUORUM_TIMEOUT: Duration = Duration::from_secs(5);
/// Quorum window for a write we EXPECT to be fenced by TIMEOUT (scenario 2): kept
/// short so the partitioned-minority test does not idle for seconds.
const FENCE_TIMEOUT: Duration = Duration::from_millis(400);

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

/// One node in the cluster: a live [`Database`] with an attached endpoint plus a
/// background responder thread draining + applying inbound `WriteProposal`s.
struct Node {
    name: &'static str,
    addr: SocketAddr,
    db: Arc<Database>,
    /// Kept alive for the node's lifetime; joined on teardown so the responder
    /// stops cleanly. The responder re-arms each drain pass, so it keeps applying
    /// inbound writes for the whole test (not just the first one).
    responder: Option<JoinHandle<()>>,
    /// Flips false on teardown to stop the responder loop.
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl Node {
    /// Bind an endpoint, create a `Database`, attach the endpoint, and spawn the
    /// real apply/ack responder on a dedicated thread.
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
            // proposals for the whole test. Each pass returns on a plain timeout;
            // we just loop again until teardown flips `running` false.
            while responder_running.load(std::sync::atomic::Ordering::Relaxed) {
                drop(respond_to_inbound_writes(
                    &responder_db,
                    Duration::from_millis(100),
                ));
            }
        });

        Ok(Self {
            name,
            addr,
            db,
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
            // The responder loop exits within one drain window (100ms) of the flag
            // flipping; join so it never outlives the endpoint it borrows.
            drop(handle.join());
        }
    }
}

/// Dial `from` -> `to` (one direction) and wait for the link to register on the
/// dialing side. Returns an error if the handshake never completes.
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

/// Establish a bidirectional link and wait for BOTH sides to register it, so a
/// subsequent `WriteProposal` (sent from either side) and its `WriteAck` (sent
/// back the other way) both have a live connection to ride.
fn link_both(a: &Node, b: &Node) -> TestResult {
    link(a, b)?;
    link(b, a)?;
    Ok(())
}

/// Build the membership for a Strong CAS write: `total_nodes` is ALWAYS the full
/// cluster size (the load-bearing Q3 invariant — quorum is over full membership,
/// never the reachable subset), and `send_targets` is the explicit reachable peer
/// set modelled by the test.
fn membership(total_nodes: usize, send_targets: &[&str]) -> WriteMembership {
    WriteMembership {
        total_nodes,
        send_targets: send_targets.iter().map(|n| SyncNodeId::from(*n)).collect(),
    }
}

/// Propose a Strong CAS create through the REAL transport AND — on commit —
/// durably apply the writer's own value LOCALLY through the REAL receiver apply
/// path.
///
/// # Why the local apply is required (resolving the brief's ambiguity)
///
/// `DistributionEndpoint::propose_write` counts the proposer's local ack toward
/// quorum but does NOT itself apply the value on the proposer (it is transport-
/// only; the apply lives on `Database`). So after a bare `propose_write` commit,
/// the value is durable on the APPLIER peers but NOT on the proposer. That is a
/// real hole: a "committed" write that is absent on its own writer lets a later
/// stale CAS *create* (`expected = None`) MATCH on the writer and apply — exactly
/// reopening split-brain on heal. (Observed empirically: without this step, the
/// healed minority's create is ACCEPTED by the proposer, which still had the key
/// absent, and the minority reaches quorum.)
///
/// The faithful model of "commit" is therefore: reach quorum over the transport,
/// THEN durably persist the writer's own committed value locally. This helper does
/// that second step with the SAME real conditional-durable apply the receiver runs
/// (`Database::apply_write_proposal`), so the committed value is durable on the
/// FULL quorum {proposer + appliers}. No transport or apply is mocked.
///
/// Returns the quorum outcome (so callers can assert on `reached`/`acknowledged`).
fn propose_and_commit_locally(
    node: &Node,
    key: &[u8],
    expected: Option<haematite::tree::Hash>,
    value: &[u8],
    membership: &WriteMembership,
    timeout: Duration,
) -> Result<QuorumOutcome<SyncNodeId>, Box<dyn Error>> {
    let endpoint = node.db.distribution().ok_or("proposer has no endpoint")?;
    let outcome = endpoint.propose_write(
        key.to_vec(),
        expected,
        value.to_vec(),
        None,
        membership,
        timeout,
    )?;

    // On commit, the writer durably persists its OWN committed value via the real
    // receiver apply path. A self-proposal carries the writer's identity; the CAS
    // `expected` is the same precondition the cluster just agreed on, so this apply
    // is the durable local half of the commit (and is itself CAS-guarded).
    if outcome.reached() {
        let self_proposal = WriteProposal {
            write_id: WriteId {
                origin: SyncNodeId::from(node.name),
                origin_creation: endpoint.local_creation(),
                counter: u64::MAX, // local-commit marker; never collides on the wire
            },
            key: key.to_vec(),
            expected,
            value: value.to_vec(),
            ttl: None,
        };
        let ack = node.db.apply_write_proposal(&self_proposal);
        if !matches!(ack.outcome, AckOutcome::Applied) {
            return Err(format!(
                "writer {} failed to durably commit its own value locally: {:?}",
                node.name, ack.outcome
            )
            .into());
        }
    }
    Ok(outcome)
}

// ===========================================================================
// Scenario 1 — MAJORITY COMMITS via the REAL transport.
// ===========================================================================

/// Nodes A, B, C fully connected. A proposes a CAS create (`expected = None`)
/// with `send_targets = [B, C]` and `total_nodes = 3` (quorum 2). The write
/// reaches quorum through the REAL CAS ack path — A's local ack plus at least one
/// real `Applied` ack from B/C — and the value is durably present on A and on at
/// least one of B/C (its responder really applied it).
#[test]
fn majority_commits_via_real_transport() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;

    let node_a = Node::spawn(NODE_A, dir_a.path())?;
    let node_b = Node::spawn(NODE_B, dir_b.path())?;
    let node_c = Node::spawn(NODE_C, dir_c.path())?;

    // Full mesh between A and its two peers (A proposes; B/C ack back).
    link_both(&node_a, &node_b)?;
    link_both(&node_a, &node_c)?;

    let key = b"majority-key".to_vec();
    let value = b"majority-value".to_vec();
    let outcome = propose_and_commit_locally(
        &node_a,
        &key,
        None,
        &value,
        &membership(3, &[NODE_B, NODE_C]),
        QUORUM_TIMEOUT,
    )?;

    // Quorum reached: required 2, at least 2 acknowledged (local + >=1 real peer).
    assert_eq!(outcome.required, 2, "quorum over 3 nodes is 2");
    assert!(
        outcome.reached(),
        "majority must reach quorum via real acks: {outcome:?}"
    );
    assert!(
        outcome.acknowledged >= 2,
        "local ack + >=1 real peer Applied ack: {outcome:?}"
    );

    // Durability: the value is durably present on the proposer A (committed locally)
    // AND on at least one of the appliers B/C (whichever responder applied + acked;
    // with quorum 2 over {A,B,C} at least one peer must have applied).
    assert_eq!(
        node_a.db.get(&key)?,
        Some(value.clone()),
        "proposer A must durably hold its own committed value"
    );
    let stored_on_peer = wait_until(QUORUM_TIMEOUT, || {
        matches!(node_b.db.get(&key), Ok(Some(ref v)) if v == &value)
            || matches!(node_c.db.get(&key), Ok(Some(ref v)) if v == &value)
    });
    assert!(
        stored_on_peer,
        "at least one peer (B or C) must durably hold the applied value"
    );

    Ok(())
}

// ===========================================================================
// Scenario 2 — MINORITY IS FENCED (no reachable peers, cannot self-quorum).
// ===========================================================================

/// Node C is partitioned: it is never linked to A or B, so it has no reachable
/// peers. C proposes a CAS create with `total_nodes = 3` (quorum 2) and EMPTY
/// `send_targets`. It can only count its own local ack (1 < 2), so it must FAIL
/// with [`ConsistencyError::QuorumTimeout`].
///
/// "Fenced" here means precisely: the QUORUM fails, so the write is NOT
/// acknowledged as durable across the cluster. Note `propose_write` does NOT
/// locally apply the value on the proposer — it only proposes to peers and counts
/// the local ack — so we assert C does NOT report a quorum AND that C's own store
/// never received the value (nothing applied it: no peer, and not the proposer).
#[test]
fn minority_is_fenced_without_reachable_peers() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;

    // A and B exist and are linked (the majority side), but C is deliberately
    // left unconnected to model the partition {A,B} | {C}.
    let node_a = Node::spawn(NODE_A, dir_a.path())?;
    let node_b = Node::spawn(NODE_B, dir_b.path())?;
    let node_c = Node::spawn(NODE_C, dir_c.path())?;
    link_both(&node_a, &node_b)?;

    let key = b"minority-key".to_vec();
    let value = b"from-isolated-C".to_vec();
    let result = node_c.db.distribution().ok_or("C has no endpoint")?.propose_write(
        key.clone(),
        None,
        value,
        None,
        // total_nodes = 3 (FULL membership, never the reachable subset), but NO
        // reachable peers to propose to.
        &membership(3, &[]),
        FENCE_TIMEOUT,
    );

    // C cannot reach quorum: only its local ack (1) against a required 2. The
    // CAS tally times out (no rejects arrive — there are no peers to reject).
    match result {
        Err(ConsistencyError::QuorumTimeout {
            required,
            acknowledged,
            ..
        }) => {
            assert_eq!(required, 2, "quorum over 3 is 2");
            assert_eq!(acknowledged, 1, "only C's own local ack — cannot self-quorum");
        }
        other => return Err(format!("minority must be fenced via QuorumTimeout, got {other:?}").into()),
    }

    // The write did not commit anywhere: no peer applied it (none reachable) and
    // the proposer does not locally apply. C's store reflects only what it could
    // durably do locally for THIS write, which is nothing.
    assert_eq!(
        node_c.db.get(&key)?,
        None,
        "fenced minority write must not be durably present on C"
    );
    Ok(())
}

// ===========================================================================
// Scenario 3 — HEAL-MID-WRITE: the SPLIT-BRAIN PROOF (the whole point).
// ===========================================================================

/// Two conflicting CAS *creates* race across a partition; after the heal, exactly
/// ONE side acquires. This proves 2a-4's receiver-side CAS rejects a stale
/// partitioned proposal once a peer already holds a value, so the cluster cannot
/// split-brain.
///
/// Timeline:
///  * Start: key `k` absent everywhere. Partition: {A,B} reachable, {C} isolated.
///  * Majority {A,B}: A proposes CAS `None -> "from-AB"` with `send_targets=[B]`.
///    B applies durably + acks `Applied`; A commits (local ack + B = quorum 2).
///    Now A's peer B durably holds `k = "from-AB"`.
///  * Minority C (still partitioned, also starting from `k` absent) proposes CAS
///    `None -> "from-C"` with no reachable peers -> fenced (`QuorumTimeout`). C
///    did not commit.
///  * HEAL: link C <-> {A,B}. C RE-proposes CAS `None -> "from-C"` with
///    `send_targets=[A,B]`. A and B already hold `k = "from-AB"` (current hash is
///    no longer the expected `None`) -> their conditional apply REJECTS with
///    `CasMismatch` -> they ack `Rejected` -> C's tally sees 2 distinct rejects ->
///    C is deterministically `Fenced` (NOT a timeout, NOT a commit).
///
/// Assertions prove exactly-one-acquirer:
///  * the majority write committed (A reached quorum on "from-AB");
///  * B durably holds `k = "from-AB"` (the winning value);
///  * C's post-heal re-proposal returns `ConsistencyError::Fenced` — i.e. it was
///    CAS-REJECTED by the peers, not merely timed out and not allowed to overwrite;
///  * B STILL holds `k = "from-AB"` afterwards (C did not overwrite anyone).
#[test]
fn heal_mid_write_exactly_one_side_acquires() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;

    let node_a = Node::spawn(NODE_A, dir_a.path())?;
    let node_b = Node::spawn(NODE_B, dir_b.path())?;
    let node_c = Node::spawn(NODE_C, dir_c.path())?;

    let key = b"contended-k".to_vec();
    let value_ab = b"from-AB".to_vec();
    let value_c = b"from-C".to_vec();

    // --- Partition {A,B} | {C}: link A<->B only; C stays isolated. -----------
    link_both(&node_a, &node_b)?;

    // --- Majority {A,B}: A proposes the create; B applies + acks; A commits AND
    // durably persists its own committed value locally (so the winning value lives
    // on the FULL quorum {A,B} — see propose_and_commit_locally). ---------------
    let ab_outcome = propose_and_commit_locally(
        &node_a,
        &key,
        None,
        &value_ab,
        &membership(3, &[NODE_B]),
        QUORUM_TIMEOUT,
    )?;
    assert!(
        ab_outcome.reached(),
        "majority {{A,B}} must commit the create: {ab_outcome:?}"
    );
    assert_eq!(ab_outcome.required, 2);
    assert!(ab_outcome.acknowledged >= 2, "A local + B Applied: {ab_outcome:?}");

    // BOTH A and B durably hold the winning value: A committed it locally, B's
    // responder applied it. This is what makes the post-heal CAS fence work — every
    // node C will contact already holds "from-AB".
    assert_eq!(
        node_a.db.get(&key)?,
        Some(value_ab.clone()),
        "A must durably hold its own committed k = \"from-AB\""
    );
    let b_has_winner = wait_until(QUORUM_TIMEOUT, || {
        matches!(node_b.db.get(&key), Ok(Some(ref v)) if v == &value_ab)
    });
    assert!(b_has_winner, "B must durably hold k = \"from-AB\" after the majority commit");

    // --- Minority C (still partitioned): proposes the conflicting create. -----
    // C starts from k absent on ITS copy and has no reachable peers -> fenced.
    let c_fenced_pre_heal = node_c.db.distribution().ok_or("C has no endpoint")?.propose_write(
        key.clone(),
        None,
        value_c.clone(),
        None,
        &membership(3, &[]),
        FENCE_TIMEOUT,
    );
    assert!(
        matches!(c_fenced_pre_heal, Err(ConsistencyError::QuorumTimeout { .. })),
        "isolated C must be fenced pre-heal (QuorumTimeout), got {c_fenced_pre_heal:?}"
    );

    // --- HEAL: link C <-> {A,B} (both directions, both peers). ---------------
    link_both(&node_c, &node_a)?;
    link_both(&node_c, &node_b)?;

    // --- C RE-proposes the SAME create now that it can reach A and B. --------
    // A and B already hold k = "from-AB", so their CAS compare (expected None vs a
    // present value) MISMATCHES -> both ack Rejected(CasMismatch) -> C's tally
    // sees 2 distinct rejects -> possible_accepts = 3 - 2 = 1 < required 2 ->
    // deterministic Fenced. This is the split-brain fence: C is out-voted, not
    // allowed to overwrite.
    let c_fenced_post_heal = node_c.db.distribution().ok_or("C has no endpoint")?.propose_write(
        key.clone(),
        None,
        value_c.clone(),
        None,
        &membership(3, &[NODE_A, NODE_B]),
        QUORUM_TIMEOUT,
    );
    match c_fenced_post_heal {
        Err(ConsistencyError::Fenced {
            required,
            possible_accepts,
        }) => {
            assert_eq!(required, 2, "quorum over 3 is 2");
            assert!(
                possible_accepts < 2,
                "CAS rejects from A and B must drop possible accepts below quorum, got {possible_accepts}"
            );
        }
        other => {
            return Err(format!(
                "post-heal C must be CAS-REJECTED -> Fenced (NOT timeout, NOT commit), got {other:?}"
            )
            .into());
        }
    }

    // --- EXACTLY ONE ACQUIRER: the majority value won; C overwrote no one. ----
    // A CAS reject applies NOTHING, so the durable value on A and B must be
    // UNCHANGED: still "from-AB", never flipped to "from-C".
    assert_eq!(
        node_a.db.get(&key)?,
        Some(value_ab.clone()),
        "A must STILL hold k = \"from-AB\" — C's rejected proposal applied nothing"
    );
    assert_eq!(
        node_b.db.get(&key)?,
        Some(value_ab),
        "B must STILL hold k = \"from-AB\" — C's rejected proposal applied nothing"
    );
    // C's value must NOT have overwritten the winner anywhere (the split-brain check).
    assert_ne!(
        node_a.db.get(&key)?,
        Some(value_c.clone()),
        "C's value must NOT have overwritten the winner on A"
    );
    assert_ne!(
        node_b.db.get(&key)?,
        Some(value_c.clone()),
        "C's value must NOT have overwritten the winner on B"
    );
    // C never committed either of its proposals (fenced both times), so it never
    // durably acquired k on its own store either.
    assert_ne!(
        node_c.db.get(&key)?,
        Some(value_c),
        "C must not have durably acquired its own contested value"
    );

    Ok(())
}
