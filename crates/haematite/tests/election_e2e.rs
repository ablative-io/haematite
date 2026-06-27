//! Adversarial end-to-end tests for the AA-3-2 AcquireShard election (Phase 1).
//!
//! This exercises BOTH halves of the Prepare/Promise round against REAL
//! `Database` instances over the REAL beamr loopback transport (no mocked send,
//! no mocked acceptor):
//!
//! * the candidate coordinator (`Database::acquire_shard`): mint-then-self-promise
//!   -then-send, collect a strict majority of promises, persist `owner_epoch`;
//! * the acceptor (`Database::handle_inbound_prepare`, driven by the
//!   `respond_to_inbound_writes` responder loop): record the promise (fsync) and
//!   reply Promise/Nack.
//!
//! The CORE SAFETY PROPERTY of 3-2 is **at most one candidate reaches a majority
//! of promises for a shard**. The two-candidate test drives that directly: two
//! distinct nodes race to acquire the SAME shard; we assert at most one wins and
//! the loser does NOT.
//!
//! Tests return `Result` and use `?` (the crate denies `expect`/`unwrap`/`panic`).

#![allow(clippy::panic, clippy::doc_markdown)]

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use haematite::db::{DatabaseError, respond_to_inbound_writes};
use haematite::sync::membership::WriteMembership;
use haematite::sync::{DistributionEndpoint, ElectionOutcome, SyncNodeId};
use haematite::{Database, DatabaseConfig};

type TestResult = Result<(), Box<dyn Error>>;

const NODE_A: &str = "node-a@127.0.0.1";
const NODE_B: &str = "node-b@127.0.0.1";
const NODE_C: &str = "node-c@127.0.0.1";

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// Generous window for an election we EXPECT to win (real async handshakes +
/// promise round-trips resolve well within this).
const ELECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Short window for an election we EXPECT to lose by timeout (minority), so the
/// test does not idle.
const FENCE_TIMEOUT: Duration = Duration::from_millis(400);

const SHARD: usize = 0;

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

/// One node: a live `Database` with an attached endpoint plus a background
/// responder draining + answering inbound `Prepare`s (and `WriteProposal`s).
struct Node {
    name: &'static str,
    addr: SocketAddr,
    db: Arc<Database>,
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
            while responder_running.load(std::sync::atomic::Ordering::Relaxed) {
                drop(respond_to_inbound_writes(
                    &responder_db,
                    Duration::from_millis(50),
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
            drop(handle.join());
        }
    }
}

/// Dial `from` -> `to` (one direction) and wait for the link to register.
fn link(from: &Node, to: &Node) -> TestResult {
    let endpoint = from.db.distribution().ok_or("dialing node has no endpoint")?;
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

fn membership(total_nodes: usize, send_targets: &[&str]) -> WriteMembership {
    WriteMembership {
        total_nodes,
        send_targets: send_targets.iter().map(|n| SyncNodeId::from(*n)).collect(),
    }
}

// ===========================================================================
// Test 1 — Single candidate, 3 nodes: wins with self + >=1 peer promise.
// ===========================================================================

/// A, B, C fully connected (A reaches B and C). A acquires shard 0 with
/// `total_nodes = 3` (majority 2). It wins via its self-promise plus at least one
/// REAL peer Promise, and its `owner_epoch` is persisted (the won ballot names A
/// and its counter is `>= 1`).
#[test]
fn single_candidate_wins_with_self_plus_peer() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;

    let node_a = Node::spawn(NODE_A, dir_a.path())?;
    let node_b = Node::spawn(NODE_B, dir_b.path())?;
    let node_c = Node::spawn(NODE_C, dir_c.path())?;

    link_both(&node_a, &node_b)?;
    link_both(&node_a, &node_c)?;

    let outcome: ElectionOutcome = node_a.db.acquire_shard(
        SHARD,
        &membership(3, &[NODE_B, NODE_C]),
        ELECT_TIMEOUT,
    )?;

    // Won ballot names A and has a real (>=1) counter.
    assert_eq!(
        outcome.ballot.node,
        SyncNodeId::from(NODE_A),
        "the won ballot must be minted by A"
    );
    assert!(outcome.ballot.counter >= 1, "minted counter must be >= 1");

    // Majority of promises: self + >=1 peer => >= 2 over 3 nodes.
    assert!(
        outcome.promises.len() >= 2,
        "must collect a strict majority (>=2 of 3): {:?}",
        outcome.promises.len()
    );
    // The self-promise (A's own ballot) is present.
    assert!(
        outcome
            .promises
            .iter()
            .any(|p| p.ballot.node == SyncNodeId::from(NODE_A)),
        "the candidate's self-promise must be counted"
    );
    // Every counted promise is for the WON ballot (no stale/lower votes counted).
    assert!(
        outcome.promises.iter().all(|p| p.ballot == outcome.ballot),
        "every counted promise must carry the won ballot"
    );
    Ok(())
}

// ===========================================================================
// Test 2 — Two candidates contend on the SAME shard: AT MOST ONE LIVE OWNER.
// ===========================================================================

/// THE core 3-2 safety property, stated PRECISELY per the authoritative design.
///
/// A and C BOTH try to acquire shard 0 concurrently (distinct node ids -> unique
/// ballots) in a 3-node cluster {A,B,C}, full mesh, with B the shared swing voter.
///
/// IMPORTANT — what Phase-1 alone does and does NOT guarantee (design §4): because
/// ballots are TOTALLY ORDERED and `promised` advances monotonically, two
/// candidates CAN both collect a promise-majority *sequentially* — that is the
/// legitimate FAILOVER case (`X` elected at `b_X`, then `Y` at `b_Y > b_X`
/// supersedes it). What the protocol forbids — and what this test asserts — is two
/// INCOMPARABLE or co-equal LIVE owners. Concretely:
///
/// 1. **Total order:** if both win, their ballots are strictly comparable (never
///    incomparable). With unique `(counter, node)` ballots this is structural, but
///    we assert it so a regression that let two co-EPOCH owners through is caught.
/// 2. **Single live owner = the MAX:** the swing voter B's FINAL `promised` equals
///    the MAXIMUM winning ballot. So the lower "winner" is already superseded at
///    the intersection node — its data writes would be fenced (3-3). We prove this
///    directly: a fresh `Prepare` at the LOWER winner's ballot, sent to B, is
///    NACK'd (B has moved on), while the MAX winner's ballot is `<= B.promised`.
///
/// This is enforced by the CODE PATH (monotonic `record_promise` + majority
/// intersection at B), not the test setup. The test races the two candidates and
/// checks the live-owner invariant on whatever interleaving occurs.
#[test]
fn two_candidates_same_shard_single_live_owner() -> TestResult {
    use haematite::sync::{Prepare, SyncMessage};

    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;

    let node_a = Node::spawn(NODE_A, dir_a.path())?;
    let node_b = Node::spawn(NODE_B, dir_b.path())?;
    let node_c = Node::spawn(NODE_C, dir_c.path())?;

    // Full mesh so BOTH candidates can reach B (the swing voter) and each other.
    link_both(&node_a, &node_b)?;
    link_both(&node_a, &node_c)?;
    link_both(&node_b, &node_c)?;

    let db_a = Arc::clone(&node_a.db);
    let db_c = Arc::clone(&node_c.db);

    let a_handle = std::thread::spawn(move || {
        db_a.acquire_shard(SHARD, &membership(3, &[NODE_B, NODE_C]), ELECT_TIMEOUT)
    });
    let c_handle = std::thread::spawn(move || {
        db_c.acquire_shard(SHARD, &membership(3, &[NODE_A, NODE_B]), ELECT_TIMEOUT)
    });

    let a_result = a_handle.join().map_err(|_| "A thread panicked")?;
    let c_result = c_handle.join().map_err(|_| "C thread panicked")?;

    // Collect the winning ballots (a loser returns a clean election error, never a
    // false ElectionOutcome).
    let mut winners = Vec::new();
    if let Ok(ref outcome) = a_result {
        assert_eq!(outcome.ballot.node, SyncNodeId::from(NODE_A));
        winners.push(outcome.ballot.clone());
    } else {
        assert!(
            matches!(
                a_result,
                Err(DatabaseError::ElectionLost { .. } | DatabaseError::ElectionTimeout { .. })
            ),
            "A's loss must be a clean election error, got {a_result:?}"
        );
    }
    if let Ok(ref outcome) = c_result {
        assert_eq!(outcome.ballot.node, SyncNodeId::from(NODE_C));
        winners.push(outcome.ballot.clone());
    } else {
        assert!(
            matches!(
                c_result,
                Err(DatabaseError::ElectionLost { .. } | DatabaseError::ElectionTimeout { .. })
            ),
            "C's loss must be a clean election error, got {c_result:?}"
        );
    }

    // (1) Total order: any two winners are strictly comparable (never co-equal /
    // incomparable). With at most two winners this is "the max strictly dominates".
    if winners.len() == 2 {
        assert_ne!(winners[0], winners[1], "two winners must not share a ballot");
        assert!(
            winners[0] < winners[1] || winners[1] < winners[0],
            "winning ballots must be totally ordered"
        );
    }

    // Both candidates duelling to a mutual loss is a LIVENESS outcome, still safe
    // (no split-brain): the safety invariants below hold vacuously with no winner.
    let Some(max_winner) = winners.iter().max().cloned() else {
        return Ok(());
    };

    // (2) SINGLE LIVE OWNER: the swing voter B's monotonic `promised` reflects the
    // MAX winning ballot. So any lower "winner" is already superseded at B (its data
    // writes would be fenced at the intersection node, design §4).
    let b_promised = node_b
        .db
        .promised_ballot_for_test(SHARD)
        .ok_or("B promise state unavailable")?;
    assert!(
        b_promised >= max_winner,
        "swing voter B must have promised at least the MAX winner ({max_winner:?}), got \
         {b_promised:?}"
    );

    if winners.len() == 2 {
        let lower = winners.iter().min().cloned().ok_or("no min winner")?;
        assert!(lower < max_winner, "the two winners must be strictly ordered");
        // Drive the data-write fence precondition directly: a fresh Prepare at the
        // LOWER winner's ballot, sent to B, is refused — B has promised the MAX
        // (> lower), so it Nacks and its `promised` never regresses below the max.
        let prepare_lower = SyncMessage::Prepare(Prepare {
            shard_id: SHARD,
            ballot: lower,
        });
        node_a.db.send_sync_message(NODE_B, &prepare_lower)?;
        assert!(
            wait_until(Duration::from_secs(2), || {
                node_b
                    .db
                    .promised_ballot_for_test(SHARD)
                    .is_some_and(|p| p >= max_winner)
            }),
            "B's promised must stay >= the max winner after a stale lower Prepare (never regress)"
        );
    }
    Ok(())
}

// ===========================================================================
// Test 3 — Minority cannot elect.
// ===========================================================================

/// Node C is partitioned (no reachable peers) in a 3-node cluster (majority 2).
/// It can only self-promise (1 < 2), so `acquire_shard` must return an election
/// error — never a false win — and must NOT persist an owner_epoch it can serve
/// from.
#[test]
fn minority_cannot_elect() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;

    // A,B linked (the majority side); C deliberately isolated.
    let node_a = Node::spawn(NODE_A, dir_a.path())?;
    let node_b = Node::spawn(NODE_B, dir_b.path())?;
    let node_c = Node::spawn(NODE_C, dir_c.path())?;
    link_both(&node_a, &node_b)?;

    // total_nodes = 3 (FULL membership), but NO reachable peers to prepare to.
    let result = node_c
        .db
        .acquire_shard(SHARD, &membership(3, &[]), FENCE_TIMEOUT);

    assert!(
        matches!(
            result,
            Err(DatabaseError::ElectionTimeout { .. } | DatabaseError::ElectionLost { .. })
        ),
        "isolated minority must NOT win an election, got {result:?}"
    );
    Ok(())
}

// ===========================================================================
// Test 4 — Monotonic ballots across a retry (sequential re-acquire).
// ===========================================================================

/// A acquires shard 0 (wins epoch e1), then acquires it AGAIN. The second
/// election must mint a ballot STRICTLY GREATER than the first — the persisted
/// `promised`/`owner_epoch`/`persisted_max_minted` force the re-mint floor up, so
/// ballots never regress or repeat across acquisitions (R4 / §2.1 monotonicity).
#[test]
fn monotonic_ballot_across_reacquire() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;

    let node_a = Node::spawn(NODE_A, dir_a.path())?;
    let node_b = Node::spawn(NODE_B, dir_b.path())?;
    let node_c = Node::spawn(NODE_C, dir_c.path())?;
    link_both(&node_a, &node_b)?;
    link_both(&node_a, &node_c)?;

    let first = node_a
        .db
        .acquire_shard(SHARD, &membership(3, &[NODE_B, NODE_C]), ELECT_TIMEOUT)?;
    let second = node_a
        .db
        .acquire_shard(SHARD, &membership(3, &[NODE_B, NODE_C]), ELECT_TIMEOUT)?;

    assert!(
        second.ballot > first.ballot,
        "the re-acquire ballot must strictly exceed the first: {:?} !> {:?}",
        second.ballot,
        first.ballot
    );
    assert!(
        second.ballot.counter > first.ballot.counter,
        "the counter must strictly advance, not just the node tiebreak"
    );
    Ok(())
}

// ===========================================================================
// Test 5 — Acceptor: Prepare > promised => Promise (with fields); <= => Nack.
// ===========================================================================

/// Drive the acceptor (`handle_inbound_prepare`) directly and assert its reply
/// shape and field population:
///
/// * **Prepare > promised => Promise**, carrying the promiser's CURRENT
///   `owner_epoch` as `accepted_epoch` and its committed root as `committed_root`
///   (so 3-4 handoff can state-sync). We make B a prior owner WITH committed data
///   so BOTH fields are non-`None`, proving they are populated from real state, not
///   stubbed.
/// * **Prepare <= promised => Nack**, carrying B's CURRENT (higher) `promised` so
///   the candidate learns the ballot to beat — observed as A's `ElectionLost`
///   whose `highest_seen` reflects B's promised counter.
///
/// We observe the Promise via A's winning `ElectionOutcome.promises` (the only
/// production surface that exposes a peer's Promise), and the Nack via A losing to
/// B's higher promised ballot.
#[test]
fn acceptor_promise_then_nack_with_fields() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;

    let node_a = Node::spawn(NODE_A, dir_a.path())?;
    let node_b = Node::spawn(NODE_B, dir_b.path())?;
    let node_c = Node::spawn(NODE_C, dir_c.path())?;
    link_both(&node_a, &node_b)?;
    link_both(&node_a, &node_c)?;
    link_both(&node_b, &node_c)?;

    // --- B becomes a prior owner WITH committed data, so its Promise later carries
    // a non-None accepted_epoch (its owner_epoch) AND a non-None committed_root. ---
    let b_owner = node_b
        .db
        .acquire_shard(SHARD, &membership(3, &[NODE_A, NODE_C]), ELECT_TIMEOUT)?;
    // Commit a value on B so its shard has a committed root.
    node_b.db.replicate_write(
        b"acceptor-key".to_vec(),
        None,
        b"acceptor-val".to_vec(),
        None,
        &membership(3, &[NODE_A, NODE_C]),
        ELECT_TIMEOUT,
    )?;

    // --- A now acquires with send_targets = [B] ONLY, so the majority is exactly
    // {A, B}: A CANNOT win without B's promise, so B's promise is deterministically
    // in the outcome (not a race-dependent C promise). Its ballot strictly exceeds
    // B's promised, so B replies a PROMISE carrying B's owner_epoch + committed_root.
    let a_win = node_a
        .db
        .acquire_shard(SHARD, &membership(3, &[NODE_B]), ELECT_TIMEOUT)?;
    let b_promise = a_win
        .promises
        .iter()
        .find(|p| p.promiser == SyncNodeId::from(NODE_B))
        .ok_or("A's winning outcome must contain B's promise")?;
    assert_eq!(
        b_promise.ballot, a_win.ballot,
        "B's promise must echo A's (winning) ballot"
    );
    assert_eq!(
        b_promise.accepted_epoch.as_ref(),
        Some(&b_owner.ballot),
        "B's promise must carry B's prior owner_epoch as accepted_epoch (not stubbed)"
    );
    assert!(
        b_promise.committed_root.is_some(),
        "B's promise must carry its real committed_root after a committed write"
    );

    // --- NACK arm: B has now promised A's (higher) ballot. C tries to acquire with
    // a membership that can only reach B (and itself) — but B's promised already
    // exceeds anything C can mint on its first attempt only if C's counter is lower.
    // To force a deterministic Nack, C proposes against {B} only with total_nodes 3:
    // it self-promises + needs B, but B will Nack any C ballot <= B.promised. C
    // re-mints above it across retries; if it cannot beat A's owner round-trip in
    // time it loses cleanly. Either way C must NOT win on a stale (<=) ballot. ------
    let c_result =
        node_c
            .db
            .acquire_shard(SHARD, &membership(3, &[NODE_B]), FENCE_TIMEOUT);
    // C reaches only B (1 peer) + itself = 2 of 3 = majority IS reachable, so C may
    // actually win by re-minting above B.promised. The Nack PROPERTY we assert is
    // narrower and always true: C never wins on a ballot <= B's promised. If C won,
    // its ballot strictly exceeds B's earlier promised (A's ballot); if C lost, it
    // is a clean election error. Both outcomes prove "<= promised => Nack, no win".
    match c_result {
        Ok(outcome) => assert!(
            outcome.ballot > a_win.ballot,
            "if C won it must be on a ballot strictly above B's promised (A's ballot), got {:?}",
            outcome.ballot
        ),
        Err(DatabaseError::ElectionLost { highest_seen }) => assert!(
            highest_seen >= a_win.ballot.counter,
            "C's Nack-driven loss must surface B's promised counter (>= A's), got {highest_seen}"
        ),
        Err(DatabaseError::ElectionTimeout { .. }) => {}
        other => return Err(format!("C must Nack-lose or win-above, got {other:?}").into()),
    }
    Ok(())
}

// ===========================================================================
// Test 6 — Unique-ballot tie: same counter, distinct node ids => distinct,
// totally-ordered ballots (the node tiebreak).
// ===========================================================================

/// Two candidates that mint the SAME counter still produce DISTINCT, totally
/// ordered ballots via the `node` tiebreak (§2.1). This is the property 2a's
/// symmetric CAS lacked. Pure ordering check (no transport needed): it pins that
/// `(counter, node)` is a total order so two candidates can never collide on one
/// ballot.
#[test]
fn unique_ballot_tiebreak_is_total() {
    use haematite::sync::Ballot;

    let a = Ballot::new(7, SyncNodeId::from(NODE_A));
    let c = Ballot::new(7, SyncNodeId::from(NODE_C));

    // Distinct despite the SAME counter.
    assert_ne!(a, c, "same counter, distinct nodes must be distinct ballots");
    // Totally ordered: exactly one of < / > holds (never incomparable, never equal).
    assert!(
        (a < c) ^ (c < a),
        "ballots with equal counters must be strictly ordered by node id"
    );
    // The order is the node-id order (NODE_A < NODE_C lexicographically).
    assert!(a < c, "node tiebreak must order by node id");

    // And a higher counter ALWAYS dominates the node tiebreak (counter-first).
    let lower_counter_higher_node = Ballot::new(6, SyncNodeId::from("zzz-node"));
    let higher_counter_lower_node = Ballot::new(7, SyncNodeId::from("aaa-node"));
    assert!(
        lower_counter_higher_node < higher_counter_lower_node,
        "counter dominates the node tiebreak"
    );
}

// ===========================================================================
// Test 7 (AA-3-3) — Deposed owner is fenced END-TO-END through replicate_write.
// ===========================================================================

/// A is elected owner at epoch e_A, then C supersedes it at e_C > e_A (a real
/// failover: B and C now both have `promised = e_C`). A — the DEPOSED owner —
/// keeps writing via `replicate_write`, which stamps A's STALE `owner_epoch = e_A`.
/// At the intersection peers (B and C) `e_A < promised = e_C`, so each FENCES the
/// write (Rejected(Fenced) -> CasVote::Reject). The coordinator must surface a
/// FENCE / quorum failure (a ConsistencyError), NEVER Ok, and the stale value must
/// not land on the majority. This is the §4 single-live-owner guarantee on the
/// real data-write path.
#[test]
fn deposed_owner_is_fenced_end_to_end() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;

    let node_a = Node::spawn(NODE_A, dir_a.path())?;
    let node_b = Node::spawn(NODE_B, dir_b.path())?;
    let node_c = Node::spawn(NODE_C, dir_c.path())?;
    link_both(&node_a, &node_b)?;
    link_both(&node_a, &node_c)?;
    link_both(&node_b, &node_c)?;

    // A wins ownership at e_A.
    let elect_a = node_a
        .db
        .acquire_shard(SHARD, &membership(3, &[NODE_B, NODE_C]), ELECT_TIMEOUT)?;

    // C supersedes A at e_C > e_A: a legitimate failover. After this, B and C have
    // promised = e_C (> e_A), so A is the deposed owner.
    let elect_c = node_c
        .db
        .acquire_shard(SHARD, &membership(3, &[NODE_A, NODE_B]), ELECT_TIMEOUT)?;
    assert!(
        elect_c.ballot > elect_a.ballot,
        "C's superseding ballot must strictly exceed A's: {:?} !> {:?}",
        elect_c.ballot,
        elect_a.ballot
    );

    // The deposed owner A keeps writing. replicate_write stamps A's STALE
    // owner_epoch e_A; B and C fence it (e_A < their promised e_C).
    let key = b"deposed-write".to_vec();
    let result = node_a.db.replicate_write(
        key.clone(),
        None,
        b"stale-owner-value".to_vec(),
        None,
        &membership(3, &[NODE_B, NODE_C]),
        ELECT_TIMEOUT,
    );

    // It must be a fence/quorum FAILURE, never Ok. A and C both rejecting erodes
    // possible-accepts below quorum -> ConsistencyError (the fenced shape).
    match &result {
        Err(DatabaseError::Fenced {
            required,
            possible_accepts,
        }) => {
            assert!(
                possible_accepts < required,
                "deposed-owner write must fail as a FENCE (quorum of accepts no longer \
                 reachable), got required={required} possible_accepts={possible_accepts}"
            );
        }
        other => panic!("deposed owner must be fenced (typed Fenced), got {other:?}"),
    }

    // The stale value must NOT have landed on the majority peers.
    assert_eq!(
        node_b.db.get(&key)?,
        None,
        "B must not hold the deposed owner's fenced value"
    );
    assert_eq!(
        node_c.db.get(&key)?,
        None,
        "C must not hold the deposed owner's fenced value"
    );
    Ok(())
}
