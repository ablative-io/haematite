//! AA-3-4d end-to-end: handoff MERGE — a freshly-elected owner reconstructs a
//! LOSSLESS committed baseline by UNION-merging the committed states of EVERY
//! promiser in its majority before serving (§2.4, R5 — THE durability guarantee).
//!
//! These run against REAL `Database` instances over the REAL beamr loopback
//! transport (no mocked send, no mocked source). Each node runs the
//! `respond_to_inbound_writes` responder, which answers an inbound
//! `ShardSyncRequest` by exporting its reachable committed node set (the source
//! side of the merge pull) — so catch-up is a genuine multi-node pull over the
//! wire and the merge is computed locally over what was pulled.
//!
//! GATE 1 — FORKED-committed-state failover (the headline): owner A commits k2 to
//! a majority {A,C} (NOT B) and k3 to a majority {A,B} (NOT C), so B holds {k3}
//! and C holds {k2}: each a committed/acked write the other lacks (a real
//! partial-partition fork, §2.4). A is then partitioned; a NEW owner is elected
//! over {B,C} and `become_live`-merges. It must serve BOTH k2 AND k3. The
//! falsifiability control proves a single-promiser fold (single-root adoption)
//! DROPS one of them.
//!
//! GATE 2 — committed DELETE survives failover: a key put to {A,B,C} then DELETED
//! (stamped tombstone) on the majority {A,B} — C still holds the old value. After
//! failover + merge over {B,C}, the tombstone (higher stamp) WINS the per-key
//! max-stamp join and the key reads ABSENT — the delete is not resurrected by the
//! laggard.
//!
//! Tests return `Result` and use `?` (the crate denies `expect`/`unwrap`/`panic`).

#![allow(clippy::panic, clippy::doc_markdown)]

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use haematite::db::respond_to_inbound_writes;
use haematite::sync::membership::WriteMembership;
use haematite::sync::{DistributionEndpoint, SyncNodeId};
use haematite::{Database, DatabaseConfig};

type TestResult = Result<(), Box<dyn Error>>;

const NODE_A: &str = "node-a@127.0.0.1";
const NODE_B: &str = "node-b@127.0.0.1";
const NODE_C: &str = "node-c@127.0.0.1";

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const OP_TIMEOUT: Duration = Duration::from_secs(5);

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
/// responder draining + answering inbound `Prepare`s, `WriteProposal`s, and
/// `ShardSyncRequest`s.
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

fn membership(total_nodes: usize, send_targets: &[&str]) -> WriteMembership {
    WriteMembership {
        total_nodes,
        send_targets: send_targets.iter().map(|n| SyncNodeId::from(*n)).collect(),
    }
}

// ===========================================================================
// GATE 1 — FORKED-committed-state failover (the headline).
// ===========================================================================

/// 3 nodes {A,B,C}, quorum 2. A owns shard 0 and constructs a real fork via two
/// partial-partition writes: k2=v2 committed to {A, C} (send_targets = [C], so B
/// lags k2), and k3=v3 committed to {A, B} (send_targets = [B], so C lags k3).
/// So B holds {k3} and C holds {k2}: each a committed/acked write the OTHER lacks
/// (incomparable committed roots, §2.4). A is partitioned away. C is elected the
/// new owner; its Promise majority is {C(self), B}. `become_live` pulls B's
/// committed tree and UNION-merges it with C's local committed tree, so C must
/// serve BOTH k2 (its own) AND k3 (from B).
///
/// Load-bearing: C did NOT hold k3 before becoming live (asserted), and the merge
/// folds BOTH promisers — a single-root adoption of either promiser's root would
/// drop one key.
#[test]
fn forked_committed_state_survives_failover() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;

    let node_a = Node::spawn(NODE_A, dir_a.path())?;
    let node_b = Node::spawn(NODE_B, dir_b.path())?;
    let node_c = Node::spawn(NODE_C, dir_c.path())?;

    link_both(&node_a, &node_b)?;
    link_both(&node_a, &node_c)?;
    link_both(&node_b, &node_c)?;

    // A wins ownership and becomes live (empty baseline).
    node_a
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B, NODE_C]), OP_TIMEOUT)?;

    // k2 committed to {A, C} ONLY: B never receives it.
    node_a.db.replicate_write(
        b"k2".to_vec(),
        None,
        b"v2".to_vec(),
        None,
        &membership(3, &[NODE_C]),
        OP_TIMEOUT,
    )?;
    // k3 committed to {A, B} ONLY: C never receives it.
    node_a.db.replicate_write(
        b"k3".to_vec(),
        None,
        b"v3".to_vec(),
        None,
        &membership(3, &[NODE_B]),
        OP_TIMEOUT,
    )?;

    // The fork, asserted: B holds {k3} not k2; C holds {k2} not k3.
    assert_eq!(node_b.db.get(b"k3")?, Some(b"v3".to_vec()), "B holds k3");
    assert_eq!(node_b.db.get(b"k2")?, None, "B lags k2");
    assert_eq!(node_c.db.get(b"k2")?, Some(b"v2".to_vec()), "C holds k2");
    assert_eq!(node_c.db.get(b"k3")?, None, "C lags k3 (load-bearing)");

    // FAILOVER: C becomes the new owner over {C, B} (A partitioned ⟹ not a send
    // target). become_live UNION-merges B's committed tree into C's.
    node_c
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B]), OP_TIMEOUT)?;

    // THE GATE (R5): the new owner serves BOTH committed writes — neither was
    // dropped by the merge. k3 could ONLY have arrived via the merge pull from B.
    assert_eq!(
        node_c.db.get(b"k2")?,
        Some(b"v2".to_vec()),
        "new owner must serve k2 (its own committed write)"
    );
    assert_eq!(
        node_c.db.get(b"k3")?,
        Some(b"v3".to_vec()),
        "new owner must serve k3 after merge (R5): the forked committed write \
         from B must not be dropped"
    );
    Ok(())
}

/// Control / falsifiability for GATE 1: a BARE `acquire_shard` (NO `become_live`
/// merge) on C leaves it WITHOUT k3 — proving the recovery is enforced by the
/// merge code path, not the test setup. With the SAME fork and election, skipping
/// the merge drops the forked write. (Single-root adoption is shown to drop a
/// forked write directly at the actor level in
/// `merge_adopt_unions_forked_promiser_state`; this is the e2e end of the same
/// falsifiability claim — remove the merge step and the property fails.)
#[test]
fn bare_acquire_without_merge_drops_forked_write() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;

    let node_a = Node::spawn(NODE_A, dir_a.path())?;
    let node_b = Node::spawn(NODE_B, dir_b.path())?;
    let node_c = Node::spawn(NODE_C, dir_c.path())?;
    link_both(&node_a, &node_b)?;
    link_both(&node_a, &node_c)?;
    link_both(&node_b, &node_c)?;

    node_a
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B, NODE_C]), OP_TIMEOUT)?;
    node_a.db.replicate_write(
        b"k2".to_vec(),
        None,
        b"v2".to_vec(),
        None,
        &membership(3, &[NODE_C]),
        OP_TIMEOUT,
    )?;
    node_a.db.replicate_write(
        b"k3".to_vec(),
        None,
        b"v3".to_vec(),
        None,
        &membership(3, &[NODE_B]),
        OP_TIMEOUT,
    )?;
    assert_eq!(node_c.db.get(b"k3")?, None, "C lags k3 before failover");

    // BARE acquire — election ONLY, no become_live. The Promise majority {C, B}
    // exists (B advertises k3's root), but with no merge C never pulls/unions it.
    node_c
        .db
        .acquire_shard(SHARD, &membership(3, &[NODE_B]), OP_TIMEOUT)?;

    assert_eq!(
        node_c.db.get(b"k3")?,
        None,
        "WITHOUT become_live's merge the new owner does NOT serve the forked write \
         — proves the recovery comes from the merge path, not setup"
    );
    Ok(())
}

// ===========================================================================
// GATE 2 — committed DELETE survives failover (no resurrection).
// ===========================================================================

/// 3 nodes {A,B,C}, quorum 2. A puts k=v to ALL of {A,B,C}, then DELETES k
/// (stamped tombstone) to the majority {A, B} ONLY — C is the laggard that still
/// holds the OLD value v. A is partitioned; C is elected the new owner over
/// {C, B}. The merge folds B's committed tree (which holds the TOMBSTONE) with
/// C's (which holds the old value): the tombstone's stamp is strictly higher
/// (later seq under the same owner), so the per-key max-stamp join keeps the
/// TOMBSTONE and k reads ABSENT — the committed delete is recovered, NOT
/// resurrected by C's stale value.
#[test]
fn committed_delete_survives_failover_not_resurrected() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;

    let node_a = Node::spawn(NODE_A, dir_a.path())?;
    let node_b = Node::spawn(NODE_B, dir_b.path())?;
    let node_c = Node::spawn(NODE_C, dir_c.path())?;

    link_both(&node_a, &node_b)?;
    link_both(&node_a, &node_c)?;
    link_both(&node_b, &node_c)?;

    node_a
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B, NODE_C]), OP_TIMEOUT)?;

    // k=v committed to ALL three: A, B, and C each hold v.
    node_a.db.replicate_write(
        b"k".to_vec(),
        None,
        b"v".to_vec(),
        None,
        &membership(3, &[NODE_B, NODE_C]),
        OP_TIMEOUT,
    )?;
    assert_eq!(node_b.db.get(b"k")?, Some(b"v".to_vec()), "B holds v");
    assert_eq!(node_c.db.get(b"k")?, Some(b"v".to_vec()), "C holds v");

    // DELETE k on the majority {A, B} ONLY: B gets the tombstone, C keeps v.
    let expected = haematite::tree::Hash::of(b"v");
    node_a.db.replicate_delete(
        b"k".to_vec(),
        Some(expected),
        &membership(3, &[NODE_B]),
        OP_TIMEOUT,
    )?;
    assert_eq!(
        node_b.db.get(b"k")?,
        None,
        "B applied the tombstone (absent)"
    );
    assert_eq!(
        node_c.db.get(b"k")?,
        Some(b"v".to_vec()),
        "C is the laggard: it still holds the OLD value (load-bearing)"
    );

    // FAILOVER: C becomes the new owner over {C, B}. The merge unions B's
    // tombstone (higher stamp) with C's stale value (lower stamp).
    node_c
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B]), OP_TIMEOUT)?;

    // THE GATE: the committed delete survives — k reads ABSENT. The tombstone won
    // the max-stamp join; C's stale value did NOT resurrect the key.
    assert_eq!(
        node_c.db.get(b"k")?,
        None,
        "the committed delete must survive failover: the tombstone (higher stamp) \
         wins the merge and is NOT resurrected by C's stale value (R-TOMB)"
    );
    Ok(())
}
