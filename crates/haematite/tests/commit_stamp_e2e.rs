//! AA-3-4a end-to-end gates: the causal commit stamp `(epoch, seq)` on the REAL
//! quorum-on-write path over the REAL beamr loopback transport.
//!
//! Two properties are proven here against live `Database` instances (no mocks):
//!
//! * **R-SEQ — every replica stores the IDENTICAL owner-assigned stamp.** A
//!   replicated write lands the same `(epoch, seq)` on the proposer AND every peer
//!   (the merge precondition of §2.4). The owner draws `seq` from its atomic
//!   per-(shard, live-epoch) counter once, carries it on the `WriteProposal`, and
//!   each replica stores it verbatim.
//!
//! * **THE GATE — R-LE prevents a duplicate `(epoch, seq)` across a crash.** A node
//!   that wins epoch `e'`, serves `(e',0)`/`(e',1)`, then CRASHES and reopens
//!   (recovering `owner_epoch = e'` from disk) must NOT stamp `(e', _)` again
//!   without a live re-acquisition: with no live election its `live_epoch` is
//!   `bottom`, so it stamps `(bottom, _)`, never colliding with the pre-crash
//!   `(e', _)`. After a re-acquire the new epoch `e'' > e'` and writes stamp
//!   `(e'', 0..)` — distinct from every `(e', _)`.
//!
//! Tests return `Result` and use `?` (the crate denies `expect`/`unwrap`/`panic`).

#![allow(clippy::panic, clippy::doc_markdown)]

use std::error::Error;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use haematite::db::respond_to_inbound_writes;
use haematite::sync::membership::WriteMembership;
use haematite::sync::{Ballot, DistributionEndpoint, Stamp, SyncNodeId};
use haematite::{Database, DatabaseConfig};

type TestResult = Result<(), Box<dyn Error>>;

const NODE_A: &str = "node-a@127.0.0.1";
const NODE_B: &str = "node-b@127.0.0.1";

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
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

fn membership(total_nodes: usize, send_targets: &[&str]) -> WriteMembership {
    WriteMembership {
        total_nodes,
        send_targets: send_targets.iter().map(|n| SyncNodeId::from(*n)).collect(),
    }
}

/// One node: a live `Database` with an attached endpoint plus a background
/// responder draining + answering inbound `WriteProposal`s and `Prepare`s. Owns
/// its `data_dir` so it can be CRASHED (dropped) and REOPENED from the same disk.
struct Node {
    name: &'static str,
    data_dir: PathBuf,
    addr: SocketAddr,
    db: Arc<Database>,
    responder: Option<JoinHandle<()>>,
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl Node {
    /// Create a fresh node (new database on disk) bound to an ephemeral port.
    fn create(name: &'static str, data_dir: PathBuf) -> Result<Self, Box<dyn Error>> {
        let db = Database::create(config_for(&data_dir))?;
        Self::attach(name, data_dir, db)
    }

    /// Simulate a CRASH + RESTART: drop the live database/endpoint, then REOPEN the
    /// SAME on-disk `data_dir`, recovering the durable `owner_epoch`/`promised` from
    /// the WAL. The reopened node binds a FRESH endpoint (new ephemeral port) — the
    /// in-memory `live_epoch` is therefore `bottom` (R-LE: never recovered).
    fn crash_and_reopen(self) -> Result<Self, Box<dyn Error>> {
        let name = self.name;
        let data_dir = self.data_dir.clone();
        // Drop the old node (stops responder, tears down endpoint) before reopen.
        drop(self);
        let db = Database::open(&data_dir)?;
        Self::attach(name, data_dir, db)
    }

    fn attach(
        name: &'static str,
        data_dir: PathBuf,
        db: Database,
    ) -> Result<Self, Box<dyn Error>> {
        let endpoint = DistributionEndpoint::bind(name, loopback()?, 1, None)?;
        let addr = endpoint.local_addr();
        let db = Arc::new(db.with_distribution(endpoint));

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
            data_dir,
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

// ===========================================================================
// Gate 1 — a replicated write lands the IDENTICAL stamp on proposer AND peer.
// ===========================================================================

/// A owns shard 0 (epoch e') and serves two writes. The stamp `(e', seq)` stored
/// on A (proposer) is byte-IDENTICAL to the stamp stored on B (peer), and the seq
/// advances 0, 1 under the one live epoch (R-SEQ). This is the §2.4 precondition
/// that the merge can treat a replicated write as one entry.
#[test]
fn replicated_write_lands_identical_stamp_on_proposer_and_peer() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let node_a = Node::create(NODE_A, dir_a.path().join("db"))?;
    let node_b = Node::create(NODE_B, dir_b.path().join("db"))?;
    link_both(&node_a, &node_b)?;

    // A wins ownership: live_epoch becomes e' (a real, >=1 ballot naming A).
    let owner = node_a
        .db
        .acquire_shard(SHARD, &membership(2, &[NODE_B]), WRITE_TIMEOUT)?;
    let e_prime = owner.ballot;
    assert_eq!(node_a.db.live_epoch_for_test(SHARD), e_prime);

    // Two committed writes (majority {A,B} both apply).
    node_a.db.replicate_write(
        b"k1".to_vec(),
        None,
        b"v1".to_vec(),
        None,
        &membership(2, &[NODE_B]),
        WRITE_TIMEOUT,
    )?;
    node_a.db.replicate_write(
        b"k2".to_vec(),
        None,
        b"v2".to_vec(),
        None,
        &membership(2, &[NODE_B]),
        WRITE_TIMEOUT,
    )?;

    // The two writes carry stamps (e',0) and (e',1) — identical on A and B.
    let a_k1 = node_a.db.stored_stamp_for_test(b"k1").ok_or("A missing k1 stamp")?;
    let b_k1 = node_b.db.stored_stamp_for_test(b"k1").ok_or("B missing k1 stamp")?;
    let a_k2 = node_a.db.stored_stamp_for_test(b"k2").ok_or("A missing k2 stamp")?;
    let b_k2 = node_b.db.stored_stamp_for_test(b"k2").ok_or("B missing k2 stamp")?;

    assert_eq!(a_k1, b_k1, "k1 stamp must be IDENTICAL on proposer and peer");
    assert_eq!(a_k2, b_k2, "k2 stamp must be IDENTICAL on proposer and peer");
    assert_eq!(a_k1.epoch, e_prime, "k1 stamped with the live epoch e'");
    assert_eq!(a_k2.epoch, e_prime, "k2 stamped with the live epoch e'");
    // Distinct seqs under one epoch, advancing per committed write.
    assert_ne!(a_k1.seq, a_k2.seq, "two writes under one epoch get distinct seq");
    assert!(a_k1.seq < a_k2.seq, "seq advances with write order");
    Ok(())
}

// ===========================================================================
// THE GATE — R-LE prevents a duplicate (epoch, seq) across a crash.
// ===========================================================================

/// THE 3-4a GATE (design §6): a recovered `owner_epoch` cannot stamp without a
/// live re-acquisition.
///
/// 1. A wins e', serves `(e',0)` and `(e',1)`.
/// 2. A CRASHES and reopens, recovering `owner_epoch = e'` from disk — but its
///    in-memory `live_epoch` is `bottom` (R-LE never recovers it).
/// 3. WITHOUT a re-acquire, A serves a write: its `live_epoch` is `bottom`, so it
///    stamps `(bottom, _)`, NOT `(e', _)`. Because `bottom < e' = B.promised`, the
///    peer FENCES it — A's write is correctly REFUSED, never committed as
///    `(e', _)`. Either way (stamp bottom / refuse) no committed write can collide
///    with the pre-crash `(e', _)` stamps.
/// 4. After a re-acquire the new epoch `e'' > e'`, and writes stamp `(e'', 0..)`,
///    distinct from every `(e', _)`.
#[test]
fn recovered_owner_epoch_cannot_restamp_without_reacquire() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let node_a = Node::create(NODE_A, dir_a.path().join("db"))?;
    let node_b = Node::create(NODE_B, dir_b.path().join("db"))?;
    link_both(&node_a, &node_b)?;

    // --- (1) A wins e' and serves (e',0), (e',1). ---
    let owner = node_a
        .db
        .acquire_shard(SHARD, &membership(2, &[NODE_B]), WRITE_TIMEOUT)?;
    let e_prime = owner.ballot;
    for key in [b"pre1".as_slice(), b"pre2".as_slice()] {
        node_a.db.replicate_write(
            key.to_vec(),
            None,
            b"pre".to_vec(),
            None,
            &membership(2, &[NODE_B]),
            WRITE_TIMEOUT,
        )?;
    }
    let pre1 = node_a.db.stored_stamp_for_test(b"pre1").ok_or("missing pre1")?;
    let pre2 = node_a.db.stored_stamp_for_test(b"pre2").ok_or("missing pre2")?;
    assert_eq!(pre1, Stamp::new(e_prime.clone(), 0));
    assert_eq!(pre2, Stamp::new(e_prime.clone(), 1));

    // --- (2) CRASH + reopen A. owner_epoch = e' recovers from disk; live_epoch
    // is bottom (R-LE: never recovered). ---
    let node_a = node_a.crash_and_reopen()?;
    // Re-establish the bidirectional link (A bound a fresh port on reopen).
    link_both(&node_a, &node_b)?;
    assert_eq!(
        node_a.db.live_epoch_for_test(SHARD),
        Ballot::bottom(),
        "R-LE: a recovered owner_epoch must NOT seed the in-memory live_epoch"
    );

    // --- (3) Without a re-acquire, a write would stamp (bottom, _), NEVER (e', _).
    // `live_epoch == bottom` means every stamp A could draw is bottom-epoch. The
    // peer B has `promised = e'`, so a bottom-stamped write is FENCED (refused) —
    // A's write cannot commit a colliding `(e', _)`. We assert BOTH facets: the
    // refusal (no false success) and that the would-be stamp is bottom, never e'.
    let stamp_if_drawn = node_a.db.next_stamp_for_test(SHARD);
    assert_eq!(
        stamp_if_drawn.epoch,
        Ballot::bottom(),
        "R-LE: a recovered owner that did NOT re-acquire must stamp bottom, never e'"
    );
    let fenced = node_a.db.replicate_write(
        b"post-crash".to_vec(),
        None,
        b"after".to_vec(),
        None,
        &membership(2, &[NODE_B]),
        WRITE_TIMEOUT,
    );
    assert!(
        fenced.is_err(),
        "a bottom-stamped write from a non-re-acquired owner must be fenced/refused, \
         never committed as (e',_): {fenced:?}"
    );
    // And it left NO committed (e',_) collision on the proposer.
    assert_eq!(
        node_a.db.stored_stamp_for_test(b"post-crash"),
        None,
        "the fenced write must have committed nothing (no (e',_) collision)"
    );

    // --- (4) After a re-acquire, e'' > e' and writes stamp (e'', 0..). ---
    let reowner = node_a
        .db
        .acquire_shard(SHARD, &membership(2, &[NODE_B]), WRITE_TIMEOUT)?;
    let e_pp = reowner.ballot;
    assert!(
        e_pp > e_prime,
        "re-acquisition must yield a strictly higher epoch: {e_pp:?} > {e_prime:?}"
    );
    assert_eq!(node_a.db.live_epoch_for_test(SHARD), e_pp);

    node_a.db.replicate_write(
        b"post-reacquire".to_vec(),
        None,
        b"again".to_vec(),
        None,
        &membership(2, &[NODE_B]),
        WRITE_TIMEOUT,
    )?;
    let after = node_a
        .db
        .stored_stamp_for_test(b"post-reacquire")
        .ok_or("missing post-reacquire stamp")?;
    assert_eq!(after.epoch, e_pp, "post-reacquire writes stamp the NEW epoch e''");
    assert_eq!(after.seq, 0, "seq restarts at 0 under the new live epoch");
    // Distinct from every pre-crash (e', _): e'' strictly dominates e'.
    assert!(after > pre1 && after > pre2, "(e'',0) dominates all (e',_)");
    Ok(())
}

// ===========================================================================
// AA-3-4b — deletes as stamped tombstones through the one fenced/quorum path.
// ===========================================================================

/// Adversarial test 1: a distributed delete is FENCED + STAMPED + quorum-
/// replicated, exactly like a put.
///
/// * The owner A commits `k=v`, then DELETEs `k` via `replicate_delete`. The
///   tombstone lands on the PEER B as a stamped tombstone (reads as None there),
///   carrying the SAME owner-assigned `(epoch, seq)` on A and B (R-SEQ).
/// * A delete from a STALE epoch is FENCED (rejected) — driven by a crashed,
///   non-re-acquired owner whose stamp is `bottom < B.promised`.
#[test]
fn distributed_delete_is_fenced_stamped_and_quorum_replicated() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let node_a = Node::create(NODE_A, dir_a.path().join("db"))?;
    let node_b = Node::create(NODE_B, dir_b.path().join("db"))?;
    link_both(&node_a, &node_b)?;

    let owner = node_a
        .db
        .acquire_shard(SHARD, &membership(2, &[NODE_B]), WRITE_TIMEOUT)?;
    let e_prime = owner.ballot;

    // Commit k=v to majority {A,B}.
    node_a.db.replicate_write(
        b"k".to_vec(),
        None,
        b"v".to_vec(),
        None,
        &membership(2, &[NODE_B]),
        WRITE_TIMEOUT,
    )?;
    let value_hash = haematite::tree::Hash::of(b"v");
    assert_eq!(node_b.db.get(b"k")?, Some(b"v".to_vec()), "B holds the value");

    // DELETE k: a stamped tombstone replicated to quorum, CAS expecting the value
    // hash. The delete draws the NEXT seq under e' (seq 1; the put was seq 0).
    node_a.db.replicate_delete(
        b"k".to_vec(),
        Some(value_hash),
        &membership(2, &[NODE_B]),
        WRITE_TIMEOUT,
    )?;

    // Reads as None on BOTH nodes (tombstone reads as absent).
    assert_eq!(node_a.db.get(b"k")?, None, "A reads deleted key as None");
    assert_eq!(node_b.db.get(b"k")?, None, "B reads deleted key as None");

    // The PEER B physically holds a STAMPED TOMBSTONE (not a removal), with the
    // IDENTICAL stamp the owner assigned (R-SEQ), under the live epoch e'.
    assert_eq!(
        node_b.db.stored_is_tombstone_for_test(b"k"),
        Some(true),
        "the committed delete landed a stamped tombstone on the peer, not a removal"
    );
    let a_stamp = node_a.db.stored_stamp_for_test(b"k").ok_or("A missing tombstone stamp")?;
    let b_stamp = node_b.db.stored_stamp_for_test(b"k").ok_or("B missing tombstone stamp")?;
    assert_eq!(a_stamp, b_stamp, "tombstone stamp IDENTICAL on proposer and peer");
    assert_eq!(a_stamp.epoch, e_prime, "tombstone stamped with the live epoch e'");
    assert_eq!(a_stamp.seq, 1, "delete drew the next seq (put was 0) under e'");

    // --- A stale-epoch delete is FENCED. Crash A so live_epoch -> bottom; B keeps
    // promised = e'. A bottom-stamped delete is below B.promised -> Fenced. ---
    let node_a = node_a.crash_and_reopen()?;
    link_both(&node_a, &node_b)?;
    assert_eq!(node_a.db.live_epoch_for_test(SHARD), Ballot::bottom());
    let fenced = node_a.db.replicate_delete(
        b"k".to_vec(),
        None,
        &membership(2, &[NODE_B]),
        WRITE_TIMEOUT,
    );
    assert!(
        fenced.is_err(),
        "a delete from a stale (bottom) epoch must be FENCED, like a put: {fenced:?}"
    );
    Ok(())
}

/// Adversarial test 2 + 3: CAS semantics on a tombstone, and delete -> recreate
/// -> delete with three strictly-increasing stamps.
///
/// (a) create-if-absent (`expected = None`) on a tombstoned key MATCHES (tombstone
///     reads as None) and succeeds.
/// (b) a CAS expecting the deleted value MISSES on a tombstone.
/// (c) delete -> recreate -> delete leaves the key deleted, each step a higher
///     stamp.
#[test]
fn tombstone_cas_semantics_and_delete_recreate_delete() -> TestResult {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let node_a = Node::create(NODE_A, dir_a.path().join("db"))?;
    let node_b = Node::create(NODE_B, dir_b.path().join("db"))?;
    link_both(&node_a, &node_b)?;
    let member = membership(2, &[NODE_B]);

    node_a.db.acquire_shard(SHARD, &member, WRITE_TIMEOUT)?;

    // put k=v1 (seq 0), then DELETE expecting v1 (seq 1).
    node_a.db.replicate_write(b"k".to_vec(), None, b"v1".to_vec(), None, &member, WRITE_TIMEOUT)?;
    let v1_hash = haematite::tree::Hash::of(b"v1");
    node_a.db.replicate_delete(b"k".to_vec(), Some(v1_hash), &member, WRITE_TIMEOUT)?;
    let s_del1 = node_a.db.stored_stamp_for_test(b"k").ok_or("missing del1 stamp")?;
    assert_eq!(node_a.db.get(b"k")?, None);

    // (b) A CAS expecting the OLD value v1 MISSES on the tombstone (its logical
    // hash is None, not v1) — the recreate must use expected = None.
    let cas_expecting_old = node_a.db.replicate_write(
        b"k".to_vec(),
        Some(v1_hash),
        b"v2".to_vec(),
        None,
        &member,
        WRITE_TIMEOUT,
    );
    assert!(
        cas_expecting_old.is_err(),
        "a CAS expecting the deleted value must MISS on a tombstone: {cas_expecting_old:?}"
    );
    assert_eq!(node_a.db.get(b"k")?, None, "the missed CAS applied nothing");

    // (a) create-if-absent (expected = None) MATCHES the tombstone and recreates.
    node_a.db.replicate_write(b"k".to_vec(), None, b"v2".to_vec(), None, &member, WRITE_TIMEOUT)?;
    let s_recreate = node_a.db.stored_stamp_for_test(b"k").ok_or("missing recreate stamp")?;
    assert_eq!(node_a.db.get(b"k")?, Some(b"v2".to_vec()), "create-if-absent recreated the key");
    assert_eq!(
        node_a.db.stored_is_tombstone_for_test(b"k"),
        Some(false),
        "the recreated key is a value, not a tombstone"
    );

    // (c) DELETE again (expecting v2). Final read None; three strictly-increasing
    // stamps: del1 < recreate < del2.
    let v2_hash = haematite::tree::Hash::of(b"v2");
    node_a.db.replicate_delete(b"k".to_vec(), Some(v2_hash), &member, WRITE_TIMEOUT)?;
    let s_del2 = node_a.db.stored_stamp_for_test(b"k").ok_or("missing del2 stamp")?;
    assert_eq!(node_a.db.get(b"k")?, None, "key is deleted again");
    assert_eq!(node_a.db.stored_is_tombstone_for_test(b"k"), Some(true));
    assert_eq!(node_b.db.get(b"k")?, None, "peer also reads None after final delete");

    assert!(s_del1 < s_recreate, "recreate stamp exceeds first delete: {s_recreate:?} > {s_del1:?}");
    assert!(s_recreate < s_del2, "second delete stamp exceeds recreate: {s_del2:?} > {s_recreate:?}");
    Ok(())
}
