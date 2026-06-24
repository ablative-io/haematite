//! SPIKE (NOT a feature to ship) — empirical validation of wiring haematite's
//! quorum to the actual write path (Aion active-active plan, step 2a).
//!
//! The prior fencing spike (`spike_fencing.rs`, E3 CRITICAL) proved that LOCAL
//! `cas` is INSUFFICIENT for active-active: two partitioned nodes each cas-bump
//! their OWN local copy of the ownership/epoch record and BOTH acquire the same
//! shard. The fix requires the ownership/epoch write to be QUORUM-ACKED so a
//! minority partition deterministically FAILS and cannot self-quorum.
//!
//! This spike RUNS real haematite primitives to determine whether/how that is
//! possible today.
//!
//! Run with output:
//!   cargo test -p haematite --test `spike_quorum` -- --nocapture --test-threads=1
//!
//! Layers used:
//!  * Q1 drives the REAL `Database::put_with_consistency` Strong path to observe
//!    EXACTLY what the production write path does for quorum today.
//!  * Q2 uses the REAL `wait_for_quorum` consistency primitive, fed by an ack
//!    channel that simulated remote nodes write to, with `total_nodes` derived
//!    from a LIVE membership set — to prove the SHAPE of a quorum-gated epoch
//!    write (majority -> commit, minority partition -> deterministic failure).
//!    The cross-instance ack TRANSPORT does not exist in haematite (see report);
//!    Q2 substitutes an in-test ack source for that missing machinery and says
//!    so explicitly.

#![allow(clippy::unwrap_used, clippy::panic, clippy::print_stdout)]

use std::collections::BTreeSet;
use std::time::Duration;

use haematite::db::{Database, DatabaseConfig, DatabaseError};
use haematite::sync::{
    Ack, ConsistencyError, ConsistencyMode, StrongConsistency, quorum_size, wait_for_quorum,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

// ===========================================================================
// Q1 — What does the REAL write path do for quorum TODAY?
//
// Drive Database::put_with_consistency(Strong { total_nodes, .. }) and observe.
// The production wait path (api/kv.rs::wait_for_consistency) creates a LOCAL
// mpsc channel, immediately drops the sender, and calls
// wait_for_quorum_from_receiver. No remote acks are ever fed in. So:
//   total_nodes = 1 -> quorum 1 -> satisfied by the local ack alone -> OK
//   total_nodes >= 2 -> quorum >= 2 -> local ack is the only ack possible on the
//                       channel, the rest never arrive -> QuorumTimeout.
// This is the exact gap: quorum-on-write is NOT enforced end-to-end because no
// remote-ack source is wired to the receiver.
// ===========================================================================

#[test]
fn q1_real_write_path_only_counts_local_ack() -> TestResult {
    println!("\n================ Q1: real Strong write path (Database) ================");
    let dir = tempfile::tempdir()?;
    let db = Database::create(DatabaseConfig {
        data_dir: dir.path().join("db"),
        shard_count: 4,
        sweep_interval: None,
        distributed: None,
    })?;

    // total_nodes = 1: quorum is 1, the local durable WAL ack satisfies it.
    let single = ConsistencyMode::Strong(StrongConsistency::new(1, Duration::from_millis(50)));
    let r1 = db.put_with_consistency(b"owner/shard-7/epoch".to_vec(), 1_u64.to_be_bytes().to_vec(), single);
    println!("[total_nodes=1] put_with_consistency(Strong) -> {r1:?}");
    assert!(r1.is_ok(), "single-node strong write should be satisfied by the local ack");

    // total_nodes = 3: quorum is 2. The production path feeds NO remote acks, so
    // the only ack available is the local one -> times out. This is the gap.
    let cluster = ConsistencyMode::Strong(StrongConsistency::new(3, Duration::from_millis(50)));
    let r3 = db.put_with_consistency(b"owner/shard-7/epoch".to_vec(), 2_u64.to_be_bytes().to_vec(), cluster);
    println!("[total_nodes=3] put_with_consistency(Strong) -> {r3:?}");
    match &r3 {
        Err(DatabaseError::ConsistencyError(msg)) => {
            println!("    -> failed honestly: {msg}");
            assert!(msg.contains("quorum") || msg.contains("timed out"), "expected a quorum failure, got: {msg}");
        }
        other => panic!("Q1 expected a quorum failure for total_nodes=3, got {other:?}"),
    }

    println!(
        "Q1 RESULT: the REAL write path enforces quorum ONLY against a local ack. \
         For total_nodes>=2 it cannot succeed because NO remote-ack source is wired \
         to the receiver (api/kv.rs::wait_for_consistency drops the sender immediately). \
         `wait_for_quorum` is a standalone primitive; the write path does not gather \
         real remote acks.\n"
    );
    Ok(())
}

// ===========================================================================
// Q2 — Prototype the SHAPE of a quorum-gated epoch write.
//
// We model the missing piece: a quorum-on-write where
//   (a) total_nodes is derived from a LIVE membership set (not a static config),
//   (b) the epoch write blocks until a quorum of live nodes ACK it,
//   (c) a minority partition fails DETERMINISTICALLY (QuorumUnavailable) before
//       it can even attempt to self-quorum.
//
// The REAL `wait_for_quorum` primitive does the quorum arithmetic. The ack
// SOURCE (an iterator of Ack<NodeId>) stands in for the cross-instance ack
// transport that haematite does NOT have today (the sync protocol is pull-only;
// SyncMessage has no write-ack variant). That substitution is the ONLY net-new
// machinery; everything else here is the real primitive.
// ===========================================================================

/// A live-membership view. In production this would come from the distribution
/// layer's membership/failure-detector; haematite only has a STATIC config
/// (`DistributedDatabaseConfig::nodes`) today, with no liveness signal.
#[derive(Clone)]
struct Membership {
    all_nodes: BTreeSet<usize>,
    reachable: BTreeSet<usize>,
}

impl Membership {
    fn new(all: &[usize], reachable: &[usize]) -> Self {
        Self {
            all_nodes: all.iter().copied().collect(),
            reachable: reachable.iter().copied().collect(),
        }
    }

    /// `total_nodes` for quorum MUST be the full cluster size — quorum is defined
    /// over the whole membership, NOT over the reachable subset. (If you size
    /// quorum from the reachable subset, a minority partition trivially
    /// "achieves quorum" with itself — the bug we are preventing.)
    fn total_nodes(&self) -> usize {
        self.all_nodes.len()
    }
}

/// A quorum-gated epoch write. `local_node` always self-acks; each OTHER reachable
/// node contributes one remote ack. Returns Ok only if a quorum (over the FULL
/// membership) of acks is collected; otherwise the real `ConsistencyError`.
fn quorum_gated_epoch_write(
    membership: &Membership,
    local_node: usize,
    timeout: Duration,
) -> Result<usize, ConsistencyError> {
    let strong = StrongConsistency::new(membership.total_nodes(), timeout);

    // The ack source: the local node self-acks (counted by StrongConsistency's
    // count_local_ack), and every OTHER reachable node yields one remote ack.
    // This iterator is the stand-in for the missing cross-instance ack transport.
    let remote_acks: Vec<Ack<usize>> = membership
        .reachable
        .iter()
        .copied()
        .filter(|node| *node != local_node)
        .map(Ack::received)
        .collect();

    let outcome = wait_for_quorum(strong, remote_acks)?;
    Ok(outcome.acknowledged)
}

#[test]
fn q2_quorum_gated_epoch_write_membership_derived() {
    println!("\n========== Q2: quorum-gated epoch write (membership-derived total_nodes) ==========");
    let timeout = Duration::from_millis(50);

    // 3-node cluster: nodes {0,1,2}. Quorum over 3 = 2.
    let cluster = [0usize, 1, 2];
    println!("cluster = {cluster:?}, quorum over full membership = {:?}", quorum_size(cluster.len()));

    // --- MAJORITY side: nodes 0 and 1 reachable (2 of 3). Node 0 writes the epoch.
    let majority = Membership::new(&cluster, &[0, 1]);
    let maj = quorum_gated_epoch_write(&majority, 0, timeout);
    println!("[majority partition {{0,1}}] epoch write -> {maj:?}");
    assert_eq!(maj, Ok(2), "majority (local 0 + remote 1) must reach quorum 2");
    println!("    -> COMMITS: local ack(0) + remote ack(1) = 2 >= quorum 2.");

    // --- MINORITY side: only node 2 reachable (1 of 3). Node 2 tries to write.
    // total_nodes is still 3 (full membership). The minority is FENCED, but note
    // HOW: `wait_for_quorum` computes `possible` from total_nodes-1 (the
    // THEORETICAL remote capacity, = 2), NOT from the reachable set, so it does
    // NOT short-circuit to QuorumUnavailable — it WAITS for acks that never come
    // and then QuorumTimeouts. The primitive has no liveness input, so it cannot
    // distinguish "node unreachable" from "node slow". Either way: FENCED.
    let minority = Membership::new(&cluster, &[2]);
    let min = quorum_gated_epoch_write(&minority, 2, timeout);
    println!("[minority partition {{2}}]   epoch write -> {min:?}");
    match &min {
        Err(ConsistencyError::QuorumTimeout { required, acknowledged, .. }) => {
            println!("    -> FENCED via QuorumTimeout: required={required}, got={acknowledged} (cannot self-quorum).");
            assert_eq!(*required, 2);
            assert_eq!(*acknowledged, 1);
        }
        Err(ConsistencyError::QuorumUnavailable { required, possible }) => {
            println!("    -> FENCED via QuorumUnavailable: required={required}, possible={possible}.");
        }
        other => panic!("Q2 expected the minority to be FENCED, got {other:?}"),
    }

    // --- The split-brain scenario from spike_fencing E3 CRITICAL, re-run under
    // quorum: BOTH partitions try to acquire. Only the MAJORITY succeeds; the
    // MINORITY is deterministically denied. No split-brain possible.
    println!("\n--- re-running E3-CRITICAL split (partition {{0,1}} | {{2}}) under quorum ---");
    let side_a = quorum_gated_epoch_write(&Membership::new(&cluster, &[0, 1]), 0, timeout);
    let side_b = quorum_gated_epoch_write(&Membership::new(&cluster, &[2]), 2, timeout);
    println!("  partition {{0,1}} acquire -> {side_a:?}");
    println!("  partition {{2}}   acquire -> {side_b:?}");
    let a_ok = side_a.is_ok();
    let b_ok = side_b.is_ok();
    println!("  exactly one side acquired? {}", a_ok ^ b_ok);
    assert!(a_ok ^ b_ok, "exactly one partition may acquire under quorum");
    assert!(a_ok && !b_ok, "the majority must win, the minority must be fenced");

    println!(
        "\nQ2 RESULT: with total_nodes derived from LIVE membership and the epoch write \
         gated on `wait_for_quorum`, the minority partition is FENCED (here via QuorumTimeout, \
         since the primitive has no liveness signal and waits out the timeout) and CANNOT \
         self-quorum. This is the SHAPE that fixes E3 CRITICAL. The quorum ARITHMETIC \
         is the real primitive; the only thing substituted here is the cross-instance ack \
         SOURCE, which haematite does not yet have (pull-only sync, no write-ack frame).\n"
    );
}

// ===========================================================================
// Q3 — Sanity: even with membership-derived total_nodes, a partition that is a
// minority must NOT be rescued by sizing quorum from the reachable subset.
// This pins the design invariant: quorum is over FULL membership.
// ===========================================================================

#[test]
fn q3_quorum_must_be_over_full_membership_not_reachable_subset() {
    println!("\n========== Q3: quorum invariant — over full membership, not reachable subset ==========");
    let timeout = Duration::from_millis(50);
    let cluster = [0usize, 1, 2];

    // WRONG sizing: total_nodes = reachable subset size (1). quorum(1) = 1, local
    // ack alone satisfies it -> the minority "wins". This is the bug to avoid.
    let wrong = StrongConsistency::new(1, timeout); // sized from reachable {2}
    let wrong_outcome = wait_for_quorum::<usize, _>(wrong, std::iter::empty());
    println!("[WRONG: total_nodes=reachable.len()=1] minority -> {wrong_outcome:?} (would self-quorum: BUG)");
    assert!(wrong_outcome.is_ok(), "demonstrates the bug if quorum is sized from the reachable subset");

    // RIGHT sizing: total_nodes = full membership (3). quorum(3)=2, local ack
    // alone (1) is insufficient -> FENCED. With NO remote acks supplied it
    // QuorumTimeouts (the primitive has no liveness, so it waits the full
    // timeout for acks that never arrive). Either way the minority cannot write.
    let right = StrongConsistency::new(cluster.len(), timeout);
    let right_outcome = wait_for_quorum::<usize, _>(right, std::iter::empty());
    println!("[RIGHT: total_nodes=full membership=3] minority -> {right_outcome:?} (correctly fenced)");
    assert!(
        matches!(right_outcome, Err(ConsistencyError::QuorumTimeout { .. } | ConsistencyError::QuorumUnavailable { .. })),
        "minority must be fenced"
    );

    // The ONLY case that yields the cheap QuorumUnavailable short-circuit is when
    // even the THEORETICAL remote capacity (total_nodes-1) plus the local ack
    // cannot reach quorum. That happens e.g. with count_local_ack=false: a
    // remote-only writer on a 2-node cluster needs quorum 2 but has remote
    // capacity 1 and 0 local -> instantly QuorumUnavailable. This shows the
    // short-circuit is keyed off the STATIC ceiling, not live reachability.
    let remote_only = StrongConsistency::remote_only(2, timeout);
    let unavailable = wait_for_quorum::<usize, _>(remote_only, std::iter::empty());
    println!("[remote_only total_nodes=2] -> {unavailable:?} (QuorumUnavailable short-circuit)");
    assert!(matches!(unavailable, Err(ConsistencyError::QuorumUnavailable { .. })));

    println!(
        "Q3 RESULT: the load-bearing invariant is total_nodes = FULL membership size. \
         haematite's StrongConsistency takes total_nodes as a plain usize, so the \
         CALLER must feed it the full live membership count. Today total_nodes is a \
         STATIC config decoupled from any liveness signal. Crucially, `wait_for_quorum` \
         sizes its QuorumUnavailable short-circuit from the STATIC total_nodes ceiling, \
         NOT from a reachable/live set — it has no membership input at all, so it cannot \
         pre-emptively reject a minority partition; it can only TIME OUT. Feeding live \
         membership AND a real remote-ack source is the net-new work.\n"
    );
}
