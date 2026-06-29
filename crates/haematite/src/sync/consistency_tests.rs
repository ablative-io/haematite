use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::branch::conflict::ConflictPolicy;
use crate::store::MemoryStore;
use crate::tree::{Cursor, Hash, LeafNode, Node, NodeError, batch_mutate};

use crate::sync::{SyncMergeRoots, merge_synced_roots, pull_from_source};

use super::{
    Ack, CasVote, ConsistencyError, ConsistencyMode, EventualConsistency, RejectKind,
    StrongConsistency, execute_with_consistency, quorum_size, wait_for_cas_quorum,
    wait_for_cas_quorum_from_receiver, wait_for_quorum, wait_for_quorum_from_receiver,
};

#[test]
fn quorum_is_majority_and_rejects_zero_nodes() {
    assert_eq!(quorum_size(1), Ok(1));
    assert_eq!(quorum_size(2), Ok(2));
    assert_eq!(quorum_size(3), Ok(2));
    assert_eq!(quorum_size(4), Ok(3));
    assert_eq!(quorum_size(5), Ok(3));
    assert_eq!(quorum_size(0), Err(ConsistencyError::InvalidNodeCount));
}

#[test]
fn eventual_mode_never_requires_write_ack_and_computes_interval_due_times() {
    let mode = EventualConsistency::new(Duration::from_millis(50));
    let start = Instant::now();

    assert!(!mode.write_requires_ack());
    assert_eq!(mode.sync_interval(), Duration::from_millis(50));
    assert_eq!(
        mode.next_sync_after(start),
        start + Duration::from_millis(50)
    );
    assert!(!mode.sync_due(start, start + Duration::from_millis(49)));
    assert!(mode.sync_due(start, start + Duration::from_millis(50)));
    assert_eq!(
        mode.intervals_elapsed(start, start + Duration::from_millis(125)),
        2
    );
}

fn empty_root(store: &mut MemoryStore) -> Result<Hash, NodeError> {
    Ok(store.put(&Node::Leaf(LeafNode::new(Vec::new())?)))
}

fn put_mutation(key: &[u8], value: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
    (key.to_vec(), Some(value.to_vec()))
}

#[test]
fn eventual_mode_triggers_sync_callback_only_when_interval_is_due() {
    let mode = EventualConsistency::new(Duration::from_millis(50));
    let start = Instant::now();
    let mut last_sync = start;
    let mut calls = 0_usize;

    assert_eq!(
        mode.trigger_if_due(&mut last_sync, start + Duration::from_millis(49), || {
            calls = calls.saturating_add(1);
            Ok::<(), ()>(())
        }),
        Ok(false)
    );
    assert_eq!(calls, 0);

    assert_eq!(
        mode.trigger_if_due(&mut last_sync, start + Duration::from_millis(50), || {
            calls = calls.saturating_add(1);
            Ok::<(), ()>(())
        }),
        Ok(true)
    );
    assert_eq!(calls, 1);
    assert_eq!(last_sync, start + Duration::from_millis(50));
}

#[test]
fn eventual_interval_trigger_can_drive_one_interval_data_propagation() -> Result<(), Box<dyn Error>>
{
    let mode = EventualConsistency::new(Duration::from_millis(50));
    let start = Instant::now();
    let mut last_sync = start;

    let mut source = MemoryStore::new();
    let mut target = MemoryStore::new();
    let base = empty_root(&mut source)?;
    empty_root(&mut target)?;
    let source_root = batch_mutate(
        &mut source,
        base,
        &[put_mutation(b"eventual-key", b"eventual-value")],
    )?;
    let mut target_root = base;

    let triggered = mode.trigger_if_due(
        &mut last_sync,
        start + Duration::from_millis(50),
        || -> Result<(), Box<dyn Error>> {
            let pull = pull_from_source(&source, &mut target, 7, Some(source_root), Some(base))?;
            let pulled_root = pull
                .source_root
                .ok_or("pull from populated source must return its root")?;
            let merge = merge_synced_roots(
                &mut target,
                7,
                SyncMergeRoots::new(target_root, pulled_root, base),
                &ConflictPolicy::Lww,
            )?;
            target_root = merge.merged_root;
            Ok(())
        },
    )?;

    assert!(triggered);
    assert_eq!(last_sync, start + Duration::from_millis(50));
    assert_eq!(target_root, source_root);
    assert_eq!(
        Cursor::new(&target, target_root).get(b"eventual-key")?,
        Some(b"eventual-value".to_vec())
    );
    Ok(())
}

#[test]
fn eventual_consistency_execution_returns_without_consuming_acks() -> Result<(), ConsistencyError> {
    let writes = AtomicUsize::new(0);
    let result = execute_with_consistency::<_, _, usize, _>(
        ConsistencyMode::eventual(Duration::from_millis(10)),
        || {
            writes.fetch_add(1, Ordering::SeqCst);
            Ok("written")
        },
        Vec::new(),
    )?;

    assert_eq!(result, "written");
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert!(!ConsistencyMode::eventual(Duration::from_millis(10)).write_requires_ack());
    Ok(())
}

#[test]
fn strong_mode_waits_until_quorum_acknowledges() -> Result<(), ConsistencyError> {
    let outcome = wait_for_quorum(
        StrongConsistency::new(5, Duration::from_secs(1)),
        vec![Ack::received("node-a"), Ack::received("node-b")],
    )?;

    assert_eq!(outcome.required, 3);
    assert_eq!(outcome.acknowledged, 3);
    assert_eq!(outcome.acknowledged_nodes, vec!["node-a", "node-b"]);
    assert!(outcome.reached());
    Ok(())
}

#[test]
fn strong_mode_does_not_count_duplicate_node_acks() {
    let result = wait_for_quorum(
        StrongConsistency::new(5, Duration::from_millis(1)),
        vec![
            Ack::received("node-a"),
            Ack::received("node-a"),
            Ack::received("node-b"),
        ],
    );

    assert!(matches!(
        result,
        Ok(outcome) if outcome.acknowledged == 3
            && outcome.acknowledged_nodes == vec!["node-a", "node-b"]
    ));
}

#[test]
fn strong_mode_returns_timeout_when_quorum_is_not_reached() {
    let timeout = Duration::from_millis(1);
    let result = wait_for_quorum(
        StrongConsistency::new(5, timeout),
        vec![Ack::received("node-a")],
    );

    assert_eq!(
        result,
        Err(ConsistencyError::QuorumTimeout {
            required: 3,
            acknowledged: 2,
            timeout,
        })
    );
}

#[test]
fn strong_mode_surfaces_unavailable_quorum_and_failed_acks() {
    assert_eq!(
        wait_for_quorum::<&str, _>(
            StrongConsistency::remote_only(1, Duration::from_millis(1)),
            Vec::new(),
        ),
        Err(ConsistencyError::QuorumUnavailable {
            required: 1,
            possible: 0,
        })
    );
    assert_eq!(
        wait_for_quorum(
            StrongConsistency::remote_only(3, Duration::from_secs(1)),
            vec![Ack::failed("node-a")],
        ),
        Err(ConsistencyError::AckFailed)
    );
}

#[test]
fn receiver_quorum_wait_blocks_until_ack_arrives() -> Result<(), ConsistencyError> {
    let (sender, receiver) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        let _ = sender.send(Ack::received("node-a"));
    });

    let outcome = wait_for_quorum_from_receiver(
        StrongConsistency::new(3, Duration::from_secs(1)),
        &receiver,
    )?;
    handle
        .join()
        .map_err(|_error| ConsistencyError::AckFailed)?;

    assert_eq!(outcome.required, 2);
    assert_eq!(outcome.acknowledged, 2);
    assert_eq!(outcome.acknowledged_nodes, vec!["node-a"]);
    Ok(())
}

#[test]
fn receiver_quorum_wait_times_out_when_ack_does_not_arrive() {
    let (_sender, receiver) = std::sync::mpsc::channel::<Ack<&str>>();
    let timeout = Duration::from_millis(5);
    let result = wait_for_quorum_from_receiver(StrongConsistency::new(3, timeout), &receiver);

    assert_eq!(
        result,
        Err(ConsistencyError::QuorumTimeout {
            required: 2,
            acknowledged: 1,
            timeout,
        })
    );
}

#[test]
fn strong_consistency_execution_runs_write_then_waits_for_quorum() -> Result<(), ConsistencyError> {
    let writes = AtomicUsize::new(0);
    let result = execute_with_consistency(
        ConsistencyMode::strong(3, Duration::from_secs(1)),
        || {
            writes.fetch_add(1, Ordering::SeqCst);
            Ok("strong-write")
        },
        vec![Ack::received("node-a")],
    )?;

    assert_eq!(result, "strong-write");
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert!(ConsistencyMode::strong(3, Duration::from_secs(1)).write_requires_ack());
    Ok(())
}

// ---------------------------------------------------------------------------
// CAS-aware tally (2a-2). Accept/Reject/Fault votes; the new `Fenced` outcome.
// Synthetic vote sources, exactly like the accept-only `wait_for_quorum` tests.
// ---------------------------------------------------------------------------

#[test]
fn cas_accept_majority_commits() -> Result<(), ConsistencyError> {
    // 3-node cluster (quorum 2): local ack + one remote accept reaches quorum.
    let outcome = wait_for_cas_quorum(
        StrongConsistency::new(3, Duration::from_secs(1)),
        vec![CasVote::accept("node-b")],
    )?;

    assert_eq!(outcome.required, 2);
    assert_eq!(outcome.acknowledged, 2);
    assert_eq!(outcome.acknowledged_nodes, vec!["node-b"]);
    assert!(outcome.reached());
    Ok(())
}

#[test]
fn cas_minority_no_votes_times_out() {
    // 5-node cluster (quorum 3): only the local ack, no remote votes → timeout.
    let timeout = Duration::from_millis(2);
    let result = wait_for_cas_quorum::<&str, _>(
        StrongConsistency::new(5, timeout),
        std::iter::empty(),
    );

    assert_eq!(
        result,
        Err(ConsistencyError::QuorumTimeout {
            required: 3,
            acknowledged: 1,
            timeout,
        })
    );
}

#[test]
fn cas_reject_majority_is_fenced_not_failed_or_timeout() {
    // 5-node cluster (quorum 3): possible = 1 local + 4 remote = 5. Three distinct
    // rejects drop possible_accepts to 2 < 3 → deterministic Fenced. The remote
    // capacity is 4, so the static `possible < required` short-circuit does NOT
    // fire (5 >= 3); the Fenced verdict is from the reject tally alone.
    let result = wait_for_cas_quorum(
        StrongConsistency::new(5, Duration::from_secs(1)),
        vec![
            CasVote::reject("node-b"),
            CasVote::reject("node-c"),
            CasVote::reject("node-d"),
        ],
    );

    assert_eq!(
        result,
        Err(ConsistencyError::Fenced {
            required: 3,
            possible_accepts: 2,
        })
    );
}

#[test]
fn cas_distinct_rejects_are_deduped_before_fencing() {
    // Duplicate rejects from one node must not over-count toward fencing. With
    // total 5 (quorum 3, possible 5) two DISTINCT rejects leave possible_accepts 3
    // (still == required, NOT fenced); the duplicate third vote changes nothing, so
    // the tally times out rather than fencing.
    let timeout = Duration::from_millis(2);
    let result = wait_for_cas_quorum(
        StrongConsistency::new(5, timeout),
        vec![
            CasVote::reject("node-b"),
            CasVote::reject("node-b"),
            CasVote::reject("node-c"),
        ],
    );

    assert_eq!(
        result,
        Err(ConsistencyError::QuorumTimeout {
            required: 3,
            acknowledged: 1,
            timeout,
        })
    );
}

#[test]
fn cas_mixed_accepts_and_rejects_fences_as_soon_as_provably_lost() {
    // 5-node cluster (quorum 3, possible 5). One accept (acknowledged → 2), then
    // rejects erode the ceiling: after 3 distinct rejects possible_accepts = 2 < 3
    // → Fenced. Proven lost before the (still un-acked) third accept could arrive.
    let result = wait_for_cas_quorum(
        StrongConsistency::new(5, Duration::from_secs(1)),
        vec![
            CasVote::accept("node-b"),
            CasVote::reject("node-c"),
            CasVote::reject("node-d"),
            CasVote::reject("node-e"),
            // never reached: quorum is already provably unreachable.
            CasVote::accept("node-f"),
        ],
    );

    assert_eq!(
        result,
        Err(ConsistencyError::Fenced {
            required: 3,
            possible_accepts: 2,
        })
    );
}

#[test]
fn cas_single_fault_does_not_abort_a_reachable_quorum() -> Result<(), ConsistencyError> {
    // 3-node cluster, quorum 2 (local + 1). One remote faults, but the OTHER
    // remote accepts → local + accept = 2 → committed. A single fault must NOT
    // abort a write the rest of the cluster can still carry (the availability fix:
    // a fault erodes the accept ceiling, it does not poison the whole tally).
    let outcome = wait_for_cas_quorum(
        StrongConsistency::new(3, Duration::from_secs(1)),
        vec![CasVote::fault("node-b"), CasVote::accept("node-c")],
    )?;

    assert_eq!(outcome.acknowledged, 2);
    Ok(())
}

#[test]
fn cas_loss_to_faults_alone_is_ack_failed_not_fenced() {
    // 3-node cluster, quorum 2, possible 3. Two DISTINCT faults erode
    // possible_accepts to 1 < 2 with NO reject in play → AckFailed (a retryable
    // infrastructure failure), distinct from the Fenced a CAS reject produces.
    let result = wait_for_cas_quorum::<&str, _>(
        StrongConsistency::new(3, Duration::from_secs(1)),
        vec![CasVote::fault("node-b"), CasVote::fault("node-c")],
    );

    assert_eq!(result, Err(ConsistencyError::AckFailed));
}

#[test]
fn cas_loss_with_any_reject_is_fenced_even_alongside_faults() {
    // 5-node cluster, quorum 3, possible 5. A reject plus faults erode
    // possible_accepts below 3; because a real CAS reject occurred (a conflicting
    // owner out-voted us), the verdict is Fenced, not AckFailed.
    let result = wait_for_cas_quorum::<&str, _>(
        StrongConsistency::new(5, Duration::from_secs(1)),
        vec![
            CasVote::reject("node-b"),
            CasVote::fault("node-c"),
            CasVote::fault("node-d"),
        ],
    );

    assert!(
        matches!(result, Err(ConsistencyError::Fenced { required: 3, .. })),
        "a reject in the loss set fences (conflicting owner), got {result:?}"
    );
}

#[test]
fn cas_static_short_circuit_still_fires_on_remote_only_two_node() {
    // remote_only on a 2-node cluster: quorum 2, possible = 0 local + 1 remote = 1
    // < 2 → the static QuorumUnavailable short-circuit fires before any vote, even
    // for the CAS tally. This is the ONLY availability short-circuit (no liveness
    // fast-fail).
    let result = wait_for_cas_quorum::<&str, _>(
        StrongConsistency::remote_only(2, Duration::from_millis(1)),
        std::iter::empty(),
    );

    assert_eq!(
        result,
        Err(ConsistencyError::QuorumUnavailable {
            required: 2,
            possible: 1,
        })
    );
}

#[test]
fn cas_accept_dedup_does_not_double_count() {
    // Duplicate accepts from one node count once. 5-node cluster (quorum 3): local
    // + node-b counted once = 2 (< 3), then node-c reaches 3 → commit.
    let outcome = wait_for_cas_quorum(
        StrongConsistency::new(5, Duration::from_secs(1)),
        vec![
            CasVote::accept("node-b"),
            CasVote::accept("node-b"),
            CasVote::accept("node-c"),
        ],
    );

    assert!(matches!(
        outcome,
        Ok(out) if out.acknowledged == 3 && out.acknowledged_nodes == vec!["node-b", "node-c"]
    ));
}

// ---------------------------------------------------------------------------
// Typed-reject classification (RejectKind): the iterator tally
// `wait_for_cas_quorum`. The pre-existing `CasVote::reject` (EpochFence) tests
// above remain unchanged and continue to fence; these exercise the NEW split.
// ---------------------------------------------------------------------------

#[test]
fn cas_pure_cas_mismatch_rejects_is_cas_conflict_not_fenced() {
    // 5-node cluster (quorum 3, possible 5). Three DISTINCT value-CAS mismatches
    // (no epoch fence) drop possible_accepts to 2 < 3. Because there is no fence in
    // the loss set, the verdict is the weaker, retryable CasConflict — NOT Fenced
    // and NOT AckFailed.
    let result = wait_for_cas_quorum(
        StrongConsistency::new(5, Duration::from_secs(1)),
        vec![
            CasVote::reject_kind("node-b", RejectKind::CasMismatch),
            CasVote::reject_kind("node-c", RejectKind::CasMismatch),
            CasVote::reject_kind("node-d", RejectKind::CasMismatch),
        ],
    );

    assert_eq!(
        result,
        Err(ConsistencyError::CasConflict {
            required: 3,
            possible_accepts: 2,
        })
    );
}

#[test]
fn cas_pure_epoch_fence_rejects_is_fenced() {
    // 5-node cluster (quorum 3, possible 5). Three DISTINCT epoch fences drop
    // possible_accepts to 2 < 3 → Fenced (a conflicting higher-ballot owner deposed
    // us). Explicit RejectKind::EpochFence, equivalent to the bare `reject` default.
    let result = wait_for_cas_quorum(
        StrongConsistency::new(5, Duration::from_secs(1)),
        vec![
            CasVote::reject_kind("node-b", RejectKind::EpochFence),
            CasVote::reject_kind("node-c", RejectKind::EpochFence),
            CasVote::reject_kind("node-d", RejectKind::EpochFence),
        ],
    );

    assert_eq!(
        result,
        Err(ConsistencyError::Fenced {
            required: 3,
            possible_accepts: 2,
        })
    );
}

#[test]
fn cas_mixed_fence_and_mismatch_fences_fence_wins() {
    // 5-node cluster (quorum 3, possible 5). One epoch fence plus two value-CAS
    // mismatches drop possible_accepts to 2 < 3. The STRONGER epoch-fence signal
    // wins even though a cas-mismatch is also present → Fenced, not CasConflict.
    let result = wait_for_cas_quorum(
        StrongConsistency::new(5, Duration::from_secs(1)),
        vec![
            CasVote::reject_kind("node-b", RejectKind::CasMismatch),
            CasVote::reject_kind("node-c", RejectKind::EpochFence),
            CasVote::reject_kind("node-d", RejectKind::CasMismatch),
        ],
    );

    assert_eq!(
        result,
        Err(ConsistencyError::Fenced {
            required: 3,
            possible_accepts: 2,
        })
    );
}

#[test]
fn cas_mismatch_alongside_faults_is_cas_conflict() {
    // 5-node cluster (quorum 3, possible 5). One value-CAS mismatch plus two faults
    // erode possible_accepts to 2 < 3. No epoch fence → the deterministic
    // vote-against (cas-mismatch) outranks the faults → CasConflict, not AckFailed.
    let result = wait_for_cas_quorum::<&str, _>(
        StrongConsistency::new(5, Duration::from_secs(1)),
        vec![
            CasVote::reject_kind("node-b", RejectKind::CasMismatch),
            CasVote::fault("node-c"),
            CasVote::fault("node-d"),
        ],
    );

    assert_eq!(
        result,
        Err(ConsistencyError::CasConflict {
            required: 3,
            possible_accepts: 2,
        })
    );
}

#[test]
fn cas_faults_only_is_ack_failed_not_cas_conflict() {
    // 3-node cluster, quorum 2, possible 3. Two DISTINCT faults with NO reject of
    // any kind → AckFailed (retryable infra failure), neither Fenced nor CasConflict.
    let result = wait_for_cas_quorum::<&str, _>(
        StrongConsistency::new(3, Duration::from_secs(1)),
        vec![CasVote::fault("node-b"), CasVote::fault("node-c")],
    );

    assert_eq!(result, Err(ConsistencyError::AckFailed));
}

// ---------------------------------------------------------------------------
// Typed-reject classification: the LIVE receiver tally
// `wait_for_cas_quorum_from_receiver`. This path previously INLINED
// reject->Fenced and bypassed `decline_outcome`; it MUST classify identically.
// ---------------------------------------------------------------------------

fn drain_cas_votes<NodeId>(
    strong: StrongConsistency,
    votes: Vec<CasVote<NodeId>>,
) -> Result<super::QuorumOutcome<NodeId>, ConsistencyError>
where
    NodeId: Clone + Eq + std::hash::Hash,
{
    let (sender, receiver) = std::sync::mpsc::channel();
    for vote in votes {
        let _ = sender.send(vote);
    }
    drop(sender);
    wait_for_cas_quorum_from_receiver(strong, &receiver)
}

#[test]
fn receiver_cas_pure_cas_mismatch_is_cas_conflict() {
    let result = drain_cas_votes(
        StrongConsistency::new(5, Duration::from_secs(1)),
        vec![
            CasVote::reject_kind("node-b", RejectKind::CasMismatch),
            CasVote::reject_kind("node-c", RejectKind::CasMismatch),
            CasVote::reject_kind("node-d", RejectKind::CasMismatch),
        ],
    );

    assert_eq!(
        result,
        Err(ConsistencyError::CasConflict {
            required: 3,
            possible_accepts: 2,
        })
    );
}

#[test]
fn receiver_cas_pure_epoch_fence_is_fenced() {
    let result = drain_cas_votes(
        StrongConsistency::new(5, Duration::from_secs(1)),
        vec![
            CasVote::reject_kind("node-b", RejectKind::EpochFence),
            CasVote::reject_kind("node-c", RejectKind::EpochFence),
            CasVote::reject_kind("node-d", RejectKind::EpochFence),
        ],
    );

    assert_eq!(
        result,
        Err(ConsistencyError::Fenced {
            required: 3,
            possible_accepts: 2,
        })
    );
}

#[test]
fn receiver_cas_mixed_any_fence_is_fenced() {
    // The fence wins even when a value-CAS mismatch is also present, on the live
    // receiver path that previously inlined the verdict.
    let result = drain_cas_votes(
        StrongConsistency::new(5, Duration::from_secs(1)),
        vec![
            CasVote::reject_kind("node-b", RejectKind::CasMismatch),
            CasVote::reject_kind("node-c", RejectKind::CasMismatch),
            CasVote::reject_kind("node-d", RejectKind::EpochFence),
        ],
    );

    assert_eq!(
        result,
        Err(ConsistencyError::Fenced {
            required: 3,
            possible_accepts: 2,
        })
    );
}

#[test]
fn receiver_cas_faults_only_is_ack_failed() {
    let result = drain_cas_votes::<&str>(
        StrongConsistency::new(3, Duration::from_secs(1)),
        vec![CasVote::fault("node-b"), CasVote::fault("node-c")],
    );

    assert_eq!(result, Err(ConsistencyError::AckFailed));
}

#[test]
fn receiver_cas_bare_reject_default_is_epoch_fence() {
    // The single-arg `CasVote::reject` constructor defaults to EpochFence on the
    // live receiver path too (historical behaviour preserved).
    let result = drain_cas_votes(
        StrongConsistency::new(5, Duration::from_secs(1)),
        vec![
            CasVote::reject("node-b"),
            CasVote::reject("node-c"),
            CasVote::reject("node-d"),
        ],
    );

    assert_eq!(
        result,
        Err(ConsistencyError::Fenced {
            required: 3,
            possible_accepts: 2,
        })
    );
}
