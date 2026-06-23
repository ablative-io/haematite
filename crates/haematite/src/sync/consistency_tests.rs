use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::branch::conflict::ConflictPolicy;
use crate::store::MemoryStore;
use crate::tree::{Cursor, Hash, LeafNode, Node, NodeError, batch_mutate};

use crate::sync::{SyncMergeRoots, merge_synced_roots, pull_from_source};

use super::{
    Ack, ConsistencyError, ConsistencyMode, EventualConsistency, StrongConsistency,
    execute_with_consistency, quorum_size, wait_for_quorum, wait_for_quorum_from_receiver,
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
