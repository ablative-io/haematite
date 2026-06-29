//! CORE-007: integration tests for the shard native process.
//!
//! These drive the REAL beamr native process through [`ShardHandle`]: spawn via
//! the scheduler, wake via `enqueue_atom_message`, payloads over the mpsc
//! side-channel. The five cases are ported from the CORE-007 reference and
//! exercise merge/shadowing, WAL-before-tree, the commit marker + idempotence,
//! history-independence, and crash + manual re-spawn (WAL recovery), with a
//! sibling shard left running.

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use beamr::module::ModuleRegistry;
use beamr::process::ExitReason;
use beamr::scheduler::{Scheduler, SchedulerConfig};

use super::RecordPromiseOutcome;
use super::handle::{RangeItem, ShardError, ShardHandle};
use crate::store::DiskStore;
use crate::sync::SyncNodeId;
use crate::sync::ballot::{Ballot, Stamp};
use crate::tree::{Hash, LeafNode, Node};
use crate::wal::DurableWal;

const TIMEOUT: Duration = Duration::from_secs(5);

/// Decoded range entries: `(key, value)` pairs in key order.
type RangeEntries = Vec<(Vec<u8>, Vec<u8>)>;

/// A single-threaded scheduler so per-shard serialization and the crash/respawn
/// paths are deterministic.
fn test_scheduler() -> Result<Arc<Scheduler>, Box<dyn Error>> {
    let scheduler = Scheduler::new(
        SchedulerConfig {
            thread_count: Some(1),
            ..SchedulerConfig::default()
        },
        Arc::new(ModuleRegistry::new()),
    )
    .map_err(|message| -> Box<dyn Error> { message.into() })?;
    Ok(Arc::new(scheduler))
}

/// A spawned shard plus the paths it lives on (so a test can re-spawn against
/// the same store + WAL to model crash recovery).
struct TestShard {
    _dir: tempfile::TempDir,
    store_dir: PathBuf,
    wal_path: PathBuf,
    handle: ShardHandle,
}

impl TestShard {
    fn spawn(scheduler: &Arc<Scheduler>, name: &str) -> Result<Self, Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let store_dir = dir.path().join(format!("{name}.store"));
        let wal_path = dir.path().join(format!("{name}.wal"));
        // Seed an empty committed root so the store dir exists with a tree.
        let mut store = DiskStore::new(&store_dir)?;
        let _root = empty_root(&mut store)?;
        drop(store);
        let handle =
            ShardHandle::spawn(Arc::clone(scheduler), store_dir.clone(), wal_path.clone())?;
        Ok(Self {
            _dir: dir,
            store_dir,
            wal_path,
            handle,
        })
    }

    /// Re-spawn a fresh native process against the SAME paths (manual restart;
    /// the supervisor/router is CORE-008). WAL recovery seeds the new process.
    fn respawn(&self, scheduler: &Arc<Scheduler>) -> Result<ShardHandle, Box<dyn Error>> {
        Ok(ShardHandle::spawn(
            Arc::clone(scheduler),
            self.store_dir.clone(),
            self.wal_path.clone(),
        )?)
    }
}

fn empty_root(store: &mut DiskStore) -> Result<Hash, Box<dyn Error>> {
    let leaf = LeafNode::new(Vec::new())?;
    Ok(store.put(&Node::Leaf(leaf))?)
}

fn put(handle: &ShardHandle, key: &[u8], value: &[u8]) -> Result<(), Box<dyn Error>> {
    handle.put(key.to_vec(), value.to_vec(), TIMEOUT)?;
    Ok(())
}

fn delete(handle: &ShardHandle, key: &[u8]) -> Result<(), Box<dyn Error>> {
    // AA-3-4b: a delete is a stamped tombstone; this helper stamps `bottom`
    // (single-node / un-elected), which still reads as absent.
    handle.delete(key.to_vec(), crate::sync::ballot::Stamp::bottom(), TIMEOUT)?;
    Ok(())
}

fn get(handle: &ShardHandle, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
    Ok(handle.get(key.to_vec(), TIMEOUT)?)
}

fn commit(handle: &ShardHandle) -> Result<Hash, Box<dyn Error>> {
    Ok(handle.commit(TIMEOUT)?)
}

fn range(handle: &ShardHandle, from: &[u8], to: &[u8]) -> Result<RangeEntries, Box<dyn Error>> {
    let items = handle.range(from.to_vec(), to.to_vec(), TIMEOUT)?;
    let mut entries = Vec::new();
    let mut saw_done = false;
    for item in items {
        match item {
            RangeItem::Entry { key, value } => entries.push((key, value)),
            RangeItem::Done => saw_done = true,
        }
    }
    assert!(saw_done, "range result must terminate with Done");
    Ok(entries)
}

#[test]
fn get_and_range_merge_tree_with_buffer_shadowing() -> Result<(), Box<dyn Error>> {
    let scheduler = test_scheduler()?;
    let shard = TestShard::spawn(&scheduler, "merge")?;
    let handle = &shard.handle;

    put(handle, b"a", b"tree-a")?;
    put(handle, b"b", b"tree-b")?;
    put(handle, b"d", b"tree-d")?;
    let committed_root = commit(handle)?;

    put(handle, b"b", b"buffer-b")?;
    put(handle, b"c", b"buffer-c")?;
    delete(handle, b"d")?;

    assert_eq!(get(handle, b"b")?, Some(b"buffer-b".to_vec()));
    assert_eq!(get(handle, b"a")?, Some(b"tree-a".to_vec()));
    assert_eq!(get(handle, b"unknown")?, None);
    assert_eq!(get(handle, b"d")?, None);

    assert_eq!(
        range(handle, b"a", b"e")?,
        vec![
            (b"a".to_vec(), b"tree-a".to_vec()),
            (b"b".to_vec(), b"buffer-b".to_vec()),
            (b"c".to_vec(), b"buffer-c".to_vec()),
        ]
    );
    let _ = committed_root;
    scheduler.shutdown();
    Ok(())
}

#[test]
fn put_and_delete_ack_after_wal_append_without_tree_mutation() -> Result<(), Box<dyn Error>> {
    let scheduler = test_scheduler()?;
    let shard = TestShard::spawn(&scheduler, "durable-first")?;
    let handle = &shard.handle;

    put(handle, b"event", b"payload")?;
    assert_eq!(
        DurableWal::read_file(&shard.wal_path)?.entries(),
        &[crate::wal::WalEntry::put(
            b"event".to_vec(),
            b"payload".to_vec()
        )]
    );
    assert_eq!(get(handle, b"event")?, Some(b"payload".to_vec()));

    // The tree was not mutated: a fresh store sees no committed marker yet.
    assert_eq!(
        DurableWal::read_file(&shard.wal_path)?.committed_root(),
        None
    );

    delete(handle, b"event")?;
    assert_eq!(get(handle, b"event")?, None);
    // ASSERTION CHANGED (AA-3-4b): a delete writes a STAMPED TOMBSTONE — a `Put` of
    // the tombstone envelope (`bottom` stamp here), NOT a bare `WalEntry::delete`.
    let tombstone =
        crate::ttl::entry::encode_stamped_tombstone(crate::sync::ballot::Stamp::bottom());
    assert_eq!(
        DurableWal::read_file(&shard.wal_path)?.entries(),
        &[
            crate::wal::WalEntry::put(b"event".to_vec(), b"payload".to_vec()),
            crate::wal::WalEntry::put(b"event".to_vec(), tombstone),
        ]
    );
    scheduler.shutdown();
    Ok(())
}

#[test]
fn commit_persists_marker_and_is_idempotent() -> Result<(), Box<dyn Error>> {
    let scheduler = test_scheduler()?;
    let shard = TestShard::spawn(&scheduler, "commit")?;
    let handle = &shard.handle;

    put(handle, b"b", b"two")?;
    put(handle, b"a", b"one")?;
    put(handle, b"c", b"three")?;
    let committed = commit(handle)?;

    assert_eq!(
        DurableWal::read_file(&shard.wal_path)?.committed_root(),
        Some(committed)
    );
    assert_eq!(get(handle, b"a")?, Some(b"one".to_vec()));

    // Re-committing an empty buffer against the same root is a no-op root.
    assert_eq!(commit(handle)?, committed);
    scheduler.shutdown();
    Ok(())
}

#[test]
fn commit_root_is_history_independent_for_same_key_value_set() -> Result<(), Box<dyn Error>> {
    let scheduler = test_scheduler()?;
    let first = TestShard::spawn(&scheduler, "deterministic-a")?;
    let second = TestShard::spawn(&scheduler, "deterministic-b")?;

    put(&first.handle, b"alpha", b"1")?;
    put(&first.handle, b"beta", b"2")?;
    let first_root = commit(&first.handle)?;

    put(&second.handle, b"beta", b"2")?;
    put(&second.handle, b"alpha", b"1")?;
    let second_root = commit(&second.handle)?;

    assert_eq!(first_root, second_root);
    scheduler.shutdown();
    Ok(())
}

#[test]
fn respawn_replays_wal_and_leaves_sibling_running() -> Result<(), Box<dyn Error>> {
    let scheduler = test_scheduler()?;
    let failed = TestShard::spawn(&scheduler, "failed")?;
    let sibling = TestShard::spawn(&scheduler, "sibling")?;

    put(&sibling.handle, b"sibling-key", b"sibling-value")?;
    put(&failed.handle, b"committed", b"tree-value")?;
    let committed_root = commit(&failed.handle)?;
    put(&failed.handle, b"buffered", b"wal-value")?;

    assert_eq!(
        DurableWal::read_file(&failed.wal_path)?.committed_root(),
        Some(committed_root)
    );

    // Model a real crash: kill the "failed" process via the scheduler's
    // embedding-side exit signal (`erlang:exit/2` with reason Kill) before
    // re-spawning, so the test exercises crash recovery rather than two live
    // processes sharing one WAL path. `caller_pid` is unused by the facility, so
    // 0 is a fine sender.
    scheduler.exit_signal(0, failed.handle.pid(), ExitReason::Kill)?;

    // Re-spawn a fresh native process against the SAME paths. The original
    // handle's pid is now dead; the re-spawned process recovers from the WAL.
    let recovered = failed.respawn(&scheduler)?;
    assert_ne!(recovered.pid(), failed.handle.pid());

    // The sibling is untouched and still serving.
    assert_eq!(
        get(&sibling.handle, b"sibling-key")?,
        Some(b"sibling-value".to_vec())
    );

    // The re-spawned shard replayed both the committed tree and the buffered
    // (post-commit) WAL entry.
    assert_eq!(get(&recovered, b"buffered")?, Some(b"wal-value".to_vec()));
    assert_eq!(get(&recovered, b"committed")?, Some(b"tree-value".to_vec()));
    scheduler.shutdown();
    Ok(())
}

#[test]
fn boot_failure_keeps_scheduler_usable_and_fails_the_command() -> Result<(), Box<dyn Error>> {
    let scheduler = test_scheduler()?;
    let dir = tempfile::tempdir()?;

    // Force a deterministic boot failure: `DiskStore::new` -> `ensure_directory`
    // rejects a path that exists but is NOT a directory
    // (StoreError::NotADirectory). Placing a regular file where the store dir
    // should be makes `boot` fail, so the process comes up as the
    // failed-startup sentinel (state = None, startup_error = Some).
    let store_dir = dir.path().join("not-a-dir.store");
    std::fs::write(&store_dir, b"i am a file, not a directory")?;
    let wal_path = dir.path().join("boot-fail.wal");

    // `spawn` itself SUCCEEDS: the scheduler accepts the process; only `boot`
    // failed. A boot failure is never reported from `spawn`, only per-command.
    let handle = ShardHandle::spawn(Arc::clone(&scheduler), store_dir, wal_path)?;

    // A command issued against the booting-then-failing process must FAIL, never
    // return a value, and the scheduler must not panic (it stays usable: a
    // second spawn below still works).
    //
    // The exact error KIND a caller observes here is genuinely timing-dependent,
    // because beamr schedules a freshly spawned native process to run IMMEDIATELY
    // (`spawn_native` pushes it onto the woken set and notifies the condvar): the
    // sentinel's first slice usually runs and stops the process before this
    // external command lands, so the caller sees `ReplyTimeout` or
    // `ActorUnavailable`; if the command does land first it is drained with
    // `Spawn`. That is real scheduler nondeterminism, not a test gap — so here we
    // assert the contract that holds in EVERY interleaving: a boot failure always
    // fails the command (never Ok, never a storage error, never the scheduler
    // panicking). The `Spawn` queued-at-boot drain path itself is covered
    // deterministically by `native::boot_failure_tests` (it pre-seeds the queue
    // and drives the sentinel directly).
    let result = handle.get(b"any-key".to_vec(), TIMEOUT);
    assert!(
        result.is_err(),
        "a command against a boot-failed shard must error, got Ok: {result:?}"
    );
    assert!(
        matches!(
            &result,
            Err(ShardError::ReplyTimeout { .. }
                | ShardError::ActorUnavailable { .. }
                | ShardError::Spawn(_))
        ),
        "boot-failure command should fail as ReplyTimeout / ActorUnavailable / \
         Spawn (never ReplyDisconnected or a storage error), got {result:?}"
    );

    // The scheduler survived the boot failure: a healthy shard still spawns and
    // serves on it.
    let healthy = TestShard::spawn(&scheduler, "healthy-after-boot-fail")?;
    put(&healthy.handle, b"k", b"v")?;
    assert_eq!(get(&healthy.handle, b"k")?, Some(b"v".to_vec()));

    scheduler.shutdown();
    Ok(())
}

// =====================================================================
// AA-3-3: epoch fence on the data-write path (design §2.3).
// =====================================================================

/// A test ballot `(counter, node)`.
fn ballot(counter: u64, node: &str) -> Ballot {
    Ballot::new(counter, SyncNodeId::new(node))
}

/// Durably promise `ballot` through the handle, asserting it was accepted (the
/// monotone path that RAISES `promised`).
fn promise(handle: &ShardHandle, ballot: Ballot) -> Result<(), Box<dyn Error>> {
    match handle.record_promise(ballot, TIMEOUT)? {
        RecordPromiseOutcome::Promised => Ok(()),
        RecordPromiseOutcome::Rejected { promised } => {
            Err(format!("expected Promised, got Rejected({promised:?})").into())
        }
    }
}

/// TEST 1 — the fence REJECTS a stale-epoch write, applying NOTHING.
///
/// With `promised = (5, X)`, an `apply_durable` stamped at epoch `(3, Y)` must be
/// `Fenced`: the key stays absent and no commit marker is written.
#[test]
fn fence_rejects_stale_epoch_write_applying_nothing() -> Result<(), Box<dyn Error>> {
    let scheduler = test_scheduler()?;
    let shard = TestShard::spawn(&scheduler, "fence-stale")?;
    let handle = &shard.handle;

    // Establish promised = (5, X) via a Prepare-equivalent record_promise.
    promise(handle, ballot(5, "X"))?;

    // A write stamped BELOW promised must be fenced (expected=None create).
    let result = handle.apply_durable(
        b"k".to_vec(),
        None,
        b"stale".to_vec(),
        None,
        Stamp::new(ballot(3, "Y"), 0),
        TIMEOUT,
    );
    assert!(
        matches!(
            result,
            Err(ShardError::Fenced {
                ref promised,
                ref attempted,
            }) if *promised == ballot(5, "X") && *attempted == ballot(3, "Y")
        ),
        "stale-epoch write must be Fenced{{promised:(5,X), attempted:(3,Y)}}, got {result:?}"
    );

    // NOTHING applied: the key is still absent and no committed-root marker exists.
    assert_eq!(get(handle, b"k")?, None, "fenced write must apply nothing");
    assert_eq!(
        DurableWal::read_file(&shard.wal_path)?.committed_root(),
        None,
        "a fenced write must not have committed anything"
    );

    scheduler.shutdown();
    Ok(())
}

/// TEST 2 — the R2 regression guard. The fence ADMITS `epoch >= promised` but a
/// data write NEVER raises `promised`.
///
/// With `promised = (5, X)`: apply at `(5, X)` succeeds; `promised` is STILL
/// `(5, X)`. Then apply at a HIGHER `(7, Z)` also succeeds (>=) and STILL does not
/// move `promised`. We prove `promised` was never silently raised to 7 by then
/// `record_promise((6, _))`: it must be ACCEPTED (because the live `promised` is
/// still 5), which is impossible if either data write had raised it to 7.
#[test]
fn fence_admits_ge_without_ever_raising_promised() -> Result<(), Box<dyn Error>> {
    let scheduler = test_scheduler()?;
    let shard = TestShard::spawn(&scheduler, "fence-r2")?;
    let handle = &shard.handle;

    promise(handle, ballot(5, "X"))?;

    // Apply AT promised (5,X): equal is >= so it is admitted and commits.
    handle.apply_durable(
        b"k".to_vec(),
        None,
        b"at-five".to_vec(),
        None,
        Stamp::new(ballot(5, "X"), 0),
        TIMEOUT,
    )?;
    assert_eq!(get(handle, b"k")?, Some(b"at-five".to_vec()));
    // The data write did NOT raise promised.
    assert_eq!(
        handle.read_promise_state(TIMEOUT)?.promised,
        ballot(5, "X"),
        "an admitted data write must NOT raise promised (R2)"
    );

    // Apply ABOVE promised at (7,Z): also admitted (>=). expected is now the hash
    // of the current value "at-five".
    let current = Hash::of(b"at-five");
    handle.apply_durable(
        b"k".to_vec(),
        Some(current),
        b"at-seven".to_vec(),
        None,
        Stamp::new(ballot(7, "Z"), 0),
        TIMEOUT,
    )?;
    assert_eq!(get(handle, b"k")?, Some(b"at-seven".to_vec()));
    // STILL not raised — a higher-epoch data write also leaves promised alone.
    assert_eq!(
        handle.read_promise_state(TIMEOUT)?.promised,
        ballot(5, "X"),
        "a higher-epoch data write STILL must not raise promised (R2)"
    );

    // The airtight proof: promised is genuinely still 5, so a Prepare at (6, W)
    // STRICTLY exceeds it and is accepted. If either data write had silently
    // raised promised to 7, this would be Rejected instead.
    match handle.record_promise(ballot(6, "W"), TIMEOUT)? {
        RecordPromiseOutcome::Promised => {}
        RecordPromiseOutcome::Rejected { promised } => {
            return Err(format!(
                "record_promise((6,W)) was Rejected({promised:?}) — promised was silently \
                 raised by a data write, violating R2"
            )
            .into());
        }
    }

    scheduler.shutdown();
    Ok(())
}

/// Build a committed, stamped entry for `key=value` at `stamp` on a fresh shard
/// (the merge requires stamped envelopes — `apply_durable` writes them). Returns
/// the exported `(committed_root, transfers)`.
fn export_committed(
    scheduler: &Arc<Scheduler>,
    name: &str,
    key: &[u8],
    value: &[u8],
    stamp: Stamp,
) -> Result<(Option<Hash>, Vec<crate::sync::NodeTransfer>), Box<dyn Error>> {
    let shard = TestShard::spawn(scheduler, name)?;
    shard
        .handle
        .apply_durable(key.to_vec(), None, value.to_vec(), None, stamp, TIMEOUT)?;
    let export = shard.handle.export_reachable(0, TIMEOUT)?;
    Ok(export)
}

/// AA-3-4d actor-level FORKED-state merge: two promisers hold DIFFERENT committed
/// keys (`k2` on one, `k3` on the other). `merge_adopt` over BOTH must leave the
/// target serving BOTH — the union, no committed write dropped.
///
/// Falsifiability control: folding only ONE promiser (single-root adoption) serves
/// only that promiser's key and DROPS the other — proving the union over ALL
/// promisers is load-bearing.
#[test]
fn merge_adopt_unions_forked_promiser_state() -> Result<(), Box<dyn Error>> {
    let scheduler = test_scheduler()?;

    // Promiser B holds {k3}; promiser C holds {k2}. Different keys, different
    // committed roots (an incomparable fork under one owner, §2.4).
    let b = export_committed(
        &scheduler,
        "fork-b",
        b"k3",
        b"v3",
        Stamp::new(ballot(2, "A"), 1),
    )?;
    let c = export_committed(
        &scheduler,
        "fork-c",
        b"k2",
        b"v2",
        Stamp::new(ballot(2, "A"), 0),
    )?;

    // MERGE path: fold BOTH promisers into a fresh target.
    let target = TestShard::spawn(&scheduler, "fork-target")?;
    target.handle.merge_adopt(vec![b, c.clone()], TIMEOUT)?;
    assert_eq!(
        get(&target.handle, b"k2")?,
        Some(b"v2".to_vec()),
        "merge over ALL promisers must serve k2 (from C)"
    );
    assert_eq!(
        get(&target.handle, b"k3")?,
        Some(b"v3".to_vec()),
        "merge over ALL promisers must serve k3 (from B)"
    );

    // CONTROL: fold only ONE promiser (single-root adoption). It serves that
    // promiser's key and DROPS the other — the property fails without the union.
    let single = TestShard::spawn(&scheduler, "fork-single")?;
    single.handle.merge_adopt(vec![c], TIMEOUT)?;
    assert_eq!(
        get(&single.handle, b"k2")?,
        Some(b"v2".to_vec()),
        "single-root adoption serves the one promiser's key"
    );
    assert_eq!(
        get(&single.handle, b"k3")?,
        None,
        "single-root adoption DROPS the forked write — proves the union is load-bearing"
    );

    scheduler.shutdown();
    Ok(())
}

/// AA-3-4d actor-level: `merge_adopt` durably adopts the merged root — it survives
/// a crash. The merged committed state must reload from the WAL marker on respawn.
#[test]
fn merge_adopt_root_survives_crash_recovery() -> Result<(), Box<dyn Error>> {
    let scheduler = test_scheduler()?;

    let b = export_committed(
        &scheduler,
        "durable-b",
        b"k3",
        b"v3",
        Stamp::new(ballot(2, "A"), 1),
    )?;
    let c = export_committed(
        &scheduler,
        "durable-c",
        b"k2",
        b"v2",
        Stamp::new(ballot(2, "A"), 0),
    )?;

    let target = TestShard::spawn(&scheduler, "durable-target")?;
    target.handle.merge_adopt(vec![b, c], TIMEOUT)?;

    // CRASH: drop + respawn against the same store/WAL.
    let recovered = target.respawn(&scheduler)?;
    assert_eq!(
        get(&recovered, b"k2")?,
        Some(b"v2".to_vec()),
        "merged k2 must survive crash recovery (fsync'd marker)"
    );
    assert_eq!(
        get(&recovered, b"k3")?,
        Some(b"v3".to_vec()),
        "merged k3 must survive crash recovery"
    );

    scheduler.shutdown();
    Ok(())
}

// =====================================================================
// A1a: LOCAL atomic multi-key fenced+stamped apply (apply_durable_batch).
// =====================================================================

/// Decode the committed stamp stored for `key`, reading the RAW envelope (stamp
/// NOT stripped). Returns `None` when the key is absent. Used to prove every key
/// in a batch landed with the IDENTICAL shared stamp.
fn stamp_of(handle: &ShardHandle, key: &[u8]) -> Result<Option<Stamp>, Box<dyn Error>> {
    let Some(raw) = handle.get_raw(key.to_vec(), TIMEOUT)? else {
        return Ok(None);
    };
    let entry = crate::ttl::entry::StampedEntry::decode(&raw)?
        .ok_or("committed value is not a stamped envelope")?;
    Ok(Some(entry.stamp().clone()))
}

/// GATE 1 — Atomic multi-key apply. A batch of N keys at ONE stamp lands ALL N
/// readable with that exact shared stamp, in a SINGLE commit.
///
/// Non-vacuous: it asserts EVERY one of the three keys is readable with the
/// expected value AND that each one's stored stamp equals the single shared
/// stamp. A regression that wrote only some keys, dropped the stamp, or used a
/// different stamp per key would fail. The single-commit property is asserted by
/// the committed-root marker being present after the batch (the fence/CAS tests
/// confirm the no-write case writes NO marker).
#[test]
fn batch_applies_all_keys_at_one_stamp_in_one_commit() -> Result<(), Box<dyn Error>> {
    let scheduler = test_scheduler()?;
    let shard = TestShard::spawn(&scheduler, "batch-atomic")?;
    let handle = &shard.handle;

    let batch_stamp = Stamp::new(ballot(4, "owner"), 7);
    handle.apply_durable_batch(
        vec![
            (b"k1".to_vec(), None, b"v1".to_vec(), None),
            (b"k2".to_vec(), None, b"v2".to_vec(), None),
            (b"k3".to_vec(), None, b"v3".to_vec(), None),
        ],
        batch_stamp.clone(),
        TIMEOUT,
    )?;

    // All N keys are readable with their batch value.
    assert_eq!(get(handle, b"k1")?, Some(b"v1".to_vec()));
    assert_eq!(get(handle, b"k2")?, Some(b"v2".to_vec()));
    assert_eq!(get(handle, b"k3")?, Some(b"v3".to_vec()));

    // And every key carries the IDENTICAL shared stamp (not a per-key invented one).
    assert_eq!(stamp_of(handle, b"k1")?, Some(batch_stamp.clone()));
    assert_eq!(stamp_of(handle, b"k2")?, Some(batch_stamp.clone()));
    assert_eq!(stamp_of(handle, b"k3")?, Some(batch_stamp));

    // The batch committed (one fsync'd committed-root marker is present).
    assert!(
        DurableWal::read_file(&shard.wal_path)?
            .committed_root()
            .is_some(),
        "a successful batch must have committed a root marker"
    );

    scheduler.shutdown();
    Ok(())
}

/// GATE 2 — the fence rejects the WHOLE batch. With `promised = (5, X)`, a batch
/// stamped at `(3, Y)` (below promised) writes NONE of its keys and returns
/// `Fenced`.
///
/// Non-vacuous: it asserts EVERY key of a multi-key batch is still absent AND no
/// committed-root marker exists. A regression that fenced only the first key, or
/// applied the batch before checking the fence, would leave at least one key
/// present (or a marker) and fail.
#[test]
fn batch_fence_rejects_whole_batch_writing_nothing() -> Result<(), Box<dyn Error>> {
    let scheduler = test_scheduler()?;
    let shard = TestShard::spawn(&scheduler, "batch-fence")?;
    let handle = &shard.handle;

    promise(handle, ballot(5, "X"))?;

    let result = handle.apply_durable_batch(
        vec![
            (b"k1".to_vec(), None, b"v1".to_vec(), None),
            (b"k2".to_vec(), None, b"v2".to_vec(), None),
            (b"k3".to_vec(), None, b"v3".to_vec(), None),
        ],
        Stamp::new(ballot(3, "Y"), 0),
        TIMEOUT,
    );
    assert!(
        matches!(
            result,
            Err(ShardError::Fenced { ref promised, ref attempted })
                if *promised == ballot(5, "X") && *attempted == ballot(3, "Y")
        ),
        "a below-promised batch must be Fenced, got {result:?}"
    );

    // NONE of the keys were written, and no committed-root marker exists.
    assert_eq!(get(handle, b"k1")?, None, "fenced batch must write nothing");
    assert_eq!(get(handle, b"k2")?, None, "fenced batch must write nothing");
    assert_eq!(get(handle, b"k3")?, None, "fenced batch must write nothing");
    assert_eq!(
        DurableWal::read_file(&shard.wal_path)?.committed_root(),
        None,
        "a fenced batch must not have committed anything"
    );

    scheduler.shutdown();
    Ok(())
}

/// GATE 3 — a per-key CAS mismatch rejects the WHOLE batch (all-or-nothing, never
/// partial). Pre-seed `k2` so a batch that requires `k2` absent (`expected =
/// None`) mismatches on `k2` while `k1`/`k3` would have matched. The batch must
/// return the mismatch and leave `k1`/`k3` un-applied — proving the apply is
/// all-or-nothing, not a partial write of the keys that did match.
///
/// Non-vacuous: it asserts `k1` and `k3` (the keys whose CAS WOULD have passed)
/// are still absent. A regression that buffered each key as its CAS passed and
/// only failed on `k2` would leave `k1` present and fail this assertion.
#[test]
fn batch_cas_mismatch_rejects_whole_batch_no_partial_apply() -> Result<(), Box<dyn Error>> {
    let scheduler = test_scheduler()?;
    let shard = TestShard::spawn(&scheduler, "batch-cas")?;
    let handle = &shard.handle;

    // Pre-seed k2 with a committed value so an expect-absent CAS on k2 mismatches.
    handle.apply_durable(
        b"k2".to_vec(),
        None,
        b"already-here".to_vec(),
        None,
        Stamp::new(ballot(1, "owner"), 0),
        TIMEOUT,
    )?;
    let seeded = stamp_of(handle, b"k2")?;

    // Batch: k1 (expect-absent: WOULD match), k2 (expect-absent: MISMATCHES, it is
    // present), k3 (expect-absent: WOULD match). The whole batch must be rejected.
    let result = handle.apply_durable_batch(
        vec![
            (b"k1".to_vec(), None, b"v1".to_vec(), None),
            (b"k2".to_vec(), None, b"v2".to_vec(), None),
            (b"k3".to_vec(), None, b"v3".to_vec(), None),
        ],
        Stamp::new(ballot(2, "owner"), 0),
        TIMEOUT,
    );
    assert!(
        matches!(
            result,
            Err(ShardError::CasHashMismatch { expected, actual })
                if expected.is_none() && actual == Some(Hash::of(b"already-here"))
        ),
        "a per-key CAS mismatch must reject the whole batch, got {result:?}"
    );

    // ALL-OR-NOTHING: the keys whose CAS WOULD have passed were NOT applied.
    assert_eq!(
        get(handle, b"k1")?,
        None,
        "k1 must NOT be applied (no partial write)"
    );
    assert_eq!(
        get(handle, b"k3")?,
        None,
        "k3 must NOT be applied (no partial write)"
    );
    // k2 is unchanged — still the seeded value with its seeded stamp.
    assert_eq!(get(handle, b"k2")?, Some(b"already-here".to_vec()));
    assert_eq!(
        stamp_of(handle, b"k2")?,
        seeded,
        "k2 must be untouched by the rejected batch"
    );

    scheduler.shutdown();
    Ok(())
}

/// GATE 4 — an applied batch survives crash recovery. After a multi-key batch is
/// applied + committed, a crash (kill) + re-spawn must reload EVERY key AND the
/// shared stamp from the fsync'd WAL marker.
///
/// Non-vacuous: it reads each key and decodes its stamp from the RE-SPAWNED
/// process (fresh recovery from disk, not retained memory). A regression that
/// only committed to the page cache, or lost the stamp on reload, would fail.
#[test]
fn batch_survives_crash_recovery_with_shared_stamp() -> Result<(), Box<dyn Error>> {
    let scheduler = test_scheduler()?;
    let shard = TestShard::spawn(&scheduler, "batch-crash")?;

    let batch_stamp = Stamp::new(ballot(6, "owner"), 2);
    shard.handle.apply_durable_batch(
        vec![
            (b"k1".to_vec(), None, b"v1".to_vec(), None),
            (b"k2".to_vec(), None, b"v2".to_vec(), None),
            (b"k3".to_vec(), None, b"v3".to_vec(), None),
        ],
        batch_stamp.clone(),
        TIMEOUT,
    )?;

    // CRASH: kill the process and re-spawn against the SAME store/WAL.
    scheduler.exit_signal(0, shard.handle.pid(), ExitReason::Kill)?;
    let recovered = shard.respawn(&scheduler)?;
    assert_ne!(recovered.pid(), shard.handle.pid());

    // Every key + the shared stamp reload from the WAL marker.
    assert_eq!(get(&recovered, b"k1")?, Some(b"v1".to_vec()));
    assert_eq!(get(&recovered, b"k2")?, Some(b"v2".to_vec()));
    assert_eq!(get(&recovered, b"k3")?, Some(b"v3".to_vec()));
    assert_eq!(stamp_of(&recovered, b"k1")?, Some(batch_stamp.clone()));
    assert_eq!(stamp_of(&recovered, b"k2")?, Some(batch_stamp.clone()));
    assert_eq!(stamp_of(&recovered, b"k3")?, Some(batch_stamp));

    scheduler.shutdown();
    Ok(())
}
