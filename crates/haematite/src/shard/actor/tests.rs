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

use super::handle::{RangeItem, ShardError, ShardHandle};
use crate::store::DiskStore;
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
    handle.delete(key.to_vec(), TIMEOUT)?;
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
    assert_eq!(
        DurableWal::read_file(&shard.wal_path)?.entries(),
        &[
            crate::wal::WalEntry::put(b"event".to_vec(), b"payload".to_vec()),
            crate::wal::WalEntry::delete(b"event".to_vec()),
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
    // The exact error KIND is timing-dependent and NOT deterministically the
    // `ShardError::Spawn` documented for a *queued-at-boot* command. The
    // `Spawn`-draining path in `ShardNativeHandler::fail_startup` only fires for
    // a command already on the queue when the sentinel runs its first slice. But
    // beamr schedules a freshly spawned native process to run IMMEDIATELY
    // (`spawn_native` pushes it onto the woken set and notifies the condvar), so
    // the sentinel's first slice runs with an empty queue and stops the process
    // BEFORE any externally-issued command can be enqueued. From the public
    // `ShardHandle` API there is no seam to pre-load a command onto the queue
    // ahead of that first slice, so the `Spawn` path is not deterministically
    // reachable from outside. In practice this caller observes `ReplyTimeout`
    // (the wake is accepted against the just-stopped pid but no further slice
    // drains the command). We therefore assert the contract that IS
    // deterministic: a boot failure never yields a successful command.
    //
    // TODO(CORE-008 / beamr seam): once a router/supervisor owns spawn and can
    // enqueue a command before the process's first slice (or beamr exposes a
    // "spawn suspended" mode), add a test that deterministically exercises the
    // `ShardError::Spawn` queued-at-boot drain path in `fail_startup`.
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
