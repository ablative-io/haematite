// API-003: sweep actor tests.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beamr::module::ModuleRegistry;
use beamr::scheduler::{Scheduler, SchedulerConfig};

use super::recover::{collect_tree, recover_view};
use super::{SweepError, SweepHandle, SweepStats};
use crate::shard::actor::ShardHandle;
use crate::store::DiskStore;
use crate::tree::{LeafNode, Node};
use crate::wal::Mutation;

const TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn sweep_stats_default_is_zero() {
    assert_eq!(SweepStats::default().scanned, 0);
    assert_eq!(SweepStats::default().expired, 0);
    assert_eq!(SweepStats::default().deleted, 0);
}

/// Reconstruct the shard's PHYSICAL state from its on-disk store + WAL, exactly
/// as `run_sweep`'s `recover_view` does, and report whether `key` physically
/// resolves to a live entry.
///
/// Crucially this performs NO TTL evaluation: it does not call
/// `is_expired_at`/`visible_value`. It answers only "are these bytes still in
/// the merged store+WAL?", so it is a pure physical-presence probe that
/// read-time expiry filtering can never satisfy.
fn physically_present(store_dir: &Path, wal_path: &Path, key: &[u8]) -> Result<bool, SweepError> {
    let (store, root, buffer) = recover_view(store_dir, wal_path)?;
    let mut merged = BTreeMap::new();
    if let Some(root) = root {
        collect_tree(&store, root, &mut merged)?;
    }
    for mutation in &buffer {
        match mutation {
            Mutation::Put { key, value } => {
                merged.insert(key.clone(), value.clone());
            }
            Mutation::Delete { key } => {
                merged.remove(key);
            }
        }
    }
    Ok(merged.contains_key(key))
}

/// FALSIFIABLE: the periodic self-tick must PHYSICALLY remove an expired entry
/// from the shard's store/WAL, NOT merely read-filter it.
///
/// `physically_present` reconstructs the raw merged store+WAL with no TTL
/// evaluation at all, so this assertion cannot be satisfied by read-time
/// filtering (which leaves the bytes in place and only hides them on read). The
/// entry physically disappears ONLY when a real sweep pass issues a
/// `shard.delete`, writing a `Delete` mutation that shadows the entry.
///
/// What makes it falsifiable: the test NEVER calls `sweep_once`. The only thing
/// that can run a sweep here is the actor's own delayed self-tick armed by
/// `schedule_next_tick`. Revert the wiring — drop the first-slice arm, or turn
/// `ctx.schedule` back into the old immediate `ctx.send`, or stop re-arming
/// after each tick — and no `tick` atom is ever delivered on the interval,
/// `run_sweep` never runs, the entry stays physically present, and the bounded
/// wait below expires with `physically_present` still true: the final assertion
/// fails. (Confirmed: removing the wiring makes this fail.)
///
/// A multi-thread scheduler is required: `run_sweep` blocks waiting for the
/// shard actor's `delete` reply, so the shard must run on a sibling thread.
#[test]
fn periodic_tick_physically_deletes_expired_entry() -> Result<(), Box<dyn std::error::Error>> {
    let scheduler = Arc::new(Scheduler::new(
        SchedulerConfig::default(),
        Arc::new(ModuleRegistry::new()),
    )?);

    let dir = tempfile::tempdir()?;
    let store_dir = dir.path().join("sweep.store");
    let wal_path = dir.path().join("sweep.wal");
    // Seed an empty committed root so the store dir exists with a tree.
    let mut store = DiskStore::new(&store_dir)?;
    let _root = store.put(&Node::Leaf(LeafNode::new(Vec::new())?))?;
    drop(store);

    let shard = ShardHandle::spawn(Arc::clone(&scheduler), store_dir.clone(), wal_path.clone())?;

    // Write an entry with a short, non-zero TTL so it is live (and therefore
    // physically present and NOT yet a sweep target) at the instant we record
    // the precondition, then expires shortly after.
    let key = b"ttl-victim".to_vec();
    shard.put_with_ttl(
        key.clone(),
        b"doomed".to_vec(),
        Some(Duration::from_millis(100)),
        TIMEOUT,
    )?;

    // Precondition: the entry is PHYSICALLY present in the raw store+WAL.
    assert!(
        physically_present(&store_dir, &wal_path, &key)?,
        "entry must be physically present before any sweep runs"
    );

    // Small explicit interval (no implicit default — R4) so the periodic tick
    // fires quickly. Spawned AFTER the precondition check so the sweep cannot
    // race ahead of it.
    let interval = Duration::from_millis(20);
    let sweep = SweepHandle::spawn(
        Arc::clone(&scheduler),
        store_dir.clone(),
        wal_path.clone(),
        shard,
        interval,
        TIMEOUT,
    )?;

    // Drive the scheduler by waiting (bounded) for the self-scheduled tick to
    // fire and physically delete the entry. No manual sweep is triggered.
    let deadline = Instant::now() + Duration::from_secs(10);
    while physically_present(&store_dir, &wal_path, &key)? && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }

    assert!(
        !physically_present(&store_dir, &wal_path, &key)?,
        "periodic sweep must physically remove the expired entry from the store/WAL"
    );

    assert!(sweep.shutdown(TIMEOUT).is_ok());
    scheduler.shutdown();
    Ok(())
}
