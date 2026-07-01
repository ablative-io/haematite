//! Lazy shard-router tests.
//!
//! GATE 2 — atomic spawn-on-miss. Two (or many) concurrent first-touchers of the
//! SAME cold shard must never spawn two actors/WALs over one directory (a WAL-
//! corrupting race). These drive the REAL router against a REAL beamr scheduler.
//!
//! Following the crate's test convention, every case returns `Result` and
//! propagates with `?` (no `unwrap`/`expect`), so no clippy allow is needed.

use std::collections::BTreeSet;
use std::error::Error;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use beamr::module::ModuleRegistry;
use beamr::scheduler::{Scheduler, SchedulerConfig};

use super::{ShardMode, ShardRouter};

const TIMEOUT: Duration = Duration::from_secs(5);

fn scheduler() -> Result<Arc<Scheduler>, Box<dyn Error>> {
    let scheduler = Scheduler::new(SchedulerConfig::default(), Arc::new(ModuleRegistry::new()))
        .map_err(|message| -> Box<dyn Error> { message.into() })?;
    Ok(Arc::new(scheduler))
}

fn build_router(dir: &std::path::Path, shard_count: usize) -> Result<ShardRouter, Box<dyn Error>> {
    ShardRouter::new(scheduler()?, dir, shard_count, ShardMode::Create, None)
        .ok_or_else(|| "non-zero shard_count".into())
}

#[test]
fn nothing_is_materialised_until_first_touch() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let router = build_router(dir.path(), 4096)?;

    // A 4096-shard router costs nothing up front: no shard is materialised.
    assert_eq!(router.materialised_shard_ids(), Vec::<usize>::new());

    // One touch materialises exactly one shard.
    let first = router.handle_for_shard(7).map_err(|error| error.message)?;
    assert_eq!(router.materialised_shard_ids(), vec![7]);

    // Re-touching the SAME shard returns the SAME actor (no re-spawn).
    let again = router.handle_for_shard(7).map_err(|error| error.message)?;
    assert_eq!(first.pid(), again.pid());
    assert_eq!(router.materialised_shard_ids(), vec![7]);

    router.shutdown_all(TIMEOUT);
    Ok(())
}

#[test]
fn out_of_range_shard_id_is_rejected() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let router = build_router(dir.path(), 4)?;
    let error = router
        .handle_for_shard(4)
        .err()
        .ok_or("id 4 must be rejected for a 4-shard router")?;
    assert_eq!(error.shard_id, 4);
    router.shutdown_all(TIMEOUT);
    Ok(())
}

/// GATE 2: many threads racing to first-touch the SAME cold shard must all get
/// ONE actor (one pid) — the double-checked spawn under the map lock guarantees
/// no two actors/WALs are ever created over the one shard directory.
#[test]
fn concurrent_first_touch_of_one_cold_shard_spawns_exactly_one_actor() -> Result<(), Box<dyn Error>>
{
    const THREADS: usize = 24;

    let dir = tempfile::tempdir()?;
    let router = Arc::new(build_router(dir.path(), 16)?);

    let barrier = Arc::new(Barrier::new(THREADS));
    let mut joins = Vec::with_capacity(THREADS);
    for _ in 0..THREADS {
        let router = Arc::clone(&router);
        let barrier = Arc::clone(&barrier);
        joins.push(thread::spawn(move || -> Result<u64, String> {
            // Release all threads at once to maximise the race on shard 3.
            barrier.wait();
            router
                .handle_for_shard(3)
                .map(|handle| handle.pid())
                .map_err(|error| error.message)
        }));
    }

    let mut pids: BTreeSet<u64> = BTreeSet::new();
    for join in joins {
        let pid = join.join().map_err(|_| "worker thread panicked")??;
        pids.insert(pid);
    }

    // Every racing thread observed the SAME single actor: no double-spawn.
    assert_eq!(
        pids.len(),
        1,
        "cold shard must spawn exactly one actor, saw {pids:?}"
    );
    // And exactly one shard is materialised in the map.
    assert_eq!(router.materialised_shard_ids(), vec![3]);

    router.shutdown_all(TIMEOUT);
    Ok(())
}

/// GATE 2 (durability): concurrent WRITES to one cold shard must all land — proof
/// the single spawned actor's WAL is intact (a double-spawn would fork the WAL
/// and lose writes). Each thread first-touches shard 3 through the router, then
/// writes a distinct key; after commit every key must read back.
#[test]
fn concurrent_writes_to_one_cold_shard_all_persist() -> Result<(), Box<dyn Error>> {
    const THREADS: usize = 16;

    let dir = tempfile::tempdir()?;
    let router = Arc::new(build_router(dir.path(), 16)?);

    let barrier = Arc::new(Barrier::new(THREADS));
    let mut joins = Vec::with_capacity(THREADS);
    for worker in 0..THREADS {
        let router = Arc::clone(&router);
        let barrier = Arc::clone(&barrier);
        joins.push(thread::spawn(move || -> Result<Vec<u8>, String> {
            barrier.wait();
            let handle = router.handle_for_shard(3).map_err(|error| error.message)?;
            let key = format!("k{worker:02}").into_bytes();
            handle
                .put(key.clone(), worker.to_le_bytes().to_vec(), TIMEOUT)
                .map_err(|error| error.to_string())?;
            Ok(key)
        }));
    }

    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(THREADS);
    for join in joins {
        keys.push(join.join().map_err(|_| "worker thread panicked")??);
    }

    let handle = router.handle_for_shard(3).map_err(|error| error.message)?;
    handle.commit(TIMEOUT)?;
    for (worker, key) in keys.iter().enumerate() {
        let value = handle.get(key.clone(), TIMEOUT)?;
        assert_eq!(
            value,
            Some(worker.to_le_bytes().to_vec()),
            "every concurrent write to the cold shard must persist"
        );
    }

    router.shutdown_all(TIMEOUT);
    Ok(())
}
