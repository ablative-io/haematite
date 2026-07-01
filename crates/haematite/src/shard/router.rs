// CORE-008/CORE-009: Shard router — stable hash-based key-to-shard mapping.
//
// LAZY SHARD MATERIALISATION: the router no longer owns a dense
// `Vec<ShardHandle>` of every shard. Routing is still `BLAKE3(key) % shard_count`
// over a FIXED `shard_count` modulus base, but a shard's actor (and its per-shard
// TTL sweep) is spawned ON FIRST TOUCH and cached in a sparse interior-mutable
// map. Boot cost becomes O(shards actually used), not O(shard_count), so a very
// high `shard_count` is ~free until the shards are exercised.
//
// The three load-bearing gates (see docs/design/ELASTIC-RESHARDING.md §5.2):
//  * GATE 1 (empty-root synthesis) lives in the commit path (`api/kv.rs`): an
//    un-materialised shard contributes `tree::empty_root_hash()`.
//  * GATE 2 (atomic spawn-on-miss) is CLOSED HERE: materialisation is
//    double-checked under the map lock, so two concurrent writers to one cold
//    shard can never spawn two actors/WALs over the same directory (a WAL-
//    corrupting race).
//  * GATE 3 (acquire/recover-before-serve) falls out of materialisation running
//    the shard's normal boot — `ShardHandle::spawn` opens the store and RECOVERS
//    the durable WAL/promise state before the handle is usable — so a cold shard
//    recovers its on-disk `promised`/`owner_epoch` BEFORE any caller (including
//    `acquire_shard`) reads promise state to mint a ballot.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beamr::scheduler::Scheduler;

use crate::shard::actor::ShardHandle;
use crate::ttl::sweep::SweepHandle;

const SHARD_STORE_DIR: &str = "store";
const SHARD_WAL_FILE: &str = "shard.wal";

/// How a router materialises a shard directory on first touch: `Create` makes the
/// directory (a fresh DB), `Open` requires it to already exist (an existing DB).
///
/// This mirrors the old boot-time distinction, but is now applied PER SHARD at
/// first touch rather than to all `shard_count` shards up front.
#[derive(Clone, Copy, Debug)]
pub enum ShardMode {
    Create,
    Open,
}

/// Per-shard TTL sweep configuration captured at router construction, so a
/// lazily-materialised shard gets the SAME sweep a boot-time shard would.
#[derive(Clone, Debug)]
pub struct SweepConfig {
    pub interval: Duration,
    pub command_timeout: Duration,
}

/// A materialised shard: its live actor handle plus the optional per-shard sweep
/// supervisor spawned alongside it.
#[derive(Debug)]
struct MaterialisedShard {
    handle: ShardHandle,
    sweep: Option<SweepHandle>,
}

/// Everything the router needs to spawn a shard (and its sweep) on first touch.
struct SpawnContext {
    scheduler: Arc<Scheduler>,
    data_dir: PathBuf,
    mode: ShardMode,
    sweep: Option<SweepConfig>,
}

impl std::fmt::Debug for SpawnContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SpawnContext")
            .field("data_dir", &self.data_dir)
            .field("mode", &self.mode)
            .field("sweep", &self.sweep)
            .finish_non_exhaustive()
    }
}

/// The shared, interior-mutable map of materialised shards. Shared (via `Arc`)
/// with the [`MaterialisedMembership`] view the sync scheduler queries each tick.
type MaterialisedMap = Arc<Mutex<BTreeMap<usize, MaterialisedShard>>>;

/// Private database router: a FIXED `shard_count` modulus base plus a sparse,
/// interior-mutable map of the shards actually materialised so far.
#[derive(Debug)]
pub struct ShardRouter {
    shard_count: usize,
    materialised: MaterialisedMap,
    context: SpawnContext,
}

/// A cloneable, read-only membership view over the router's materialised shard
/// set — the seam the sync scheduler uses to sync ONLY materialised shards
/// (`crate::sync::scheduler::SyncShardSource`). Holds the SAME map the router
/// mutates, so it always reflects the current materialised set.
#[derive(Clone, Debug)]
pub struct MaterialisedMembership {
    materialised: MaterialisedMap,
}

impl MaterialisedMembership {
    /// The shard ids materialised so far, ascending.
    pub(crate) fn shard_ids(&self) -> Vec<usize> {
        self.materialised
            .lock()
            .map(|map| map.keys().copied().collect())
            .unwrap_or_default()
    }
}

impl crate::sync::scheduler::SyncShardSource for MaterialisedMembership {
    fn shards_to_sync(&self) -> Vec<usize> {
        self.shard_ids()
    }
}

/// A spawn failure for a shard the router tried to materialise on first touch.
#[derive(Debug)]
pub struct MaterialiseError {
    pub shard_id: usize,
    pub message: String,
}

impl ShardRouter {
    /// Build a router over a fixed `shard_count` modulus base. No shard actor is
    /// spawned here — every shard is materialised on first touch.
    ///
    /// Returns `None` for a zero `shard_count` (there would be no shard to route
    /// any key to), preserving the old `ShardRouter::new` non-empty invariant.
    pub(crate) fn new(
        scheduler: Arc<Scheduler>,
        data_dir: &Path,
        shard_count: usize,
        mode: ShardMode,
        sweep: Option<SweepConfig>,
    ) -> Option<Self> {
        if shard_count == 0 {
            return None;
        }
        Some(Self {
            shard_count,
            materialised: Arc::new(Mutex::new(BTreeMap::new())),
            context: SpawnContext {
                scheduler,
                data_dir: data_dir.to_path_buf(),
                mode,
                sweep,
            },
        })
    }

    /// A cloneable membership view over the materialised set, for the sync
    /// scheduler's lazy shard source.
    pub(crate) fn membership(&self) -> MaterialisedMembership {
        MaterialisedMembership {
            materialised: Arc::clone(&self.materialised),
        }
    }

    /// The shard index that owns `key`: `BLAKE3(key)[..8] % shard_count`.
    pub(crate) fn shard_for(&self, key: &[u8]) -> usize {
        let digest = blake3::hash(key);
        let mut prefix = [0_u8; 8];
        for (target, source) in prefix.iter_mut().zip(digest.as_bytes().iter()) {
            *target = *source;
        }
        let value = u64::from_be_bytes(prefix);
        (value % self.shard_count as u64) as usize
    }

    /// Materialise-on-miss the shard owning `key` and return a handle clone.
    pub(crate) fn handle_for(&self, key: &[u8]) -> Result<ShardHandle, MaterialiseError> {
        self.handle_for_shard(self.shard_for(key))
    }

    /// Route directly to a shard by its index, materialising it on first touch.
    ///
    /// A `Prepare`/`acquire_shard` carries the target shard index (not a key), so
    /// the acceptor selects the owning shard by id. Materialisation runs the
    /// shard's normal boot (store open + durable WAL/promise recovery), so a cold
    /// shard recovers its on-disk state BEFORE the returned handle serves any
    /// command — GATE 3 (acquire/recover-before-serve).
    pub(crate) fn handle_for_shard(
        &self,
        shard_id: usize,
    ) -> Result<ShardHandle, MaterialiseError> {
        if shard_id >= self.shard_count {
            return Err(MaterialiseError {
                shard_id,
                message: format!(
                    "shard id {shard_id} out of range for shard_count {}",
                    self.shard_count
                ),
            });
        }
        self.materialise(shard_id)
    }

    /// GATE 2 — atomic spawn-on-miss under the map lock (double-checked).
    ///
    /// The lock is held across the presence check AND the spawn+insert, so two
    /// concurrent first-touchers of the same cold shard cannot both spawn: the
    /// loser observes the winner's entry after re-acquiring the lock. Because the
    /// whole spawn happens under the lock, no two actors/WALs are ever created
    /// over the same shard directory (which would corrupt the WAL).
    fn materialise(&self, shard_id: usize) -> Result<ShardHandle, MaterialiseError> {
        // The map guard is held across the presence check AND the spawn+insert —
        // this is load-bearing (GATE 2), NOT an oversight: dropping it earlier
        // would reopen the double-spawn race. `handle` is computed, then the guard
        // is dropped explicitly before returning so the significant-drop lint is
        // satisfied without narrowing the critical section.
        let mut map = self.materialised.lock().map_err(|_| poisoned(shard_id))?;
        let handle = if let Some(existing) = map.get(&shard_id) {
            existing.handle.clone()
        } else {
            let shard = self.context.spawn_shard(shard_id)?;
            let handle = shard.handle.clone();
            map.insert(shard_id, shard);
            handle
        };
        drop(map);
        Ok(handle)
    }

    /// Handles for every shard MATERIALISED so far, in ascending shard-id order.
    ///
    /// This is the lazy replacement for the old dense `handles_in_order`: an
    /// un-materialised shard holds no data (it would commit to the empty root),
    /// so cross-shard fan-outs that only need to VISIT shards with data (scans,
    /// shutdown, sweep bookkeeping) iterate exactly the materialised set. The
    /// commit path does NOT use this — it must synthesise the empty root for
    /// un-materialised slots (GATE 1) and so iterates `0..shard_count` instead.
    pub(crate) fn materialised_handles(&self) -> Vec<ShardHandle> {
        self.materialised
            .lock()
            .map(|map| map.values().map(|shard| shard.handle.clone()).collect())
            .unwrap_or_default()
    }

    /// The shard ids materialised so far, ascending. A thin projection of
    /// [`Self::materialised_snapshot`] used by tests to assert exactly which
    /// shards a workload touched.
    #[cfg(test)]
    pub(crate) fn materialised_shard_ids(&self) -> Vec<usize> {
        self.materialised
            .lock()
            .map(|map| map.keys().copied().collect())
            .unwrap_or_default()
    }

    /// A single consistent snapshot of the materialised shards under one lock:
    /// their ids and their handles, index-aligned (`ids[i]` owns `handles[i]`),
    /// both in ascending shard-id order. The commit path needs id AND handle from
    /// the SAME snapshot so a concurrent first-touch can never desynchronise them.
    pub(crate) fn materialised_snapshot(&self) -> (Vec<usize>, Vec<ShardHandle>) {
        self.materialised
            .lock()
            .map(|map| {
                let ids = map.keys().copied().collect();
                let handles = map.values().map(|shard| shard.handle.clone()).collect();
                (ids, handles)
            })
            .unwrap_or_default()
    }

    /// Shut down every materialised shard's sweep and actor. Idempotent-ish: a
    /// second call finds an empty map. Used by `Database::drop` and by the
    /// startup rollback path.
    pub(crate) fn shutdown_all(&self, timeout: Duration) {
        let drained: Vec<MaterialisedShard> = match self.materialised.lock() {
            Ok(mut map) => std::mem::take(&mut *map).into_values().collect(),
            Err(_) => return,
        };
        for shard in drained {
            if let Some(sweep) = shard.sweep
                && let Err(error) = sweep.shutdown(timeout)
            {
                log::debug!(
                    "router sweep shutdown skipped for supervisor pid {}: {error}",
                    sweep.supervisor_pid()
                );
            }
            if let Err(error) = shard.handle.shutdown(timeout) {
                log::debug!(
                    "router shard shutdown skipped for pid {}: {error}",
                    shard.handle.pid()
                );
            }
        }
    }
}

impl SpawnContext {
    fn spawn_shard(&self, shard_id: usize) -> Result<MaterialisedShard, MaterialiseError> {
        let shard_dir = shard_dir(&self.data_dir, shard_id);
        // Both modes create the directory on first touch. `Open` of a shard
        // directory that was never materialised is legitimate under lazy
        // materialisation — it simply held no data on the prior run, so its tree
        // is empty and its committed root is the synthesised empty root. This is
        // the one intended relaxation of the old "validate every shard dir exists
        // on open" rule; `Create` and `Open` therefore share the same ensure-dir.
        std::fs::create_dir_all(&shard_dir).map_err(|error| MaterialiseError {
            shard_id,
            message: format!("shard directory create failed: {error}"),
        })?;

        let store_dir = shard_dir.join(SHARD_STORE_DIR);
        let wal_path = shard_dir.join(SHARD_WAL_FILE);
        let handle = ShardHandle::spawn(Arc::clone(&self.scheduler), &store_dir, &wal_path)
            .map_err(|error| MaterialiseError {
                shard_id,
                message: format!("shard spawn failed: {error:?}"),
            })?;

        let sweep = match &self.sweep {
            None => None,
            Some(config) => Some(
                SweepHandle::spawn(
                    Arc::clone(&self.scheduler),
                    store_dir,
                    wal_path,
                    handle.clone(),
                    config.interval,
                    config.command_timeout,
                )
                .map_err(|error| {
                    // Roll the just-spawned shard back so a sweep failure never
                    // leaks a live actor with no sweep.
                    drop(handle.shutdown(config.command_timeout));
                    MaterialiseError {
                        shard_id,
                        message: format!("sweep spawn failed: {error}"),
                    }
                })?,
            ),
        };

        Ok(MaterialisedShard { handle, sweep })
    }
}

fn poisoned(shard_id: usize) -> MaterialiseError {
    MaterialiseError {
        shard_id,
        message: "shard router map lock poisoned".to_owned(),
    }
}

fn shard_dir(data_dir: &Path, index: usize) -> PathBuf {
    data_dir.join(format!("shard-{index}"))
}

#[cfg(test)]
#[path = "router_tests.rs"]
mod tests;
