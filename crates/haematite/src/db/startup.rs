//! Database startup and lazy shard-router construction.
//!
//! LAZY SHARD MATERIALISATION: startup no longer pre-creates all `shard_count`
//! directories, nor spawns/probes/sweeps one actor per shard. It builds a
//! [`ShardRouter`] over the fixed `shard_count` modulus base and lets the router
//! spawn each shard (and its per-shard TTL sweep) on first touch. Boot cost is
//! therefore O(shards actually used), not `O(shard_count)` — a very high
//! `shard_count` is ~free until its shards are exercised.

use std::fs;
use std::sync::Arc;
use std::time::Duration;

use beamr::module::ModuleRegistry;
use beamr::scheduler::{Scheduler, SchedulerConfig};

use crate::shard::router::{ShardMode, ShardRouter, SweepConfig};
use crate::sync::scheduler::{
    NoopSyncPullTrigger, SyncSchedulerConfig, SyncSchedulerError, SyncSchedulerHandle,
};

use super::config::{validate_database_config, write_config};
use super::{Database, DatabaseConfig, DatabaseError};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
pub(super) enum StartupMode {
    Create,
    Open,
}

impl StartupMode {
    const fn shard_mode(self) -> ShardMode {
        match self {
            Self::Create => ShardMode::Create,
            Self::Open => ShardMode::Open,
        }
    }
}

pub(super) fn initialise_database(config: DatabaseConfig) -> Result<Database, DatabaseError> {
    // Only the database root directory is created up front; per-shard directories
    // are created lazily by the router on each shard's first touch. This is what
    // makes create() O(1) in `shard_count` instead of O(shard_count).
    fs::create_dir_all(&config.data_dir).map_err(DatabaseError::DirectoryCreate)?;
    write_config(&config)?;
    start_database(config, StartupMode::Create)
}

pub(super) fn start_database(
    config: DatabaseConfig,
    mode: StartupMode,
) -> Result<Database, DatabaseError> {
    validate_database_config(&config)?;
    let scheduler = create_scheduler()?;
    let router = build_router(&scheduler, &config, mode)?;
    let sync_schedulers = match spawn_sync_schedulers(&scheduler, &config, &router) {
        Ok(handles) => handles,
        Err(error) => {
            router.shutdown_all(DEFAULT_TIMEOUT);
            return Err(error);
        }
    };
    Ok(Database {
        config,
        scheduler,
        router,
        sync_schedulers,
        distribution: None,
        owner_stamps: super::owner_stamp::OwnerStamps::default(),
        timeout: DEFAULT_TIMEOUT,
    })
}

fn create_scheduler() -> Result<Arc<Scheduler>, DatabaseError> {
    Scheduler::new(SchedulerConfig::default(), Arc::new(ModuleRegistry::new()))
        .map(Arc::new)
        .map_err(DatabaseError::ShardSpawn)
}

fn build_router(
    scheduler: &Arc<Scheduler>,
    config: &DatabaseConfig,
    mode: StartupMode,
) -> Result<ShardRouter, DatabaseError> {
    let sweep = config.sweep_interval.map(|interval_millis| SweepConfig {
        interval: Duration::from_millis(interval_millis),
        command_timeout: DEFAULT_TIMEOUT,
    });
    ShardRouter::new(
        Arc::clone(scheduler),
        &config.data_dir,
        config.shard_count,
        mode.shard_mode(),
        sweep,
    )
    .ok_or(DatabaseError::InvalidShardCount)
}

fn spawn_sync_schedulers(
    scheduler: &Arc<Scheduler>,
    config: &DatabaseConfig,
    router: &ShardRouter,
) -> Result<Vec<SyncSchedulerHandle>, DatabaseError> {
    let Some(distributed) = &config.distributed else {
        return Ok(Vec::new());
    };
    let topology = distributed
        .topology
        .clone()
        .ok_or(DatabaseError::MissingSyncTopology)?;
    let scheduler_config = SyncSchedulerConfig::new(
        distributed.local_node.clone(),
        distributed.nodes.clone(),
        topology,
        config.shard_count,
        Duration::from_millis(distributed.sync_interval),
    );
    // LAZY: the scheduler syncs ONLY materialised shards (via the router's
    // membership view) — an un-materialised shard holds no data, so pulling it
    // would be pure waste.
    SyncSchedulerHandle::spawn_with_shard_source(
        Arc::clone(scheduler),
        scheduler_config,
        Arc::new(NoopSyncPullTrigger),
        Arc::new(router.membership()),
        DEFAULT_TIMEOUT,
    )
    .map(|handle| vec![handle])
    .map_err(map_sync_scheduler_error)
}

fn map_sync_scheduler_error(error: SyncSchedulerError) -> DatabaseError {
    match error {
        SyncSchedulerError::Spawn(message) => DatabaseError::SyncSchedulerSpawn(message),
        other => DatabaseError::SyncSchedulerError(other.to_string()),
    }
}
