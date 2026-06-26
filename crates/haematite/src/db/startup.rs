//! Database startup, shard spawning, and cleanup helpers.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use beamr::module::ModuleRegistry;
use beamr::scheduler::{Scheduler, SchedulerConfig};

use crate::shard::actor::ShardHandle;
use crate::shard::router::ShardRouter;
use crate::sync::scheduler::{
    NoopSyncPullTrigger, SyncSchedulerConfig, SyncSchedulerError, SyncSchedulerHandle,
};
use crate::ttl::sweep::{SweepError, SweepHandle};

use super::config::{validate_database_config, write_config};
use super::helpers::{map_shard_error, map_spawn_error};
use super::{Database, DatabaseConfig, DatabaseError};

const SHARD_STORE_DIR: &str = "store";
const SHARD_WAL_FILE: &str = "shard.wal";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
pub(super) enum StartupMode {
    Create,
    Open,
}

pub(super) fn initialise_database(config: DatabaseConfig) -> Result<Database, DatabaseError> {
    for index in 0..config.shard_count {
        fs::create_dir_all(shard_dir(&config.data_dir, index))
            .map_err(DatabaseError::DirectoryCreate)?;
    }
    write_config(&config)?;
    start_database(config, StartupMode::Create)
}

pub(super) fn start_database(
    config: DatabaseConfig,
    mode: StartupMode,
) -> Result<Database, DatabaseError> {
    validate_database_config(&config)?;
    let scheduler = create_scheduler()?;
    let router = spawn_router(&scheduler, &config.data_dir, config.shard_count, mode)?;
    let sweeps = spawn_sweeps(&scheduler, &config, &router)?;
    let sync_schedulers = match spawn_sync_schedulers(&scheduler, &config) {
        Ok(handles) => handles,
        Err(error) => {
            shutdown_sweeps(&sweeps);
            shutdown_handles(router.handles_in_order());
            return Err(error);
        }
    };
    Ok(Database {
        config,
        scheduler,
        router,
        sweeps,
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

fn spawn_router(
    scheduler: &Arc<Scheduler>,
    data_dir: &Path,
    shard_count: usize,
    mode: StartupMode,
) -> Result<ShardRouter, DatabaseError> {
    let mut handles = Vec::with_capacity(shard_count);
    for index in 0..shard_count {
        match spawn_one_shard(scheduler, data_dir, index, mode) {
            Ok(handle) => handles.push(handle),
            Err(error) => {
                shutdown_handles(&handles);
                return Err(error);
            }
        }
    }
    if let Err(error) = probe_shards(&handles) {
        shutdown_handles(&handles);
        return Err(error);
    }
    ShardRouter::new(handles).ok_or(DatabaseError::InvalidShardCount)
}

fn spawn_sweeps(
    scheduler: &Arc<Scheduler>,
    config: &DatabaseConfig,
    router: &ShardRouter,
) -> Result<Vec<SweepHandle>, DatabaseError> {
    let Some(interval_millis) = config.sweep_interval else {
        return Ok(Vec::new());
    };
    let interval = Duration::from_millis(interval_millis);
    let mut sweeps = Vec::with_capacity(config.shard_count);
    for (index, shard) in router.handles_in_order().iter().cloned().enumerate() {
        let shard_dir = shard_dir(&config.data_dir, index);
        match SweepHandle::spawn(
            Arc::clone(scheduler),
            shard_dir.join(SHARD_STORE_DIR),
            shard_dir.join(SHARD_WAL_FILE),
            shard,
            interval,
            DEFAULT_TIMEOUT,
        ) {
            Ok(handle) => sweeps.push(handle),
            Err(error) => {
                shutdown_sweeps(&sweeps);
                return Err(map_sweep_spawn_error(error));
            }
        }
    }
    Ok(sweeps)
}

fn spawn_sync_schedulers(
    scheduler: &Arc<Scheduler>,
    config: &DatabaseConfig,
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
    SyncSchedulerHandle::spawn(
        Arc::clone(scheduler),
        scheduler_config,
        Arc::new(NoopSyncPullTrigger),
        DEFAULT_TIMEOUT,
    )
    .map(|handle| vec![handle])
    .map_err(map_sync_scheduler_error)
}

fn spawn_one_shard(
    scheduler: &Arc<Scheduler>,
    data_dir: &Path,
    index: usize,
    mode: StartupMode,
) -> Result<ShardHandle, DatabaseError> {
    let shard_dir = shard_dir(data_dir, index);
    match mode {
        StartupMode::Create => {
            fs::create_dir_all(&shard_dir).map_err(DatabaseError::DirectoryCreate)?;
        }
        StartupMode::Open => validate_existing_shard_dir(&shard_dir)?,
    }
    ShardHandle::spawn(
        Arc::clone(scheduler),
        shard_dir.join(SHARD_STORE_DIR),
        shard_dir.join(SHARD_WAL_FILE),
    )
    .map_err(map_spawn_error)
}

fn validate_existing_shard_dir(path: &Path) -> Result<(), DatabaseError> {
    if path.is_dir() {
        Ok(())
    } else {
        Err(DatabaseError::IoError(io::Error::new(
            io::ErrorKind::NotFound,
            format!("missing shard directory {}", path.display()),
        )))
    }
}

fn probe_shards(handles: &[ShardHandle]) -> Result<(), DatabaseError> {
    for handle in handles {
        handle
            .get(b"__haematite_startup_probe__".to_vec(), DEFAULT_TIMEOUT)
            .map(drop)
            .map_err(map_shard_error)?;
    }
    Ok(())
}

fn shutdown_handles(handles: &[ShardHandle]) {
    for handle in handles {
        if let Err(error) = handle.shutdown(DEFAULT_TIMEOUT) {
            log::debug!(
                "shard cleanup shutdown skipped for pid {}: {error}",
                handle.pid()
            );
        }
    }
}

fn shutdown_sweeps(handles: &[SweepHandle]) {
    for handle in handles {
        if let Err(error) = handle.shutdown(DEFAULT_TIMEOUT) {
            log::debug!(
                "sweep cleanup shutdown skipped for supervisor pid {}: {error}",
                handle.supervisor_pid()
            );
        }
    }
}

fn map_sweep_spawn_error(error: SweepError) -> DatabaseError {
    match error {
        SweepError::Spawn(message) => DatabaseError::SweepSpawn(message),
        other => DatabaseError::SweepError(other.to_string()),
    }
}

fn map_sync_scheduler_error(error: SyncSchedulerError) -> DatabaseError {
    match error {
        SyncSchedulerError::Spawn(message) => DatabaseError::SyncSchedulerSpawn(message),
        other => DatabaseError::SyncSchedulerError(other.to_string()),
    }
}

fn shard_dir(data_dir: &Path, index: usize) -> PathBuf {
    data_dir.join(format!("shard-{index}"))
}
