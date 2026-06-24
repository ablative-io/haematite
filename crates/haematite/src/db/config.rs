use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::sync::topology::{SyncNodeId, SyncTopology};

use super::{CONFIG_FILE, DatabaseError};

/// Explicit database configuration; no field has a silent default.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DatabaseConfig {
    pub data_dir: PathBuf,
    pub shard_count: usize,
    /// Sweep interval in milliseconds. `None` disables TTL writes.
    pub sweep_interval: Option<u64>,
    /// Distributed sync configuration. `None` keeps the database single-node.
    pub distributed: Option<DistributedDatabaseConfig>,
}

/// Explicit distributed database configuration; no topology is implied.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DistributedDatabaseConfig {
    pub local_node: SyncNodeId,
    pub nodes: Vec<SyncNodeId>,
    /// Sync topology chosen by the caller. `None` is rejected for distributed
    /// creation/opening so there is no silent default topology.
    pub topology: Option<SyncTopology>,
    /// Sync interval in milliseconds.
    pub sync_interval: u64,
}

pub(super) fn write_config(config: &DatabaseConfig) -> Result<(), DatabaseError> {
    let bytes = serde_json::to_vec_pretty(config).map_err(|error| {
        DatabaseError::ConfigWrite(io::Error::new(io::ErrorKind::InvalidData, error))
    })?;
    fs::write(config.data_dir.join(CONFIG_FILE), bytes).map_err(DatabaseError::ConfigWrite)
}

pub(super) fn read_config(path: &Path) -> Result<DatabaseConfig, DatabaseError> {
    let bytes = fs::read(path.join(CONFIG_FILE)).map_err(DatabaseError::ConfigRead)?;
    serde_json::from_slice(&bytes).map_err(|error| DatabaseError::ConfigParse(error.to_string()))
}

pub(super) fn validate_database_config(config: &DatabaseConfig) -> Result<(), DatabaseError> {
    validate_shard_count(config.shard_count)?;
    validate_sweep_interval(config.sweep_interval)?;
    validate_distributed_config(config)?;
    Ok(())
}

const fn validate_shard_count(shard_count: usize) -> Result<(), DatabaseError> {
    if shard_count == 0 {
        Err(DatabaseError::InvalidShardCount)
    } else {
        Ok(())
    }
}

const fn validate_sweep_interval(interval: Option<u64>) -> Result<(), DatabaseError> {
    match interval {
        Some(0) => Err(DatabaseError::InvalidSweepInterval),
        Some(_) | None => Ok(()),
    }
}

fn validate_distributed_config(config: &DatabaseConfig) -> Result<(), DatabaseError> {
    let Some(distributed) = &config.distributed else {
        return Ok(());
    };
    let Some(topology) = &distributed.topology else {
        return Err(DatabaseError::MissingSyncTopology);
    };
    if distributed.sync_interval == 0 {
        return Err(DatabaseError::InvalidSyncInterval);
    }
    topology
        .partners_for(&distributed.local_node, &distributed.nodes)
        .map_err(|error| DatabaseError::SyncSchedulerError(error.to_string()))?;
    Ok(())
}
