use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use beamr::module::ModuleRegistry;
use beamr::scheduler::{Scheduler, SchedulerConfig};

use crate::shard::actor::{RangeItem, ShardError, ShardHandle};
use crate::shard::router::ShardRouter;
use crate::tree::Hash;

const CONFIG_FILE: &str = "config.json";
const SHARD_STORE_DIR: &str = "store";
const SHARD_WAL_FILE: &str = "shard.wal";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

type DbEntry = (Vec<u8>, Vec<u8>);
type DbRange = Vec<DbEntry>;
type ShardCommitResult = (usize, Result<Hash, ShardError>);

/// Explicit database configuration; no field has a silent default.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DatabaseConfig {
    pub data_dir: PathBuf,
    pub shard_count: usize,
}

/// Errors surfaced by the top-level database handle.
#[derive(Debug)]
pub enum DatabaseError {
    DirectoryCreate(io::Error),
    ConfigWrite(io::Error),
    ConfigRead(io::Error),
    ConfigParse(String),
    InvalidShardCount,
    ShardSpawn(String),
    ShardError(String),
    IoError(io::Error),
    SequenceConflict { expected: u64, actual: u64 },
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DirectoryCreate(error) => {
                write!(formatter, "failed to create database directory: {error}")
            }
            Self::ConfigWrite(error) => {
                write!(formatter, "failed to write database config: {error}")
            }
            Self::ConfigRead(error) => write!(formatter, "failed to read database config: {error}"),
            Self::ConfigParse(message) => {
                write!(formatter, "failed to parse database config: {message}")
            }
            Self::InvalidShardCount => write!(formatter, "database shard_count must be at least 1"),
            Self::ShardSpawn(message) => {
                write!(formatter, "failed to spawn shard actor: {message}")
            }
            Self::ShardError(message) => write!(formatter, "shard operation failed: {message}"),
            Self::IoError(error) => write!(formatter, "database I/O error: {error}"),
            Self::SequenceConflict { expected, actual } => write!(
                formatter,
                "sequence conflict on append: expected {expected}, actual {actual}"
            ),
        }
    }
}

impl std::error::Error for DatabaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::DirectoryCreate(error)
            | Self::ConfigWrite(error)
            | Self::ConfigRead(error)
            | Self::IoError(error) => Some(error),
            Self::ConfigParse(_)
            | Self::InvalidShardCount
            | Self::ShardSpawn(_)
            | Self::ShardError(_)
            | Self::SequenceConflict { .. } => None,
        }
    }
}

impl From<io::Error> for DatabaseError {
    fn from(error: io::Error) -> Self {
        Self::IoError(error)
    }
}

/// Top-level database handle. Callers use this API instead of shard actors.
pub struct Database {
    config: DatabaseConfig,
    scheduler: Arc<Scheduler>,
    router: ShardRouter,
    timeout: Duration,
}

impl fmt::Debug for Database {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Database")
            .field("config", &self.config)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl Database {
    /// Create a new database directory, write its config, and spawn all shards.
    pub fn create(config: DatabaseConfig) -> Result<Self, DatabaseError> {
        validate_shard_count(config.shard_count)?;
        let data_dir = config.data_dir.clone();
        let should_cleanup = !data_dir.exists();
        fs::create_dir_all(&data_dir).map_err(DatabaseError::DirectoryCreate)?;
        let result = initialise_database(config);
        if result.is_err() && should_cleanup {
            drop(fs::remove_dir_all(&data_dir));
        }
        result
    }

    /// Open an existing database directory and restart its shard actors.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let path = path.as_ref().to_path_buf();
        let mut config = read_config(&path)?;
        validate_shard_count(config.shard_count)?;
        config.data_dir = path;
        start_database(config, StartupMode::Open)
    }

    /// Return the shard index that owns `key`.
    pub fn shard_for(&self, key: &[u8]) -> usize {
        self.router.shard_for(key)
    }

    /// Read one key through the owning shard.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        self.handle_for(key)?
            .get(key.to_vec(), self.timeout)
            .map_err(map_shard_error)
    }

    /// Buffer a put through the owning shard.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DatabaseError> {
        self.handle_for(&key)?
            .put(key, value, self.timeout)
            .map_err(map_shard_error)
    }

    /// Buffer a delete through the owning shard.
    pub fn delete(&self, key: Vec<u8>) -> Result<(), DatabaseError> {
        self.handle_for(&key)?
            .delete(key, self.timeout)
            .map_err(map_shard_error)
    }

    /// Commit every shard in parallel and return root hashes in shard order.
    pub fn commit(&self) -> Result<Vec<Hash>, DatabaseError> {
        let handles = self.router.handles_in_order().to_vec();
        let timeout = self.timeout;
        let results = run_indexed_parallel(handles, |handle: ShardHandle| handle.commit(timeout))?;
        ordered_hashes(results, self.config.shard_count)
    }

    /// Read a single-shard key range in ascending key order.
    pub fn range(&self, from: &[u8], to: &[u8]) -> Result<DbRange, DatabaseError> {
        if from >= to {
            return Ok(Vec::new());
        }
        range_on_handle(self.handle_for(from)?, from, to, self.timeout)
    }

    /// Atomically append event entries under `key` using optimistic concurrency.
    pub fn append(
        &self,
        key: Vec<u8>,
        entries: Vec<Vec<u8>>,
        expected_seq: u64,
    ) -> Result<u64, DatabaseError> {
        self.handle_for(&key)?
            .append(key, entries, expected_seq, self.timeout)
            .map_err(map_shard_error)
    }

    /// Read all appended events for `key` in sequence order.
    pub fn read_events(&self, key: &[u8]) -> Result<Vec<Vec<u8>>, DatabaseError> {
        self.read_events_from(key, 0)
    }

    /// Read appended events for `key` from `from_seq` onward.
    pub fn read_events_from(
        &self,
        key: &[u8],
        from_seq: u64,
    ) -> Result<Vec<Vec<u8>>, DatabaseError> {
        let from = event_range_start(key, from_seq);
        let to = event_range_end(key);
        let entries = range_on_handle(self.handle_for(key)?, &from, &to, self.timeout)?;
        Ok(entries.into_iter().map(|(_, value)| value).collect())
    }

    fn handle_for(&self, key: &[u8]) -> Result<&ShardHandle, DatabaseError> {
        self.router
            .handle_for(key)
            .ok_or(DatabaseError::InvalidShardCount)
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        for handle in self.router.handles_in_order() {
            if let Err(error) = handle.shutdown(self.timeout) {
                log::debug!(
                    "database shard shutdown skipped for pid {}: {error}",
                    handle.pid()
                );
            }
        }
        self.scheduler.shutdown();
    }
}

#[derive(Clone, Copy, Debug)]
enum StartupMode {
    Create,
    Open,
}

fn initialise_database(config: DatabaseConfig) -> Result<Database, DatabaseError> {
    for index in 0..config.shard_count {
        fs::create_dir_all(shard_dir(&config.data_dir, index))
            .map_err(DatabaseError::DirectoryCreate)?;
    }
    write_config(&config)?;
    start_database(config, StartupMode::Create)
}

fn start_database(config: DatabaseConfig, mode: StartupMode) -> Result<Database, DatabaseError> {
    validate_shard_count(config.shard_count)?;
    let scheduler = create_scheduler()?;
    let router = spawn_router(&scheduler, &config.data_dir, config.shard_count, mode)?;
    Ok(Database {
        config,
        scheduler,
        router,
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

fn write_config(config: &DatabaseConfig) -> Result<(), DatabaseError> {
    let bytes = serde_json::to_vec_pretty(config).map_err(|error| {
        DatabaseError::ConfigWrite(io::Error::new(io::ErrorKind::InvalidData, error))
    })?;
    fs::write(config.data_dir.join(CONFIG_FILE), bytes).map_err(DatabaseError::ConfigWrite)
}

fn read_config(path: &Path) -> Result<DatabaseConfig, DatabaseError> {
    let bytes = fs::read(path.join(CONFIG_FILE)).map_err(DatabaseError::ConfigRead)?;
    serde_json::from_slice(&bytes).map_err(|error| DatabaseError::ConfigParse(error.to_string()))
}

const fn validate_shard_count(shard_count: usize) -> Result<(), DatabaseError> {
    if shard_count == 0 {
        Err(DatabaseError::InvalidShardCount)
    } else {
        Ok(())
    }
}

fn shard_dir(data_dir: &Path, index: usize) -> PathBuf {
    data_dir.join(format!("shard-{index}"))
}

fn run_indexed_parallel<Item, Output, Work>(
    items: Vec<Item>,
    work: Work,
) -> Result<Vec<(usize, Output)>, DatabaseError>
where
    Item: Send,
    Output: Send,
    Work: Fn(Item) -> Output + Sync,
{
    std::thread::scope(|scope| {
        let mut joins = Vec::with_capacity(items.len());
        for (index, item) in items.into_iter().enumerate() {
            let work = &work;
            joins.push(scope.spawn(move || (index, work(item))));
        }
        let mut results = Vec::with_capacity(joins.len());
        for join in joins {
            match join.join() {
                Ok(result) => results.push(result),
                Err(_) => {
                    return Err(DatabaseError::ShardError(
                        "parallel worker thread panicked".to_owned(),
                    ));
                }
            }
        }
        Ok(results)
    })
}

fn map_spawn_error(error: ShardError) -> DatabaseError {
    match error {
        ShardError::Spawn(message) => DatabaseError::ShardSpawn(message),
        other => map_shard_error(other),
    }
}

fn map_shard_error(error: ShardError) -> DatabaseError {
    match error {
        ShardError::SequenceConflict { expected, actual } => {
            DatabaseError::SequenceConflict { expected, actual }
        }
        ShardError::Spawn(message) => DatabaseError::ShardSpawn(message),
        other => DatabaseError::ShardError(other.to_string()),
    }
}

fn ordered_hashes(
    results: Vec<ShardCommitResult>,
    shard_count: usize,
) -> Result<Vec<Hash>, DatabaseError> {
    let mut ordered = vec![None; shard_count];
    for (index, result) in results {
        match result {
            Ok(hash) => {
                if let Some(slot) = ordered.get_mut(index) {
                    *slot = Some(hash);
                }
            }
            Err(error) => return Err(map_shard_error(error)),
        }
    }
    let mut hashes = Vec::with_capacity(shard_count);
    for hash in ordered {
        let Some(hash) = hash else {
            return Err(DatabaseError::ShardError(
                "missing shard commit result".to_owned(),
            ));
        };
        hashes.push(hash);
    }
    Ok(hashes)
}

fn range_on_handle(
    handle: &ShardHandle,
    from: &[u8],
    to: &[u8],
    timeout: Duration,
) -> Result<DbRange, DatabaseError> {
    let items = handle
        .range(from.to_vec(), to.to_vec(), timeout)
        .map_err(map_shard_error)?;
    collect_range_items(items)
}

fn collect_range_items(items: Vec<RangeItem>) -> Result<DbRange, DatabaseError> {
    let mut entries = Vec::new();
    for item in items {
        match item {
            RangeItem::Entry { key, value } => entries.push((key, value)),
            RangeItem::Done => return Ok(entries),
        }
    }
    Err(DatabaseError::ShardError(
        "range result missing Done".to_owned(),
    ))
}

fn event_range_start(key: &[u8], seq: u64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len().saturating_add(9));
    encoded.extend_from_slice(key);
    encoded.push(0);
    encoded.extend_from_slice(&seq.to_be_bytes());
    encoded
}

fn event_range_end(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len().saturating_add(1));
    encoded.extend_from_slice(key);
    encoded.push(1);
    encoded
}

#[cfg(test)]
#[path = "db_tests.rs"]
mod tests;
