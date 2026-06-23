use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use beamr::module::ModuleRegistry;
use beamr::scheduler::{Scheduler, SchedulerConfig};

use crate::shard::actor::{ShardError, ShardHandle};
use crate::shard::router::ShardRouter;
use crate::tree::Hash;
use crate::ttl::sweep::{SweepError, SweepHandle};

pub(crate) mod helpers;

use helpers::{
    event_range_end, event_range_start, map_shard_error, map_spawn_error, range_on_handle,
};

const CONFIG_FILE: &str = "config.json";
const SHARD_STORE_DIR: &str = "store";
const SHARD_WAL_FILE: &str = "shard.wal";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) type DbEntry = (Vec<u8>, Vec<u8>);
pub(crate) type DbRange = Vec<DbEntry>;
pub(crate) type ShardCommitResult = (usize, Result<Hash, ShardError>);

/// Explicit database configuration; no field has a silent default.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DatabaseConfig {
    pub data_dir: PathBuf,
    pub shard_count: usize,
    /// Sweep interval in milliseconds. `None` disables TTL writes.
    pub sweep_interval: Option<u64>,
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
    SweepSpawn(String),
    ShardError(String),
    SweepError(String),
    IoError(io::Error),
    MissingSweepInterval,
    InvalidSweepInterval,
    SequenceConflict {
        expected: u64,
        actual: u64,
    },
    CasMismatch {
        expected: Option<u64>,
        actual: Option<u64>,
    },
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
            Self::SweepSpawn(message) => {
                write!(formatter, "failed to spawn sweep actor: {message}")
            }
            Self::ShardError(message) => write!(formatter, "shard operation failed: {message}"),
            Self::SweepError(message) => write!(formatter, "sweep operation failed: {message}"),
            Self::IoError(error) => write!(formatter, "database I/O error: {error}"),
            Self::MissingSweepInterval => write!(formatter, "ttl writes require sweep_interval"),
            Self::InvalidSweepInterval => {
                write!(formatter, "sweep_interval must be greater than zero")
            }
            Self::SequenceConflict { expected, actual } => write!(
                formatter,
                "sequence conflict on append: expected {expected}, actual {actual}"
            ),
            Self::CasMismatch { expected, actual } => write!(
                formatter,
                "cas mismatch: expected {expected:?}, actual {actual:?}"
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
            | Self::SweepSpawn(_)
            | Self::ShardError(_)
            | Self::SweepError(_)
            | Self::MissingSweepInterval
            | Self::InvalidSweepInterval
            | Self::SequenceConflict { .. }
            | Self::CasMismatch { .. } => None,
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
    sweeps: Vec<SweepHandle>,
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

    /// Atomically append event entries under `key` using optimistic concurrency.
    pub fn append(
        &self,
        key: Vec<u8>,
        entries: Vec<Vec<u8>>,
        expected_seq: u64,
    ) -> Result<u64, DatabaseError> {
        self.append_with_ttl(key, entries, expected_seq, None)
    }

    /// Atomically append event entries with optional TTL metadata.
    pub fn append_with_ttl(
        &self,
        key: Vec<u8>,
        entries: Vec<Vec<u8>>,
        expected_seq: u64,
        ttl: Option<Duration>,
    ) -> Result<u64, DatabaseError> {
        self.validate_ttl_write(ttl)?;
        self.handle_for(&key)?
            .append_with_ttl(key, entries, expected_seq, ttl, self.timeout)
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

    /// Read appended event entries for `key` from `from_seq` onward as raw
    /// `(encoded_key, value)` pairs, in sequence order.
    ///
    /// Unlike [`Self::read_events_from`], this preserves the encoded tree key so
    /// the caller (the `EventStore`) can decode each event's sequence number from
    /// its key rather than trusting a value-side copy.
    pub fn read_event_entries_from(
        &self,
        key: &[u8],
        from_seq: u64,
    ) -> Result<DbRange, DatabaseError> {
        let from = event_range_start(key, from_seq);
        let to = event_range_end(key);
        range_on_handle(self.handle_for(key)?, &from, &to, self.timeout)
    }

    /// Read the next sequence metadata for an event stream, if the stream exists.
    pub fn read_stream_next_seq(&self, key: &[u8]) -> Result<Option<u64>, DatabaseError> {
        // Route on the STREAM key — `append` writes the sequence metadata into the
        // shard of the stream key, so the read must select the same shard. Routing
        // on `event_sequence_key(key)` (a different hash) would read the wrong
        // shard for `shard_count > 1` and miss the metadata entirely.
        self.handle_for(key)?
            .read_value(event_sequence_key(key), self.timeout)
            .map_err(map_shard_error)
    }

    /// Return true if a stream has at least one non-expired event visible now.
    pub fn stream_has_live_events(&self, key: &[u8]) -> Result<bool, DatabaseError> {
        self.read_event_entries_from(key, 1)
            .map(|entries| !entries.is_empty())
    }

    /// Read the scalar `u64` value at `key`, or `None` if it is unset.
    pub fn read_value(&self, key: &[u8]) -> Result<Option<u64>, DatabaseError> {
        self.handle_for(key)?
            .read_value(key.to_vec(), self.timeout)
            .map_err(map_shard_error)
    }

    /// Atomically compare-and-swap the scalar `u64` value at `key`.
    ///
    /// The read-compare-write executes inside the owning shard's single-threaded
    /// actor, so concurrent CAS calls against the same key are serialised and
    /// cannot race. Returns [`DatabaseError::CasMismatch`] if the current value
    /// is not `expected`.
    pub fn cas(&self, key: Vec<u8>, expected: Option<u64>, new: u64) -> Result<(), DatabaseError> {
        self.handle_for(&key)?
            .cas(key, expected, new, self.timeout)
            .map_err(map_shard_error)
    }

    /// Collect every stream's `(stream_key, next_seq)` pair across all shards.
    ///
    /// This walks each shard in parallel, scanning its full key range for the
    /// per-stream sequence-metadata keys and decoding each one. It is the
    /// O(total entries) traversal that backs the `EventStore` `scan` predicate.
    pub fn scan_sequence_keys(&self) -> Result<Vec<(Vec<u8>, u64)>, DatabaseError> {
        let handles = self.router.handles_in_order().to_vec();
        let timeout = self.timeout;
        let results = run_indexed_parallel(handles, |handle: ShardHandle| {
            handle.scan_sequences(timeout)
        })?;
        let mut streams = Vec::new();
        for (_, result) in results {
            streams.extend(result.map_err(map_shard_error)?);
        }
        Ok(streams)
    }

    pub(crate) const fn shard_count(&self) -> usize {
        self.config.shard_count
    }

    pub(crate) const fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) fn shard_handles_in_order(&self) -> &[ShardHandle] {
        self.router.handles_in_order()
    }

    pub(crate) fn handle_for(&self, key: &[u8]) -> Result<&ShardHandle, DatabaseError> {
        self.router
            .handle_for(key)
            .ok_or(DatabaseError::InvalidShardCount)
    }

    pub(crate) const fn validate_ttl_write(
        &self,
        ttl: Option<Duration>,
    ) -> Result<(), DatabaseError> {
        if ttl.is_some() && self.config.sweep_interval.is_none() {
            return Err(DatabaseError::MissingSweepInterval);
        }
        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        for handle in &self.sweeps {
            if let Err(error) = handle.shutdown(self.timeout) {
                log::debug!(
                    "database sweep shutdown skipped for supervisor pid {}: {error}",
                    handle.supervisor_pid()
                );
            }
        }
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
    validate_sweep_interval(config.sweep_interval)?;
    let scheduler = create_scheduler()?;
    let router = spawn_router(&scheduler, &config.data_dir, config.shard_count, mode)?;
    let sweeps = spawn_sweeps(&scheduler, &config, &router)?;
    Ok(Database {
        config,
        scheduler,
        router,
        sweeps,
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

const fn validate_sweep_interval(interval: Option<u64>) -> Result<(), DatabaseError> {
    match interval {
        Some(0) => Err(DatabaseError::InvalidSweepInterval),
        Some(_) | None => Ok(()),
    }
}

fn event_sequence_key(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len().saturating_add(4));
    encoded.extend_from_slice(key);
    encoded.extend_from_slice(&[0xff, b's', b'e', b'q']);
    encoded
}

fn shard_dir(data_dir: &Path, index: usize) -> PathBuf {
    data_dir.join(format!("shard-{index}"))
}

pub(crate) fn run_indexed_parallel<Item, Output, Work>(
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

#[cfg(test)]
#[path = "db_tests.rs"]
mod tests;
