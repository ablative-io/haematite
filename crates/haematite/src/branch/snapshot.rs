//! BRANCH-003: Snapshot registry and commit log.
//!
//! The snapshot registry (R1) maps human-readable names to committed root
//! hashes and persists across restarts. The commit log (R2) records every
//! committed root hash in commit order with a timestamp. Snapshot listing (R5)
//! returns named snapshots with their hashes and timestamps in chronological
//! order.
//!
//! Timestamp semantics: an entry's timestamp is *naming time* (when `name` was
//! called), not the root's commit time (which lives in the [`CommitLog`]).

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use super::persist::{CodecError, Reader, push_bytes, push_u64, write_atomic};
use crate::tree::Hash;

/// Wall-clock timestamp in nanoseconds since the Unix epoch.
pub type Timestamp = u64;

const REGISTRY_MAGIC: &[u8; 4] = b"HSR1";
const LOG_MAGIC: &[u8; 4] = b"HCL1";

/// Captures the current wall-clock time as a [`Timestamp`]. A reading before the
/// Unix epoch (which a sane host clock never produces) collapses to `0`.
pub fn current_timestamp() -> Timestamp {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// One named snapshot: a name bound to a committed root hash at a point in time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotEntry {
    pub name: String,
    pub root_hash: Hash,
    pub timestamp: Timestamp,
}

/// One commit-log record: a committed root hash and when it was committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitLogEntry {
    pub root_hash: Hash,
    pub timestamp: Timestamp,
}

/// Errors raised by the snapshot registry and commit log.
#[derive(Debug)]
pub enum SnapshotError {
    /// A snapshot with this name already exists.
    DuplicateName(String),
    /// The persisted file could not be decoded.
    Corrupt(String),
    /// An I/O error occurred while persisting or loading.
    Io(std::io::Error),
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateName(name) => write!(f, "snapshot name already exists: {name}"),
            Self::Corrupt(reason) => write!(f, "snapshot store corrupted: {reason}"),
            Self::Io(error) => write!(f, "snapshot store I/O error: {error}"),
        }
    }
}

impl std::error::Error for SnapshotError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::DuplicateName(_) | Self::Corrupt(_) => None,
        }
    }
}

impl From<std::io::Error> for SnapshotError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<CodecError> for SnapshotError {
    fn from(error: CodecError) -> Self {
        match error {
            CodecError::Corrupt(reason) => Self::Corrupt(reason),
            CodecError::Io(io_error) => Self::Io(io_error),
        }
    }
}

/// Persistent mapping from human-readable names to committed root hashes.
///
/// Names are unique: binding a name that already exists is an error. Entries
/// retain insertion order so that [`SnapshotRegistry::list_snapshots`] reports
/// them chronologically. When constructed with [`SnapshotRegistry::open`], every
/// mutation is flushed to disk so the registry survives a restart.
#[derive(Debug)]
pub struct SnapshotRegistry {
    entries: Vec<SnapshotEntry>,
    index: HashMap<String, Hash>,
    path: Option<PathBuf>,
}

impl SnapshotRegistry {
    /// Creates an empty, in-memory registry with no backing file.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            index: HashMap::new(),
            path: None,
        }
    }

    /// Opens (or creates) a registry persisted at `path`, loading any entries.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, SnapshotError> {
        let path = path.as_ref().to_path_buf();
        let entries = match fs::read(&path) {
            Ok(bytes) => decode_registry(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(SnapshotError::Io(error)),
        };
        let mut index = HashMap::with_capacity(entries.len());
        for entry in &entries {
            index.insert(entry.name.clone(), entry.root_hash);
        }
        Ok(Self {
            entries,
            index,
            path: Some(path),
        })
    }

    /// Binds `name` to `root_hash`, stamped with the current naming time.
    ///
    /// Returns [`SnapshotError::DuplicateName`] if the name is already taken,
    /// leaving the registry unchanged.
    pub fn name(&mut self, name: &str, root_hash: Hash) -> Result<(), SnapshotError> {
        self.name_at(name, root_hash, current_timestamp())
    }

    /// Binds `name` to `root_hash` with an explicit timestamp.
    pub fn name_at(
        &mut self,
        name: &str,
        root_hash: Hash,
        timestamp: Timestamp,
    ) -> Result<(), SnapshotError> {
        if self.index.contains_key(name) {
            return Err(SnapshotError::DuplicateName(name.to_owned()));
        }
        self.entries.push(SnapshotEntry {
            name: name.to_owned(),
            root_hash,
            timestamp,
        });
        self.index.insert(name.to_owned(), root_hash);
        if let Err(error) = self.persist() {
            // Roll the in-memory state back so it stays consistent with disk.
            self.entries.pop();
            self.index.remove(name);
            return Err(error);
        }
        Ok(())
    }

    /// Returns the root hash bound to `name`, or `None` if unknown.
    pub fn get(&self, name: &str) -> Option<Hash> {
        self.index.get(name).copied()
    }

    /// Lists every named snapshot as `(name, root_hash, timestamp)` tuples in
    /// chronological (naming) order; `timestamp` is naming time (see module doc).
    pub fn list_snapshots(&self) -> Vec<(String, Hash, Timestamp)> {
        self.entries
            .iter()
            .map(|entry| (entry.name.clone(), entry.root_hash, entry.timestamp))
            .collect()
    }

    /// Number of named snapshots.
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the registry holds no snapshots.
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn persist(&self) -> Result<(), SnapshotError> {
        if let Some(path) = &self.path {
            write_atomic(path, &encode_registry(&self.entries))?;
        }
        Ok(())
    }
}

impl Default for SnapshotRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Append-only log of every committed root hash, in commit order.
///
/// `Database::commit` appends each new composite root hash here together with a
/// timestamp. When constructed with [`CommitLog::open`], every append is flushed
/// to disk so the log survives a restart.
#[derive(Debug)]
pub struct CommitLog {
    entries: Vec<CommitLogEntry>,
    path: Option<PathBuf>,
}

impl CommitLog {
    /// Creates an empty, in-memory log with no backing file.
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
            path: None,
        }
    }

    /// Opens (or creates) a log persisted at `path`, loading any existing entries.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, SnapshotError> {
        let path = path.as_ref().to_path_buf();
        let entries = match fs::read(&path) {
            Ok(bytes) => decode_log(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(SnapshotError::Io(error)),
        };
        Ok(Self {
            entries,
            path: Some(path),
        })
    }

    /// Appends `root_hash` to the log with the given commit `timestamp`.
    pub fn append(&mut self, root_hash: Hash, timestamp: Timestamp) -> Result<(), SnapshotError> {
        self.entries.push(CommitLogEntry {
            root_hash,
            timestamp,
        });
        if let Err(error) = self.persist() {
            self.entries.pop();
            return Err(error);
        }
        Ok(())
    }

    /// Lists every commit-log entry in chronological (commit) order.
    pub const fn list(&self) -> &[CommitLogEntry] {
        self.entries.as_slice()
    }

    /// Number of recorded commits.
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the log holds no commits.
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn persist(&self) -> Result<(), SnapshotError> {
        if let Some(path) = &self.path {
            write_atomic(path, &encode_log(&self.entries))?;
        }
        Ok(())
    }
}

impl Default for CommitLog {
    fn default() -> Self {
        Self::new()
    }
}

fn encode_registry(entries: &[SnapshotEntry]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(REGISTRY_MAGIC);
    push_u64(&mut bytes, entries.len() as u64);
    for entry in entries {
        push_bytes(&mut bytes, entry.name.as_bytes());
        bytes.extend_from_slice(entry.root_hash.as_bytes());
        push_u64(&mut bytes, entry.timestamp);
    }
    bytes
}

fn decode_registry(bytes: &[u8]) -> Result<Vec<SnapshotEntry>, SnapshotError> {
    let mut reader = Reader::new(bytes);
    reader.expect_magic(*REGISTRY_MAGIC)?;
    let count = reader.read_usize()?;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let name_bytes = reader.read_bytes()?;
        let name = String::from_utf8(name_bytes)
            .map_err(|_error| SnapshotError::Corrupt("snapshot name is not valid UTF-8".into()))?;
        let root_hash = reader.read_hash()?;
        let timestamp = reader.read_u64()?;
        entries.push(SnapshotEntry {
            name,
            root_hash,
            timestamp,
        });
    }
    reader.finish()?;
    Ok(entries)
}

fn encode_log(entries: &[CommitLogEntry]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(LOG_MAGIC);
    push_u64(&mut bytes, entries.len() as u64);
    for entry in entries {
        bytes.extend_from_slice(entry.root_hash.as_bytes());
        push_u64(&mut bytes, entry.timestamp);
    }
    bytes
}

fn decode_log(bytes: &[u8]) -> Result<Vec<CommitLogEntry>, SnapshotError> {
    let mut reader = Reader::new(bytes);
    reader.expect_magic(*LOG_MAGIC)?;
    let count = reader.read_usize()?;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let root_hash = reader.read_hash()?;
        let timestamp = reader.read_u64()?;
        entries.push(CommitLogEntry {
            root_hash,
            timestamp,
        });
    }
    reader.finish()?;
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::{CommitLog, SnapshotError, SnapshotRegistry, current_timestamp};
    use crate::tree::Hash;

    fn hash(byte: u8) -> Hash {
        Hash::from_bytes([byte; 32])
    }

    #[test]
    fn name_stores_and_get_returns_mapping() -> Result<(), SnapshotError> {
        let mut registry = SnapshotRegistry::new();
        registry.name("nightly", hash(1))?;
        assert_eq!(registry.get("nightly"), Some(hash(1)));
        Ok(())
    }

    #[test]
    fn get_returns_none_for_unknown_name() {
        let registry = SnapshotRegistry::new();
        assert_eq!(registry.get("missing"), None);
    }

    #[test]
    fn naming_duplicate_is_an_error_and_keeps_original() -> Result<(), SnapshotError> {
        let mut registry = SnapshotRegistry::new();
        registry.name("release", hash(1))?;
        let result = registry.name("release", hash(2));
        assert!(matches!(result, Err(SnapshotError::DuplicateName(name)) if name == "release"));
        assert_eq!(registry.get("release"), Some(hash(1)));
        assert_eq!(registry.len(), 1);
        Ok(())
    }

    #[test]
    fn list_snapshots_is_empty_for_empty_registry() {
        let registry = SnapshotRegistry::new();
        assert!(registry.is_empty());
        assert!(registry.list_snapshots().is_empty());
    }

    #[test]
    fn list_snapshots_reports_names_hashes_and_timestamps_in_order() -> Result<(), SnapshotError> {
        let mut registry = SnapshotRegistry::new();
        registry.name_at("first", hash(1), 100)?;
        registry.name_at("second", hash(2), 200)?;
        registry.name_at("third", hash(3), 300)?;
        let listed = registry.list_snapshots();
        assert_eq!(
            listed,
            vec![
                ("first".to_owned(), hash(1), 100),
                ("second".to_owned(), hash(2), 200),
                ("third".to_owned(), hash(3), 300),
            ]
        );
        Ok(())
    }

    #[test]
    fn snapshot_timestamp_is_naming_time_not_commit_time() -> Result<(), SnapshotError> {
        // A long-committed root named now lists with the naming timestamp.
        let mut registry = SnapshotRegistry::new();
        registry.name_at("tagged", hash(1), 999)?;
        assert_eq!(
            registry.list_snapshots(),
            vec![("tagged".to_owned(), hash(1), 999)]
        );
        Ok(())
    }

    #[test]
    fn registry_survives_restart() -> Result<(), SnapshotError> {
        let dir = tempfile::tempdir().map_err(SnapshotError::Io)?;
        let path = dir.path().join("registry.bin");
        {
            let mut registry = SnapshotRegistry::open(&path)?;
            registry.name_at("alpha", hash(7), 10)?;
            registry.name_at("beta", hash(8), 20)?;
        }
        let reopened = SnapshotRegistry::open(&path)?;
        assert_eq!(reopened.get("alpha"), Some(hash(7)));
        assert_eq!(reopened.get("beta"), Some(hash(8)));
        assert_eq!(
            reopened.list_snapshots(),
            vec![
                ("alpha".to_owned(), hash(7), 10),
                ("beta".to_owned(), hash(8), 20),
            ]
        );
        Ok(())
    }

    #[test]
    fn duplicate_name_does_not_corrupt_persisted_registry() -> Result<(), SnapshotError> {
        let dir = tempfile::tempdir().map_err(SnapshotError::Io)?;
        let path = dir.path().join("registry.bin");
        let mut registry = SnapshotRegistry::open(&path)?;
        registry.name_at("only", hash(1), 5)?;
        let _ = registry.name("only", hash(2));
        let reopened = SnapshotRegistry::open(&path)?;
        assert_eq!(reopened.list_snapshots().len(), 1);
        assert_eq!(reopened.get("only"), Some(hash(1)));
        Ok(())
    }

    #[test]
    fn commit_log_appends_in_chronological_order() -> Result<(), SnapshotError> {
        let mut log = CommitLog::new();
        log.append(hash(1), 100)?;
        log.append(hash(2), 200)?;
        let entries = log.list();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].root_hash, hash(1));
        assert_eq!(entries[0].timestamp, 100);
        assert_eq!(entries[1].root_hash, hash(2));
        assert_eq!(entries[1].timestamp, 200);
        Ok(())
    }

    #[test]
    fn commit_log_survives_restart() -> Result<(), SnapshotError> {
        let dir = tempfile::tempdir().map_err(SnapshotError::Io)?;
        let path = dir.path().join("commit.log");
        {
            let mut log = CommitLog::open(&path)?;
            log.append(hash(3), 30)?;
            log.append(hash(4), 40)?;
        }
        let reopened = CommitLog::open(&path)?;
        let entries = reopened.list();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].root_hash, hash(3));
        assert_eq!(entries[1].root_hash, hash(4));
        assert_eq!(entries[1].timestamp, 40);
        Ok(())
    }

    #[test]
    fn current_timestamp_is_after_epoch() {
        assert!(current_timestamp() > 0);
    }

    #[test]
    fn open_rejects_corrupt_file() -> Result<(), SnapshotError> {
        let dir = tempfile::tempdir().map_err(SnapshotError::Io)?;
        let path = dir.path().join("registry.bin");
        std::fs::write(&path, b"not a valid registry").map_err(SnapshotError::Io)?;
        assert!(matches!(
            SnapshotRegistry::open(&path),
            Err(SnapshotError::Corrupt(_))
        ));
        Ok(())
    }
}
