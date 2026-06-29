// CORE-005: Durable WAL writer (append-only file)

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::tree::Hash;
use crate::tree::node::HASH_SIZE;

use super::buffer::{Mutation, WalError};
use super::entry::WalEntry;

const FRAME_LEN_SIZE: usize = 4;
const MARKER_TAG_SIZE: usize = 1;
const MARKER_CHECKSUM_SIZE: usize = 4;
const TAG_TRUNCATION_MARKER: u8 = 0x03;

/// Durability policy for appended WAL entries.
///
/// The caller must choose this explicitly when constructing [`DurableWal`]; the
/// writer intentionally has no default policy because append latency and crash
/// durability are workload-specific trade-offs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// Flush file contents and metadata after every append.
    PerWrite,
    /// Flush after every `n` appends.
    Batched(usize),
    /// Flush appended entries only when [`DurableWal::commit`] is called.
    CommitOnly,
}

/// Result of scanning a durable WAL file.
///
/// This is intentionally a low-level file summary, not crash recovery replay:
/// replay is implemented by PERSIST-003. The scanner is provided so commit
/// truncation can be verified and future recovery code has a checksum-validated
/// primitive to build on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalFileContents {
    entries: Vec<WalEntry>,
    committed_root: Option<Hash>,
}

impl WalFileContents {
    /// Replayable entries found in the WAL file.
    #[must_use]
    pub fn entries(&self) -> &[WalEntry] {
        &self.entries
    }

    /// Committed root hash recorded by a truncation marker, if present.
    #[must_use]
    pub const fn committed_root(&self) -> Option<Hash> {
        self.committed_root
    }
}

/// Append-only, crash-safe WAL writer (ADR-003).
///
/// Every appended mutation is serialised as a [`WalEntry`] and then wrapped in a
/// length-prefixed frame: `[entry length: u32 LE][entry bytes]`. The inner entry
/// bytes carry the CRC32 mandated by CN2. `append` uses `write_all` and applies
/// the configured [`FsyncPolicy`] before it returns `Ok`, so callers can enforce
/// write-before-acknowledge by appending here before updating the in-memory WAL
/// buffer or returning success to their own caller.
#[derive(Debug)]
pub struct DurableWal {
    path: PathBuf,
    file: File,
    policy: FsyncPolicy,
    writes_since_sync: usize,
    /// Latest durable promise-state snapshot (AA-3-0). Held so a `commit`
    /// truncation — which rewrites the WAL to just the committed-root marker —
    /// can re-emit it and never drop ownership/promise durability.
    promise: Option<super::promise::PromiseRecord>,
}

impl DurableWal {
    /// Open (or create) the WAL file at `path` in append mode.
    ///
    /// An existing file is opened without truncation so prior frames survive.
    /// When the file is newly created, the parent directory is fsynced so the
    /// new directory entry — not just the file's contents — survives a crash.
    ///
    /// A [`FsyncPolicy`] is required; no one-argument constructor is provided.
    pub fn new<P: AsRef<Path>>(path: P, policy: FsyncPolicy) -> Result<Self, WalError> {
        validate_policy(policy)?;
        let path = path.as_ref();
        fs::create_dir_all(parent_dir(path))?;
        let existed = path.exists();
        let file = open_append_file(path)?;
        if !existed {
            sync_parent_dir(path)?;
        }
        Ok(Self {
            path: path.to_path_buf(),
            file,
            policy,
            writes_since_sync: 0,
            promise: None,
        })
    }

    /// Seed the latest promise snapshot recovered from disk so a later `commit`
    /// truncation re-emits it (AA-3-0). Startup calls this when recovery found a
    /// persisted [`super::promise::PromiseRecord`]; without it, the first commit
    /// after a restart-without-new-promise would drop the recovered promise frame.
    pub fn seed_promise(&mut self, record: super::promise::PromiseRecord) {
        self.promise = Some(record);
    }

    /// Append one WAL entry to the end of the file.
    ///
    /// The file is opened with `O_APPEND` semantics, and this method never seeks
    /// backward or overwrites existing bytes. `Ok` is returned only after the
    /// complete frame has been written and any policy-required fsync succeeds.
    pub fn append(&mut self, entry: &WalEntry) -> Result<(), WalError> {
        let bytes = entry.serialise();
        self.write_entry_frame(&bytes)?;
        self.writes_since_sync = self.writes_since_sync.saturating_add(1);
        if self.should_sync_after_append() {
            self.file.sync_all()?;
            self.writes_since_sync = 0;
        }
        Ok(())
    }

    /// Convert and append one in-memory mutation as a durable WAL entry.
    ///
    /// This convenience preserves the caller discipline documented on
    /// [`super::buffer::WalBuffer`]: append here before mutating the in-memory
    /// buffer or acknowledging the caller.
    pub fn append_mutation(&mut self, mutation: &Mutation) -> Result<(), WalError> {
        self.append(&WalEntry::from(mutation))
    }

    /// Append a step-3 promise-state frame and FORCE an fsync before returning,
    /// regardless of the configured [`FsyncPolicy`] (AA-3-0, design §3).
    ///
    /// Promise state must be durable BEFORE the caller acts on it (reply Promise,
    /// serve as owner, send Prepare), so this never defers the sync the way a
    /// `CommitOnly`/`Batched` data append would — it always `sync_all`s the file
    /// (contents + metadata) before `Ok`. The frame shares the WAL's outer
    /// `[frame_len: u32 LE]` framing and CRC discipline, so it co-exists with
    /// data entries and the committed-root marker in one fsync domain.
    pub fn append_promise(
        &mut self,
        record: &super::promise::PromiseRecord,
    ) -> Result<(), WalError> {
        let bytes = record.serialise();
        self.write_entry_frame(&bytes)?;
        self.file.sync_all()?;
        // The forced sync clears any pending batched writes too: the file is now
        // fully durable, so the batched-policy counter must reset.
        self.writes_since_sync = 0;
        // Remember the snapshot so a later `commit` truncation re-emits it and
        // never drops promise durability when it rewrites the WAL to the marker.
        self.promise = Some(record.clone());
        Ok(())
    }

    /// Atomically truncate the WAL to a committed-root marker.
    ///
    /// The replacement file contains exactly one marker frame with the committed
    /// root hash. Before replacement begins, the existing WAL is synced so the
    /// old file is recoverable if a crash happens before the marker rename. The
    /// marker bytes are then written and synced to a temp file in the same
    /// directory, atomically renamed over the WAL path, and followed by a
    /// parent-directory fsync on Unix. A crash during replacement therefore leaves
    /// either the synced old WAL frames or the new marker file, never an empty
    /// file that loses both replay entries and the commit reference.
    pub fn commit(&mut self, root_hash: Hash) -> Result<(), WalError> {
        self.file.sync_all()?;
        // Build the replacement file as [marker][latest promise snapshot] and
        // install it in ONE atomic rename, so a commit truncation never drops
        // ownership/promise durability (AA-3-0, §3 "same fsync domain").
        //
        // It is NOT safe to write a marker-only file and then re-append the
        // promise as a separate step: that leaves a crash window in which the
        // on-disk WAL is marker-only and the promise frame is gone, so recovery
        // would regress `promised` to bottom — violating the non-regression
        // invariant the §4 majority-intersection fence rests on (a node could
        // then promise a ballot lower than one it already granted). Folding the
        // promise into the atomically-renamed replacement closes that window by
        // construction: the rename publishes marker and promise together or
        // neither.
        let mut replacement = truncation_marker_frame(root_hash);
        if let Some(record) = &self.promise {
            let bytes = record.serialise();
            replacement.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            replacement.extend_from_slice(&bytes);
        }
        write_atomic(&self.path, &replacement)?;
        self.file = open_append_file(&self.path)?;
        self.writes_since_sync = 0;
        Ok(())
    }

    /// Read and checksum-validate a WAL file into replay entries plus an
    /// optional committed-root marker.
    ///
    /// This does not replay entries into a tree; it only decodes the durable
    /// file format. Recovery logic remains out of scope for PERSIST-002.
    pub fn read_file<P: AsRef<Path>>(path: P) -> Result<WalFileContents, WalError> {
        parse_wal_file(&fs::read(path)?)
    }

    fn write_entry_frame(&mut self, bytes: &[u8]) -> Result<(), WalError> {
        let mut frame = Vec::with_capacity(FRAME_LEN_SIZE + bytes.len());
        frame.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        frame.extend_from_slice(bytes);
        self.file.write_all(&frame)?;
        Ok(())
    }

    const fn should_sync_after_append(&self) -> bool {
        match self.policy {
            FsyncPolicy::PerWrite => true,
            FsyncPolicy::Batched(interval) => self.writes_since_sync >= interval,
            FsyncPolicy::CommitOnly => false,
        }
    }
}

const fn validate_policy(policy: FsyncPolicy) -> Result<(), WalError> {
    match policy {
        FsyncPolicy::Batched(0) => Err(WalError::InvalidFsyncPolicy { interval: 0 }),
        FsyncPolicy::PerWrite | FsyncPolicy::Batched(_) | FsyncPolicy::CommitOnly => Ok(()),
    }
}

fn open_append_file(path: &Path) -> Result<File, WalError> {
    Ok(OpenOptions::new().create(true).append(true).open(path)?)
}

fn truncation_marker_frame(root_hash: Hash) -> Vec<u8> {
    let mut marker = Vec::with_capacity(MARKER_TAG_SIZE + HASH_SIZE + MARKER_CHECKSUM_SIZE);
    marker.push(TAG_TRUNCATION_MARKER);
    marker.extend_from_slice(root_hash.as_bytes());
    marker.extend_from_slice(&crc32fast::hash(&marker).to_le_bytes());

    let mut frame = Vec::with_capacity(FRAME_LEN_SIZE + marker.len());
    frame.extend_from_slice(&(marker.len() as u32).to_le_bytes());
    frame.extend_from_slice(&marker);
    frame
}

fn parse_wal_file(bytes: &[u8]) -> Result<WalFileContents, WalError> {
    let mut cursor = ByteCursor::new(bytes);
    let mut entries = Vec::new();
    let mut committed_root = None;

    while !cursor.is_finished() {
        let frame = cursor.read_frame()?;
        if is_truncation_marker(frame) {
            committed_root = Some(parse_truncation_marker(frame)?);
        } else if super::promise::is_promise_payload(frame) {
            // Promise-state frames (AA-3-0) are coordination metadata, not data
            // entries or commit markers. This low-level file summary only reports
            // replayable data entries and the committed root, so skip them here;
            // promise recovery is handled by `WalRecovery`.
        } else {
            entries.push(WalEntry::deserialise(frame)?);
        }
    }

    Ok(WalFileContents {
        entries,
        committed_root,
    })
}

fn is_truncation_marker(frame: &[u8]) -> bool {
    frame.len() == MARKER_TAG_SIZE + HASH_SIZE + MARKER_CHECKSUM_SIZE
        && frame.first() == Some(&TAG_TRUNCATION_MARKER)
}

fn parse_truncation_marker(frame: &[u8]) -> Result<Hash, WalError> {
    if !is_truncation_marker(frame) {
        return Err(WalError::InvalidTag {
            found: frame.first().copied().unwrap_or_default(),
        });
    }
    let payload_len = MARKER_TAG_SIZE + HASH_SIZE;
    let (payload, checksum_bytes) = frame.split_at(payload_len);
    let expected = u32::from_le_bytes([
        checksum_bytes[0],
        checksum_bytes[1],
        checksum_bytes[2],
        checksum_bytes[3],
    ]);
    let actual = crc32fast::hash(payload);
    if expected != actual {
        return Err(WalError::ChecksumMismatch { expected, actual });
    }
    let mut bytes = [0; HASH_SIZE];
    bytes.copy_from_slice(&payload[MARKER_TAG_SIZE..]);
    Ok(Hash::from_bytes(bytes))
}

/// Atomically writes `bytes` to `path` via a temp file in the same directory.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), WalError> {
    let parent = parent_dir(path);
    fs::create_dir_all(parent)?;

    let mut temp_file = tempfile::Builder::new()
        .prefix(".wal-")
        .suffix(".tmp")
        .tempfile_in(parent)?;
    temp_file.write_all(bytes)?;
    temp_file.as_file_mut().sync_all()?;
    temp_file
        .persist(path)
        .map(drop)
        .map_err(|error| WalError::Io(error.error))?;

    sync_dir(parent)
}

/// Fsync the directory containing `path` so directory entry changes are durable.
fn sync_parent_dir(path: &Path) -> Result<(), WalError> {
    sync_dir(parent_dir(path))
}

fn parent_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

#[cfg(unix)]
fn sync_dir(parent: &Path) -> Result<(), WalError> {
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(parent: &Path) -> Result<(), WalError> {
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    Ok(())
}

#[derive(Debug)]
struct ByteCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ByteCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    const fn is_finished(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn read_frame(&mut self) -> Result<&'a [u8], WalError> {
        let len = usize::try_from(self.read_u32()?).map_err(|_| WalError::LengthOverflow)?;
        self.read_bytes(len)
    }

    fn read_u32(&mut self) -> Result<u32, WalError> {
        let bytes = self.read_bytes(FRAME_LEN_SIZE)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], WalError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(WalError::LengthOverflow)?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or(WalError::Truncated)?;
        self.offset = end;
        Ok(slice)
    }
}

#[cfg(test)]
#[path = "durable_tests.rs"]
mod tests;
