// CORE-006: WAL recovery replay from durable frames

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use crate::store::NodeStore;
use crate::tree::Hash;
use crate::tree::node::HASH_SIZE;

use super::buffer::{
    Mutation, RECOVERY_MALFORMED_PAYLOAD_EXPECTED, RECOVERY_UNKNOWN_TAG_EXPECTED, WalBuffer,
    WalError,
};
use super::entry::{TAG_DELETE, TAG_PUT};

const CHECKSUM_SIZE: usize = 4;
const FRAME_LEN_SIZE: usize = 4;
const TAG_COMMIT: u8 = 0x03;

/// Recovered state for one shard WAL.
///
/// Recovery is intentionally shard-local: this value contains only the committed
/// root marker and replayed mutations from one WAL file. Startup code can hand
/// the buffer to the shard actor and reopen the same WAL in append mode so new
/// writes follow the replayed frames without recovery rewriting the file.
#[derive(Debug)]
pub struct RecoveredWal {
    committed_root: Option<Hash>,
    buffer: WalBuffer,
    replayed_mutations: usize,
    stopped_at_corruption: bool,
}

impl RecoveredWal {
    fn empty() -> Self {
        Self {
            committed_root: None,
            buffer: WalBuffer::new(),
            replayed_mutations: 0,
            stopped_at_corruption: false,
        }
    }

    /// Last committed root hash read from a truncation marker, if one exists.
    pub const fn committed_root(&self) -> Option<Hash> {
        self.committed_root
    }

    /// Replayed in-memory WAL buffer containing mutations after the last marker.
    pub const fn buffer(&self) -> &WalBuffer {
        &self.buffer
    }

    /// Number of valid mutation frames replayed after the last truncation marker.
    pub const fn replayed_mutations(&self) -> usize {
        self.replayed_mutations
    }

    /// Whether replay stopped at a checksum-corrupted frame.
    pub const fn stopped_at_corruption(&self) -> bool {
        self.stopped_at_corruption
    }

    /// Consume the recovery result and return its replayed buffer.
    pub fn into_buffer(self) -> WalBuffer {
        self.buffer
    }
}

/// Read-only WAL recovery scanner.
///
/// Recovery owns an open read handle and never truncates, compacts, or appends to
/// the WAL. Replay reconstructs only the in-memory [`WalBuffer`]; the shard
/// actor remains responsible for tree state and root management.
#[derive(Debug)]
pub struct WalRecovery {
    file: File,
}

impl WalRecovery {
    /// Open an existing WAL file for read-only recovery.
    ///
    /// Missing files are surfaced as [`WalError::Io`] and the file is never
    /// created, truncated, or opened for append.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WalError> {
        let file = File::open(path)?;
        Ok(Self { file })
    }

    /// Recover one shard WAL from `path`, treating a missing WAL as a fresh
    /// database and verifying any committed root against `store`.
    pub fn recover_path<P, S>(path: P, store: &S) -> Result<RecoveredWal, WalError>
    where
        P: AsRef<Path>,
        S: NodeStore + ?Sized,
    {
        match Self::open(path.as_ref()) {
            Ok(mut recovery) => recovery.recover_with_store(store),
            Err(WalError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                Ok(RecoveredWal::empty())
            }
            Err(error) => Err(error),
        }
    }

    /// Replay one already-open shard WAL and verify any committed root against
    /// the node store before returning recovered state.
    pub fn recover_with_store<S>(&mut self, store: &S) -> Result<RecoveredWal, WalError>
    where
        S: NodeStore + ?Sized,
    {
        let recovered = self.recover_unverified()?;
        verify_committed_root(recovered.committed_root, store)?;
        Ok(recovered)
    }

    /// Read the next length-prefixed frame, validate its payload CRC32, and
    /// return the frame payload without the checksum bytes.
    ///
    /// `Ok(None)` is the sentinel for either clean EOF or a truncated tail. A
    /// complete frame with a bad checksum is a hard corruption error.
    pub fn read_frame(&mut self) -> Result<Option<Vec<u8>>, WalError> {
        let position = self.file.stream_position()?;
        let file_len = self.file.metadata()?.len();
        let remaining = file_len.saturating_sub(position);
        read_frame_payload(&mut self.file, remaining)
    }

    /// Replay durable WAL mutations after the last commit marker into recovered
    /// shard state without committed-root verification.
    ///
    /// Use [`Self::recover_with_store`] or [`Self::recover_path`] for normal
    /// startup so committed roots are verified before the shard accepts writes.
    pub fn recover_unverified(&mut self) -> Result<RecoveredWal, WalError> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut buffer = WalBuffer::new();
        let mut committed_root = None;
        let mut replayed_mutations = 0;
        let mut stopped_at_corruption = false;

        loop {
            let payload = match self.read_frame() {
                Ok(Some(payload)) => payload,
                Ok(None) => break,
                Err(error @ WalError::ChecksumMismatch { .. }) => {
                    log::warn!("wal recovery stopped at corrupted frame: {error}");
                    stopped_at_corruption = true;
                    break;
                }
                Err(error) => return Err(error),
            };

            match decode_payload(&payload) {
                Ok(RecoveredEntry::Mutation(Mutation::Put { key, value })) => {
                    buffer.put(key, value);
                    replayed_mutations += 1;
                }
                Ok(RecoveredEntry::Mutation(Mutation::Delete { key })) => {
                    buffer.delete(key);
                    replayed_mutations += 1;
                }
                Ok(RecoveredEntry::Commit(root)) => {
                    committed_root = Some(root);
                    buffer = WalBuffer::new();
                    replayed_mutations = 0;
                }
                Err(error @ WalError::ChecksumMismatch { .. }) => {
                    log::warn!("wal recovery stopped at corrupted payload: {error}");
                    stopped_at_corruption = true;
                    break;
                }
                Err(error) => return Err(error),
            }
        }

        log_recovery_complete(committed_root, replayed_mutations, stopped_at_corruption);
        Ok(RecoveredWal {
            committed_root,
            buffer,
            replayed_mutations,
            stopped_at_corruption,
        })
    }

    /// Compatibility helper returning only the replayed buffer.
    pub fn recover(&mut self, initial_root: Hash) -> Result<WalBuffer, WalError> {
        log::debug!("wal recovery buffer-only baseline root={initial_root}");
        self.recover_unverified().map(RecoveredWal::into_buffer)
    }
}

fn verify_committed_root<S>(root: Option<Hash>, store: &S) -> Result<(), WalError>
where
    S: NodeStore + ?Sized,
{
    let Some(root) = root else {
        return Ok(());
    };
    match store.get(&root) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(WalError::MissingCommittedRoot { root }),
        Err(error) => Err(WalError::TreeError(format!(
            "failed to verify committed root {root}: {error}"
        ))),
    }
}

fn log_recovery_complete(root: Option<Hash>, replayed_mutations: usize, corrupted_tail: bool) {
    match root {
        Some(root) => log::info!(
            "wal recovery completed: committed_root={root}, replayed_mutations={replayed_mutations}, corrupted_tail={corrupted_tail}"
        ),
        None => log::info!(
            "wal recovery completed: committed_root=<none>, replayed_mutations={replayed_mutations}, corrupted_tail={corrupted_tail}"
        ),
    }
}

fn read_frame_payload<R: Read>(
    reader: &mut R,
    remaining: u64,
) -> Result<Option<Vec<u8>>, WalError> {
    let mut len_bytes = [0; FRAME_LEN_SIZE];
    if read_exact_or_none(reader, &mut len_bytes)?.is_none() {
        return Ok(None);
    }

    let len =
        usize::try_from(u32::from_le_bytes(len_bytes)).map_err(|_| WalError::LengthOverflow)?;
    // PERSIST-003 hardening: never allocate from an untrusted on-disk length. A
    // frame longer than the bytes remaining after its length prefix cannot be
    // complete, so treat it as a truncated/garbage tail instead of allocating up
    // to ~4 GiB before the CRC check can run.
    let max_frame = remaining.saturating_sub(FRAME_LEN_SIZE as u64);
    if len as u64 > max_frame {
        return Ok(None);
    }
    let mut frame = vec![0; len];
    if read_exact_or_none(reader, &mut frame)?.is_none() {
        return Ok(None);
    }

    let Some(payload_len) = frame.len().checked_sub(CHECKSUM_SIZE) else {
        return Ok(None);
    };
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

    Ok(Some(payload.to_vec()))
}

fn read_exact_or_none<R: Read>(reader: &mut R, bytes: &mut [u8]) -> Result<Option<()>, WalError> {
    match reader.read_exact(bytes) {
        Ok(()) => Ok(Some(())),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(error) => Err(WalError::Io(error)),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RecoveredEntry {
    Mutation(Mutation),
    Commit(Hash),
}

fn decode_payload(payload: &[u8]) -> Result<RecoveredEntry, WalError> {
    let mut cursor = PayloadCursor::new(payload);
    let tag = cursor.read_u8().map_err(|_| malformed_payload())?;

    match tag {
        TAG_PUT => decode_put(&mut cursor),
        TAG_DELETE => decode_delete(&mut cursor),
        TAG_COMMIT => decode_commit(&mut cursor),
        found => Err(unknown_tag(found)),
    }
}

fn decode_put(cursor: &mut PayloadCursor<'_>) -> Result<RecoveredEntry, WalError> {
    let key = cursor
        .read_len_prefixed_bytes()
        .map_err(|_| malformed_payload())?
        .to_vec();
    let value = cursor
        .read_len_prefixed_bytes()
        .map_err(|_| malformed_payload())?
        .to_vec();
    cursor.finish().map_err(|_| malformed_payload())?;
    Ok(RecoveredEntry::Mutation(Mutation::Put { key, value }))
}

fn decode_delete(cursor: &mut PayloadCursor<'_>) -> Result<RecoveredEntry, WalError> {
    let key = cursor
        .read_len_prefixed_bytes()
        .map_err(|_| malformed_payload())?
        .to_vec();
    cursor.finish().map_err(|_| malformed_payload())?;
    Ok(RecoveredEntry::Mutation(Mutation::Delete { key }))
}

fn decode_commit(cursor: &mut PayloadCursor<'_>) -> Result<RecoveredEntry, WalError> {
    let bytes = cursor
        .read_bytes(HASH_SIZE)
        .map_err(|_| malformed_payload())?;
    let mut root = [0; HASH_SIZE];
    root.copy_from_slice(bytes);
    cursor.finish().map_err(|_| malformed_payload())?;
    Ok(RecoveredEntry::Commit(Hash::from_bytes(root)))
}

const fn unknown_tag(found: u8) -> WalError {
    WalError::ChecksumMismatch {
        expected: RECOVERY_UNKNOWN_TAG_EXPECTED,
        actual: found as u32,
    }
}

const fn malformed_payload() -> WalError {
    WalError::ChecksumMismatch {
        expected: RECOVERY_MALFORMED_PAYLOAD_EXPECTED,
        actual: 0,
    }
}

#[derive(Debug)]
struct PayloadCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> PayloadCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, WalError> {
        let byte = *self.bytes.get(self.offset).ok_or(WalError::Truncated)?;
        self.offset += 1;
        Ok(byte)
    }

    fn read_len_prefixed_bytes(&mut self) -> Result<&'a [u8], WalError> {
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
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(WalError::Truncated)?;
        self.offset = end;
        Ok(bytes)
    }

    const fn finish(&self) -> Result<(), WalError> {
        let trailing = self.bytes.len().saturating_sub(self.offset);
        if trailing == 0 {
            Ok(())
        } else {
            Err(WalError::TrailingBytes { trailing })
        }
    }
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;
