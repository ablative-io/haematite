// CORE-006: WAL recovery replay from durable frames

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

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

    /// Read the next length-prefixed frame, validate its payload CRC32, and
    /// return the frame payload without the checksum bytes.
    ///
    /// `Ok(None)` is the sentinel for either clean EOF or a truncated tail. A
    /// complete frame with a bad checksum is a hard corruption error.
    pub fn read_frame(&mut self) -> Result<Option<Vec<u8>>, WalError> {
        read_frame_payload(&mut self.file)
    }

    /// Replay durable WAL mutations after the last commit marker into a fresh
    /// in-memory buffer.
    ///
    /// The provided root is the caller's tree baseline; this method does not
    /// inspect or mutate the prolly tree.
    pub fn recover(&mut self, initial_root: Hash) -> Result<WalBuffer, WalError> {
        // The shard actor owns the tree at this baseline root; recovery only
        // rebuilds the post-commit buffer and performs no tree lookups.
        let _ = initial_root;
        self.file.seek(SeekFrom::Start(0))?;
        let mut buffer = WalBuffer::new();

        while let Some(payload) = self.read_frame()? {
            match decode_payload(&payload)? {
                RecoveredEntry::Mutation(Mutation::Put { key, value }) => buffer.put(key, value),
                RecoveredEntry::Mutation(Mutation::Delete { key }) => buffer.delete(key),
                RecoveredEntry::Commit(_) => buffer = WalBuffer::new(),
            }
        }

        Ok(buffer)
    }
}

fn read_frame_payload<R: Read>(reader: &mut R) -> Result<Option<Vec<u8>>, WalError> {
    let mut len_bytes = [0; FRAME_LEN_SIZE];
    if read_exact_or_none(reader, &mut len_bytes)?.is_none() {
        return Ok(None);
    }

    let len =
        usize::try_from(u32::from_le_bytes(len_bytes)).map_err(|_| WalError::LengthOverflow)?;
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
