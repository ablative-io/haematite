//! Binary persistence primitives shared by branch metadata stores.
//!
//! Branch metadata (the snapshot registry, the commit log, and — in later
//! briefs — branch handles) is small, append-mostly, and content-agnostic. This
//! module provides a length-prefixed little-endian codec ([`Reader`],
//! [`push_u64`], [`push_bytes`]) and an atomic file writer ([`write_atomic`])
//! that those stores build on, so each store only defines its own record shape.

use std::fmt;
use std::fs;
use std::io::Write;
use std::path::Path;

use crate::tree::Hash;
use crate::tree::node::HASH_SIZE;

const U64_LEN: usize = 8;

/// Error decoding or persisting a branch-metadata file.
#[derive(Debug)]
pub enum CodecError {
    /// The persisted bytes could not be decoded.
    Corrupt(String),
    /// An I/O error occurred while persisting.
    Io(std::io::Error),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Corrupt(reason) => write!(f, "branch metadata corrupted: {reason}"),
            Self::Io(error) => write!(f, "branch metadata I/O error: {error}"),
        }
    }
}

impl std::error::Error for CodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Corrupt(_) => None,
        }
    }
}

/// Appends `value` to `buffer` as little-endian bytes.
pub fn push_u64(buffer: &mut Vec<u8>, value: u64) {
    buffer.extend_from_slice(&value.to_le_bytes());
}

/// Appends `bytes` to `buffer`, prefixed by its length.
pub fn push_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) {
    push_u64(buffer, bytes.len() as u64);
    buffer.extend_from_slice(bytes);
}

/// A forward-only reader over a persisted metadata file.
pub struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    /// Wraps `bytes` for sequential decoding.
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    /// Consumes and verifies a fixed file-header magic.
    pub fn expect_magic(&mut self, magic: [u8; 4]) -> Result<(), CodecError> {
        let found = self.read_exact(magic.len())?;
        if found == magic.as_slice() {
            Ok(())
        } else {
            Err(CodecError::Corrupt("unrecognised file header".into()))
        }
    }

    /// Reads a little-endian `u64`.
    pub fn read_u64(&mut self) -> Result<u64, CodecError> {
        let bytes = self.read_exact(U64_LEN)?;
        let array: [u8; U64_LEN] = bytes
            .try_into()
            .map_err(|_error| CodecError::Corrupt("truncated integer".into()))?;
        Ok(u64::from_le_bytes(array))
    }

    /// Reads a length stored as a `u64`, narrowing to `usize`.
    pub fn read_usize(&mut self) -> Result<usize, CodecError> {
        let value = self.read_u64()?;
        usize::try_from(value)
            .map_err(|_error| CodecError::Corrupt("length exceeds platform width".into()))
    }

    /// Reads a length-prefixed byte string.
    pub fn read_bytes(&mut self) -> Result<Vec<u8>, CodecError> {
        let len = self.read_usize()?;
        Ok(self.read_exact(len)?.to_vec())
    }

    /// Reads a 32-byte content hash.
    pub fn read_hash(&mut self) -> Result<Hash, CodecError> {
        let bytes = self.read_exact(HASH_SIZE)?;
        let array: [u8; HASH_SIZE] = bytes
            .try_into()
            .map_err(|_error| CodecError::Corrupt("truncated hash".into()))?;
        Ok(Hash::from_bytes(array))
    }

    /// Confirms the whole input was consumed.
    pub fn finish(&self) -> Result<(), CodecError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(CodecError::Corrupt("trailing bytes after entries".into()))
        }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| CodecError::Corrupt("length overflow".into()))?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| CodecError::Corrupt("unexpected end of file".into()))?;
        self.offset = end;
        Ok(slice)
    }
}

/// Atomically writes `bytes` to `path` via a temp file in the same directory.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), CodecError> {
    let parent = path
        .parent()
        .ok_or_else(|| CodecError::Corrupt("metadata path has no parent directory".into()))?;
    fs::create_dir_all(parent).map_err(CodecError::Io)?;

    let mut temp_file = tempfile::Builder::new()
        .prefix(".branch-")
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(CodecError::Io)?;
    temp_file.write_all(bytes).map_err(CodecError::Io)?;
    temp_file.as_file_mut().sync_all().map_err(CodecError::Io)?;
    temp_file
        .persist(path)
        .map(drop)
        .map_err(|error| CodecError::Io(error.error))
}
