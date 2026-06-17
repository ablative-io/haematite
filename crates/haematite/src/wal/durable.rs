// CORE-005: Durable WAL writer (append-only file)

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

use crate::tree::Hash;

use super::buffer::{Mutation, WalError};

/// Frame tag for a buffered `Put`.
const TAG_PUT: u8 = 0x01;
/// Frame tag for a buffered `Delete`.
const TAG_DELETE: u8 = 0x02;
/// Frame tag for a commit marker recording the durable root hash.
const TAG_COMMIT: u8 = 0x03;

/// Append-only, crash-safe WAL writer (ADR-003).
///
/// Each `append` or `write_commit` writes one self-describing frame —
/// `[payload length: u32 LE][payload][CRC32 of payload: u32 LE]` — and fsyncs
/// before returning, so a mutation is durable before it ever reaches the
/// in-memory buffer (C26). Frames are written one at a time and never buffered
/// or batched. WAL entries are uncompressed for append speed (ADR-006).
#[derive(Debug)]
pub struct DurableWal {
    file: File,
}

impl DurableWal {
    /// Open (or create) the WAL file at `path` in append mode.
    ///
    /// An existing file is opened without truncation so prior frames survive.
    /// When the file is newly created, the parent directory is fsynced so the
    /// new directory entry — not just the file's contents — survives a crash.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WalError> {
        let path = path.as_ref();
        let existed = path.exists();
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        if !existed {
            sync_parent_dir(path)?;
        }
        Ok(Self { file })
    }

    /// Append one mutation as a durable frame, fsyncing before returning.
    pub fn append(&mut self, mutation: &Mutation) -> Result<(), WalError> {
        let payload = serialise_mutation(mutation);
        self.write_frame(&payload)
    }

    /// Append a commit marker (tag `0x03` + 32-byte root hash) as a durable
    /// frame. CORE-006 reads this marker during recovery to locate the last
    /// committed state; CORE-007 writes it after each tree flush.
    pub fn write_commit(&mut self, root_hash: Hash) -> Result<(), WalError> {
        let mut payload = Vec::with_capacity(1 + root_hash.as_bytes().len());
        payload.push(TAG_COMMIT);
        payload.extend_from_slice(root_hash.as_bytes());
        self.write_frame(&payload)
    }

    /// Write `[len][payload][crc32]` as a single contiguous frame and fsync.
    fn write_frame(&mut self, payload: &[u8]) -> Result<(), WalError> {
        let checksum = crc32fast::hash(payload);
        let mut frame = Vec::with_capacity(4 + payload.len() + 4);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(payload);
        frame.extend_from_slice(&checksum.to_le_bytes());

        self.file.write_all(&frame)?;
        self.file.sync_all()?;
        Ok(())
    }
}

/// Fsync the directory containing `path` so a newly created file's directory
/// entry is durable. An empty parent (a bare filename) resolves to `.`.
fn sync_parent_dir(path: &Path) -> Result<(), WalError> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    File::open(parent)?.sync_all()?;
    Ok(())
}

/// Serialise a mutation payload (without the outer length/CRC frame).
///
/// `Put`  → `[0x01][key len: u32 LE][key][value len: u32 LE][value]`
/// `Delete` → `[0x02][key len: u32 LE][key]`
fn serialise_mutation(mutation: &Mutation) -> Vec<u8> {
    match mutation {
        Mutation::Put { key, value } => {
            let mut payload = Vec::with_capacity(1 + 4 + key.len() + 4 + value.len());
            payload.push(TAG_PUT);
            payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
            payload.extend_from_slice(key);
            payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
            payload.extend_from_slice(value);
            payload
        }
        Mutation::Delete { key } => {
            let mut payload = Vec::with_capacity(1 + 4 + key.len());
            payload.push(TAG_DELETE);
            payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
            payload.extend_from_slice(key);
            payload
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DurableWal, TAG_COMMIT, TAG_DELETE, TAG_PUT, serialise_mutation};
    use crate::tree::Hash;
    use crate::wal::buffer::{Mutation, WalError};
    use std::path::PathBuf;

    fn temp_path(name: &str) -> Result<(tempfile::TempDir, PathBuf), WalError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join(name);
        Ok((dir, path))
    }

    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(payload);
        bytes.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
        bytes
    }

    #[test]
    fn open_creates_file_when_missing() -> Result<(), WalError> {
        let (_dir, path) = temp_path("create.wal")?;
        assert!(!path.exists());
        let _wal = DurableWal::open(&path)?;
        assert!(path.exists());
        Ok(())
    }

    #[test]
    fn open_existing_appends_without_truncating() -> Result<(), WalError> {
        let (_dir, path) = temp_path("append.wal")?;
        {
            let mut wal = DurableWal::open(&path)?;
            wal.append(&Mutation::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            })?;
        }
        let first_len = std::fs::read(&path)?.len();
        {
            let mut wal = DurableWal::open(&path)?;
            wal.append(&Mutation::Delete { key: b"k".to_vec() })?;
        }
        let total_len = std::fs::read(&path)?.len();
        assert!(total_len > first_len);
        Ok(())
    }

    #[test]
    fn put_frame_has_length_payload_and_crc() -> Result<(), WalError> {
        let (_dir, path) = temp_path("put.wal")?;
        let mutation = Mutation::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        {
            let mut wal = DurableWal::open(&path)?;
            wal.append(&mutation)?;
        }
        let bytes = std::fs::read(&path)?;

        let payload = serialise_mutation(&mutation);
        // 4-byte length + payload + 4-byte CRC32.
        assert_eq!(bytes.len(), 4 + payload.len() + 4);
        assert_eq!(&bytes[0..4], &(payload.len() as u32).to_le_bytes());
        assert_eq!(&bytes[4..4 + payload.len()], payload.as_slice());
        let crc = &bytes[4 + payload.len()..];
        assert_eq!(crc, &crc32fast::hash(&payload).to_le_bytes());
        Ok(())
    }

    #[test]
    fn put_payload_layout_is_tagged_and_length_prefixed() {
        let payload = serialise_mutation(&Mutation::Put {
            key: b"ab".to_vec(),
            value: b"xyz".to_vec(),
        });
        let mut expected = vec![TAG_PUT];
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(b"ab");
        expected.extend_from_slice(&3u32.to_le_bytes());
        expected.extend_from_slice(b"xyz");
        assert_eq!(payload, expected);
    }

    #[test]
    fn delete_payload_layout_is_tagged_and_length_prefixed() {
        let payload = serialise_mutation(&Mutation::Delete {
            key: b"ab".to_vec(),
        });
        let mut expected = vec![TAG_DELETE];
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(b"ab");
        assert_eq!(payload, expected);
    }

    #[test]
    fn two_appends_are_consecutive_frames_with_no_gaps() -> Result<(), WalError> {
        let (_dir, path) = temp_path("two.wal")?;
        let first = Mutation::Put {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
        };
        let second = Mutation::Delete {
            key: b"k2".to_vec(),
        };
        {
            let mut wal = DurableWal::open(&path)?;
            wal.append(&first)?;
            wal.append(&second)?;
        }
        let bytes = std::fs::read(&path)?;

        let mut expected = frame(&serialise_mutation(&first));
        expected.extend_from_slice(&frame(&serialise_mutation(&second)));
        assert_eq!(bytes, expected);
        Ok(())
    }

    #[test]
    fn write_commit_frames_tag_and_root_hash() -> Result<(), WalError> {
        let (_dir, path) = temp_path("commit.wal")?;
        let root = Hash::from_bytes([7; 32]);
        {
            let mut wal = DurableWal::open(&path)?;
            wal.write_commit(root)?;
        }
        let bytes = std::fs::read(&path)?;

        let mut payload = vec![TAG_COMMIT];
        payload.extend_from_slice(root.as_bytes());
        assert_eq!(payload.len(), 33);
        assert_eq!(bytes, frame(&payload));
        Ok(())
    }

    #[test]
    fn durable_wal_is_debug() -> Result<(), WalError> {
        let (_dir, path) = temp_path("debug.wal")?;
        let wal = DurableWal::open(&path)?;
        assert!(format!("{wal:?}").contains("DurableWal"));
        Ok(())
    }
}
