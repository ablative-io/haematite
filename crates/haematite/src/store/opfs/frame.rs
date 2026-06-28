//! Platform-neutral WAL framing over the shared entry codec (WASM-002 R2).
//!
//! This is the load-bearing portability layer: it reuses [`crate::wal::WalEntry`]
//! verbatim and reproduces the native durable WAL's outer
//! `[frame_len: u32 LE][entry bytes]` wrapper so that bytes produced here are
//! accepted by the native [`crate::wal::DurableWal`] reader and vice versa. It
//! compiles on every target so its byte-identity can be proven by native
//! `#[test]`s (see [`super::parity_tests`]).

use crate::wal::{WalEntry, WalError};

/// Width of the little-endian `u32` frame-length prefix, matching the native
/// durable WAL (`FRAME_LEN_SIZE`).
pub const FRAME_LEN_SIZE: usize = 4;

/// Wrap one already-serialised entry in the durable WAL frame:
/// `[frame_len: u32 LE][entry bytes]`.
///
/// `entry_bytes` is the output of [`WalEntry::serialise`]; this only adds the
/// length prefix the native writer adds in `write_entry_frame`, so the result
/// is byte-identical to a native WAL frame for the same entry.
#[must_use]
pub fn wrap(entry_bytes: &[u8]) -> Vec<u8> {
    let len = u32::try_from(entry_bytes.len()).unwrap_or(u32::MAX);
    let mut frame = Vec::with_capacity(FRAME_LEN_SIZE + entry_bytes.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(entry_bytes);
    frame
}

/// Serialise a [`WalEntry`] through the shared codec and frame it for append.
#[must_use]
pub fn frame_entry(entry: &WalEntry) -> Vec<u8> {
    wrap(&entry.serialise())
}

/// Decode a concatenation of WAL frames back into entries.
///
/// Each frame is `[frame_len: u32 LE][entry bytes]`; the entry bytes are
/// checksum-verified by the shared [`WalEntry::deserialise`]. This is the
/// inverse of repeated [`frame_entry`] and accepts files written by the
/// native durable WAL (data-entry frames only — commit markers and promise
/// frames are a native-recovery concern and out of scope here).
pub fn decode_entries(bytes: &[u8]) -> Result<Vec<WalEntry>, WalError> {
    let mut cursor = FrameCursor::new(bytes);
    let mut entries = Vec::new();
    while !cursor.is_finished() {
        let entry_bytes = cursor.read_frame()?;
        entries.push(WalEntry::deserialise(entry_bytes)?);
    }
    Ok(entries)
}

struct FrameCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> FrameCursor<'a> {
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
        let array: [u8; FRAME_LEN_SIZE] = bytes.try_into().map_err(|_| WalError::Truncated)?;
        Ok(u32::from_le_bytes(array))
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
mod tests {
    use super::{decode_entries, frame_entry};
    use crate::wal::WalEntry;

    #[test]
    fn frame_entry_matches_native_frame_layout() {
        // The frame must be [len: u32 LE][entry serialise()] — byte-for-byte the
        // native durable WAL `write_entry_frame` layout.
        let entry = WalEntry::put(b"key".to_vec(), b"value".to_vec());
        let entry_bytes = entry.serialise();
        let framed = frame_entry(&entry);

        let mut expected = (entry_bytes.len() as u32).to_le_bytes().to_vec();
        expected.extend_from_slice(&entry_bytes);
        assert_eq!(framed, expected);
        assert_eq!(&framed[..4], &(entry_bytes.len() as u32).to_le_bytes());
    }

    #[test]
    fn decode_entries_round_trips_a_single_put() -> Result<(), crate::wal::WalError> {
        let entry = WalEntry::put(b"alpha".to_vec(), b"beta".to_vec());
        let framed = frame_entry(&entry);
        let decoded = decode_entries(&framed)?;
        assert_eq!(decoded, vec![entry]);
        Ok(())
    }

    #[test]
    fn decode_entries_round_trips_mixed_sequence() -> Result<(), crate::wal::WalError> {
        let entries = vec![
            WalEntry::put(b"a".to_vec(), b"1".to_vec()),
            WalEntry::delete(b"b".to_vec()),
            WalEntry::put(b"c".to_vec(), b"3".to_vec()),
        ];
        let mut framed = Vec::new();
        for entry in &entries {
            framed.extend_from_slice(&frame_entry(entry));
        }
        let decoded = decode_entries(&framed)?;
        assert_eq!(decoded, entries);
        Ok(())
    }

    #[test]
    fn decode_entries_rejects_corrupted_checksum() {
        let entry = WalEntry::put(b"k".to_vec(), b"v".to_vec());
        let mut framed = frame_entry(&entry);
        // Flip the last byte (the entry CRC32) — decode must reject via the
        // shared codec rather than silently accept (R2/C13).
        if let Some(byte) = framed.last_mut() {
            *byte ^= 0xff;
        }
        assert!(matches!(
            decode_entries(&framed),
            Err(crate::wal::WalError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn decode_entries_rejects_truncated_frame() {
        let entry = WalEntry::put(b"k".to_vec(), b"v".to_vec());
        let framed = frame_entry(&entry);
        // Drop the final byte so the declared frame length overruns the buffer.
        let truncated = &framed[..framed.len() - 1];
        assert!(matches!(
            decode_entries(truncated),
            Err(crate::wal::WalError::Truncated)
        ));
    }
}
