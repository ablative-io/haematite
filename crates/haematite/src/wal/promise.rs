//! Step-3 (AA-3-0) durable promise-state WAL frame.
//!
//! [`PromiseRecord`] is the per-shard ownership/promise metadata that must
//! survive a crash and be fsync'd BEFORE it is acted on (reply Promise, serve as
//! owner, send Prepare) — see `docs/ACTIVE-ACTIVE-STEP3-EPOCH-FENCE-DESIGN.md` §3.
//! It rides the SAME WAL file (and therefore the same fsync domain) as the
//! committed-root marker, so ownership durability shares the data durability path
//! with no second store.
//!
//! A record carries a full snapshot of all three values:
//!
//! - `promised` — highest ballot this node promised in a Prepare (§2.2).
//! - `owner_epoch` — ballot under which this node was elected owner, if any.
//! - `persisted_max_minted` — highest ballot counter this node ever minted (R4).
//!
//! Because every mutator rewrites the full snapshot, recovery simply keeps the
//! LAST promise frame seen — that reconstructs the latest of each value with no
//! per-field merge. The frame is exempt from the data fence (§2.5): it is
//! coordination metadata, not a data key, so it never regresses through the
//! epoch fence.
//!
//! Wire layout of the CRC32-covered payload:
//!
//! ```text
//! [TAG_PROMISE: u8]
//! [promised.counter: u64 LE]
//! [promised.node_len: u32 LE][promised.node bytes]
//! [has_owner_epoch: u8 (0|1)]
//! ( if 1: [owner.counter: u64 LE][owner.node_len: u32 LE][owner.node bytes] )
//! [persisted_max_minted: u64 LE]
//! ```
//!
//! The frame as written to the file appends `[crc32: u32 LE]` over that payload
//! and is wrapped in the durable WAL's outer `[frame_len: u32 LE]` prefix, so it
//! shares the exact framing/checksum discipline of every other WAL frame.

use crate::sync::ballot::Ballot;
use crate::sync::topology::SyncNodeId;

use super::buffer::WalError;

/// Tag byte for a promise-state frame. Distinct from `TAG_PUT` (0x01),
/// `TAG_DELETE` (0x02), and the truncation/commit marker (0x03).
pub(crate) const TAG_PROMISE: u8 = 0x04;

const U32_SIZE: usize = 4;
const U64_SIZE: usize = 8;

/// A full snapshot of one shard's durable promise state (§3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromiseRecord {
    /// Highest ballot this node has promised in a Prepare.
    pub promised: Ballot,
    /// Ballot under which this node was elected owner, if any.
    pub owner_epoch: Option<Ballot>,
    /// Highest ballot counter this node ever minted, fsync'd before any Prepare.
    pub persisted_max_minted: u64,
}

impl PromiseRecord {
    /// The default promise state for a shard with no persisted record: bottom
    /// `(0, "")` / no owner / `0` (§2.1 / §3).
    #[must_use]
    pub fn initial() -> Self {
        Self {
            promised: Ballot::bottom(),
            owner_epoch: None,
            persisted_max_minted: 0,
        }
    }

    /// Serialise the CRC-covered payload (without the outer frame-length prefix,
    /// which the durable writer adds). The trailing CRC32 is appended here so the
    /// returned bytes match the on-disk frame body byte-for-byte.
    #[must_use]
    pub fn serialise(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(TAG_PROMISE);
        append_ballot(&mut payload, &self.promised);
        match &self.owner_epoch {
            Some(owner) => {
                payload.push(1);
                append_ballot(&mut payload, owner);
            }
            None => payload.push(0),
        }
        payload.extend_from_slice(&self.persisted_max_minted.to_le_bytes());

        let crc = crc32fast::hash(&payload);
        payload.extend_from_slice(&crc.to_le_bytes());
        payload
    }

    /// Decode a promise frame payload (CRC already validated by the caller's
    /// frame reader, which strips the trailing checksum). `payload` therefore
    /// begins at [`TAG_PROMISE`] and ends at `persisted_max_minted`.
    pub(crate) fn decode(payload: &[u8]) -> Result<Self, WalError> {
        let mut cursor = Cursor::new(payload);
        let tag = cursor.read_u8()?;
        if tag != TAG_PROMISE {
            return Err(WalError::InvalidTag { found: tag });
        }
        let promised = read_ballot(&mut cursor)?;
        let owner_epoch = match cursor.read_u8()? {
            0 => None,
            1 => Some(read_ballot(&mut cursor)?),
            found => return Err(WalError::InvalidTag { found }),
        };
        let persisted_max_minted = cursor.read_u64()?;
        cursor.finish()?;
        Ok(Self {
            promised,
            owner_epoch,
            persisted_max_minted,
        })
    }
}

/// True when `payload` is a promise frame (first byte is [`TAG_PROMISE`]).
pub(crate) fn is_promise_payload(payload: &[u8]) -> bool {
    payload.first() == Some(&TAG_PROMISE)
}

fn append_ballot(bytes: &mut Vec<u8>, ballot: &Ballot) {
    bytes.extend_from_slice(&ballot.counter.to_le_bytes());
    let node = ballot.node.as_str().as_bytes();
    bytes.extend_from_slice(&(node.len() as u32).to_le_bytes());
    bytes.extend_from_slice(node);
}

fn read_ballot(cursor: &mut Cursor<'_>) -> Result<Ballot, WalError> {
    let counter = cursor.read_u64()?;
    let node_bytes = cursor.read_len_prefixed()?;
    let node = std::str::from_utf8(node_bytes)
        .map_err(|_| WalError::TreeError("promise node id is not valid utf-8".to_owned()))?;
    Ok(Ballot::new(counter, SyncNodeId::new(node)))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, WalError> {
        let byte = *self.bytes.get(self.offset).ok_or(WalError::Truncated)?;
        self.offset += 1;
        Ok(byte)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], WalError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(WalError::LengthOverflow)?;
        let slice = self.bytes.get(self.offset..end).ok_or(WalError::Truncated)?;
        self.offset = end;
        Ok(slice)
    }

    fn read_u32(&mut self) -> Result<u32, WalError> {
        let bytes = self.read_bytes(U32_SIZE)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> Result<u64, WalError> {
        let bytes = self.read_bytes(U64_SIZE)?;
        let mut raw = [0u8; U64_SIZE];
        raw.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(raw))
    }

    fn read_len_prefixed(&mut self) -> Result<&'a [u8], WalError> {
        let len = usize::try_from(self.read_u32()?).map_err(|_| WalError::LengthOverflow)?;
        self.read_bytes(len)
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
mod tests {
    use super::{PromiseRecord, is_promise_payload};
    use crate::sync::ballot::Ballot;
    use crate::sync::topology::SyncNodeId;

    fn ballot(counter: u64, node: &str) -> Ballot {
        Ballot::new(counter, SyncNodeId::from(node))
    }

    /// Strip the trailing CRC32 the way the frame reader does, leaving the
    /// payload `decode` expects.
    fn payload_of(record: &PromiseRecord) -> Vec<u8> {
        let mut bytes = record.serialise();
        bytes.truncate(bytes.len() - 4);
        bytes
    }

    #[test]
    fn round_trips_full_state() -> Result<(), super::WalError> {
        let record = PromiseRecord {
            promised: ballot(5, "node-x"),
            owner_epoch: Some(ballot(5, "node-x")),
            persisted_max_minted: 9,
        };
        assert_eq!(PromiseRecord::decode(&payload_of(&record))?, record);
        Ok(())
    }

    #[test]
    fn round_trips_without_owner_epoch() -> Result<(), super::WalError> {
        let record = PromiseRecord {
            promised: ballot(3, "node-y"),
            owner_epoch: None,
            persisted_max_minted: 3,
        };
        assert_eq!(PromiseRecord::decode(&payload_of(&record))?, record);
        Ok(())
    }

    #[test]
    fn initial_is_bottom_none_zero() {
        let initial = PromiseRecord::initial();
        assert_eq!(initial.promised, Ballot::bottom());
        assert_eq!(initial.owner_epoch, None);
        assert_eq!(initial.persisted_max_minted, 0);
    }

    #[test]
    fn promise_payload_is_distinguishable() {
        assert!(is_promise_payload(&payload_of(&PromiseRecord::initial())));
        assert!(!is_promise_payload(&[0x01, 0, 0]));
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let mut payload = payload_of(&PromiseRecord::initial());
        payload.push(0xaa);
        assert!(matches!(
            PromiseRecord::decode(&payload),
            Err(super::WalError::TrailingBytes { .. })
        ));
    }
}
