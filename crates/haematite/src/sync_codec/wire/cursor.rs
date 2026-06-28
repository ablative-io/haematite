//! Bounds-checked reader for the sync wire codec.

use std::time::Duration;

use crate::ids::ShardId;
use crate::sync_codec::ballot::{Ballot, Stamp};
use crate::sync_codec::error::SyncError;
use crate::sync_codec::ids::SyncNodeId;
use crate::sync_codec::message::write::{AckOutcome, RejectReason};
use crate::sync_codec::message::WriteId;
use crate::sync_codec::target::TargetNodeSummary;
use crate::tree::Hash;

use super::{ACK_OUTCOME_APPLIED, ACK_OUTCOME_REJECTED, WIRE_USIZE_BYTES, clamp_capacity};

pub(crate) struct MessageCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> MessageCursor<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    /// Bytes not yet consumed — used to bound pre-allocation against a hostile
    /// element count.
    pub(crate) const fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    pub(crate) fn read_u8(&mut self) -> Result<u8, SyncError> {
        let bytes = self.read_exact(1)?;
        bytes.first().copied().ok_or(SyncError::InvalidMessage)
    }

    /// Read a single boolean byte (`0` or `1`); any other value is rejected so a
    /// malformed frame fails closed (AA-3-4b tombstone flag).
    pub(crate) fn read_bool(&mut self) -> Result<bool, SyncError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(SyncError::InvalidMessage),
        }
    }

    pub(crate) fn read_u32_as_usize(&mut self) -> Result<usize, SyncError> {
        let bytes = self.read_exact(4)?;
        let mut value = [0_u8; 4];
        value.copy_from_slice(bytes);
        usize::try_from(u32::from_be_bytes(value)).map_err(|_error| SyncError::InvalidMessage)
    }

    pub(crate) fn read_u32(&mut self) -> Result<u32, SyncError> {
        let bytes = self.read_exact(4)?;
        let mut value = [0_u8; 4];
        value.copy_from_slice(bytes);
        Ok(u32::from_be_bytes(value))
    }

    pub(crate) fn read_u64(&mut self) -> Result<u64, SyncError> {
        let bytes = self.read_exact(8)?;
        let mut value = [0_u8; 8];
        value.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(value))
    }

    pub(crate) fn read_usize(&mut self) -> Result<usize, SyncError> {
        let bytes = self.read_exact(8)?;
        let mut value = [0_u8; 8];
        value.copy_from_slice(bytes);
        usize::try_from(u64::from_be_bytes(value)).map_err(|_error| SyncError::InvalidMessage)
    }

    pub(crate) fn read_shard_id(&mut self) -> Result<ShardId, SyncError> {
        self.read_usize()
    }

    pub(crate) fn read_optional_hash(&mut self) -> Result<Option<Hash>, SyncError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_hash().map(Some),
            _ => Err(SyncError::InvalidMessage),
        }
    }

    pub(crate) fn read_optional_target_summary(
        &mut self,
    ) -> Result<Option<TargetNodeSummary>, SyncError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(TargetNodeSummary::Leaf)),
            2 => {
                let child_count = self.read_usize()?;
                // `child_count` is attacker-controlled; clamp the pre-allocation
                // to what the remaining bytes could hold (each child needs >= a
                // u64 separator-length prefix and a 32-byte hash).
                let mut children = Vec::with_capacity(clamp_capacity(
                    child_count,
                    self.remaining(),
                    WIRE_USIZE_BYTES + 32,
                ));
                for _ in 0..child_count {
                    let separator = self.read_len_prefixed_bytes()?;
                    let hash = self.read_hash()?;
                    children.push((separator, hash));
                }
                Ok(Some(TargetNodeSummary::Internal(children)))
            }
            _ => Err(SyncError::InvalidMessage),
        }
    }

    pub(crate) fn read_len_prefixed_bytes(&mut self) -> Result<Vec<u8>, SyncError> {
        let len = self.read_usize()?;
        self.read_exact(len).map(<[u8]>::to_vec)
    }

    pub(crate) fn read_sync_node_id(&mut self) -> Result<SyncNodeId, SyncError> {
        let bytes = self.read_len_prefixed_bytes()?;
        let name = String::from_utf8(bytes).map_err(|_error| SyncError::InvalidMessage)?;
        Ok(SyncNodeId::new(name))
    }

    pub(crate) fn read_ballot(&mut self) -> Result<Ballot, SyncError> {
        let counter = self.read_u64()?;
        let node = self.read_sync_node_id()?;
        Ok(Ballot::new(counter, node))
    }

    pub(crate) fn read_stamp(&mut self) -> Result<Stamp, SyncError> {
        let epoch = self.read_ballot()?;
        let seq = self.read_u64()?;
        Ok(Stamp::new(epoch, seq))
    }

    pub(crate) fn read_optional_ballot(&mut self) -> Result<Option<Ballot>, SyncError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_ballot().map(Some),
            _ => Err(SyncError::InvalidMessage),
        }
    }

    pub(crate) fn read_write_id(&mut self) -> Result<WriteId, SyncError> {
        let origin = self.read_sync_node_id()?;
        let origin_creation = self.read_u32()?;
        let counter = self.read_u64()?;
        Ok(WriteId {
            origin,
            origin_creation,
            counter,
        })
    }

    pub(crate) fn read_optional_duration(&mut self) -> Result<Option<Duration>, SyncError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => {
                let secs = self.read_u64()?;
                let nanos = self.read_u32()?;
                // Reject denormalized sub-second nanos so the `Duration::new`
                // carry can never overflow `secs` and panic on a hostile buffer.
                if nanos >= 1_000_000_000 {
                    return Err(SyncError::InvalidMessage);
                }
                Ok(Some(Duration::new(secs, nanos)))
            }
            _ => Err(SyncError::InvalidMessage),
        }
    }

    pub(crate) fn read_ack_outcome(&mut self) -> Result<AckOutcome, SyncError> {
        match self.read_u8()? {
            ACK_OUTCOME_APPLIED => Ok(AckOutcome::Applied),
            ACK_OUTCOME_REJECTED => {
                let reason = RejectReason::from_wire(self.read_u8()?)?;
                Ok(AckOutcome::Rejected(reason))
            }
            _ => Err(SyncError::InvalidMessage),
        }
    }

    pub(crate) fn read_hash(&mut self) -> Result<Hash, SyncError> {
        let bytes = self.read_exact(32)?;
        let mut hash = [0_u8; 32];
        hash.copy_from_slice(bytes);
        Ok(Hash::from_bytes(hash))
    }

    pub(crate) fn read_exact(&mut self, len: usize) -> Result<&'a [u8], SyncError> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(SyncError::InvalidMessage)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(SyncError::InvalidMessage)?;
        self.position = end;
        Ok(bytes)
    }

    pub(crate) const fn finish(&self) -> Result<(), SyncError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(SyncError::InvalidMessage)
        }
    }
}
