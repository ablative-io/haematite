//! Binary WAL entry codec with CRC32 validation.

use super::buffer::{Mutation, WalError};

pub(crate) const TAG_PUT: u8 = 0x01;
pub(crate) const TAG_DELETE: u8 = 0x02;

const U32_SIZE: usize = 4;

/// The operation encoded by a WAL entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationType {
    /// Store `value` at `key`.
    Put,
    /// Remove `key`.
    Delete,
}

impl OperationType {
    pub(crate) const fn tag(self) -> u8 {
        match self {
            Self::Put => TAG_PUT,
            Self::Delete => TAG_DELETE,
        }
    }

    const fn from_tag(tag: u8) -> Result<Self, WalError> {
        match tag {
            TAG_PUT => Ok(Self::Put),
            TAG_DELETE => Ok(Self::Delete),
            found => Err(WalError::InvalidTag { found }),
        }
    }
}

/// A length-prefixed, checksummed write-ahead-log entry.
///
/// The deterministic payload used for the CRC is:
///
/// - `Put`: `[operation_type][key_len: u32 LE][key][value_len: u32 LE][value]`
/// - `Delete`: `[operation_type][key_len: u32 LE][key]`
///
/// The serialised entry appends `[crc32: u32 LE]`, where the checksum covers the
/// payload bytes above, including the length prefixes that make the binary
/// representation deterministic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalEntry {
    /// Store `value` under `key`.
    Put {
        operation_type: OperationType,
        key: Vec<u8>,
        value: Vec<u8>,
        crc32: u32,
    },
    /// Delete `key`.
    Delete {
        operation_type: OperationType,
        key: Vec<u8>,
        crc32: u32,
    },
}

impl WalEntry {
    /// Build a `Put` entry and compute its CRC32 checksum.
    pub fn put<K, V>(key: K, value: V) -> Self
    where
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        let key = key.into();
        let value = value.into();
        let operation_type = OperationType::Put;
        let crc32 = checksum_for(operation_type, &key, Some(&value));
        Self::Put {
            operation_type,
            key,
            value,
            crc32,
        }
    }

    /// Build a `Delete` entry and compute its CRC32 checksum.
    pub fn delete<K>(key: K) -> Self
    where
        K: Into<Vec<u8>>,
    {
        let key = key.into();
        let operation_type = OperationType::Delete;
        let crc32 = checksum_for(operation_type, &key, None);
        Self::Delete {
            operation_type,
            key,
            crc32,
        }
    }

    /// Serialise this entry as `[payload][crc32: u32 LE]`.
    #[must_use]
    pub fn serialise(&self) -> Vec<u8> {
        let mut bytes = self.payload_bytes();
        bytes.extend_from_slice(&self.crc32().to_le_bytes());
        bytes
    }

    /// Deserialise and checksum-verify a WAL entry.
    pub fn deserialise(bytes: &[u8]) -> Result<Self, WalError> {
        let payload_len = bytes
            .len()
            .checked_sub(U32_SIZE)
            .ok_or(WalError::Truncated)?;
        let (payload, checksum_bytes) = bytes.split_at(payload_len);
        let expected = read_u32_le(checksum_bytes)?;
        let actual = crc32fast::hash(payload);
        if expected != actual {
            return Err(WalError::ChecksumMismatch { expected, actual });
        }

        let mut cursor = ByteCursor::new(payload);
        let operation_type = OperationType::from_tag(cursor.read_u8()?)?;
        match operation_type {
            OperationType::Put => {
                let key = cursor.read_len_prefixed_bytes()?.to_vec();
                let value = cursor.read_len_prefixed_bytes()?.to_vec();
                cursor.finish()?;
                Ok(Self::Put {
                    operation_type,
                    key,
                    value,
                    crc32: expected,
                })
            }
            OperationType::Delete => {
                let key = cursor.read_len_prefixed_bytes()?.to_vec();
                cursor.finish()?;
                Ok(Self::Delete {
                    operation_type,
                    key,
                    crc32: expected,
                })
            }
        }
    }

    /// Return this entry's operation type field.
    #[must_use]
    pub const fn operation_type(&self) -> OperationType {
        match self {
            Self::Put { operation_type, .. } | Self::Delete { operation_type, .. } => {
                *operation_type
            }
        }
    }

    /// Return this entry's key bytes.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key, .. } => key,
        }
    }

    /// Return this entry's value bytes, or `None` for deletes.
    #[must_use]
    pub fn value(&self) -> Option<&[u8]> {
        match self {
            Self::Put { value, .. } => Some(value),
            Self::Delete { .. } => None,
        }
    }

    /// Return the checksum stored in this entry.
    #[must_use]
    pub const fn crc32(&self) -> u32 {
        match self {
            Self::Put { crc32, .. } | Self::Delete { crc32, .. } => *crc32,
        }
    }

    /// Recompute the checksum from the deterministic payload fields.
    #[must_use]
    pub fn computed_crc32(&self) -> u32 {
        crc32fast::hash(&self.payload_bytes())
    }

    fn payload_bytes(&self) -> Vec<u8> {
        match self {
            Self::Put {
                operation_type,
                key,
                value,
                ..
            } => payload_bytes(*operation_type, key, Some(value)),
            Self::Delete {
                operation_type,
                key,
                ..
            } => payload_bytes(*operation_type, key, None),
        }
    }
}

impl From<&Mutation> for WalEntry {
    fn from(mutation: &Mutation) -> Self {
        match mutation {
            Mutation::Put { key, value } => Self::put(key.clone(), value.clone()),
            Mutation::Delete { key } => Self::delete(key.clone()),
        }
    }
}

impl From<Mutation> for WalEntry {
    fn from(mutation: Mutation) -> Self {
        match mutation {
            Mutation::Put { key, value } => Self::put(key, value),
            Mutation::Delete { key } => Self::delete(key),
        }
    }
}

fn checksum_for(operation_type: OperationType, key: &[u8], value: Option<&[u8]>) -> u32 {
    crc32fast::hash(&payload_bytes(operation_type, key, value))
}

fn payload_bytes(operation_type: OperationType, key: &[u8], value: Option<&[u8]>) -> Vec<u8> {
    let value_len = value.map_or(0, <[u8]>::len);
    let mut bytes = Vec::with_capacity(1 + U32_SIZE + key.len() + U32_SIZE + value_len);
    bytes.push(operation_type.tag());
    append_len(&mut bytes, key.len());
    bytes.extend_from_slice(key);
    if let Some(value) = value {
        append_len(&mut bytes, value.len());
        bytes.extend_from_slice(value);
    }
    bytes
}

fn append_len(bytes: &mut Vec<u8>, len: usize) {
    bytes.extend_from_slice(&(len as u32).to_le_bytes());
}

fn read_u32_le(bytes: &[u8]) -> Result<u32, WalError> {
    if bytes.len() != U32_SIZE {
        return Err(WalError::Truncated);
    }
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
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

    fn read_u8(&mut self) -> Result<u8, WalError> {
        let byte = *self.bytes.get(self.offset).ok_or(WalError::Truncated)?;
        self.offset += 1;
        Ok(byte)
    }

    fn read_u32(&mut self) -> Result<u32, WalError> {
        read_u32_le(self.read_bytes(U32_SIZE)?)
    }

    fn read_len_prefixed_bytes(&mut self) -> Result<&'a [u8], WalError> {
        let len = usize::try_from(self.read_u32()?).map_err(|_| WalError::LengthOverflow)?;
        self.read_bytes(len)
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
    use super::{OperationType, TAG_DELETE, TAG_PUT, WalEntry, payload_bytes};
    use crate::wal::buffer::{Mutation, WalError};

    fn entry_bytes(payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::from(payload);
        bytes.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
        bytes
    }

    #[test]
    fn put_entry_contains_operation_key_value_and_crc() {
        let entry = WalEntry::put(b"key".to_vec(), b"value".to_vec());
        let payload = payload_bytes(OperationType::Put, b"key", Some(b"value"));

        if let WalEntry::Put {
            operation_type,
            key,
            value,
            crc32,
        } = entry
        {
            assert_eq!(operation_type, OperationType::Put);
            assert_eq!(key, b"key".to_vec());
            assert_eq!(value, b"value".to_vec());
            assert_eq!(crc32, crc32fast::hash(&payload));
        } else {
            unreachable_put_entry();
        }
    }

    #[test]
    fn delete_entry_contains_operation_key_and_crc() {
        let entry = WalEntry::delete(b"key".to_vec());
        let payload = payload_bytes(OperationType::Delete, b"key", None);

        if let WalEntry::Delete {
            operation_type,
            key,
            crc32,
        } = entry
        {
            assert_eq!(operation_type, OperationType::Delete);
            assert_eq!(key, b"key".to_vec());
            assert_eq!(crc32, crc32fast::hash(&payload));
        } else {
            unreachable_delete_entry();
        }
    }

    #[test]
    fn crc_covers_operation_type_key_and_value_payload() {
        let entry = WalEntry::put(b"ab".to_vec(), b"xyz".to_vec());
        let mut payload = vec![TAG_PUT];
        payload.extend_from_slice(&2u32.to_le_bytes());
        payload.extend_from_slice(b"ab");
        payload.extend_from_slice(&3u32.to_le_bytes());
        payload.extend_from_slice(b"xyz");

        assert_eq!(entry.computed_crc32(), crc32fast::hash(&payload));
        assert_eq!(entry.crc32(), entry.computed_crc32());
    }

    #[test]
    fn put_serialise_deserialise_round_trips() -> Result<(), WalError> {
        let entry = WalEntry::put(b"alpha".to_vec(), b"beta".to_vec());
        let decoded = WalEntry::deserialise(&entry.serialise())?;
        assert_eq!(decoded, entry);
        Ok(())
    }

    #[test]
    fn delete_serialise_deserialise_round_trips() -> Result<(), WalError> {
        let entry = WalEntry::delete(b"alpha".to_vec());
        let decoded = WalEntry::deserialise(&entry.serialise())?;
        assert_eq!(decoded, entry);
        Ok(())
    }

    #[test]
    fn serialised_put_is_tagged_and_length_prefixed() {
        let bytes = WalEntry::put(b"ab".to_vec(), b"xyz".to_vec()).serialise();
        let mut expected = vec![TAG_PUT];
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(b"ab");
        expected.extend_from_slice(&3u32.to_le_bytes());
        expected.extend_from_slice(b"xyz");
        expected.extend_from_slice(&crc32fast::hash(&expected).to_le_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn serialised_delete_is_tagged_and_length_prefixed() {
        let bytes = WalEntry::delete(b"ab".to_vec()).serialise();
        let mut expected = vec![TAG_DELETE];
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(b"ab");
        expected.extend_from_slice(&crc32fast::hash(&expected).to_le_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn deserialise_detects_checksum_mismatch() {
        let mut bytes = WalEntry::put(b"k".to_vec(), b"v".to_vec()).serialise();
        if let Some(byte) = bytes.last_mut() {
            *byte ^= 0xff;
        }

        assert!(matches!(
            WalEntry::deserialise(&bytes),
            Err(WalError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn deserialise_rejects_invalid_operation_tag() {
        let payload = [0xff, 0, 0, 0, 0];
        let bytes = entry_bytes(&payload);
        assert!(matches!(
            WalEntry::deserialise(&bytes),
            Err(WalError::InvalidTag { found: 0xff })
        ));
    }

    #[test]
    fn deserialise_rejects_truncated_input() {
        assert!(matches!(
            WalEntry::deserialise(&[TAG_PUT]),
            Err(WalError::Truncated)
        ));
    }

    #[test]
    fn deserialise_rejects_trailing_bytes() {
        let payload = [TAG_DELETE, 0, 0, 0, 0, 0xaa];
        let bytes = entry_bytes(&payload);
        assert!(matches!(
            WalEntry::deserialise(&bytes),
            Err(WalError::TrailingBytes { trailing: 1 })
        ));
    }

    #[test]
    fn wal_entry_is_debug() {
        let entry = WalEntry::delete(b"debug".to_vec());
        assert!(format!("{entry:?}").contains("Delete"));
    }

    #[test]
    fn mutation_converts_to_wal_entry() {
        let mutation = Mutation::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        let entry = WalEntry::from(&mutation);
        assert_eq!(entry, WalEntry::put(b"k".to_vec(), b"v".to_vec()));
    }

    fn unreachable_put_entry() {
        unreachable!("WalEntry::put must construct the Put variant");
    }

    fn unreachable_delete_entry() {
        unreachable!("WalEntry::delete must construct the Delete variant");
    }
}
