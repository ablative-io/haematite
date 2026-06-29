// API-003: TTL entry metadata

use std::fmt;
use std::time::Duration;

use crate::branch::{Timestamp, current_timestamp};
use crate::sync::ballot::{Ballot, Stamp};
use crate::sync::topology::SyncNodeId;

const MAGIC: &[u8; 8] = b"HMTTL001";
const HEADER_LEN: usize = MAGIC.len() + EXPIRY_WIDTH;
const EXPIRY_WIDTH: usize = 8;
const NEVER_EXPIRES: u64 = u64::MAX;

/// Magic of the AA-3-4a STAMPED value envelope. Distinct from [`MAGIC`] (the
/// plain TTL envelope) so `decode`/visibility can tell the two apart, and so a
/// stamped value never collides with a legacy/raw value or a TTL-only envelope.
const STAMP_MAGIC: &[u8; 8] = b"HMSTMP01";
const COUNTER_WIDTH: usize = 8;
const SEQ_WIDTH: usize = 8;
const NODE_LEN_WIDTH: usize = 4;
/// Width of the AA-3-4b kind discriminator byte (value vs tombstone).
const KIND_WIDTH: usize = 1;

/// Absolute expiry timestamp in nanoseconds since the Unix epoch.
pub type ExpiryTimestamp = Timestamp;

/// Stored value envelope carrying optional TTL metadata next to user bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TtlEntry {
    expires_at: Option<ExpiryTimestamp>,
    value: Vec<u8>,
}

impl TtlEntry {
    /// Build an entry that never expires.
    #[must_use]
    pub const fn never(value: Vec<u8>) -> Self {
        Self {
            expires_at: None,
            value,
        }
    }

    /// Build an entry with an absolute expiry timestamp.
    #[must_use]
    pub const fn expiring(value: Vec<u8>, expires_at: ExpiryTimestamp) -> Self {
        Self {
            expires_at: Some(expires_at),
            value,
        }
    }

    /// Build an entry whose expiry is computed from `ttl` and the current clock.
    pub fn with_ttl(value: Vec<u8>, ttl: Duration) -> Result<Self, TtlError> {
        Ok(Self::expiring(value, expires_at_from_ttl(ttl)?))
    }

    /// Optional absolute expiry timestamp.
    #[must_use]
    pub const fn expires_at(&self) -> Option<ExpiryTimestamp> {
        self.expires_at
    }

    /// User value bytes, excluding the TTL envelope header.
    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Consume the envelope and return the user value bytes.
    #[must_use]
    pub fn into_value(self) -> Vec<u8> {
        self.value
    }

    /// True when the entry has an expiry timestamp at or before `now`.
    #[must_use]
    pub fn is_expired_at(&self, now: ExpiryTimestamp) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }

    /// Deterministically encode this TTL envelope.
    ///
    /// Layout: `b"HMTTL001" || expires_at_or_u64_max.to_be_bytes() || value`.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(HEADER_LEN.saturating_add(self.value.len()));
        encoded.extend_from_slice(MAGIC);
        encoded.extend_from_slice(&self.expires_at.unwrap_or(NEVER_EXPIRES).to_be_bytes());
        encoded.extend_from_slice(&self.value);
        encoded
    }

    /// Decode a TTL envelope.
    ///
    /// Returns `Ok(None)` when `bytes` does not carry the TTL magic, allowing
    /// legacy/raw values to remain valid never-expiring entries.
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>, TtlDecodeError> {
        if !bytes.starts_with(MAGIC) {
            return Ok(None);
        }
        let expires_at = decode_expiry(bytes)?;
        let value = bytes
            .get(HEADER_LEN..)
            .ok_or(TtlDecodeError::TruncatedEnvelope { len: bytes.len() })?
            .to_vec();
        Ok(Some(Self { expires_at, value }))
    }
}

/// Envelope kind byte (AA-3-4b): the first byte after [`STAMP_MAGIC`]
/// discriminates a stamped VALUE from a stamped TOMBSTONE. Both kinds carry the
/// `(epoch, seq)` stamp; a tombstone additionally reads as ABSENT (it has no
/// logical value and no TTL). The byte sits inside the stamped magic boundary, so
/// raw / TTL-only / stamped-value / stamped-tombstone still decode unambiguously
/// (only a stamped envelope ever reaches this byte).
const KIND_VALUE: u8 = 0x00;
const KIND_TOMBSTONE: u8 = 0x01;

/// The kind of a stamped entry (AA-3-4b): a live value or a tombstone.
///
/// A tombstone is a first-class STAMPED entry persisted in the tree (R-TOMB) that
/// reads as ABSENT — so `get` returns `None`, `current_value_hash` returns `None`,
/// and create-if-absent (CAS `expected = None`) MATCHES on a tombstoned key. It
/// is a comparable, mergeable, stamped delete, not a bare key-removal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryKind {
    /// A live logical value with optional TTL.
    Value {
        expires_at: Option<ExpiryTimestamp>,
        value: Vec<u8>,
    },
    /// A committed delete: stamped, persisted, reads as absent (R-TOMB).
    Tombstone,
}

/// Stored value envelope carrying the causal commit stamp (AA-3-4a, §2.4).
///
/// The envelope holds `{ stamp, kind }`, where `kind` is either a
/// `Value { ttl, bytes }` or a `Tombstone` (AA-3-4b), alongside the existing TTL
/// metadata and user bytes.
///
/// CRITICAL — the stamp is NOT part of the CAS identity. The CAS hash
/// (`ShardActor::current_value_hash`) is taken over the LOGICAL value bytes that
/// the read path returns, which strip BOTH the stamp and the TTL (and which is
/// `None` for a tombstone). So two writes with identical `value` bytes but
/// different stamps decode to the same logical value and therefore hash
/// identically — the 3-3 fence/CAS semantics are unchanged and the stamp is pure
/// merge metadata (3-4c). A tombstone's logical value is absent, so the CAS sees
/// it exactly as it sees a never-written key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StampedEntry {
    stamp: Stamp,
    kind: EntryKind,
}

impl StampedEntry {
    /// Build a stamped VALUE entry over an optional absolute expiry and user bytes.
    #[must_use]
    pub const fn new(stamp: Stamp, expires_at: Option<ExpiryTimestamp>, value: Vec<u8>) -> Self {
        Self {
            stamp,
            kind: EntryKind::Value { expires_at, value },
        }
    }

    /// Build a stamped TOMBSTONE entry (AA-3-4b): a committed delete that carries
    /// a `(epoch, seq)` stamp, persists in the tree, and reads as absent.
    #[must_use]
    pub const fn tombstone(stamp: Stamp) -> Self {
        Self {
            stamp,
            kind: EntryKind::Tombstone,
        }
    }

    /// The commit stamp `(epoch, seq)`.
    #[must_use]
    pub const fn stamp(&self) -> &Stamp {
        &self.stamp
    }

    /// The entry kind (value or tombstone).
    #[must_use]
    pub const fn kind(&self) -> &EntryKind {
        &self.kind
    }

    /// True when this entry is a tombstone (a committed delete, R-TOMB).
    #[must_use]
    pub const fn is_tombstone(&self) -> bool {
        matches!(self.kind, EntryKind::Tombstone)
    }

    /// Optional absolute expiry timestamp (TTL semantics identical to
    /// [`TtlEntry`]). A tombstone never expires (it is `None`).
    #[must_use]
    pub const fn expires_at(&self) -> Option<ExpiryTimestamp> {
        match self.kind {
            EntryKind::Value { expires_at, .. } => expires_at,
            EntryKind::Tombstone => None,
        }
    }

    /// The LOGICAL user value bytes (stamp- AND TTL-stripped), or `None` for a
    /// tombstone. This is what the read path returns and what the CAS hash is
    /// taken over.
    #[must_use]
    pub fn value(&self) -> Option<&[u8]> {
        match &self.kind {
            EntryKind::Value { value, .. } => Some(value),
            EntryKind::Tombstone => None,
        }
    }

    /// Consume the envelope and return the logical user value bytes, or `None`
    /// for a tombstone.
    #[must_use]
    pub fn into_value(self) -> Option<Vec<u8>> {
        match self.kind {
            EntryKind::Value { value, .. } => Some(value),
            EntryKind::Tombstone => None,
        }
    }

    /// True when the entry has an expiry at or before `now`. A tombstone is never
    /// "expired" in this sense — it has no TTL (R-TOMB: it is immortal); it simply
    /// reads as absent at every clock.
    #[must_use]
    pub fn is_expired_at(&self, now: ExpiryTimestamp) -> bool {
        self.expires_at()
            .is_some_and(|expires_at| expires_at <= now)
    }

    /// Deterministically encode this stamped envelope.
    ///
    /// Layout: `STAMP_MAGIC || epoch.counter.be || node_len.be || node_bytes ||
    /// seq.be || kind`, then for a VALUE `|| expiry_or_u64_max.be || value`. A
    /// TOMBSTONE has no trailing expiry/value bytes (it carries no value).
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let node = self.stamp.epoch.node.as_str().as_bytes();
        let value_len = self.value().map_or(0, <[u8]>::len);
        let header = STAMP_MAGIC.len()
            + COUNTER_WIDTH
            + NODE_LEN_WIDTH
            + node.len()
            + SEQ_WIDTH
            + KIND_WIDTH
            + EXPIRY_WIDTH;
        let mut encoded = Vec::with_capacity(header.saturating_add(value_len));
        encoded.extend_from_slice(STAMP_MAGIC);
        encoded.extend_from_slice(&self.stamp.epoch.counter.to_be_bytes());
        // node id length is bounded; a name longer than u32::MAX is not
        // representable on the wire either, so the cast is sound (saturating).
        let node_len = u32::try_from(node.len()).unwrap_or(u32::MAX);
        encoded.extend_from_slice(&node_len.to_be_bytes());
        encoded.extend_from_slice(node);
        encoded.extend_from_slice(&self.stamp.seq.to_be_bytes());
        match &self.kind {
            EntryKind::Value { expires_at, value } => {
                encoded.push(KIND_VALUE);
                encoded.extend_from_slice(&expires_at.unwrap_or(NEVER_EXPIRES).to_be_bytes());
                encoded.extend_from_slice(value);
            }
            EntryKind::Tombstone => {
                encoded.push(KIND_TOMBSTONE);
            }
        }
        encoded
    }

    /// Decode a stamped envelope.
    ///
    /// Returns `Ok(None)` when `bytes` does not carry [`STAMP_MAGIC`], so callers
    /// can fall back to the plain-TTL / raw decode paths. A trailing `kind` byte
    /// (AA-3-4b) selects value vs tombstone; an unknown kind is a truncation-class
    /// error (fail closed rather than silently treating an unknown kind as a
    /// value).
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>, TtlDecodeError> {
        if !bytes.starts_with(STAMP_MAGIC) {
            return Ok(None);
        }
        let mut cursor = STAMP_MAGIC.len();
        let counter = read_u64(bytes, &mut cursor)?;
        let node_len = read_u32(bytes, &mut cursor)? as usize;
        let node_bytes = read_slice(bytes, &mut cursor, node_len)?;
        let node = String::from_utf8(node_bytes.to_vec())
            .map_err(|_error| TtlDecodeError::TruncatedEnvelope { len: bytes.len() })?;
        let seq = read_u64(bytes, &mut cursor)?;
        let kind_byte = read_slice(bytes, &mut cursor, KIND_WIDTH)?[0];
        let stamp = Stamp::new(Ballot::new(counter, SyncNodeId::new(node)), seq);
        let kind = match kind_byte {
            KIND_TOMBSTONE => EntryKind::Tombstone,
            KIND_VALUE => {
                let expiry_raw = read_u64(bytes, &mut cursor)?;
                let value = bytes
                    .get(cursor..)
                    .ok_or(TtlDecodeError::TruncatedEnvelope { len: bytes.len() })?
                    .to_vec();
                EntryKind::Value {
                    expires_at: (expiry_raw != NEVER_EXPIRES).then_some(expiry_raw),
                    value,
                }
            }
            _ => return Err(TtlDecodeError::TruncatedEnvelope { len: bytes.len() }),
        };
        Ok(Some(Self { stamp, kind }))
    }
}

/// Encode a value with a causal commit stamp and optional TTL.
///
/// EVERY committed write goes through this, so — unlike the plain-TTL path —
/// there is no raw/unenveloped variant: the stamp must always travel with the
/// value. The logical `value` bytes are stored verbatim, so the read path
/// recovers them exactly and the CAS hash is unchanged.
#[must_use]
pub fn encode_stamped(
    value: Vec<u8>,
    stamp: Stamp,
    expires_at: Option<ExpiryTimestamp>,
) -> Vec<u8> {
    StampedEntry::new(stamp, expires_at, value).encode()
}

/// Encode a stamped TOMBSTONE (AA-3-4b): a committed delete carrying `stamp`.
///
/// EVERY committed delete goes through this — a delete is a stamped, mergeable
/// entry, never a bare key-removal. The tombstone persists in the tree and reads
/// as absent (`get` → `None`, CAS hash → `None`).
#[must_use]
pub fn encode_stamped_tombstone(stamp: Stamp) -> Vec<u8> {
    StampedEntry::tombstone(stamp).encode()
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, TtlDecodeError> {
    let slice = read_slice(bytes, cursor, 8)?;
    let array: [u8; 8] = slice
        .try_into()
        .map_err(|_error| TtlDecodeError::TruncatedEnvelope { len: bytes.len() })?;
    Ok(u64::from_be_bytes(array))
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, TtlDecodeError> {
    let slice = read_slice(bytes, cursor, 4)?;
    let array: [u8; 4] = slice
        .try_into()
        .map_err(|_error| TtlDecodeError::TruncatedEnvelope { len: bytes.len() })?;
    Ok(u32::from_be_bytes(array))
}

fn read_slice<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
) -> Result<&'a [u8], TtlDecodeError> {
    let end = cursor
        .checked_add(len)
        .ok_or(TtlDecodeError::TruncatedEnvelope { len: bytes.len() })?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or(TtlDecodeError::TruncatedEnvelope { len: bytes.len() })?;
    *cursor = end;
    Ok(slice)
}

/// Compute an absolute expiry timestamp as `current_timestamp() + ttl`.
pub fn expires_at_from_ttl(ttl: Duration) -> Result<ExpiryTimestamp, TtlError> {
    let ttl_nanos = duration_nanos(ttl)?;
    let now = current_timestamp();
    now.checked_add(ttl_nanos)
        .filter(|expires_at| *expires_at != NEVER_EXPIRES)
        .ok_or(TtlError::TimestampOverflow)
}

/// Encode `value` with TTL metadata.
///
/// `Some(ttl)` wraps the value in the TTL envelope with a computed expiry.
/// `None` stores the value RAW (unenveloped) so a non-expiring entry carries no
/// 16-byte header and keeps its original content hash — the read path decodes a
/// value without the magic as never-expiring (see [`TtlEntry::decode`] and
/// `crate::ttl::filter::visible_value_at`). The sole exception: a raw value that
/// itself begins with the TTL magic is enveloped as never-expiring so it cannot
/// be misparsed as a TTL header on read.
pub fn encode_optional_ttl(value: Vec<u8>, ttl: Option<Duration>) -> Result<Vec<u8>, TtlError> {
    match ttl {
        Some(ttl) => TtlEntry::with_ttl(value, ttl).map(|entry| entry.encode()),
        // A raw value beginning with EITHER envelope magic must be wrapped
        // (never-expiring) so the read path cannot misparse it as a TTL or
        // stamped header.
        None if value.starts_with(MAGIC) || value.starts_with(STAMP_MAGIC) => {
            Ok(TtlEntry::never(value).encode())
        }
        None => Ok(value),
    }
}

/// Encode `value` with a causal commit stamp (AA-3-4a) and optional TTL.
///
/// Computes the absolute expiry from `ttl` and the current clock. This is the
/// stamped counterpart of [`encode_optional_ttl`]: it always produces a stamped
/// envelope (the stamp must travel with every committed write), so there is no
/// raw variant.
pub fn encode_stamped_optional_ttl(
    value: Vec<u8>,
    stamp: Stamp,
    ttl: Option<Duration>,
) -> Result<Vec<u8>, TtlError> {
    let expires_at = match ttl {
        Some(ttl) => Some(expires_at_from_ttl(ttl)?),
        None => None,
    };
    Ok(encode_stamped(value, stamp, expires_at))
}

fn duration_nanos(ttl: Duration) -> Result<u64, TtlError> {
    u64::try_from(ttl.as_nanos()).map_err(|_| TtlError::TimestampOverflow)
}

fn decode_expiry(bytes: &[u8]) -> Result<Option<ExpiryTimestamp>, TtlDecodeError> {
    let expiry_bytes: [u8; EXPIRY_WIDTH] = bytes
        .get(MAGIC.len()..HEADER_LEN)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(TtlDecodeError::TruncatedEnvelope { len: bytes.len() })?;
    let raw = u64::from_be_bytes(expiry_bytes);
    Ok((raw != NEVER_EXPIRES).then_some(raw))
}

/// Errors raised while computing TTL metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TtlError {
    /// The requested TTL cannot be represented as a `u64` nanosecond timestamp.
    TimestampOverflow,
}

impl fmt::Display for TtlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimestampOverflow => write!(formatter, "ttl expiry timestamp overflow"),
        }
    }
}

impl std::error::Error for TtlError {}

/// Errors raised when decoding TTL envelopes from storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TtlDecodeError {
    /// The value started with the TTL magic but did not contain a complete header.
    TruncatedEnvelope { len: usize },
}

impl fmt::Display for TtlDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TruncatedEnvelope { len } => {
                write!(formatter, "truncated ttl envelope with length {len}")
            }
        }
    }
}

impl std::error::Error for TtlDecodeError {}

#[cfg(test)]
mod tests {
    use super::{
        MAGIC, STAMP_MAGIC, StampedEntry, TtlEntry, encode_optional_ttl, encode_stamped,
        encode_stamped_tombstone,
    };
    use crate::sync::ballot::{Ballot, Stamp};
    use crate::sync::topology::SyncNodeId;
    use crate::tree::Hash;
    use crate::ttl::filter::{Visibility, visible_value_at};

    fn stamp(counter: u64, node: &str, seq: u64) -> Stamp {
        Stamp::new(Ballot::new(counter, SyncNodeId::new(node)), seq)
    }

    #[test]
    fn raw_values_decode_as_legacy_never_expiring() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(TtlEntry::decode(b"plain")?, None);
        Ok(())
    }

    #[test]
    fn stamped_envelope_round_trips_stamp_ttl_and_value() -> Result<(), Box<dyn std::error::Error>>
    {
        // A stamped envelope carries `{ stamp, ttl, value }` and decodes back to
        // exactly those — including a multi-byte node id and a high seq/counter.
        let entry = StampedEntry::new(
            stamp(7, "owner-node-\u{00e9}", 0xdead_beef),
            Some(42),
            b"payload".to_vec(),
        );
        let decoded = StampedEntry::decode(&entry.encode())?.ok_or("missing stamped envelope")?;
        assert_eq!(
            decoded.stamp(),
            &stamp(7, "owner-node-\u{00e9}", 0xdead_beef)
        );
        assert_eq!(decoded.expires_at(), Some(42));
        assert_eq!(decoded.value(), Some(b"payload".as_slice()));
        assert!(!decoded.is_tombstone());

        // A never-expiring stamped value also round-trips (expiry None).
        let never = StampedEntry::new(stamp(1, "n", 5), None, b"v".to_vec());
        let decoded = StampedEntry::decode(&never.encode())?.ok_or("missing")?;
        assert_eq!(decoded.expires_at(), None);
        assert_eq!(decoded.stamp(), &stamp(1, "n", 5));
        Ok(())
    }

    #[test]
    fn non_stamped_bytes_decode_to_none() -> Result<(), Box<dyn std::error::Error>> {
        // A plain TTL envelope and a raw value are NOT stamped envelopes.
        assert_eq!(StampedEntry::decode(b"plain")?, None);
        assert_eq!(
            StampedEntry::decode(&TtlEntry::expiring(b"v".to_vec(), 9).encode())?,
            None
        );
        Ok(())
    }

    /// AA-3-4b: a stamped TOMBSTONE round-trips its stamp, decodes as a tombstone,
    /// reads as ABSENT (`visible_value_at` is Expired at every clock), and its
    /// CAS hash is `None` (it has no logical value, so create-if-absent matches).
    #[test]
    fn stamped_tombstone_round_trips_and_reads_as_absent() -> Result<(), Box<dyn std::error::Error>>
    {
        let encoded = encode_stamped_tombstone(stamp(11, "owner", 4));
        let decoded = StampedEntry::decode(&encoded)?.ok_or("missing tombstone")?;
        assert!(decoded.is_tombstone());
        assert_eq!(decoded.stamp(), &stamp(11, "owner", 4));
        assert_eq!(decoded.value(), None);
        assert_eq!(decoded.expires_at(), None);
        assert_eq!(decoded.into_value(), None);
        // A tombstone reads as absent at every clock (it is not TTL-expiry, but it
        // surfaces identically — Expired → None to the read path).
        assert_eq!(visible_value_at(&encoded, 0)?, Visibility::Expired);
        assert_eq!(visible_value_at(&encoded, u64::MAX)?, Visibility::Expired);
        // CAS hash crux: a tombstone's visible value is None, so it hashes exactly
        // like a never-written key — create-if-absent (expected = None) matches.
        assert_eq!(visible_value_at(&encoded, 0)?.into_option(), None);
        Ok(())
    }

    /// A stamped value and a stamped tombstone are DISTINGUISHABLE on the wire
    /// (the kind byte), and a tombstone is never misparsed as a value even if a
    /// value happens to be empty.
    #[test]
    fn empty_value_is_not_a_tombstone() -> Result<(), Box<dyn std::error::Error>> {
        let empty_value = encode_stamped(Vec::new(), stamp(3, "n", 0), None);
        let tombstone = encode_stamped_tombstone(stamp(3, "n", 0));
        assert_ne!(
            empty_value, tombstone,
            "empty value must differ from tombstone"
        );

        let value = StampedEntry::decode(&empty_value)?.ok_or("missing value")?;
        assert!(!value.is_tombstone());
        assert_eq!(value.value(), Some(b"".as_slice()));
        // An empty value is Live(empty) — present-but-empty, NOT absent.
        assert_eq!(
            visible_value_at(&empty_value, 0)?,
            Visibility::Live(Vec::new())
        );

        let tomb = StampedEntry::decode(&tombstone)?.ok_or("missing tombstone")?;
        assert!(tomb.is_tombstone());
        Ok(())
    }

    /// CAS-SAFETY CRUX (AA-3-4a): two committed writes with the SAME logical value
    /// bytes but DIFFERENT stamps must produce the SAME `current_value_hash`, i.e.
    /// the read-visible logical value is identical. The stamp must NOT enter the
    /// CAS identity. We hash the visibility-filtered (stamp-stripped) value exactly
    /// as `ShardActor::current_value_hash` does.
    #[test]
    fn same_value_different_stamps_hash_identically() -> Result<(), Box<dyn std::error::Error>> {
        let value = b"logical-value".to_vec();
        let a = encode_stamped(value.clone(), stamp(3, "A", 0), None);
        let b = encode_stamped(value.clone(), stamp(9, "B", 17), None);
        assert_ne!(
            a, b,
            "different stamps must encode to different envelope bytes"
        );

        let visible_a = visible_value_at(&a, 0)?;
        let visible_b = visible_value_at(&b, 0)?;
        assert_eq!(visible_a, Visibility::Live(value.clone()));
        assert_eq!(visible_b, Visibility::Live(value.clone()));

        // The CAS hash is over the logical value the read path returns.
        let hash_a = Hash::of(&visible_a.into_option().ok_or("expired")?);
        let hash_b = Hash::of(&visible_b.into_option().ok_or("expired")?);
        assert_eq!(hash_a, hash_b, "the stamp must NOT enter the CAS identity");
        // And it equals the hash of the bare logical bytes (the proposer's CAS).
        assert_eq!(hash_a, Hash::of(&value));
        Ok(())
    }

    #[test]
    fn stamped_value_colliding_with_magic_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        // A logical value that itself begins with STAMP_MAGIC survives the stamped
        // envelope unambiguously: decode strips the outer header and returns the
        // inner bytes verbatim (the length-delimited fields make this exact).
        let mut value = STAMP_MAGIC.to_vec();
        value.extend_from_slice(b"inner");
        let encoded = encode_stamped(value.clone(), stamp(2, "z", 1), None);
        let decoded = StampedEntry::decode(&encoded)?.ok_or("missing")?;
        assert_eq!(decoded.value(), Some(value.as_slice()));
        assert_eq!(visible_value_at(&encoded, 0)?, Visibility::Live(value));
        Ok(())
    }

    #[test]
    fn envelope_round_trips_expiry_and_value() -> Result<(), Box<dyn std::error::Error>> {
        let entry = TtlEntry::expiring(b"payload".to_vec(), 42);
        let decoded = TtlEntry::decode(&entry.encode())?.ok_or("missing ttl envelope")?;

        assert_eq!(decoded.expires_at(), Some(42));
        assert_eq!(decoded.value(), b"payload");
        Ok(())
    }

    #[test]
    fn optional_none_stores_raw_value_unenveloped() -> Result<(), Box<dyn std::error::Error>> {
        // A non-TTL value is stored RAW: no 16-byte header, original bytes
        // preserved (so its content hash is unchanged), and it decodes as
        // never-expiring at read time because it carries no magic.
        let encoded = encode_optional_ttl(b"value".to_vec(), None)?;
        assert_eq!(encoded, b"value");
        assert_eq!(TtlEntry::decode(&encoded)?, None);
        Ok(())
    }

    #[test]
    fn optional_none_envelopes_a_value_that_collides_with_the_magic()
    -> Result<(), Box<dyn std::error::Error>> {
        // The one case a non-TTL value IS enveloped: when the raw bytes begin
        // with the TTL magic, they must be wrapped (never-expiring) so the read
        // path cannot misparse them as a TTL header. The original bytes survive.
        let mut raw = MAGIC.to_vec();
        raw.extend_from_slice(b"payload");
        let encoded = encode_optional_ttl(raw.clone(), None)?;
        let decoded =
            TtlEntry::decode(&encoded)?.ok_or("magic-colliding value must be enveloped")?;

        assert_eq!(decoded.expires_at(), None);
        assert_eq!(decoded.value(), raw.as_slice());
        Ok(())
    }
}
