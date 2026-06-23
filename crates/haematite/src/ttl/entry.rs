// API-003: TTL entry metadata

use std::fmt;
use std::time::Duration;

use crate::branch::{Timestamp, current_timestamp};

const MAGIC: &[u8; 8] = b"HMTTL001";
const HEADER_LEN: usize = MAGIC.len() + EXPIRY_WIDTH;
const EXPIRY_WIDTH: usize = 8;
const NEVER_EXPIRES: u64 = u64::MAX;

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
        None if value.starts_with(MAGIC) => Ok(TtlEntry::never(value).encode()),
        None => Ok(value),
    }
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
    use super::{MAGIC, TtlEntry, encode_optional_ttl};

    #[test]
    fn raw_values_decode_as_legacy_never_expiring() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(TtlEntry::decode(b"plain")?, None);
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
