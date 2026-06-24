// API-003: TTL-aware read filters

use crate::branch::{Timestamp, current_timestamp};

use super::entry::{StampedEntry, TtlDecodeError, TtlEntry};

/// Result of evaluating a stored value against TTL metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Visibility {
    /// The entry is live and the value has been unwrapped for the caller.
    Live(Vec<u8>),
    /// The entry exists but its TTL has elapsed.
    Expired,
}

impl Visibility {
    /// Convert a live value into `Some(value)`, or expired into `None`.
    #[must_use]
    pub fn into_option(self) -> Option<Vec<u8>> {
        match self {
            Self::Live(value) => Some(value),
            Self::Expired => None,
        }
    }
}

/// Decode and filter a value using the current clock.
pub fn visible_value(encoded: &[u8]) -> Result<Visibility, TtlDecodeError> {
    visible_value_at(encoded, current_timestamp())
}

/// Decode and filter a value using a supplied timestamp for deterministic tests.
///
/// The stamped envelope (AA-3-4a) is tried FIRST: a stamped value strips both the
/// stamp and the TTL, returning the LOGICAL value bytes — so the read path (and
/// thus the CAS hash taken over its result) never sees the stamp. A non-stamped
/// value falls back to the plain-TTL decode, and a raw value is live as-is.
pub fn visible_value_at(encoded: &[u8], now: Timestamp) -> Result<Visibility, TtlDecodeError> {
    if let Some(stamped) = StampedEntry::decode(encoded)? {
        // A tombstone (AA-3-4b) reads as ABSENT and an expired VALUE reads as
        // Expired — both surface as `None` to the read path (`get` → `None`,
        // `current_value_hash` → `None`, create-if-absent matches a tombstone).
        if stamped.is_expired_at(now) {
            return Ok(Visibility::Expired);
        }
        return Ok(stamped
            .into_value()
            .map_or(Visibility::Expired, Visibility::Live));
    }
    let Some(entry) = TtlEntry::decode(encoded)? else {
        return Ok(Visibility::Live(encoded.to_vec()));
    };
    if entry.is_expired_at(now) {
        Ok(Visibility::Expired)
    } else {
        Ok(Visibility::Live(entry.into_value()))
    }
}

/// True when `encoded` is a stamped or TTL envelope that has expired at `now`.
pub fn is_expired_at(encoded: &[u8], now: Timestamp) -> Result<bool, TtlDecodeError> {
    if let Some(stamped) = StampedEntry::decode(encoded)? {
        return Ok(stamped.is_expired_at(now));
    }
    Ok(TtlEntry::decode(encoded)?.is_some_and(|entry| entry.is_expired_at(now)))
}

#[cfg(test)]
mod tests {
    use super::{Visibility, is_expired_at, visible_value_at};
    use crate::ttl::entry::TtlEntry;

    #[test]
    fn raw_value_is_live_and_unmodified() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            visible_value_at(b"raw", 10)?,
            Visibility::Live(b"raw".to_vec())
        );
        assert!(!is_expired_at(b"raw", 10)?);
        Ok(())
    }

    #[test]
    fn expired_envelope_is_suppressed() -> Result<(), Box<dyn std::error::Error>> {
        let encoded = TtlEntry::expiring(b"payload".to_vec(), 10).encode();

        assert_eq!(
            visible_value_at(&encoded, 9)?,
            Visibility::Live(b"payload".to_vec())
        );
        assert_eq!(visible_value_at(&encoded, 10)?, Visibility::Expired);
        assert!(is_expired_at(&encoded, 10)?);
        Ok(())
    }
}
