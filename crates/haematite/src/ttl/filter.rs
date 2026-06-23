// API-003: TTL-aware read filters

use crate::branch::{Timestamp, current_timestamp};

use super::entry::{TtlDecodeError, TtlEntry};

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
pub fn visible_value_at(encoded: &[u8], now: Timestamp) -> Result<Visibility, TtlDecodeError> {
    let Some(entry) = TtlEntry::decode(encoded)? else {
        return Ok(Visibility::Live(encoded.to_vec()));
    };
    if entry.is_expired_at(now) {
        Ok(Visibility::Expired)
    } else {
        Ok(Visibility::Live(entry.into_value()))
    }
}

/// True when `encoded` is a TTL envelope that has expired at `now`.
pub fn is_expired_at(encoded: &[u8], now: Timestamp) -> Result<bool, TtlDecodeError> {
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
