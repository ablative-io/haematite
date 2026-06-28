//! Distributed-node identity shared by the native sync layer and the wasm codec.
//!
//! [`SyncNodeId`] is a transparent `String` newtype carried inside several
//! sync-protocol message types (`WriteId`, `WriteAck`, `Promise`, `Ballot`, …).
//! It lives here in the ungated [`crate::sync_codec`] module so those message
//! types and the wire codec compile on `wasm32-unknown-unknown`. The native
//! `crate::sync::topology` module re-exports it, so `crate::sync::SyncNodeId`
//! resolves to this identical type with zero API change.

use std::fmt;

/// Identifier for a distributed haematite node.
///
/// This is deliberately separate from shard ids: topology edges connect nodes,
/// while shard ids select per-node actor partitions.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
#[serde(transparent)]
pub struct SyncNodeId(String);

impl SyncNodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for SyncNodeId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for SyncNodeId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<usize> for SyncNodeId {
    fn from(value: usize) -> Self {
        Self::new(value.to_string())
    }
}

impl fmt::Display for SyncNodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
