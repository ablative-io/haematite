//! Platform-neutral identifier and byte-payload aliases shared by the native
//! database and the wasm sync codec.
//!
//! These are the trivial scalar/byte aliases the sync-protocol message types and
//! wire codec bottom out in. They live in their own ungated module (alongside
//! [`crate::sync_codec`]) so the codec compiles on `wasm32-unknown-unknown`
//! without pulling in the native `api`/`branch` layers. The native `branch` and
//! `api` modules re-export these same aliases, so every existing
//! `crate::ShardId`/`crate::KvKey`/`crate::api::KvKey`/`crate::branch::ShardId`
//! path keeps resolving to the identical type with zero API change.

/// Shard identifier. A shard selects one per-node actor partition; the database
/// router maps a key to its owning shard with `BLAKE3(key) % shard_count`.
pub type ShardId = usize;

/// Key bytes used by the general KV API and replicated over the sync protocol.
pub type KvKey = Vec<u8>;

/// Value bytes used by the general KV API and replicated over the sync protocol.
pub type KvValue = Vec<u8>;
