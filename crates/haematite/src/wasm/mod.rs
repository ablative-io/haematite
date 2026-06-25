//! WASM runtime adapter for haematite.
//!
//! This module is only compiled for the `wasm` feature. It drives shard actors
//! on web workers via the `beamr-wasm` scheduler (R3) and exposes runtime
//! detection helpers (`detect`). The IndexedDB-backed node store lives in
//! [`crate::store::indexeddb`].

pub mod detect;

#[cfg(feature = "wasm-runtime")]
pub mod runtime;

#[cfg(feature = "wasm-runtime")]
pub use runtime::{WasmRuntime, WasmRuntimeError, WasmShardHandle, WasmShardRuntime};
