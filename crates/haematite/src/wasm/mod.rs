//! WASM runtime adapter for haematite.
//!
//! The browser-only pieces — shard actors driven on web workers via the
//! `beamr-wasm` scheduler (R3), the runtime-detection helpers (`detect`), and
//! the `web_sys::WebSocket` carrier inside [`transport`] — are gated behind the
//! `wasm`/`wasm-runtime` features and `cfg(wasm32)`. The IndexedDB-backed node
//! store lives in [`crate::store::indexeddb`]. The [`transport`] module's
//! platform-neutral frame core is compiled on every target so its sync-codec
//! parity tests (WASM-003) run under the native `cargo test --lib`.

// The browser transport (WASM-003). Its platform-neutral frame core and the
// native parity tests compile on every target; the `web_sys::WebSocket` carrier
// inside is `cfg(wasm32)`-gated. Declared unconditionally so the parity tests
// run under the native `cargo test --lib` against the shared `crate::sync` codec.
pub mod transport;

#[cfg(feature = "wasm")]
pub mod detect;

#[cfg(feature = "wasm-runtime")]
pub mod runtime;

#[cfg(feature = "wasm-runtime")]
pub use runtime::{WasmRuntime, WasmRuntimeError, WasmShardHandle, WasmShardRuntime};
