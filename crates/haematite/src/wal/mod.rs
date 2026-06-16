pub mod buffer;
pub mod entry;

// The durable WAL writer and its recovery reader are filesystem-backed and
// native-only; the browser uses an OPFS-backed WAL instead (WASM-002). The
// in-memory write buffer and the on-disk entry format stay shared so WAL files
// remain byte-portable across backends (WASM-001 R1, CN6).
#[cfg(not(feature = "wasm"))]
pub mod durable;
#[cfg(not(feature = "wasm"))]
pub mod recovery;
