//! Runtime capability detection for the WASM target.
//!
//! Used by the runtime to decide whether `IndexedDB` is available (the WASM-001
//! node-store backend). OPFS detection and the OPFS WAL backend are WASM-002.

use wasm_bindgen::JsCast;

/// Whether the current global scope exposes `IndexedDB` (`self.indexedDB`).
///
/// Returns `true` in window and dedicated-worker scopes that expose an
/// `IDBFactory`. `IndexedDB` transactions must be awaited on a worker, so the
/// node store is intended to be driven from a [`web_sys::WorkerGlobalScope`].
pub fn indexed_db_available() -> bool {
    let global = js_sys::global();

    if let Some(scope) = global.dyn_ref::<web_sys::WorkerGlobalScope>() {
        return matches!(scope.indexed_db(), Ok(Some(_)));
    }

    if let Some(window) = global.dyn_ref::<web_sys::Window>() {
        return matches!(window.indexed_db(), Ok(Some(_)));
    }

    false
}

/// Whether the current scope is a [`web_sys::WorkerGlobalScope`], the only place
/// `IndexedDB` transactions should be driven so the main thread stays unblocked.
pub fn is_worker_scope() -> bool {
    js_sys::global().is_instance_of::<web_sys::WorkerGlobalScope>()
}
