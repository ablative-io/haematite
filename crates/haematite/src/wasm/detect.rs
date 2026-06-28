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

/// Runtime OPFS capability for WAL backend selection (WASM-002 R3).
///
/// OPFS *synchronous* access handles — the form the WAL needs for append-only
/// writes — exist only inside a web worker. The fallback to `IndexedDB`
/// ([`OpfsCapability::OpfsUnavailable`]) covers the main thread and browsers
/// without OPFS at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpfsCapability {
    /// A web worker scope exposing the OPFS API (`navigator.storage`).
    OpfsAvailable,
    /// No OPFS sync access: main thread, or a browser without OPFS.
    OpfsUnavailable,
}

/// Detect whether OPFS synchronous file access is usable here (R3).
///
/// Returns [`OpfsCapability::OpfsAvailable`] only when the current global scope
/// is a [`web_sys::WorkerGlobalScope`] **and** that worker exposes
/// `navigator.storage` (the `StorageManager` entry point to OPFS). It returns
/// [`OpfsCapability::OpfsUnavailable`] on the main thread (where synchronous
/// access handles do not exist) and in browsers that do not implement OPFS, so
/// the caller can transparently fall back to `IndexedDB` (R4).
pub fn detect_opfs() -> OpfsCapability {
    let global = js_sys::global();
    let Some(scope) = global.dyn_ref::<web_sys::WorkerGlobalScope>() else {
        return OpfsCapability::OpfsUnavailable;
    };

    // `WorkerNavigator::storage` returns a `StorageManager` value type; a missing
    // implementation surfaces as an `undefined` property, so probe it as a JS
    // value before trusting the binding.
    let navigator = scope.navigator();
    let storage = js_sys::Reflect::get(navigator.as_ref(), &"storage".into());
    match storage {
        Ok(value) if !value.is_undefined() && !value.is_null() => OpfsCapability::OpfsAvailable,
        _ => OpfsCapability::OpfsUnavailable,
    }
}
