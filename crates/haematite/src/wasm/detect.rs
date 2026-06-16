// WASM-001: runtime detection of available browser storage backends.
//
// The runtime adapter uses this to pick a NodeStore backend. WASM-001 ships the
// IndexedDB backend; OPFS detection is wired here so WASM-002 can prefer OPFS
// and transparently fall back to IndexedDB (CN5) without changing the call site.

/// Which browser storage backends are reachable from the current global scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StorageCapabilities {
    /// `indexedDB` is present on the global scope (window or worker).
    pub indexed_db: bool,
    /// The Origin Private File System (`navigator.storage.getDirectory`) is
    /// present. Consumed by WASM-002; always reported here for completeness.
    pub opfs: bool,
}

impl StorageCapabilities {
    /// Whether any persistent backend is available.
    pub const fn any(self) -> bool {
        self.indexed_db || self.opfs
    }
}

/// Detect the storage backends available in the current environment.
///
/// On native targets this always reports nothing available, since there is no
/// browser global scope; native builds use the filesystem `DiskStore` instead.
#[cfg(not(target_arch = "wasm32"))]
pub fn detect() -> StorageCapabilities {
    StorageCapabilities::default()
}

/// Detect the storage backends available in the current browser global scope.
#[cfg(target_arch = "wasm32")]
pub fn detect() -> StorageCapabilities {
    let global = js_sys::global();
    StorageCapabilities {
        indexed_db: global_has(&global, "indexedDB"),
        opfs: detect_opfs(&global),
    }
}

/// Return whether `global[name]` exists and is neither `null` nor `undefined`.
#[cfg(target_arch = "wasm32")]
fn global_has(global: &wasm_bindgen::JsValue, name: &str) -> bool {
    js_sys::Reflect::get(global, &wasm_bindgen::JsValue::from_str(name))
        .is_ok_and(|value| !value.is_undefined() && !value.is_null())
}

/// OPFS is reached via `navigator.storage.getDirectory`; probe for `navigator`
/// then its `storage` property.
#[cfg(target_arch = "wasm32")]
fn detect_opfs(global: &wasm_bindgen::JsValue) -> bool {
    let Ok(navigator) = js_sys::Reflect::get(global, &wasm_bindgen::JsValue::from_str("navigator"))
    else {
        return false;
    };
    if navigator.is_undefined() || navigator.is_null() {
        return false;
    }
    global_has(&navigator, "storage")
}

#[cfg(test)]
mod tests {
    use super::{StorageCapabilities, detect};

    #[test]
    fn native_reports_no_browser_storage() {
        let caps = detect();
        assert!(!caps.indexed_db);
        assert!(!caps.opfs);
        assert!(!caps.any());
    }

    #[test]
    fn any_is_true_when_a_backend_is_present() {
        let caps = StorageCapabilities {
            indexed_db: true,
            opfs: false,
        };
        assert!(caps.any());
    }
}
