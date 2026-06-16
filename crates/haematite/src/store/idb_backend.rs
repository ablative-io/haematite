// WASM-001: browser IndexedDB backend for the content-addressed node store.
//
// This is the concrete [`BlobStore`](super::indexeddb::BlobStore) used in the
// browser. It must run on a web worker — the factory is obtained from
// `WorkerGlobalScope`, so constructing it on the main thread fails by design,
// keeping IndexedDB transactions off the UI thread (R4, C7, CN4).
//
// Each node is one object: the BLAKE3 hash, rendered as 64-char hex, is the key
// (a universally supported string key), and the zstd-compressed bytes are stored
// as a `Uint8Array` value. Compression and the LRU cache live in
// `super::indexeddb`; this file is purely the IndexedDB transport.

use js_sys::{Function, Promise, Uint8Array};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    Event, IdbDatabase, IdbObjectStore, IdbOpenDbRequest, IdbRequest, IdbTransaction,
    IdbTransactionMode, WorkerGlobalScope,
};

use super::indexeddb::BlobStore;
use crate::tree::Hash;

/// IndexedDB-backed blob store, keyed by content hash.
#[derive(Debug)]
pub struct IdbBlobStore {
    db: IdbDatabase,
    store_name: String,
}

impl IdbBlobStore {
    /// Open (creating if necessary) the named database and object store on the
    /// current web worker.
    ///
    /// # Errors
    /// Returns an error if called off a web worker, if IndexedDB is unavailable,
    /// or if the open transaction fails.
    pub async fn open(db_name: &str, store_name: &str) -> Result<Self, IdbError> {
        let scope = js_sys::global()
            .dyn_into::<WorkerGlobalScope>()
            .map_err(|_| IdbError::new("IndexedDB store must be opened on a web worker"))?;
        let factory = scope
            .indexed_db()
            .map_err(|error| IdbError::from_js("indexedDB access", &error))?
            .ok_or_else(|| IdbError::new("indexedDB is not available in this worker"))?;

        let open_request = factory
            .open_with_u32(db_name, 1)
            .map_err(|error| IdbError::from_js("open database", &error))?;
        install_upgrade_handler(&open_request, store_name.to_owned());

        let result = await_request(open_request.unchecked_ref()).await?;
        let db = result
            .dyn_into::<IdbDatabase>()
            .map_err(|_| IdbError::new("open request did not yield a database"))?;

        Ok(Self {
            db,
            store_name: store_name.to_owned(),
        })
    }

    fn object_store(&self, mode: IdbTransactionMode) -> Result<IdbObjectStore, IdbError> {
        let transaction: IdbTransaction = self
            .db
            .transaction_with_str_and_mode(&self.store_name, mode)
            .map_err(|error| IdbError::from_js("begin transaction", &error))?;
        transaction
            .object_store(&self.store_name)
            .map_err(|error| IdbError::from_js("open object store", &error))
    }
}

impl BlobStore for IdbBlobStore {
    type Error = IdbError;

    async fn load(&self, key: &Hash) -> Result<Option<Vec<u8>>, Self::Error> {
        let store = self.object_store(IdbTransactionMode::Readonly)?;
        let request = store
            .get(&key_value(key))
            .map_err(|error| IdbError::from_js("get", &error))?;
        let value = await_request(&request).await?;
        if value.is_undefined() || value.is_null() {
            return Ok(None);
        }
        let bytes = value
            .dyn_into::<Uint8Array>()
            .map_err(|_| IdbError::new("stored value was not a byte array"))?;
        Ok(Some(bytes.to_vec()))
    }

    async fn store(&self, key: &Hash, bytes: Vec<u8>) -> Result<(), Self::Error> {
        let store = self.object_store(IdbTransactionMode::Readwrite)?;
        let value = Uint8Array::from(bytes.as_slice());
        let request = store
            .put_with_key(&value, &key_value(key))
            .map_err(|error| IdbError::from_js("put", &error))?;
        await_request(&request).await.map(|_| ())
    }

    async fn contains(&self, key: &Hash) -> Result<bool, Self::Error> {
        let store = self.object_store(IdbTransactionMode::Readonly)?;
        let request = store
            .count_with_key(&key_value(key))
            .map_err(|error| IdbError::from_js("count", &error))?;
        let value = await_request(&request).await?;
        Ok(value.as_f64().unwrap_or(0.0) > 0.0)
    }
}

/// Render a content hash as the string key used in IndexedDB.
fn key_value(hash: &Hash) -> JsValue {
    JsValue::from_str(&hash.to_string())
}

/// Create the object store the first time the database is opened.
fn install_upgrade_handler(open_request: &IdbOpenDbRequest, store_name: String) {
    let on_upgrade = wasm_bindgen::closure::Closure::once_into_js(move |event: Event| {
        let Some(target) = event.target() else {
            return;
        };
        let Ok(request) = target.dyn_into::<IdbRequest>() else {
            return;
        };
        let Ok(result) = request.result() else {
            return;
        };
        if let Ok(db) = result.dyn_into::<IdbDatabase>() {
            // Ignored: a concurrent upgrade may have already created the store.
            let _ = db.create_object_store(&store_name);
        }
    });
    open_request.set_onupgradeneeded(Some(on_upgrade.unchecked_ref()));
}

/// Await an `IdbRequest` by bridging its success/error events to a JS promise.
async fn await_request(request: &IdbRequest) -> Result<JsValue, IdbError> {
    let request = request.clone();
    let promise = Promise::new(&mut |resolve: Function, reject: Function| {
        let success_request = request.clone();
        let success_reject = reject.clone();
        let on_success =
            wasm_bindgen::closure::Closure::once_into_js(move || match success_request.result() {
                Ok(value) => {
                    let _ = resolve.call1(&JsValue::NULL, &value);
                }
                Err(error) => {
                    let _ = success_reject.call1(&JsValue::NULL, &error);
                }
            });
        request.set_onsuccess(Some(on_success.unchecked_ref()));

        let error_request = request.clone();
        let on_error = wasm_bindgen::closure::Closure::once_into_js(move || {
            let message = error_request.error().ok().flatten().map_or_else(
                || JsValue::from_str("IndexedDB request failed"),
                JsValue::from,
            );
            let _ = reject.call1(&JsValue::NULL, &message);
        });
        request.set_onerror(Some(on_error.unchecked_ref()));
    });

    JsFuture::from(promise)
        .await
        .map_err(|error| IdbError::from_js("await request", &error))
}

/// Error raised by the IndexedDB backend. JS errors are captured as text so the
/// type stays `Send`-free-of-`JsValue` and implements `std::error::Error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdbError(String);

impl IdbError {
    fn new(message: &str) -> Self {
        Self(message.to_owned())
    }

    fn from_js(context: &str, value: &JsValue) -> Self {
        Self(format!("{context}: {value:?}"))
    }
}

impl std::fmt::Display for IdbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IndexedDB error: {}", self.0)
    }
}

impl std::error::Error for IdbError {}
