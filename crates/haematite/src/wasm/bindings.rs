// WASM-001: JavaScript bindings — the public browser API surface.
//
// This is what an application imports after `wasm-pack build`. It exposes the
// same content-addressed store contract used on native (open / put / get), so
// frontend code uses one API everywhere (S1). It is also the entry point that
// keeps the store, IndexedDB backend, ruzstd codec, and BLAKE3 hashing reachable
// in the linked `.wasm` artifact, making the R7 size budget a real measurement.

use js_sys::Uint8Array;
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::wasm_bindgen;

use crate::store::idb_backend::IdbBlobStore;
use crate::store::indexeddb::IndexedDbStore;
use crate::tree::{Hash, Node};

/// A content-addressed node store backed by IndexedDB, exported to JavaScript.
#[wasm_bindgen]
pub struct HaematiteStore {
    inner: IndexedDbStore<IdbBlobStore>,
}

#[wasm_bindgen]
impl HaematiteStore {
    /// Open the named database/object store on the current web worker.
    pub async fn open(db_name: String, store_name: String) -> Result<Self, JsValue> {
        let backend = IdbBlobStore::open(&db_name, &store_name)
            .await
            .map_err(to_js)?;
        let inner = IndexedDbStore::new(backend).map_err(to_js)?;
        Ok(Self { inner })
    }

    /// Store a serialised node and return its 64-char hex content hash.
    #[wasm_bindgen(js_name = putNode)]
    pub async fn put_node(&self, serialised: Vec<u8>) -> Result<String, JsValue> {
        let node = Node::deserialise(&serialised)
            .map_err(|error| JsValue::from_str(&format!("invalid node: {error}")))?;
        let hash = self.inner.put(&node).await.map_err(to_js)?;
        Ok(hash.to_string())
    }

    /// Fetch a node by hex hash, returning its serialised bytes, or `null` if
    /// the node is unknown.
    #[wasm_bindgen(js_name = getNode)]
    pub async fn get_node(&self, hash_hex: String) -> Result<Option<Uint8Array>, JsValue> {
        let hash = parse_hash(&hash_hex)?;
        let node = self.inner.get(&hash).await.map_err(to_js)?;
        Ok(node.map(|node| Uint8Array::from(node.serialise().as_slice())))
    }
}

/// The crate version, useful for verifying which build is loaded in a tab.
#[wasm_bindgen]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_owned()
}

fn to_js<E: core::fmt::Display>(error: E) -> JsValue {
    JsValue::from_str(&error.to_string())
}

/// Parse a 64-character lowercase-hex string into a content [`Hash`].
fn parse_hash(hex: &str) -> Result<Hash, JsValue> {
    let bytes = hex.as_bytes();
    if bytes.len() != 64 {
        return Err(JsValue::from_str("hash must be 64 hex characters"));
    }
    let mut out = [0u8; 32];
    for (index, chunk) in bytes.chunks_exact(2).enumerate() {
        let high = hex_digit(chunk[0])?;
        let low = hex_digit(chunk[1])?;
        out[index] = (high << 4) | low;
    }
    Ok(Hash::from_bytes(out))
}

fn hex_digit(byte: u8) -> Result<u8, JsValue> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(JsValue::from_str("hash contains a non-hex character")),
    }
}
