// WASM-001: browser-target test suite.
//
// These run under `wasm-pack test --headless --firefox crates/haematite
// --features wasm`, inside a dedicated web worker. Unlike the native `#[test]`
// suite they exercise the real wasm32 execution model: the portable BLAKE3
// backend (R2/C3), cooperative async futures driven on the worker event loop
// (R4/CN4), and the actual IndexedDB API (R4). The worker-scope test confirms
// the code is placed off the main thread (R3/C7).
#![cfg(target_arch = "wasm32")]

use haematite::store::IdbBlobStore;
use haematite::wasm::runtime::require_worker_scope;
use haematite::{Hash, IndexedDbStore, LeafNode, MemoryBlobStore, Node};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

fn to_js<E: core::fmt::Display>(error: E) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn leaf(key: &[u8], value: &[u8]) -> Result<Node, JsValue> {
    LeafNode::new(vec![(key.to_vec(), value.to_vec())])
        .map(Node::Leaf)
        .map_err(to_js)
}

/// The portable (non-SIMD) BLAKE3 backend used on wasm32 must produce the exact
/// same known-answer vectors as native, proving cross-target hash parity (C3,
/// CN2). This is the test the native suite structurally cannot provide.
#[wasm_bindgen_test]
fn blake3_portable_matches_known_answer_vectors() {
    let empty = Hash::from_bytes(*blake3::hash(b"").as_bytes()).to_string();
    assert_eq!(
        empty,
        "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
    );
    let zero = Hash::from_bytes(*blake3::hash(&[0u8]).as_bytes()).to_string();
    assert_eq!(
        zero,
        "2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213"
    );
}

/// Confirms this code runs on a web worker rather than the main thread (R3, C7).
#[wasm_bindgen_test]
fn runs_on_a_web_worker() {
    assert!(require_worker_scope().is_ok());
}

/// The async store round-trips through real cooperative-future suspension, which
/// pollster on native does not model.
#[wasm_bindgen_test]
async fn store_round_trips_over_memory_backend() -> Result<(), JsValue> {
    let store = IndexedDbStore::new(MemoryBlobStore::new()).map_err(to_js)?;
    let node = leaf(b"a", b"one")?;
    let hash = store.put(&node).await.map_err(to_js)?;
    assert_eq!(store.get(&hash).await.map_err(to_js)?, Some(node));
    Ok(())
}

#[wasm_bindgen_test]
async fn get_returns_none_for_unknown_hash() -> Result<(), JsValue> {
    let store = IndexedDbStore::new(MemoryBlobStore::new()).map_err(to_js)?;
    assert_eq!(
        store.get(&Hash::from_bytes([7; 32])).await.map_err(to_js)?,
        None
    );
    Ok(())
}

#[wasm_bindgen_test]
async fn duplicate_put_is_idempotent() -> Result<(), JsValue> {
    let store = IndexedDbStore::new(MemoryBlobStore::new()).map_err(to_js)?;
    let node = leaf(b"a", b"one")?;
    let first = store.put(&node).await.map_err(to_js)?;
    let second = store.put(&node).await.map_err(to_js)?;
    assert_eq!(first, second);
    assert_eq!(store.backend().len(), 1);
    Ok(())
}

#[wasm_bindgen_test]
async fn cache_hit_avoids_backend() -> Result<(), JsValue> {
    let store = IndexedDbStore::new(MemoryBlobStore::new()).map_err(to_js)?;
    let node = leaf(b"a", b"one")?;
    let hash = store.put(&node).await.map_err(to_js)?;
    store.backend().forget(&hash);
    assert_eq!(store.get(&hash).await.map_err(to_js)?, Some(node));
    Ok(())
}

/// The real IndexedDB path: open a database in the worker, write a node, then
/// read it back through a cold-cache store to force a decode from IndexedDB.
#[wasm_bindgen_test]
async fn indexeddb_backend_round_trips_in_worker() -> Result<(), JsValue> {
    let writer = IndexedDbStore::new(
        IdbBlobStore::open("haematite-test", "nodes")
            .await
            .map_err(to_js)?,
    )
    .map_err(to_js)?;
    let node = leaf(b"browser", b"node")?;
    let hash = writer.put(&node).await.map_err(to_js)?;

    let reader = IndexedDbStore::with_cache_capacity(
        IdbBlobStore::open("haematite-test", "nodes")
            .await
            .map_err(to_js)?,
        4,
    )
    .map_err(to_js)?;
    assert_eq!(reader.get(&hash).await.map_err(to_js)?, Some(node));
    Ok(())
}
