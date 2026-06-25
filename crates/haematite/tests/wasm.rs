//! WASM-001 verification tests.
//!
//! Two kinds of test live here:
//!
//! * **BLAKE3 portable parity (R2):** a known-answer test that runs on every
//!   target. The crate uses the top-level `blake3::hash` API in both builds; on
//!   native this is SIMD-accelerated, on wasm32 it is the `pure` portable
//!   backend (selected in Cargo.toml). Both must reproduce the official BLAKE3
//!   test vectors, which proves byte-identical hashes across implementations
//!   (S6, S10, CN2).
//!
//! * **`IndexedDB` node store (R4, R6):** `wasm_bindgen_test` cases that exercise
//!   `put`/`get`/idempotency against a real browser `IndexedDB`. These are gated
//!   to wasm32 and require a browser runtime; they are compile-verified in this
//!   environment and executed under `wasm-pack test` in a browser.

/// Official BLAKE3 test vector for the empty input.
const BLAKE3_EMPTY: &str = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
/// Official BLAKE3 test vector for the input `abc`.
const BLAKE3_ABC: &str = "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85";

#[test]
fn blake3_matches_known_vectors() {
    assert_eq!(blake3::hash(b"").to_hex().as_str(), BLAKE3_EMPTY);
    assert_eq!(blake3::hash(b"abc").to_hex().as_str(), BLAKE3_ABC);
}

#[test]
fn blake3_is_deterministic_for_repeated_input() {
    let input = b"haematite-wasm-parity";
    assert_eq!(blake3::hash(input), blake3::hash(input));
    assert_eq!(blake3::hash(input).as_bytes().len(), 32);
}

#[cfg(all(feature = "wasm", target_arch = "wasm32", target_os = "unknown"))]
mod indexeddb {
    use haematite::store::indexeddb::{IndexedDbStore, IndexedDbStoreError};
    use haematite::tree::{LeafNode, Node, NodeError};
    use wasm_bindgen::JsValue;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    fn leaf(key: &[u8], value: &[u8]) -> Result<Node, NodeError> {
        LeafNode::new(vec![(key.to_vec(), value.to_vec())]).map(Node::Leaf)
    }

    fn to_js<E: core::fmt::Display>(error: E) -> JsValue {
        JsValue::from_str(&error.to_string())
    }

    async fn fresh_store(name: &str) -> Result<IndexedDbStore, IndexedDbStoreError> {
        IndexedDbStore::open(name).await
    }

    #[wasm_bindgen_test]
    async fn put_then_get_round_trips() -> Result<(), JsValue> {
        let store = fresh_store("haematite-test-roundtrip")
            .await
            .map_err(to_js)?;
        let node = leaf(b"a", b"one").map_err(to_js)?;

        let hash = store.put_async(&node).await.map_err(to_js)?;
        assert_eq!(hash, node.hash());

        let fetched = store.get_async(&hash).await.map_err(to_js)?;
        assert_eq!(fetched, Some(node));
        Ok(())
    }

    #[wasm_bindgen_test]
    async fn get_unknown_hash_returns_none() -> Result<(), JsValue> {
        let store = fresh_store("haematite-test-unknown").await.map_err(to_js)?;
        let missing = leaf(b"z", b"absent").map_err(to_js)?.hash();
        assert_eq!(store.get_async(&missing).await.map_err(to_js)?, None);
        Ok(())
    }

    #[wasm_bindgen_test]
    async fn put_is_idempotent() -> Result<(), JsValue> {
        let store = fresh_store("haematite-test-idempotent")
            .await
            .map_err(to_js)?;
        let node = leaf(b"b", b"two").map_err(to_js)?;

        let first = store.put_async(&node).await.map_err(to_js)?;
        let second = store.put_async(&node).await.map_err(to_js)?;
        assert_eq!(first, second);
        Ok(())
    }

    #[wasm_bindgen_test]
    async fn cache_hit_avoids_indexeddb_transaction() -> Result<(), JsValue> {
        let store = fresh_store("haematite-test-cache").await.map_err(to_js)?;
        let node = leaf(b"c", b"three").map_err(to_js)?;
        let hash = store.put_async(&node).await.map_err(to_js)?;

        let before = store.indexeddb_transaction_count();
        let fetched = store.get_async(&hash).await.map_err(to_js)?;
        assert_eq!(fetched, Some(node));
        assert_eq!(store.indexeddb_transaction_count(), before);
        Ok(())
    }
}
