// WASM-001: browser / edge target support.
//
// This module hosts the WASM-specific runtime adapter and storage-capability
// detection. The IndexedDB NodeStore itself lives in `crate::store::indexeddb`
// so it can be unit-tested on native against an in-memory backend.
//
// Content addressing is identical across targets: both native and WASM use the
// `blake3` crate, which selects the SIMD backend on native and the portable
// (non-SIMD) backend on wasm32 automatically. There is deliberately no
// WASM-specific hash implementation — identical hashes are what make hash-based
// sync between a server and a browser tab work without translation (R2, C3,
// CN2 / ADR-002). The `blake3` tests below pin known-answer vectors so a
// regression in either backend is caught.

pub mod detect;

// The runtime adapter and JS bindings bind to `web_sys` worker APIs and only
// compile for the browser target.
#[cfg(target_arch = "wasm32")]
pub mod bindings;
#[cfg(target_arch = "wasm32")]
pub mod runtime;

// WASM-003: browser transport for hash-based sync (out of scope for WASM-001).
// Placeholder module; hidden from public docs until implemented.
#[doc(hidden)]
pub mod transport;

#[cfg(test)]
mod tests {
    use crate::tree::Hash;

    /// Render the content hash of `input` exactly as the store keys nodes.
    fn content_hash_hex(input: &[u8]) -> String {
        Hash::from_bytes(*blake3::hash(input).as_bytes()).to_string()
    }

    /// The BLAKE3 backend must produce the official known-answer vectors. These
    /// constants are fixed by the BLAKE3 specification and are identical for the
    /// SIMD (native) and portable (wasm32) backends, so this test fails if
    /// either target diverges (C3, CN2).
    #[test]
    fn blake3_matches_known_answer_vectors() {
        assert_eq!(
            content_hash_hex(b""),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
        assert_eq!(
            content_hash_hex(&[0u8]),
            "2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213"
        );
    }

    /// Hashing is deterministic and order-stable: the same bytes always hash to
    /// the same 32-byte key regardless of how often or where it is computed.
    #[test]
    fn content_hash_is_deterministic() {
        let input = b"haematite content-addressed node";
        assert_eq!(content_hash_hex(input), content_hash_hex(input));
        assert_eq!(content_hash_hex(input).len(), 64);
    }
}
