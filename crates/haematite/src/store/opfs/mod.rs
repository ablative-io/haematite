//! Browser WAL backend: OPFS when available, `IndexedDB` otherwise (WASM-002).
//!
//! The WAL on the browser target must be **byte-identical** to the native
//! durable WAL so that WAL files are portable between a native server and a
//! browser tab (CN6, R2). This module achieves that by reusing the *same*
//! platform-neutral entry codec the native [`DurableWal`] uses —
//! [`crate::wal::WalEntry::serialise`] / [`crate::wal::WalEntry::deserialise`] —
//! and replicating only the trivial outer framing
//! (`[frame_len: u32 LE][entry bytes]`) the native writer wraps each entry in.
//! The entry format (operation type, length-prefixed key/value, CRC32) is never
//! forked here; it lives in [`crate::wal::entry`], which is compiled for both
//! native and `wasm32` targets.
//!
//! ## Layering
//!
//! * [`frame`] — platform-neutral framing over the shared entry codec. Compiled
//!   on every target and covered by native `#[test]`s that prove byte-identity
//!   against the native [`DurableWal`] reader (R2).
//! * The OPFS file I/O ([`OpfsWal`]) and the `IndexedDB` fallback
//!   ([`IndexedDbWal`]) are browser-only (`wasm32`) and only **compile-checked**
//!   here; OPFS synchronous access handles exist only inside a browser web
//!   worker, which has no native or headless equivalent.
//!
//! ## Transparency (R4 / C16)
//!
//! [`BrowserWal`] is the backend-agnostic entry point a shard actor holds. It
//! selects OPFS or `IndexedDB` once at construction via runtime capability
//! detection ([`crate::wasm::detect::detect_opfs`]) and thereafter exposes one
//! interface (`append` / `read` / `truncate`) regardless of which backend backs
//! it — the shard actor never learns which one it got.
//!
//! ## Durability limitation (R4 / C15)
//!
//! OPFS `FileSystemSyncAccessHandle::flush` provides durable, append-only writes
//! comparable to the native `fsync` discipline. The `IndexedDB` fallback does
//! **not**: `IndexedDB` transactions are asynchronous and are **not guaranteed to
//! flush before page unload**, so a tab closed mid-write can lose the most
//! recent appends. `IndexedDB` mode is therefore best-effort durability for
//! environments without OPFS (no web worker, or a browser lacking the API).

pub mod frame;

#[cfg(all(feature = "wasm", target_arch = "wasm32", target_os = "unknown"))]
mod browser;

#[cfg(all(feature = "wasm", target_arch = "wasm32", target_os = "unknown"))]
pub use browser::{BrowserWal, IndexedDbWal, OpfsWal, OpfsWalError, WalBackend};

/// Native cross-backend byte-identity tests (R2): prove that frames produced by
/// the platform-neutral [`frame`] layer — the exact bytes the OPFS/`IndexedDB`
/// backends write — round-trip through the native [`crate::wal::DurableWal`]
/// reader, and that the native writer's bytes decode through [`frame`]. This is
/// the load-bearing portability proof; it needs no OPFS, so it runs natively.
#[cfg(all(test, not(feature = "wasm")))]
mod parity_tests {
    use super::frame;
    use crate::wal::{DurableWal, FsyncPolicy, WalEntry, WalError};

    fn temp_wal_path() -> std::path::PathBuf {
        // A process-wide atomic counter guarantees each test gets a distinct WAL
        // path even when tests run concurrently within the same nanosecond — the
        // durable writer opens in APPEND mode, so a shared path would interleave
        // two tests' frames into one file.
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let mut path = std::env::temp_dir();
        let unique = format!(
            "haematite-opfs-parity-{}-{}.wal",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        path.push(unique);
        path
    }

    #[test]
    fn frame_bytes_are_byte_identical_to_native_writer() -> Result<(), WalError> {
        // Write entries natively, read the raw file bytes, and assert they equal
        // what the OPFS frame layer would emit for the same entries.
        let entries = vec![
            WalEntry::put(b"alpha".to_vec(), b"beta".to_vec()),
            WalEntry::delete(b"gamma".to_vec()),
        ];
        let path = temp_wal_path();
        let _ = std::fs::remove_file(&path);
        {
            let mut wal = DurableWal::new(&path, FsyncPolicy::PerWrite)?;
            for entry in &entries {
                wal.append(entry)?;
            }
        }
        let native_bytes = std::fs::read(&path)?;
        let _ = std::fs::remove_file(&path);

        let mut frame_bytes = Vec::new();
        for entry in &entries {
            frame_bytes.extend_from_slice(&frame::frame_entry(entry));
        }
        assert_eq!(frame_bytes, native_bytes);
        Ok(())
    }

    #[test]
    fn opfs_frames_decode_via_native_reader() -> Result<(), WalError> {
        // A "WAL file" assembled by the OPFS frame layer must be read back
        // correctly by the native DurableWal reader (R2 acceptance #1).
        let entries = vec![
            WalEntry::put(b"k1".to_vec(), b"v1".to_vec()),
            WalEntry::put(b"k2".to_vec(), b"v2".to_vec()),
            WalEntry::delete(b"k3".to_vec()),
        ];
        let mut frame_bytes = Vec::new();
        for entry in &entries {
            frame_bytes.extend_from_slice(&frame::frame_entry(entry));
        }

        let path = temp_wal_path();
        std::fs::write(&path, &frame_bytes)?;
        let contents = DurableWal::read_file(&path)?;
        let _ = std::fs::remove_file(&path);

        assert_eq!(contents.entries(), entries.as_slice());
        Ok(())
    }

    #[test]
    fn native_frames_decode_via_opfs_layer() -> Result<(), WalError> {
        // Bytes written by the native writer must decode through the OPFS frame
        // layer (R2 acceptance #2).
        let entries = vec![
            WalEntry::put(b"one".to_vec(), b"1".to_vec()),
            WalEntry::delete(b"two".to_vec()),
        ];
        let path = temp_wal_path();
        let _ = std::fs::remove_file(&path);
        {
            let mut wal = DurableWal::new(&path, FsyncPolicy::PerWrite)?;
            for entry in &entries {
                wal.append(entry)?;
            }
        }
        let native_bytes = std::fs::read(&path)?;
        let _ = std::fs::remove_file(&path);

        let decoded = frame::decode_entries(&native_bytes)?;
        assert_eq!(decoded, entries);
        Ok(())
    }

    #[test]
    fn crc32_identical_across_backends() {
        // The CRC32 stored by the shared entry codec is exactly the CRC the
        // native writer stores — same codec, same checksum (R2 acceptance #3).
        let entry = WalEntry::put(b"checksum".to_vec(), b"payload".to_vec());
        let framed = frame::frame_entry(&entry);
        // Entry bytes sit after the 4-byte frame-length prefix; the last 4 bytes
        // are the CRC32 the codec computed.
        let entry_bytes = &framed[4..];
        let crc_offset = entry_bytes.len() - 4;
        let stored = u32::from_le_bytes([
            entry_bytes[crc_offset],
            entry_bytes[crc_offset + 1],
            entry_bytes[crc_offset + 2],
            entry_bytes[crc_offset + 3],
        ]);
        assert_eq!(stored, entry.crc32());
        assert_eq!(stored, entry.computed_crc32());
        assert_eq!(entry.operation_type(), crate::wal::OperationType::Put);
    }
}
