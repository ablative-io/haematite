//! Browser-only WAL backends: OPFS and the `IndexedDB` fallback (WASM-002 R1, R4).
//!
//! Compiled only for `wasm32-unknown-unknown` with the `wasm` feature. OPFS
//! synchronous access handles exist solely inside a browser web worker, so this
//! module is **compile-checked** in this environment but exercised only in a
//! real browser. See the [`super`] module header for the layering and the
//! `IndexedDB` durability limitation (C15).

use super::frame;
use crate::wal::{Mutation, WalEntry, WalError};
use crate::wasm::detect::{OpfsCapability, detect_opfs};

use indexed_db_futures::database::Database as IdbDatabase;
use indexed_db_futures::prelude::{Build, BuildPrimitive, QuerySource};
use indexed_db_futures::transaction::TransactionMode;
use indexed_db_futures::typed_array::{Uint8Array, Uint8ArraySlice};
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetFileOptions,
    FileSystemReadWriteOptions, FileSystemSyncAccessHandle, WorkerGlobalScope,
};

const IDB_VERSION: u32 = 1;
const WAL_STORE: &str = "wal";
const WAL_RECORD_KEY: &str = "frames";

/// Backend-agnostic WAL interface shared by both browser backends (R1, R4).
///
/// `append`, `read`, and `truncate` are identical regardless of whether OPFS
/// or `IndexedDB` backs the WAL, so a shard actor holding a [`BrowserWal`]
/// cannot tell which backend it received (C16).
pub trait WalBackend {
    /// Append one entry as a framed record, byte-identical to native (R2).
    fn append(&mut self, entry: &WalEntry) -> Result<(), OpfsWalError>;

    /// Append an in-memory mutation, mirroring `DurableWal::append_mutation`.
    fn append_mutation(&mut self, mutation: &Mutation) -> Result<(), OpfsWalError> {
        self.append(&WalEntry::from(mutation))
    }

    /// Decode all currently persisted entries (checksum-verified).
    fn read(&self) -> Result<Vec<WalEntry>, OpfsWalError>;

    /// Discard all persisted entries, resetting the WAL to empty.
    fn truncate(&mut self) -> Result<(), OpfsWalError>;
}

/// The browser WAL a shard actor holds: OPFS when available, else `IndexedDB`.
///
/// The backend is chosen once at construction by runtime capability
/// detection and is opaque thereafter (R4, C16).
pub enum BrowserWal {
    /// OPFS-backed (synchronous file access in a web worker).
    Opfs(OpfsWal),
    /// `IndexedDB`-backed fallback (best-effort durability — see module docs).
    IndexedDb(IndexedDbWal),
}

impl BrowserWal {
    /// Open the WAL `name`, selecting the backend by capability detection.
    ///
    /// When [`detect_opfs`] reports [`OpfsCapability::OpfsAvailable`] the
    /// OPFS backend is used; otherwise this transparently falls back to
    /// `IndexedDB` (R4 / C15) without surfacing the difference to the caller.
    pub async fn open(name: &str) -> Result<Self, OpfsWalError> {
        match detect_opfs() {
            OpfsCapability::OpfsAvailable => Ok(Self::Opfs(OpfsWal::open(name).await?)),
            OpfsCapability::OpfsUnavailable => Ok(Self::IndexedDb(IndexedDbWal::open(name).await?)),
        }
    }

    /// Whether this WAL is OPFS-backed (`true`) or `IndexedDB`-backed.
    ///
    /// For diagnostics/tests only; correctness must not depend on it (C16).
    #[must_use]
    pub const fn is_opfs(&self) -> bool {
        matches!(self, Self::Opfs(_))
    }
}

impl WalBackend for BrowserWal {
    fn append(&mut self, entry: &WalEntry) -> Result<(), OpfsWalError> {
        match self {
            Self::Opfs(wal) => wal.append(entry),
            Self::IndexedDb(wal) => wal.append(entry),
        }
    }

    fn read(&self) -> Result<Vec<WalEntry>, OpfsWalError> {
        match self {
            Self::Opfs(wal) => wal.read(),
            Self::IndexedDb(wal) => wal.read(),
        }
    }

    fn truncate(&mut self) -> Result<(), OpfsWalError> {
        match self {
            Self::Opfs(wal) => wal.truncate(),
            Self::IndexedDb(wal) => wal.truncate(),
        }
    }
}

/// OPFS-backed WAL using a synchronous access handle in a web worker (R1).
///
/// Appends are sequential writes at the current end of file followed by a
/// `flush`, mirroring the native append-then-fsync discipline. Each entry is
/// framed by the shared [`frame`] helpers, so the on-disk bytes are identical
/// to the native durable WAL (R2).
pub struct OpfsWal {
    handle: FileSystemSyncAccessHandle,
    write_offset: f64,
}

impl OpfsWal {
    /// Open (creating if needed) the OPFS file `name` and a sync access
    /// handle on it, positioning the write cursor at end of file.
    ///
    /// Must run inside a web worker scope; OPFS synchronous access handles do
    /// not exist on the main thread (boundary: "SHALL NOT implement OPFS on
    /// the main thread").
    pub async fn open(name: &str) -> Result<Self, OpfsWalError> {
        let root = opfs_root().await?;
        let options = FileSystemGetFileOptions::new();
        options.set_create(true);
        let file_handle: FileSystemFileHandle =
            JsFuture::from(root.get_file_handle_with_options(name, &options))
                .await
                .map_err(|error| OpfsWalError::from_js(&error))?
                .dyn_into()
                .map_err(|_| OpfsWalError::Opfs("file handle cast failed".to_owned()))?;
        let handle: FileSystemSyncAccessHandle =
            JsFuture::from(file_handle.create_sync_access_handle())
                .await
                .map_err(|error| OpfsWalError::from_js(&error))?
                .dyn_into()
                .map_err(|_| OpfsWalError::Opfs("sync access handle cast failed".to_owned()))?;
        let write_offset = handle
            .get_size()
            .map_err(|error| OpfsWalError::from_js(&error))?;
        Ok(Self {
            handle,
            write_offset,
        })
    }
}

impl WalBackend for OpfsWal {
    fn append(&mut self, entry: &WalEntry) -> Result<(), OpfsWalError> {
        let bytes = frame::frame_entry(entry);
        let options = FileSystemReadWriteOptions::new();
        options.set_at(self.write_offset);
        let written = self
            .handle
            .write_with_u8_array_and_options(&bytes, &options)
            .map_err(|error| OpfsWalError::from_js(&error))?;
        self.handle
            .flush()
            .map_err(|error| OpfsWalError::from_js(&error))?;
        self.write_offset += written;
        Ok(())
    }

    fn read(&self) -> Result<Vec<WalEntry>, OpfsWalError> {
        let size = self
            .handle
            .get_size()
            .map_err(|error| OpfsWalError::from_js(&error))?;
        let len = usize_from_f64(size)?;
        let mut buffer = vec![0_u8; len];
        let options = FileSystemReadWriteOptions::new();
        options.set_at(0.0);
        self.handle
            .read_with_u8_array_and_options(&mut buffer, &options)
            .map_err(|error| OpfsWalError::from_js(&error))?;
        frame::decode_entries(&buffer).map_err(OpfsWalError::Wal)
    }

    fn truncate(&mut self) -> Result<(), OpfsWalError> {
        self.handle
            .truncate_with_u32(0)
            .map_err(|error| OpfsWalError::from_js(&error))?;
        self.handle
            .flush()
            .map_err(|error| OpfsWalError::from_js(&error))?;
        self.write_offset = 0.0;
        Ok(())
    }
}

/// `IndexedDB`-backed WAL fallback (R4).
///
/// The entire framed WAL byte-stream is stored as one `IndexedDB` record so
/// that the persisted bytes remain byte-identical to native (append rewrites
/// the record with the prior frames plus the new one). This is the
/// best-effort durability path documented in the module header: `IndexedDB`
/// transactions are not guaranteed to flush before page unload (C15).
pub struct IndexedDbWal {
    db: IdbDatabase,
    frames: Vec<u8>,
}

impl IndexedDbWal {
    /// Open (creating if needed) the `IndexedDB` WAL database `name` and load
    /// any previously persisted frame bytes into memory.
    pub async fn open(name: &str) -> Result<Self, OpfsWalError> {
        let db = IdbDatabase::open(name.to_owned())
            .with_version(IDB_VERSION)
            .with_on_upgrade_needed(|_event, db| {
                let has_store = db.object_store_names().any(|store| store == WAL_STORE);
                if !has_store {
                    db.create_object_store(WAL_STORE).build()?;
                }
                Ok(())
            })
            .await
            .map_err(|error| OpfsWalError::IndexedDb(error.to_string()))?;
        let frames = load_frames(&db).await?;
        Ok(Self { db, frames })
    }

    async fn persist(&self) -> Result<(), OpfsWalError> {
        let tx = self
            .db
            .transaction(WAL_STORE)
            .with_mode(TransactionMode::Readwrite)
            .build()
            .map_err(|error| OpfsWalError::IndexedDb(error.to_string()))?;
        let store = tx
            .object_store(WAL_STORE)
            .map_err(|error| OpfsWalError::IndexedDb(error.to_string()))?;
        store
            .put(Uint8ArraySlice::new(&self.frames))
            .with_key(WAL_RECORD_KEY)
            .without_key_type()
            .primitive()
            .map_err(|error| OpfsWalError::IndexedDb(error.to_string()))?
            .await
            .map_err(|error| OpfsWalError::IndexedDb(error.to_string()))?;
        drop(store);
        tx.commit()
            .await
            .map_err(|error| OpfsWalError::IndexedDb(error.to_string()))
    }
}

impl WalBackend for IndexedDbWal {
    fn append(&mut self, entry: &WalEntry) -> Result<(), OpfsWalError> {
        self.frames.extend_from_slice(&frame::frame_entry(entry));
        spawn_persist(self);
        Ok(())
    }

    fn read(&self) -> Result<Vec<WalEntry>, OpfsWalError> {
        frame::decode_entries(&self.frames).map_err(OpfsWalError::Wal)
    }

    fn truncate(&mut self) -> Result<(), OpfsWalError> {
        self.frames.clear();
        spawn_persist(self);
        Ok(())
    }
}

/// Persist the in-memory frames synchronously from the trait's sync `append`.
///
/// The shard actor's WAL interface is synchronous (matching native), but
/// `IndexedDB` writes are async; this drives the persist future to completion.
/// On a web worker that future resolves on the same task, so this does not
/// block the main thread (CN4).
fn spawn_persist(wal: &IndexedDbWal) {
    wasm_bindgen_futures::spawn_local({
        let frames = wal.frames.clone();
        let db = wal.db.clone();
        async move {
            let staged = IndexedDbWal { db, frames };
            let _ = staged.persist().await;
        }
    });
}

async fn load_frames(db: &IdbDatabase) -> Result<Vec<u8>, OpfsWalError> {
    let tx = db
        .transaction(WAL_STORE)
        .with_mode(TransactionMode::Readonly)
        .build()
        .map_err(|error| OpfsWalError::IndexedDb(error.to_string()))?;
    let store = tx
        .object_store(WAL_STORE)
        .map_err(|error| OpfsWalError::IndexedDb(error.to_string()))?;
    let record: Option<Uint8Array> = store
        .get(WAL_RECORD_KEY)
        .primitive()
        .map_err(|error| OpfsWalError::IndexedDb(error.to_string()))?
        .await
        .map_err(|error| OpfsWalError::IndexedDb(error.to_string()))?;
    drop(store);
    tx.commit()
        .await
        .map_err(|error| OpfsWalError::IndexedDb(error.to_string()))?;
    Ok(record.map(|bytes| bytes.to_vec()).unwrap_or_default())
}

async fn opfs_root() -> Result<FileSystemDirectoryHandle, OpfsWalError> {
    let global = js_sys::global();
    let scope: WorkerGlobalScope = global
        .dyn_into()
        .map_err(|_| OpfsWalError::NotWorkerScope)?;
    let storage = scope.navigator().storage();
    JsFuture::from(storage.get_directory())
        .await
        .map_err(|error| OpfsWalError::from_js(&error))?
        .dyn_into()
        .map_err(|_| OpfsWalError::Opfs("directory handle cast failed".to_owned()))
}

fn usize_from_f64(value: f64) -> Result<usize, OpfsWalError> {
    // OPFS sizes are non-negative integral byte counts. Validate first, then
    // convert via the signed `i64` path: `f64 as i64` carries no sign-loss
    // (both signed) and `usize::try_from` rejects anything out of range, so
    // no value is silently truncated. `cast_possible_truncation` is bounded
    // out by the prior `value <= MAX_SAFE_INTEGER` and `fract()` checks.
    if !(value.is_finite() && value >= 0.0 && value.fract() == 0.0 && value <= MAX_SAFE_INTEGER) {
        return Err(OpfsWalError::Opfs(format!(
            "invalid OPFS file size: {value}"
        )));
    }
    let as_i64 = value as i64;
    usize::try_from(as_i64)
        .map_err(|_| OpfsWalError::Opfs(format!("OPFS file size exceeds usize: {value}")))
}

/// `2^53`, the largest integer an `f64` represents exactly. OPFS byte counts
/// above this cannot be trusted and are rejected rather than truncated.
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_992.0;

/// Errors raised by the browser WAL backends.
#[derive(Debug)]
pub enum OpfsWalError {
    /// OPFS was requested outside a web worker scope.
    NotWorkerScope,
    /// An OPFS API call failed.
    Opfs(String),
    /// An `IndexedDB` API call failed.
    IndexedDb(String),
    /// A WAL frame/entry failed to encode or decode (shared codec).
    Wal(WalError),
}

impl OpfsWalError {
    fn from_js(value: &JsValue) -> Self {
        Self::Opfs(value.as_string().unwrap_or_else(|| format!("{value:?}")))
    }
}

impl core::fmt::Display for OpfsWalError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NotWorkerScope => write!(
                formatter,
                "OPFS synchronous access requires a web worker scope"
            ),
            Self::Opfs(error) => write!(formatter, "OPFS error: {error}"),
            Self::IndexedDb(error) => write!(formatter, "IndexedDB WAL error: {error}"),
            Self::Wal(error) => write!(formatter, "WAL codec error: {error}"),
        }
    }
}

impl std::error::Error for OpfsWalError {}

impl From<WalError> for OpfsWalError {
    fn from(error: WalError) -> Self {
        Self::Wal(error)
    }
}
