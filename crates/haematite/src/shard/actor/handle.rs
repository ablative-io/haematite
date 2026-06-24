//! CORE-007: the host-side handle for a shard native process.
//!
//! [`ShardHandle`] is the cloneable, single-spawn handle a caller uses to talk
//! to one shard. It owns nothing of the shard's storage; it caches the shard's
//! beamr pid and a shared command queue. A command is pushed onto the queue and
//! the process is woken with a plain atom via
//! [`beamr::scheduler::Scheduler::enqueue_atom_message`] — the binary payload
//! NEVER crosses the beamr term boundary, it travels as a real Rust value on the
//! queue and the reply travels back over a per-command [`mpsc::SyncSender`].
//!
//! Re-spawn / reconnect on a dead pid is deliberately out of scope here: this is
//! a fixed-pid handle (CORE-007). A router/supervisor (CORE-008) owns restart.

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beamr::atom::{Atom, AtomTable};
use beamr::scheduler::Scheduler;

use crate::store::StoreError;
use crate::sync::ballot::{Ballot, Stamp};
use crate::tree::{Hash, TreeError};
use crate::wal::WalError;

use super::native::{self, ShardNativeHandler};
use super::{PromiseState, RecordPromiseOutcome};

/// Name of the wake atom pushed into the shard process mailbox. The handler
/// never inspects it — one mailbox token corresponds to one queued command — so
/// the only requirement is that it is a valid interned atom.
const WAKE_ATOM_NAME: &str = "haematite_shard_wake";

/// A single result item streamed back from a `range` request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RangeItem {
    /// One key/value entry within the requested range, in key order.
    Entry { key: Vec<u8>, value: Vec<u8> },
    /// Sentinel marking the end of the range stream.
    Done,
}

/// Errors surfaced to a caller of a [`ShardHandle`] method.
#[derive(Debug)]
pub enum ShardError {
    /// The target process is not live (dead/never-spawned pid). Retryable once
    /// a supervisor re-spawns the shard.
    ActorUnavailable { pid: u64 },
    /// The reply channel disconnected before a reply arrived (handler dropped
    /// the sender, typically because the process exited mid-command).
    ReplyDisconnected { pid: u64 },
    /// No reply arrived within the caller's timeout.
    ReplyTimeout { pid: u64 },
    /// A durable-WAL error raised inside the shard.
    Wal(WalError),
    /// A tree error raised inside the shard.
    Tree(TreeError),
    /// A node-store error raised inside the shard.
    Store(StoreError),
    /// The shard process failed to spawn.
    Spawn(String),
    /// An append expected one sequence number but found another.
    SequenceConflict { expected: u64, actual: u64 },
    /// A compare-and-swap saw a scalar value other than the one expected.
    CasMismatch {
        expected: Option<u64>,
        actual: Option<u64>,
    },
    /// A receiver-side conditional-durable apply (active-active 2a-4) saw a tree
    /// value hash other than the one the proposing writer expected. This is a CAS
    /// vote-against, NOT an apply fault: the replica is ahead and applies nothing.
    CasHashMismatch {
        expected: Option<Hash>,
        actual: Option<Hash>,
    },
    /// The epoch fence (AA-3-3, §2.3) rejected a data write whose `attempted`
    /// epoch was strictly below this shard's actor-local `promised` ballot. A
    /// stale/deposed owner is fenced: NOTHING was applied (no put, no commit).
    /// Like [`Self::CasHashMismatch`] this is a vote-against, never an apply
    /// fault.
    Fenced {
        promised: Ballot,
        attempted: Ballot,
    },
}

impl fmt::Display for ShardError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ActorUnavailable { pid } => {
                write!(formatter, "shard actor {pid} is unavailable")
            }
            Self::ReplyDisconnected { pid } => {
                write!(formatter, "shard actor {pid} reply channel disconnected")
            }
            Self::ReplyTimeout { pid } => {
                write!(formatter, "timed out waiting for shard actor {pid}")
            }
            Self::Wal(error) => write!(formatter, "shard WAL error: {error}"),
            Self::Tree(error) => write!(formatter, "shard tree error: {error}"),
            Self::Store(error) => write!(formatter, "shard store error: {error}"),
            Self::Spawn(message) => write!(formatter, "shard spawn failed: {message}"),
            Self::SequenceConflict { expected, actual } => write!(
                formatter,
                "sequence conflict on append: expected {expected}, actual {actual}"
            ),
            Self::CasMismatch { expected, actual } => write!(
                formatter,
                "cas mismatch: expected {expected:?}, actual {actual:?}"
            ),
            Self::CasHashMismatch { expected, actual } => write!(
                formatter,
                "cas hash mismatch: expected {expected:?}, actual {actual:?}"
            ),
            Self::Fenced {
                promised,
                attempted,
            } => write!(
                formatter,
                "epoch fenced: attempted {attempted:?} < promised {promised:?}"
            ),
        }
    }
}

impl std::error::Error for ShardError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Wal(error) => Some(error),
            Self::Tree(error) => Some(error),
            Self::Store(error) => Some(error),
            Self::ActorUnavailable { .. }
            | Self::ReplyDisconnected { .. }
            | Self::ReplyTimeout { .. }
            | Self::Spawn(_)
            | Self::SequenceConflict { .. }
            | Self::CasMismatch { .. }
            | Self::CasHashMismatch { .. }
            | Self::Fenced { .. } => None,
        }
    }
}

impl From<WalError> for ShardError {
    fn from(error: WalError) -> Self {
        Self::Wal(error)
    }
}

impl From<TreeError> for ShardError {
    fn from(error: TreeError) -> Self {
        Self::Tree(error)
    }
}

impl From<StoreError> for ShardError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

/// One stream's decoded sequence metadata: `(stream_key, next_seq)`.
pub(super) type StreamSeq = (Vec<u8>, u64);

/// Reply channel payload for a `scan_sequences` request.
pub(super) type ScanReply = SyncSender<Result<Vec<StreamSeq>, ShardError>>;

/// A queued command: a monotonic id (so a failed enqueue can be rolled back)
/// plus the typed request and its reply channel.
pub(super) struct ShardCommand {
    pub(super) id: u64,
    pub(super) kind: ShardCommandKind,
}

/// The typed shard requests. Each carries owned bytes and a reply sender; the
/// bytes never touch a beamr term.
pub(super) enum ShardCommandKind {
    Get {
        key: Vec<u8>,
        reply: SyncSender<Result<Option<Vec<u8>>, ShardError>>,
    },
    #[doc(hidden)]
    GetRaw {
        key: Vec<u8>,
        reply: SyncSender<Result<Option<Vec<u8>>, ShardError>>,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
        reply: SyncSender<Result<(), ShardError>>,
    },
    Delete {
        key: Vec<u8>,
        reply: SyncSender<Result<(), ShardError>>,
    },
    DeleteIfExpired {
        key: Vec<u8>,
        reply: SyncSender<Result<bool, ShardError>>,
    },
    Commit {
        reply: SyncSender<Result<Hash, ShardError>>,
    },
    Range {
        from: Vec<u8>,
        to: Vec<u8>,
        reply: SyncSender<Result<Vec<RangeItem>, ShardError>>,
    },
    Append {
        key: Vec<u8>,
        entries: Vec<Vec<u8>>,
        expected_seq: u64,
        ttl: Option<Duration>,
        reply: SyncSender<Result<u64, ShardError>>,
    },
    ReadValue {
        key: Vec<u8>,
        reply: SyncSender<Result<Option<u64>, ShardError>>,
    },
    Cas {
        key: Vec<u8>,
        expected: Option<u64>,
        new: u64,
        reply: SyncSender<Result<(), ShardError>>,
    },
    ApplyDurable {
        key: Vec<u8>,
        expected: Option<Hash>,
        value: Vec<u8>,
        ttl: Option<Duration>,
        stamp: Stamp,
        reply: SyncSender<Result<(), ShardError>>,
    },
    RecordPromise {
        ballot: Ballot,
        reply: SyncSender<Result<RecordPromiseOutcome, ShardError>>,
    },
    RecordOwnerEpoch {
        ballot: Ballot,
        reply: SyncSender<Result<(), ShardError>>,
    },
    ReserveMinted {
        counter: u64,
        reply: SyncSender<Result<u64, ShardError>>,
    },
    ReadPromiseState {
        reply: SyncSender<Result<PromiseState, ShardError>>,
    },
    ScanSequences {
        reply: ScanReply,
    },
    Shutdown {
        reply: SyncSender<Result<(), ShardError>>,
    },
}

/// Shared command queue between a [`ShardHandle`] and its native handler.
pub(super) type CommandQueue = Arc<Mutex<VecDeque<ShardCommand>>>;

/// Host-side handle to one shard native process.
///
/// Cloning a handle shares the same pid, queue, and id counter, so clones talk
/// to the same shard. Re-spawn on a dead pid is the router's job (CORE-008).
#[derive(Clone)]
pub struct ShardHandle {
    pid: u64,
    scheduler: Arc<Scheduler>,
    commands: CommandQueue,
    wake_atom: Atom,
    next_command_id: Arc<AtomicU64>,
}

impl fmt::Debug for ShardHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShardHandle")
            .field("pid", &self.pid)
            .finish_non_exhaustive()
    }
}

impl ShardHandle {
    /// Spawn one shard native process and return a handle to it.
    ///
    /// The shard opens its [`crate::store::DiskStore`] at `store_dir` and its
    /// durable WAL at `wal_path`, recovering any committed root and replayed
    /// buffer before it accepts commands. A startup failure does not panic the
    /// scheduler: the process boots into a sentinel that, on its first slice,
    /// drains every queued command with the boot error and then stops cleanly.
    ///
    /// Because `spawn` only fails if the *scheduler* refuses to spawn (see the
    /// `# Errors` section), a boot failure (bad `store_dir`/`wal_path`, failed
    /// WAL recovery) is reported per-command, not from `spawn` itself. The
    /// per-command error kind depends on WHEN the command reaches the sentinel:
    /// - A command already on the queue when the sentinel runs its first slice
    ///   is drained by [`ShardNativeHandler::fail_startup`](super::native) with
    ///   [`ShardError::Spawn`] carrying the boot-error message.
    /// - A command enqueued *after* the sentinel has already stopped the process
    ///   is not drained: depending on the scheduler's view of the dead pid it
    ///   either fails the wake and is rolled back with
    ///   [`ShardError::ActorUnavailable`] (see [`Self::enqueue`]), or the wake is
    ///   accepted against the just-stopped pid but no slice ever drains it, so
    ///   the caller observes [`ShardError::ReplyTimeout`].
    ///
    /// (Boot failure is therefore NOT surfaced as
    /// [`ShardError::ReplyDisconnected`]; that kind is reserved for a reply
    /// channel that disconnects mid-command, e.g. a live process exiting after
    /// it accepted the command. In practice — because beamr gives a freshly
    /// spawned native process its first slice immediately — an externally-issued
    /// command typically lands in the second case above.)
    ///
    /// # Errors
    /// Returns [`ShardError::Spawn`] if the scheduler cannot spawn the process.
    pub fn spawn(
        scheduler: Arc<Scheduler>,
        store_dir: impl Into<std::path::PathBuf>,
        wal_path: impl Into<std::path::PathBuf>,
    ) -> Result<Self, ShardError> {
        let commands: CommandQueue = Arc::new(Mutex::new(VecDeque::new()));
        let factory = ShardNativeHandler::make_factory(
            store_dir.into(),
            wal_path.into(),
            Arc::clone(&commands),
        );
        let pid = scheduler
            .spawn_native(factory)
            .map_err(|error| ShardError::Spawn(format!("{error:?}")))?;
        // Interning from a fresh local table is sound ONLY because the handler
        // never inspects the wake atom's value (a mailbox token = "drain one
        // command"; see native.rs). If a future change matches on the atom
        // value in the mailbox, intern from the scheduler's table instead or the
        // match will silently fail.
        let atoms = AtomTable::with_common_atoms();
        let wake_atom = atoms.intern(WAKE_ATOM_NAME);
        Ok(Self {
            pid,
            scheduler,
            commands,
            wake_atom,
            next_command_id: Arc::new(AtomicU64::new(1)),
        })
    }

    /// Beamr pid of this shard process.
    #[must_use]
    pub const fn pid(&self) -> u64 {
        self.pid
    }

    /// Read the value for `key`, blocking up to `timeout` for the reply.
    ///
    /// # Errors
    /// Returns a [`ShardError`] if the shard is unavailable, the reply times
    /// out or disconnects, or the storage layer errors.
    pub fn get(&self, key: Vec<u8>, timeout: Duration) -> Result<Option<Vec<u8>>, ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::Get { key, reply })?;
        recv(&response, self.pid, timeout)?
    }

    /// Read the RAW stored envelope bytes for `key` (stamp + TTL NOT stripped).
    /// Test-support for AA-3-4a stamp-equality assertions.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`].
    #[doc(hidden)]
    pub fn get_raw(&self, key: Vec<u8>, timeout: Duration) -> Result<Option<Vec<u8>>, ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::GetRaw { key, reply })?;
        recv(&response, self.pid, timeout)?
    }

    /// Append a put (durable-WAL first, then buffered), blocking for the ack.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`].
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>, timeout: Duration) -> Result<(), ShardError> {
        self.put_with_ttl(key, value, None, timeout)
    }

    /// Append a put with optional TTL metadata, blocking for the ack.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`].
    pub fn put_with_ttl(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
        timeout: Duration,
    ) -> Result<(), ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::Put {
            key,
            value,
            ttl,
            reply,
        })?;
        recv(&response, self.pid, timeout)?
    }

    /// Append a delete (durable-WAL first, then buffered), blocking for the ack.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`].
    pub fn delete(&self, key: Vec<u8>, timeout: Duration) -> Result<(), ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::Delete { key, reply })?;
        recv(&response, self.pid, timeout)?
    }

    /// Delete `key` only if it is currently present and expired, re-checked
    /// atomically inside the actor so a concurrent refresh is never clobbered.
    /// Returns whether a delete was issued.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::delete`].
    pub fn delete_if_expired(&self, key: Vec<u8>, timeout: Duration) -> Result<bool, ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::DeleteIfExpired { key, reply })?;
        recv(&response, self.pid, timeout)?
    }

    /// Flush the buffer to the tree and persist the committed-root marker,
    /// blocking for the new root.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`].
    pub fn commit(&self, timeout: Duration) -> Result<Hash, ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::Commit { reply })?;
        recv(&response, self.pid, timeout)?
    }

    /// Read the merged tree+buffer range `[from, to)` in key order, blocking
    /// for the full result.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`].
    pub fn range(
        &self,
        from: Vec<u8>,
        to: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<RangeItem>, ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::Range { from, to, reply })?;
        recv(&response, self.pid, timeout)?
    }

    /// Atomically append event entries for one logical key.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`], or
    /// [`ShardError::SequenceConflict`] when optimistic concurrency fails.
    pub fn append(
        &self,
        key: Vec<u8>,
        entries: Vec<Vec<u8>>,
        expected_seq: u64,
        timeout: Duration,
    ) -> Result<u64, ShardError> {
        self.append_with_ttl(key, entries, expected_seq, None, timeout)
    }

    /// Atomically append event entries for one logical key with optional TTL.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`], or
    /// [`ShardError::SequenceConflict`] when optimistic concurrency fails.
    pub fn append_with_ttl(
        &self,
        key: Vec<u8>,
        entries: Vec<Vec<u8>>,
        expected_seq: u64,
        ttl: Option<Duration>,
        timeout: Duration,
    ) -> Result<u64, ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::Append {
            key,
            entries,
            expected_seq,
            ttl,
            reply,
        })?;
        recv(&response, self.pid, timeout)?
    }

    /// Read the scalar `u64` value for `key`, blocking up to `timeout`.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`].
    pub fn read_value(&self, key: Vec<u8>, timeout: Duration) -> Result<Option<u64>, ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::ReadValue { key, reply })?;
        recv(&response, self.pid, timeout)?
    }

    /// Atomically compare-and-swap the scalar `u64` value at `key`.
    ///
    /// The read-compare-write executes inside the shard's single-threaded
    /// process handler, so concurrent CAS calls on the same key cannot race.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`], or
    /// [`ShardError::CasMismatch`] when the current value is not `expected`.
    pub fn cas(
        &self,
        key: Vec<u8>,
        expected: Option<u64>,
        new: u64,
        timeout: Duration,
    ) -> Result<(), ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::Cas {
            key,
            expected,
            new,
            reply,
        })?;
        recv(&response, self.pid, timeout)?
    }

    /// Conditionally and DURABLY apply a replicated write (active-active 2a-4).
    ///
    /// The read of the current tree value hash, the comparison against
    /// `expected` (the proposing writer's CAS precondition; `None` =
    /// expect-absent), the write of `value` (with `ttl`), and the fsync to stable
    /// storage all run inside ONE actor slice, so the compare and the apply cannot
    /// race against another command and the value is on disk before this returns.
    ///
    /// Before the CAS read runs, the epoch fence (AA-3-3, §2.3) checks the write's
    /// epoch (`stamp.epoch`) against this shard's actor-local `promised` ballot IN
    /// THE SAME slice: a `stamp.epoch < promised` is rejected with
    /// [`ShardError::Fenced`] (nothing applied — a stale/deposed owner's write); a
    /// `stamp.epoch >= promised` proceeds to the CAS WITHOUT raising `promised` (R2
    /// — only a Prepare advances `promised`).
    ///
    /// On a hash mismatch nothing is written and [`ShardError::CasHashMismatch`]
    /// is returned (a CAS vote-against). On a match the write is committed in the
    /// STAMPED envelope carrying `stamp` (AA-3-4a) — which fsyncs the tree nodes to
    /// the [`crate::store::DiskStore`] and the WAL's committed-root marker — BEFORE
    /// the reply, so an `Ok` attests durability. The `stamp` is the IDENTICAL stamp
    /// the owner assigned (R-SEQ), stored alongside the value; the CAS hash stays
    /// over the logical value, so the stamp is not part of the CAS identity.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`],
    /// [`ShardError::CasHashMismatch`] when the current value hash is not
    /// `expected`, or [`ShardError::Fenced`] when `stamp.epoch` is below the
    /// shard's `promised` ballot.
    pub fn apply_durable(
        &self,
        key: Vec<u8>,
        expected: Option<Hash>,
        value: Vec<u8>,
        ttl: Option<Duration>,
        stamp: Stamp,
        timeout: Duration,
    ) -> Result<(), ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::ApplyDurable {
            key,
            expected,
            value,
            ttl,
            stamp,
            reply,
        })?;
        recv(&response, self.pid, timeout)?
    }

    /// Durably record a Prepare promise for this shard (AA-3-0, §2.2). The
    /// ballot is fsync'd before the reply ONLY if it strictly exceeds the
    /// persisted `promised`; otherwise the call is a no-op and reports the
    /// current `promised` (monotonic, never regresses — even across restart).
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`].
    pub fn record_promise(
        &self,
        ballot: Ballot,
        timeout: Duration,
    ) -> Result<RecordPromiseOutcome, ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::RecordPromise { ballot, reply })?;
        recv(&response, self.pid, timeout)?
    }

    /// Durably record the ballot under which this node was elected owner of this
    /// shard (AA-3-0, §2.2). Fsync'd before the reply.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`].
    pub fn record_owner_epoch(&self, ballot: Ballot, timeout: Duration) -> Result<(), ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::RecordOwnerEpoch { ballot, reply })?;
        recv(&response, self.pid, timeout)?
    }

    /// Durably reserve a minted ballot counter for this shard (AA-3-0 R4, §2.2),
    /// fsync'd before the reply. Returns `max(persisted, counter)` so the next
    /// minted ballot strictly exceeds every ballot ever minted, across restart.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`].
    pub fn reserve_minted(&self, counter: u64, timeout: Duration) -> Result<u64, ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::ReserveMinted { counter, reply })?;
        recv(&response, self.pid, timeout)?
    }

    /// Snapshot this shard's election-relevant state in one in-slice read
    /// (AA-3-2): `(promised, owner_epoch, persisted_max_minted, committed_root)`.
    /// The candidate reads this to compute its mint floor (§2.2 step 1); the
    /// acceptor reads it to populate a Promise's `accepted_epoch`/`committed_root`.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`].
    pub fn read_promise_state(&self, timeout: Duration) -> Result<PromiseState, ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::ReadPromiseState { reply })?;
        recv(&response, self.pid, timeout)?
    }

    /// Walk this shard's entire keyspace and return every stream's decoded
    /// `(stream_key, next_seq)` sequence-metadata pair.
    ///
    /// # Errors
    /// Returns a [`ShardError`] as for [`Self::get`].
    pub fn scan_sequences(&self, timeout: Duration) -> Result<Vec<StreamSeq>, ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::ScanSequences { reply })?;
        recv(&response, self.pid, timeout)?
    }

    /// Ask the shard process to stop after it drains prior queued commands.
    ///
    /// # Errors
    /// Returns a [`ShardError`] when the process cannot be reached or does not
    /// acknowledge shutdown within `timeout`.
    pub(crate) fn shutdown(&self, timeout: Duration) -> Result<(), ShardError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(ShardCommandKind::Shutdown { reply })?;
        recv(&response, self.pid, timeout)?
    }

    /// Push a command and wake the process. On a dead pid the command is rolled
    /// back off the queue and [`ShardError::ActorUnavailable`] is returned.
    fn enqueue(&self, kind: ShardCommandKind) -> Result<(), ShardError> {
        let id = self.next_command_id.fetch_add(1, Ordering::Relaxed);
        native::lock_queue(&self.commands).push_back(ShardCommand { id, kind });
        if self
            .scheduler
            .enqueue_atom_message(self.pid, self.wake_atom)
        {
            Ok(())
        } else {
            self.remove_command(id);
            Err(ShardError::ActorUnavailable { pid: self.pid })
        }
    }

    /// Remove a queued command by id (rollback for a failed wake).
    fn remove_command(&self, id: u64) {
        native::lock_queue(&self.commands).retain(|command| command.id != id);
    }
}

/// Block on a per-command reply, mapping channel failures to [`ShardError`].
fn recv<T>(response: &mpsc::Receiver<T>, pid: u64, timeout: Duration) -> Result<T, ShardError> {
    response.recv_timeout(timeout).map_err(|error| match error {
        RecvTimeoutError::Timeout => ShardError::ReplyTimeout { pid },
        RecvTimeoutError::Disconnected => ShardError::ReplyDisconnected { pid },
    })
}
