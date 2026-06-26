//! CORE-007: the shard's beamr native process body.
//!
//! [`ShardNativeHandler`] is a real [`beamr::NativeHandler`] — it runs as a
//! first-class, scheduler-supervised process (real pid, mailbox, factory
//! restart). When the host wakes it with the shard wake atom, it drains one
//! mailbox token per queued command, pops the command under a tight lock,
//! releases the lock, runs the storage op against the wrapped [`ShardActor`]
//! (which keeps the WAL-before-buffer and committed-root invariants), and
//! replies over the command's own channel. Binary never touches a term.
//!
//! Lock discipline (spec Landmine 4): the command-queue mutex is held ONLY for
//! the `pop_front` in [`lock_queue`] / [`pop_command`]; it is released before
//! any storage op and is never held across a [`NativeOutcome`] return.
//!
//! Wake-atom constraint: the handler never inspects the wake atom's VALUE — a
//! received mailbox token (`ctx.recv().is_some()`) simply means "drain one
//! command". That is WHY the host can intern the wake atom from a fresh local
//! `AtomTable` (see `ShardHandle::spawn`) without sharing the scheduler's table.
//! WARNING: if future code ever pattern-matches on the atom value in the mailbox
//! (e.g. to distinguish a wake from an exit signal), it MUST intern that atom
//! from the scheduler's own atom table, not a fresh local one — a fresh-table
//! atom has a different id and the match would silently fail.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::MutexGuard;
use std::sync::mpsc::SyncSender;

use beamr::process::ExitReason;
use beamr::{NativeContext, NativeHandler, NativeOutcome};

use crate::shard::actor::ShardActor;
use crate::store::DiskStore;
use crate::tree::Cursor;
use crate::ttl::filter::{Visibility, visible_value};
use crate::wal::{DurableWal, FsyncPolicy, Mutation, WalRecovery};

use super::handle::{
    CommandQueue, RangeItem, ShardCommand, ShardCommandKind, ShardError, StreamSeq,
};
use super::{GroupOutcome, GroupWrite};

/// The reply channel shared by every groupable durable write (`Cas`,
/// `ApplyDurable`, `ApplyDurableTombstone` all reply `Result<(), ShardError>`).
type GroupReply = SyncSender<Result<(), ShardError>>;

/// The wrapped-storage state plus the bridge queue, run as a beamr process.
///
/// On a successful boot `state` is `Some`. If opening the store/WAL or
/// recovering failed, `state` is `None` and `startup_error` carries the cause:
/// the first slice replies-drains the queue with that error and stops cleanly
/// rather than panicking the scheduler.
pub struct ShardNativeHandler {
    state: Option<ShardState>,
    startup_error: Option<ShardError>,
    commands: CommandQueue,
}

/// The live storage owned by a booted shard process.
struct ShardState {
    actor: ShardActor,
    store: DiskStore,
}

impl ShardNativeHandler {
    /// Build the restart-capable factory the scheduler stores and re-invokes.
    ///
    /// Each invocation re-opens the store + WAL and re-runs recovery against the
    /// SAME paths, so a re-spawn replays the durable WAL and resumes the shard.
    pub(super) fn make_factory(
        store_dir: PathBuf,
        wal_path: PathBuf,
        commands: CommandQueue,
    ) -> beamr::native::native_process::NativeHandlerFactory {
        Box::new(move || Box::new(Self::build(&store_dir, &wal_path, Arc::clone(&commands))))
    }

    /// Open the store + WAL and recover. Any failure yields a sentinel handler
    /// that stops cleanly on its first slice.
    fn build(store_dir: &Path, wal_path: &Path, commands: CommandQueue) -> Self {
        match Self::boot(store_dir, wal_path) {
            Ok(state) => Self {
                state: Some(state),
                startup_error: None,
                commands,
            },
            Err(error) => Self {
                state: None,
                startup_error: Some(error),
                commands,
            },
        }
    }

    /// Open the store, recover the WAL against it, and seed a [`ShardActor`].
    fn boot(store_dir: &Path, wal_path: &Path) -> Result<ShardState, ShardError> {
        let store = DiskStore::new(store_dir)?;
        let recovered = WalRecovery::recover_path(wal_path, &store)?;
        let wal = DurableWal::new(wal_path, FsyncPolicy::CommitOnly)?;
        let actor = ShardActor::from_recovered(wal, recovered, &store)?;
        Ok(ShardState { actor, store })
    }

    /// Drain the next unit of work for one mailbox token, replying over each
    /// command's channel. Returns a stop outcome when a command requested process
    /// shutdown.
    ///
    /// Group commit (audit E): if the front of the queue is a groupable durable
    /// WRITE (`Cas` / `ApplyDurable` / `ApplyDurableTombstone`), a maximal run of
    /// consecutive such commands is popped under ONE tight lock and committed in
    /// ONE fsync via [`ShardActor::apply_group`], collapsing N fsyncs to one.
    /// Otherwise exactly one command is popped and run as before. (A group of K
    /// commands consumes K queued items but only this one token; the K-1 surplus
    /// tokens later find an empty queue — the existing `QueueEmpty` = spurious
    /// case — so no work is lost or double-run.)
    fn pop_and_execute(state: &mut ShardState, commands: &CommandQueue) -> Option<NativeOutcome> {
        match pop_next_unit(commands)? {
            DrainUnit::Group(group) => {
                state.run_group(group);
                None
            }
            DrainUnit::Single(command) => state.execute(command),
        }
    }
}

/// One unit of work drained for a single mailbox token: either a coalesced run of
/// groupable durable writes, or one non-groupable command run on its own.
enum DrainUnit {
    Group(Vec<(GroupWrite, GroupReply)>),
    Single(ShardCommand),
}

/// Pop the next unit of work under a TIGHT lock (spec Landmine 4): the queue mutex
/// is held ONLY for the peek/pop here and released before any storage op or fsync.
///
/// If the front command is a groupable durable write, pop the maximal CONSECUTIVE
/// run of them (stopping at the first non-groupable command, which is left on the
/// queue and handled on its own next time). Otherwise pop exactly one command.
fn pop_next_unit(commands: &CommandQueue) -> Option<DrainUnit> {
    let mut queue = lock_queue(commands);
    if !queue
        .front()
        .is_some_and(|command| is_groupable(&command.kind))
    {
        return queue.pop_front().map(DrainUnit::Single);
    }
    let mut group = Vec::new();
    while queue
        .front()
        .is_some_and(|command| is_groupable(&command.kind))
    {
        // The front is groupable, so `pop_front` yields it and `into_group_write`
        // returns `Some` — but never `unwrap`: a (logically impossible) `None`
        // simply ends the run rather than panicking.
        if let Some(command) = queue.pop_front()
            && let Some(entry) = into_group_write(command.kind)
        {
            group.push(entry);
        }
    }
    Some(DrainUnit::Group(group))
}

/// Whether a command is a groupable durable WRITE that may be coalesced into a
/// group commit. ONLY single-key CAS / stamped value apply / stamped tombstone
/// apply qualify — they each fsync one root today. Everything else (promise-state
/// mutators, `Prepare`/`merge_adopt`, the all-or-nothing `ApplyDurableBatch`,
/// `Append`, reads, `Commit`, `Shutdown`) is NOT groupable and keeps its own
/// semantics and ordering.
const fn is_groupable(kind: &ShardCommandKind) -> bool {
    matches!(
        kind,
        ShardCommandKind::Cas { .. }
            | ShardCommandKind::ApplyDurable { .. }
            | ShardCommandKind::ApplyDurableTombstone { .. }
    )
}

/// Convert a groupable command kind into its [`GroupWrite`] descriptor plus reply
/// channel. Returns `None` for any non-groupable kind (the caller only ever passes
/// kinds for which [`is_groupable`] is true, so `None` never occurs in practice).
fn into_group_write(kind: ShardCommandKind) -> Option<(GroupWrite, GroupReply)> {
    match kind {
        ShardCommandKind::Cas {
            key,
            expected,
            new,
            reply,
        } => Some((GroupWrite::Cas { key, expected, new }, reply)),
        ShardCommandKind::ApplyDurable {
            key,
            expected,
            value,
            ttl,
            stamp,
            reply,
        } => Some((
            GroupWrite::ApplyValue {
                key,
                expected,
                value,
                ttl,
                stamp,
            },
            reply,
        )),
        ShardCommandKind::ApplyDurableTombstone {
            key,
            expected,
            stamp,
            reply,
        } => Some((
            GroupWrite::ApplyTombstone {
                key,
                expected,
                stamp,
            },
            reply,
        )),
        _ => None,
    }
}

impl ShardState {
    /// Run a coalesced group of durable writes in ONE group commit and fan the
    /// per-write outcome to each command's reply channel (audit E).
    ///
    /// The writes are split from their reply senders, staged + committed once by
    /// [`ShardActor::apply_group`] (which returns one outcome per write, in order),
    /// then each outcome is mapped back to its sender: a survivor gets `Ok(())`, a
    /// rejected write gets its own CAS/fence error, and — if the shared commit
    /// failed — every survivor gets the retryable commit error.
    fn run_group(&mut self, group: Vec<(GroupWrite, GroupReply)>) {
        let mut writes = Vec::with_capacity(group.len());
        let mut replies = Vec::with_capacity(group.len());
        for (write, reply) in group {
            writes.push(write);
            replies.push(reply);
        }
        let outcomes = self.actor.apply_group(writes, &mut self.store);
        for (reply, outcome) in replies.into_iter().zip(outcomes) {
            let result = match outcome {
                GroupOutcome::Committed => Ok(()),
                GroupOutcome::Rejected(error) | GroupOutcome::CommitFailed(error) => Err(error),
            };
            drop(reply.send(result));
        }
    }

    /// Run one command against the wrapped storage and send the reply.
    fn execute(&mut self, command: ShardCommand) -> Option<NativeOutcome> {
        match command.kind {
            ShardCommandKind::Get { key, reply } => {
                let result = self.actor.get(&key, &self.store).map_err(ShardError::from);
                drop(reply.send(result));
                None
            }
            ShardCommandKind::GetRaw { key, reply } => {
                let result = self
                    .actor
                    .get_raw(&key, &self.store)
                    .map_err(ShardError::from);
                drop(reply.send(result));
                None
            }
            ShardCommandKind::Put {
                key,
                value,
                ttl,
                reply,
            } => {
                let result = self
                    .actor
                    .put_with_ttl(key, value, ttl)
                    .map_err(ShardError::from);
                drop(reply.send(result));
                None
            }
            ShardCommandKind::Delete { key, stamp, reply } => {
                let result = self.actor.delete(key, stamp).map_err(ShardError::from);
                drop(reply.send(result));
                None
            }
            ShardCommandKind::DeleteIfExpired { key, reply } => {
                let result = self
                    .actor
                    .delete_if_expired(&key, &self.store)
                    .map_err(ShardError::from);
                drop(reply.send(result));
                None
            }
            ShardCommandKind::Commit { reply } => {
                let result = self.actor.commit(&mut self.store).map_err(ShardError::from);
                drop(reply.send(result));
                None
            }
            range @ (ShardCommandKind::Range { .. } | ShardCommandKind::HasLiveInRange(..)) => {
                self.execute_range(range)
            }
            ShardCommandKind::Append {
                key,
                entries,
                expected_seq,
                ttl,
                reply,
            } => {
                let result = self
                    .actor
                    .append(&key, entries, expected_seq, ttl, &mut self.store)
                    .map_err(ShardError::from);
                drop(reply.send(result));
                None
            }
            ShardCommandKind::ReadValue { key, reply } => {
                let result = self
                    .actor
                    .read_value(&key, &self.store)
                    .map_err(ShardError::from);
                drop(reply.send(result));
                None
            }
            ShardCommandKind::ReadPromiseState { reply } => {
                drop(reply.send(Ok(self.actor.promise_state())));
                None
            }
            extra @ (ShardCommandKind::Cas { .. }
            | ShardCommandKind::ApplyDurable { .. }
            | ShardCommandKind::ApplyDurableTombstone { .. }
            | ShardCommandKind::ApplyDurableBatch { .. }
            | ShardCommandKind::RecordPromise { .. }
            | ShardCommandKind::RecordOwnerEpoch { .. }
            | ShardCommandKind::ReserveMinted { .. }
            | ShardCommandKind::ExportReachable { .. }
            | ShardCommandKind::MergeAdopt { .. }) => self.execute_extra(extra),
            ShardCommandKind::ScanSequences { reply } => {
                drop(reply.send(self.scan_sequences()));
                None
            }
            ShardCommandKind::Shutdown { reply } => {
                drop(reply.send(Ok(())));
                Some(NativeOutcome::Stop(ExitReason::Normal))
            }
        }
    }

    /// Run one CAS / durable-apply / AA-3-0 promise-state command against the
    /// actor (the promise mutators fsync before their reply). Split out of
    /// [`Self::execute`] to keep that dispatch under the line budget.
    fn execute_extra(&mut self, command: ShardCommandKind) -> Option<NativeOutcome> {
        match command {
            ShardCommandKind::Cas {
                key,
                expected,
                new,
                reply,
            } => {
                let result = self
                    .actor
                    .cas(&key, expected, new, &mut self.store)
                    .map_err(ShardError::from);
                drop(reply.send(result));
            }
            ShardCommandKind::ApplyDurable {
                key,
                expected,
                value,
                ttl,
                stamp,
                reply,
            } => {
                let result = self
                    .actor
                    .apply_durable(&key, expected, value, ttl, stamp, &mut self.store)
                    .map_err(ShardError::from);
                drop(reply.send(result));
            }
            ShardCommandKind::ApplyDurableTombstone {
                key,
                expected,
                stamp,
                reply,
            } => {
                let result = self
                    .actor
                    .apply_durable_tombstone(&key, expected, stamp, &mut self.store)
                    .map_err(ShardError::from);
                drop(reply.send(result));
            }
            ShardCommandKind::ApplyDurableBatch {
                items,
                stamp,
                reply,
            } => {
                let result = self
                    .actor
                    .apply_durable_batch(items, stamp, &mut self.store)
                    .map_err(ShardError::from);
                drop(reply.send(result));
            }
            ShardCommandKind::RecordPromise { ballot, reply } => {
                let result = self.actor.record_promise(ballot).map_err(ShardError::from);
                drop(reply.send(result));
            }
            ShardCommandKind::RecordOwnerEpoch { ballot, reply } => {
                let result = self
                    .actor
                    .record_owner_epoch(ballot)
                    .map_err(ShardError::from);
                drop(reply.send(result));
            }
            ShardCommandKind::ReserveMinted { counter, reply } => {
                let result = self.actor.reserve_minted(counter).map_err(ShardError::from);
                drop(reply.send(result));
            }
            ShardCommandKind::ExportReachable { shard_id, reply } => {
                let result = self
                    .actor
                    .export_reachable(shard_id, &self.store)
                    .map_err(ShardError::from);
                drop(reply.send(result));
            }
            ShardCommandKind::MergeAdopt { promisers, reply } => {
                let result = self
                    .actor
                    .merge_adopt(&promisers, &mut self.store)
                    .map_err(ShardError::from);
                drop(reply.send(result));
            }
            _ => {}
        }
        None
    }

    /// Decode every stream's sequence metadata from the actor-owned index.
    fn scan_sequences(&self) -> Result<Vec<StreamSeq>, ShardError> {
        self.actor.scan_sequences()
    }

    /// Run range-shaped commands against the merged tree+buffer read view.
    fn execute_range(&self, command: ShardCommandKind) -> Option<NativeOutcome> {
        match command {
            ShardCommandKind::Range { from, to, reply } => {
                drop(reply.send(self.collect_range(&from, &to)));
            }
            ShardCommandKind::HasLiveInRange(from, to, reply) => {
                let result = super::liveness::has_live_in_range(
                    &self.store,
                    self.actor.committed_root(),
                    self.actor.buffer(),
                    from.as_slice(),
                    to.as_slice(),
                );
                drop(reply.send(result));
            }
            _ => {}
        }
        None
    }

    /// Merge committed-tree entries with the live buffer for `[from, to)`.
    fn collect_range(&self, from: &[u8], to: &[u8]) -> Result<Vec<RangeItem>, ShardError> {
        let tree_entries = match self.actor.committed_root() {
            Some(root) => Cursor::new(&self.store, root)
                .range(from, to)
                .collect::<Result<Vec<_>, _>>()
                .map_err(ShardError::from)?,
            None => Vec::new(),
        };
        let buffer_entries: Vec<&Mutation> = self
            .actor
            .buffer()
            .iter()
            .filter(|mutation| in_range(mutation, from, to))
            .collect();
        merge_range(&tree_entries, &buffer_entries)
    }
}

impl NativeHandler for ShardNativeHandler {
    fn handle(&mut self, ctx: &mut NativeContext<'_>) -> NativeOutcome {
        let Some(state) = self.state.as_mut() else {
            return self.fail_startup();
        };
        // Drain one queued command per mailbox token; QueueEmpty = spurious.
        while ctx.recv().is_some() {
            if let Some(outcome) = Self::pop_and_execute(state, &self.commands) {
                return outcome;
            }
        }
        NativeOutcome::Wait
    }
}

impl ShardNativeHandler {
    /// Sentinel slice: a shard that failed to boot drains the queue with the
    /// startup error so callers fail fast, then stops cleanly.
    fn fail_startup(&self) -> NativeOutcome {
        let message = self
            .startup_error
            .as_ref()
            .map_or_else(|| "shard startup failed".to_owned(), ToString::to_string);
        while let Some(command) = pop_command(&self.commands) {
            super::startup::reply_startup_error(command, &message);
        }
        NativeOutcome::Stop(ExitReason::Error)
    }
}

/// Lock the command queue. The guard is held only by the immediate caller for a
/// `push_back` / `retain` / `pop_front`; it must never span a storage op.
pub(super) fn lock_queue(commands: &CommandQueue) -> MutexGuard<'_, VecDeque<ShardCommand>> {
    commands
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Pop one command under a tight lock, releasing it before returning.
fn pop_command(commands: &CommandQueue) -> Option<ShardCommand> {
    lock_queue(commands).pop_front()
}

/// True when `mutation`'s key lies in `[from, to)`.
fn in_range(mutation: &Mutation, from: &[u8], to: &[u8]) -> bool {
    let key = mutation.key();
    from <= key && key < to
}

/// Two-way merge of committed-tree entries and buffered mutations into an
/// ordered range result. A buffered key shadows the tree; a buffered delete
/// removes the key from the result. Ported from the CORE-007 reference.
fn merge_range(
    tree_entries: &[(Vec<u8>, Vec<u8>)],
    buffer_entries: &[&Mutation],
) -> Result<Vec<RangeItem>, ShardError> {
    // Upper bound: every tree entry plus every buffered mutation can yield at
    // most one item, plus the trailing `Done` sentinel. Both counts are local
    // (not attacker-controlled), so this single allocation never reallocates.
    let capacity = tree_entries
        .len()
        .saturating_add(buffer_entries.len())
        .saturating_add(1);
    let mut items = Vec::with_capacity(capacity);
    let mut tree_index = 0;
    let mut buffer_index = 0;
    loop {
        match (
            tree_entries.get(tree_index),
            buffer_entries.get(buffer_index),
        ) {
            (Some((tree_key, tree_value)), Some(buffer_mutation)) => {
                match tree_key.as_slice().cmp(buffer_mutation.key()) {
                    Ordering::Less => {
                        push_entry(&mut items, tree_key, tree_value)?;
                        tree_index = tree_index.saturating_add(1);
                    }
                    Ordering::Equal => {
                        push_mutation(&mut items, buffer_mutation)?;
                        tree_index = tree_index.saturating_add(1);
                        buffer_index = buffer_index.saturating_add(1);
                    }
                    Ordering::Greater => {
                        push_mutation(&mut items, buffer_mutation)?;
                        buffer_index = buffer_index.saturating_add(1);
                    }
                }
            }
            (Some((tree_key, tree_value)), None) => {
                push_entry(&mut items, tree_key, tree_value)?;
                tree_index = tree_index.saturating_add(1);
            }
            (None, Some(buffer_mutation)) => {
                push_mutation(&mut items, buffer_mutation)?;
                buffer_index = buffer_index.saturating_add(1);
            }
            (None, None) => break,
        }
    }
    items.push(RangeItem::Done);
    Ok(items)
}

/// Append a visible tree entry to the range result.
fn push_entry(items: &mut Vec<RangeItem>, key: &[u8], value: &[u8]) -> Result<(), ShardError> {
    match visible_value(value)
        .map_err(|error| ShardError::Wal(crate::wal::WalError::TreeError(error.to_string())))?
    {
        Visibility::Live(value) => items.push(RangeItem::Entry {
            key: key.to_vec(),
            value,
        }),
        Visibility::Expired => {}
    }
    Ok(())
}

/// Append a buffered mutation: a put becomes an entry, a delete is skipped.
fn push_mutation(items: &mut Vec<RangeItem>, mutation: &Mutation) -> Result<(), ShardError> {
    if let Mutation::Put { key, value } = mutation {
        push_entry(items, key, value)?;
    }
    Ok(())
}

/// Pre-flight (spec Phase 0): a [`DiskStore`] must be `Send` so it can live
/// inside a [`NativeHandler`] (which is `Send + 'static`). `RefCell<LruCache>`
/// is `Send` when its contents are `Send`, so no store change is needed; this
/// fails to compile if that ever regresses.
#[cfg(test)]
const fn assert_disk_store_send() {
    const fn require_send<T: Send>() {}
    require_send::<DiskStore>();
    require_send::<ShardNativeHandler>();
    let _ = require_send::<DiskStore>;
}

#[cfg(test)]
const _: () = assert_disk_store_send();

#[cfg(test)]
mod boot_failure_tests {
    use super::ShardNativeHandler;
    use crate::shard::actor::handle::{
        CommandQueue, RangeItem, ShardCommand, ShardCommandKind, ShardError,
    };
    use beamr::NativeOutcome;
    use beamr::process::ExitReason;
    use std::collections::VecDeque;
    use std::error::Error;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};

    /// Build a handler whose boot DETERMINISTICALLY fails: the store path is a
    /// regular file, so `DiskStore::new` errors and the handler boots into the
    /// sentinel (`state = None`, `startup_error = Some`). The `TempDir` is
    /// returned so the caller keeps it alive.
    fn boot_failed_handler(
        commands: CommandQueue,
    ) -> Result<(ShardNativeHandler, tempfile::TempDir), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let store_path = dir.path().join("store-is-a-file");
        std::fs::write(&store_path, b"not a directory")?;
        let wal_path = dir.path().join("shard.wal");
        let handler = ShardNativeHandler::build(&store_path, &wal_path, commands);
        Ok((handler, dir))
    }

    /// The queued-at-boot drain path: a command already on the queue when a
    /// boot-failed shard runs its sentinel slice must fail fast with
    /// [`ShardError::Spawn`] (never hang, never a storage error), the sentinel
    /// must stop the process, and the queue must be fully drained. One command of
    /// EACH kind exercises every `reply_startup_error` arm. This covers the path
    /// that is racy to force through the live scheduler (the host runs the first
    /// slice before an external command can be enqueued), so it is asserted here
    /// against the sentinel directly.
    #[test]
    fn fail_startup_drains_every_command_kind_with_spawn_then_stops() -> Result<(), Box<dyn Error>>
    {
        let commands: CommandQueue = Arc::new(Mutex::new(VecDeque::new()));
        let (get_tx, get_rx) = mpsc::sync_channel(1);
        let (put_tx, put_rx) = mpsc::sync_channel(1);
        let (del_tx, del_rx) = mpsc::sync_channel(1);
        let (commit_tx, commit_rx) = mpsc::sync_channel(1);
        let (range_tx, range_rx) = mpsc::sync_channel::<Result<Vec<RangeItem>, ShardError>>(1);
        {
            let mut queue = commands.lock().map_err(|_| "queue poisoned")?;
            queue.push_back(ShardCommand {
                id: 1,
                kind: ShardCommandKind::Get {
                    key: b"k".to_vec(),
                    reply: get_tx,
                },
            });
            queue.push_back(ShardCommand {
                id: 2,
                kind: ShardCommandKind::Put {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    ttl: None,
                    reply: put_tx,
                },
            });
            queue.push_back(ShardCommand {
                id: 3,
                kind: ShardCommandKind::Delete {
                    key: b"k".to_vec(),
                    stamp: crate::sync::ballot::Stamp::bottom(),
                    reply: del_tx,
                },
            });
            queue.push_back(ShardCommand {
                id: 4,
                kind: ShardCommandKind::Commit { reply: commit_tx },
            });
            queue.push_back(ShardCommand {
                id: 5,
                kind: ShardCommandKind::Range {
                    from: b"a".to_vec(),
                    to: b"z".to_vec(),
                    reply: range_tx,
                },
            });
        }

        let (handler, _dir) = boot_failed_handler(Arc::clone(&commands))?;
        assert!(handler.state.is_none(), "boot must have failed");
        assert!(handler.startup_error.is_some(), "startup_error must be set");

        let outcome = handler.fail_startup();

        assert!(
            matches!(get_rx.try_recv(), Ok(Err(ShardError::Spawn(_)))),
            "Get arm"
        );
        assert!(
            matches!(put_rx.try_recv(), Ok(Err(ShardError::Spawn(_)))),
            "Put arm"
        );
        assert!(
            matches!(del_rx.try_recv(), Ok(Err(ShardError::Spawn(_)))),
            "Delete arm"
        );
        assert!(
            matches!(commit_rx.try_recv(), Ok(Err(ShardError::Spawn(_)))),
            "Commit arm"
        );
        assert!(
            matches!(range_rx.try_recv(), Ok(Err(ShardError::Spawn(_)))),
            "Range arm"
        );

        assert!(
            matches!(outcome, NativeOutcome::Stop(ExitReason::Error)),
            "sentinel stops"
        );
        assert!(
            commands.lock().map_err(|_| "queue poisoned")?.is_empty(),
            "queue fully drained"
        );
        Ok(())
    }
}

/// Group commit (audit E): the native drain loop's grouping/forwarding logic,
/// tested against a hand-built queue (deterministic, no scheduler race). These
/// prove `pop_next_unit` coalesces a CONSECUTIVE run of groupable writes and STOPS
/// at the first non-groupable command — so promise-state mutators (`RecordPromise`)
/// and friends are never swallowed into a group and keep their own slice.
#[cfg(test)]
mod group_drain_tests {
    use super::{DrainUnit, lock_queue, pop_next_unit};
    use crate::shard::actor::handle::{CommandQueue, ShardCommand, ShardCommandKind};
    use crate::sync::ballot::{Ballot, Stamp};
    use crate::sync::topology::SyncNodeId;
    use std::collections::VecDeque;
    use std::error::Error;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};

    fn stamp() -> Stamp {
        Stamp::new(Ballot::new(1, SyncNodeId::new("owner")), 0)
    }

    fn apply_command(id: u64, key: &[u8]) -> ShardCommand {
        let (reply, _rx) = mpsc::sync_channel(1);
        ShardCommand {
            id,
            kind: ShardCommandKind::ApplyDurable {
                key: key.to_vec(),
                expected: None,
                value: b"v".to_vec(),
                ttl: None,
                stamp: stamp(),
                reply,
            },
        }
    }

    fn cas_command(id: u64, key: &[u8]) -> ShardCommand {
        let (reply, _rx) = mpsc::sync_channel(1);
        ShardCommand {
            id,
            kind: ShardCommandKind::Cas {
                key: key.to_vec(),
                expected: None,
                new: 1,
                reply,
            },
        }
    }

    fn promise_command(id: u64) -> ShardCommand {
        let (reply, _rx) = mpsc::sync_channel(1);
        ShardCommand {
            id,
            kind: ShardCommandKind::RecordPromise {
                ballot: Ballot::new(2, SyncNodeId::new("owner")),
                reply,
            },
        }
    }

    /// A consecutive run of groupable writes is coalesced into ONE group, the run
    /// STOPS at the non-groupable `RecordPromise` (which is left on the queue and
    /// drained on its own next), and the trailing groupable write forms its own
    /// group. This is the wiring-level proof that Prepare/promise are NOT coalesced.
    #[test]
    fn drain_groups_consecutive_writes_and_stops_at_non_groupable() -> Result<(), Box<dyn Error>> {
        let commands: CommandQueue = Arc::new(Mutex::new(VecDeque::new()));
        {
            let mut queue = lock_queue(&commands);
            queue.push_back(apply_command(1, b"a")); // groupable
            queue.push_back(cas_command(2, b"b")); // groupable
            queue.push_back(promise_command(3)); // NOT groupable — stops the run
            queue.push_back(apply_command(4, b"c")); // groupable (own group)
        }

        // First unit: a group of exactly the first TWO groupable writes.
        match pop_next_unit(&commands).ok_or("expected a first unit")? {
            DrainUnit::Group(group) => assert_eq!(group.len(), 2, "coalesce the leading run"),
            DrainUnit::Single(_) => return Err("expected a group, got a single".into()),
        }

        // Second unit: the non-groupable RecordPromise, on its own (NOT coalesced).
        match pop_next_unit(&commands).ok_or("expected a second unit")? {
            DrainUnit::Single(command) => assert!(
                matches!(command.kind, ShardCommandKind::RecordPromise { .. }),
                "RecordPromise must be handled as a single, never grouped"
            ),
            DrainUnit::Group(_) => return Err("RecordPromise must not be grouped".into()),
        }

        // Third unit: the trailing groupable write as its own group of one.
        match pop_next_unit(&commands).ok_or("expected a third unit")? {
            DrainUnit::Group(group) => assert_eq!(group.len(), 1, "trailing write groups alone"),
            DrainUnit::Single(_) => return Err("expected a trailing group".into()),
        }

        // Queue is now empty.
        assert!(pop_next_unit(&commands).is_none(), "queue fully drained");
        Ok(())
    }

    /// A non-groupable command at the FRONT is popped as a single, never wrapped in
    /// a group — even when groupable writes follow it.
    #[test]
    fn non_groupable_front_is_single_even_with_groupable_behind() -> Result<(), Box<dyn Error>> {
        let commands: CommandQueue = Arc::new(Mutex::new(VecDeque::new()));
        {
            let mut queue = lock_queue(&commands);
            queue.push_back(promise_command(1)); // NOT groupable, at the front
            queue.push_back(apply_command(2, b"a")); // groupable, behind it
        }
        match pop_next_unit(&commands).ok_or("expected a unit")? {
            DrainUnit::Single(command) => assert!(matches!(
                command.kind,
                ShardCommandKind::RecordPromise { .. }
            )),
            DrainUnit::Group(_) => return Err("front non-groupable must be single".into()),
        }
        match pop_next_unit(&commands).ok_or("expected a unit")? {
            DrainUnit::Group(group) => assert_eq!(group.len(), 1),
            DrainUnit::Single(_) => return Err("trailing write should group".into()),
        }
        Ok(())
    }
}
