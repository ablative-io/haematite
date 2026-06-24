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
use std::sync::{Arc, MutexGuard};

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
        let actor = ShardActor::from_recovered(wal, recovered);
        Ok(ShardState { actor, store })
    }

    /// Pop and run exactly one queued command, replying over its channel.
    /// Returns a stop outcome when the command requested process shutdown.
    fn pop_and_execute(state: &mut ShardState, commands: &CommandQueue) -> Option<NativeOutcome> {
        let command = pop_command(commands)?;
        state.execute(command)
    }
}

impl ShardState {
    /// Run one command against the wrapped storage and send the reply.
    fn execute(&mut self, command: ShardCommand) -> Option<NativeOutcome> {
        match command.kind {
            ShardCommandKind::Get { key, reply } => {
                let result = self.actor.get(&key, &self.store).map_err(ShardError::from);
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
            ShardCommandKind::Delete { key, reply } => {
                let result = self.actor.delete(key).map_err(ShardError::from);
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
            ShardCommandKind::Range { from, to, reply } => {
                drop(reply.send(self.collect_range(&from, &to)));
                None
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
            | ShardCommandKind::RecordPromise { .. }
            | ShardCommandKind::RecordOwnerEpoch { .. }
            | ShardCommandKind::ReserveMinted { .. }) => self.execute_extra(extra),
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
                write_epoch,
                reply,
            } => {
                let result = self
                    .actor
                    .apply_durable(&key, expected, value, ttl, write_epoch, &mut self.store)
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
            _ => {}
        }
        None
    }

    /// Walk the whole shard and decode every stream's sequence metadata; see
    /// [`super::scan::scan_sequences`].
    fn scan_sequences(&self) -> Result<Vec<StreamSeq>, ShardError> {
        super::scan::scan_sequences(
            &self.store,
            self.actor.committed_root(),
            self.actor.buffer(),
        )
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
    let mut items = Vec::new();
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
