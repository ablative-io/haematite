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
use crate::wal::{DurableWal, FsyncPolicy, Mutation, WalRecovery};

use super::handle::{CommandQueue, RangeItem, ShardCommand, ShardCommandKind, ShardError};

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
    /// Returns `false` when the queue was empty (a spurious wake).
    fn pop_and_execute(state: &mut ShardState, commands: &CommandQueue) -> bool {
        let Some(command) = pop_command(commands) else {
            return false;
        };
        state.execute(command);
        true
    }
}

impl ShardState {
    /// Run one command against the wrapped storage and send the reply.
    fn execute(&mut self, command: ShardCommand) {
        match command.kind {
            ShardCommandKind::Get { key, reply } => {
                let result = self.actor.get(&key, &self.store).map_err(ShardError::from);
                let _sent = reply.send(result);
            }
            ShardCommandKind::Put { key, value, reply } => {
                let result = self.actor.put(key, value).map_err(ShardError::from);
                let _sent = reply.send(result);
            }
            ShardCommandKind::Delete { key, reply } => {
                let result = self.actor.delete(key).map_err(ShardError::from);
                let _sent = reply.send(result);
            }
            ShardCommandKind::Commit { reply } => {
                let result = self.actor.commit(&mut self.store).map_err(ShardError::from);
                let _sent = reply.send(result);
            }
            ShardCommandKind::Range { from, to, reply } => {
                let _sent = reply.send(self.collect_range(&from, &to));
            }
        }
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
        Ok(merge_range(&tree_entries, &buffer_entries))
    }
}

impl NativeHandler for ShardNativeHandler {
    fn handle(&mut self, ctx: &mut NativeContext<'_>) -> NativeOutcome {
        let Some(state) = self.state.as_mut() else {
            return self.fail_startup();
        };
        // Drain one queued command per mailbox token; QueueEmpty = spurious.
        while ctx.recv().is_some() {
            let _executed = Self::pop_and_execute(state, &self.commands);
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
            reply_startup_error(command, &message);
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

/// Reply to a queued command with a startup error so its caller fails fast.
fn reply_startup_error(command: ShardCommand, message: &str) {
    let error = || ShardError::Spawn(message.to_owned());
    match command.kind {
        ShardCommandKind::Get { reply, .. } => {
            let _sent = reply.send(Err(error()));
        }
        ShardCommandKind::Put { reply, .. } | ShardCommandKind::Delete { reply, .. } => {
            let _sent = reply.send(Err(error()));
        }
        ShardCommandKind::Commit { reply } => {
            let _sent = reply.send(Err(error()));
        }
        ShardCommandKind::Range { reply, .. } => {
            let _sent = reply.send(Err(error()));
        }
    }
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
) -> Vec<RangeItem> {
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
                        push_entry(&mut items, tree_key, tree_value);
                        tree_index = tree_index.saturating_add(1);
                    }
                    Ordering::Equal => {
                        push_mutation(&mut items, buffer_mutation);
                        tree_index = tree_index.saturating_add(1);
                        buffer_index = buffer_index.saturating_add(1);
                    }
                    Ordering::Greater => {
                        push_mutation(&mut items, buffer_mutation);
                        buffer_index = buffer_index.saturating_add(1);
                    }
                }
            }
            (Some((tree_key, tree_value)), None) => {
                push_entry(&mut items, tree_key, tree_value);
                tree_index = tree_index.saturating_add(1);
            }
            (None, Some(buffer_mutation)) => {
                push_mutation(&mut items, buffer_mutation);
                buffer_index = buffer_index.saturating_add(1);
            }
            (None, None) => break,
        }
    }
    items.push(RangeItem::Done);
    items
}

/// Append a tree entry (always a live value) to the range result.
fn push_entry(items: &mut Vec<RangeItem>, key: &[u8], value: &[u8]) {
    items.push(RangeItem::Entry {
        key: key.to_vec(),
        value: value.to_vec(),
    });
}

/// Append a buffered mutation: a put becomes an entry, a delete is skipped.
fn push_mutation(items: &mut Vec<RangeItem>, mutation: &Mutation) {
    if let Mutation::Put { key, value } = mutation {
        push_entry(items, key, value);
    }
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
