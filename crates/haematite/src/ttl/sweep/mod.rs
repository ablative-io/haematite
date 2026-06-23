// API-003: TTL sweep actor

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use beamr::atom::{Atom, AtomTable};
use beamr::process::ExitReason;
use beamr::scheduler::Scheduler;
use beamr::term::{Term, boxed::Tuple};
use beamr::{NativeContext, NativeHandler, NativeOutcome};

use self::recover::{collect_tree, recover_view};
use crate::shard::actor::ShardHandle;
use crate::ttl::filter::is_expired_at;
use crate::wal::Mutation;

mod recover;

const WAKE_ATOM_NAME: &str = "haematite_sweep_wake";
const STOP_ATOM_NAME: &str = "haematite_sweep_stop";
const TICK_ATOM_NAME: &str = "haematite_sweep_tick";
const SUPERVISOR_STOP_ATOM_NAME: &str = "haematite_sweep_supervisor_stop";

/// Statistics returned by a sweep pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SweepStats {
    pub scanned: usize,
    pub expired: usize,
    pub deleted: usize,
}

/// Errors surfaced by the sweep actor handle.
#[derive(Debug)]
pub enum SweepError {
    ActorUnavailable { pid: u64 },
    ReplyDisconnected { pid: u64 },
    ReplyTimeout { pid: u64 },
    Spawn(String),
    Store(String),
    Shard(String),
}

impl fmt::Display for SweepError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ActorUnavailable { pid } => write!(formatter, "sweep actor {pid} is unavailable"),
            Self::ReplyDisconnected { pid } => {
                write!(formatter, "sweep actor {pid} reply channel disconnected")
            }
            Self::ReplyTimeout { pid } => {
                write!(formatter, "timed out waiting for sweep actor {pid}")
            }
            Self::Spawn(message) => write!(formatter, "sweep spawn failed: {message}"),
            Self::Store(message) => write!(formatter, "sweep storage error: {message}"),
            Self::Shard(message) => write!(formatter, "sweep shard delete failed: {message}"),
        }
    }
}

impl std::error::Error for SweepError {}

type SweepReply = SyncSender<Result<SweepStats, SweepError>>;
type UnitReply = SyncSender<Result<(), SweepError>>;

enum SweepCommandKind {
    Run { reply: SweepReply },
    Shutdown { reply: UnitReply },
}

struct SweepCommand {
    id: u64,
    kind: SweepCommandKind,
}

type CommandQueue = Arc<Mutex<Vec<SweepCommand>>>;

/// Host-side handle to one per-shard TTL sweep process.
#[derive(Clone)]
pub struct SweepHandle {
    child_pid: Arc<Mutex<Option<u64>>>,
    scheduler: Arc<Scheduler>,
    commands: CommandQueue,
    wake_atom: Atom,
    supervisor_pid: u64,
    supervisor_stop_atom: Atom,
    next_command_id: Arc<AtomicU64>,
}

impl fmt::Debug for SweepHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SweepHandle")
            .field("pid", &self.pid())
            .field("supervisor_pid", &self.supervisor_pid)
            .finish_non_exhaustive()
    }
}

impl SweepHandle {
    /// Spawn a supervised per-shard sweep process on the normal beamr scheduler.
    pub fn spawn(
        scheduler: Arc<Scheduler>,
        store_dir: impl Into<std::path::PathBuf>,
        wal_path: impl Into<std::path::PathBuf>,
        shard: ShardHandle,
        interval: Duration,
        command_timeout: Duration,
    ) -> Result<Self, SweepError> {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let atoms = SweepAtoms::new();
        let child_pid = Arc::new(Mutex::new(None));
        let spec = Arc::new(SweepSpec {
            store_dir: store_dir.into(),
            wal_path: wal_path.into(),
            shard,
            interval,
            command_timeout,
            commands: Arc::clone(&commands),
            atoms,
        });
        let child_pid_for_factory = Arc::clone(&child_pid);
        let spec_for_factory = Arc::clone(&spec);
        let supervisor_pid = scheduler
            .spawn_native(Box::new(move || {
                Box::new(SweepSupervisor::new(
                    Arc::clone(&spec_for_factory),
                    Arc::clone(&child_pid_for_factory),
                ))
            }))
            .map_err(|error| SweepError::Spawn(format!("{error:?}")))?;
        wait_for_child_pid(&child_pid, supervisor_pid, command_timeout)?;
        Ok(Self {
            child_pid,
            scheduler,
            commands,
            wake_atom: atoms.wake,
            supervisor_pid,
            supervisor_stop_atom: atoms.supervisor_stop,
            next_command_id: Arc::new(AtomicU64::new(1)),
        })
    }

    /// Beamr pid of the current sweep child process, if it has started.
    #[must_use]
    pub fn pid(&self) -> Option<u64> {
        *lock_child_pid(&self.child_pid)
    }

    /// Beamr pid of the supervising process that restarts this sweep child.
    #[must_use]
    pub const fn supervisor_pid(&self) -> u64 {
        self.supervisor_pid
    }

    /// Run one sweep pass immediately.
    pub fn sweep_once(&self, timeout: Duration) -> Result<SweepStats, SweepError> {
        let pid = self.current_pid()?;
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(pid, SweepCommandKind::Run { reply })?;
        recv(&response, pid, timeout)?
    }

    /// Stop the sweep child and its supervisor.
    pub(crate) fn shutdown(&self, timeout: Duration) -> Result<(), SweepError> {
        if let Some(pid) = self.pid() {
            let (reply, response) = mpsc::sync_channel(1);
            self.enqueue(pid, SweepCommandKind::Shutdown { reply })?;
            recv(&response, pid, timeout)??;
        }
        if self
            .scheduler
            .enqueue_atom_message(self.supervisor_pid, self.supervisor_stop_atom)
        {
            Ok(())
        } else {
            Err(SweepError::ActorUnavailable {
                pid: self.supervisor_pid,
            })
        }
    }

    fn current_pid(&self) -> Result<u64, SweepError> {
        self.pid().ok_or(SweepError::ActorUnavailable {
            pid: self.supervisor_pid,
        })
    }

    fn enqueue(&self, pid: u64, kind: SweepCommandKind) -> Result<(), SweepError> {
        let id = self.next_command_id.fetch_add(1, Ordering::Relaxed);
        lock_queue(&self.commands).push(SweepCommand { id, kind });
        if self.scheduler.enqueue_atom_message(pid, self.wake_atom) {
            Ok(())
        } else {
            self.remove_command(id);
            Err(SweepError::ActorUnavailable { pid })
        }
    }

    fn remove_command(&self, id: u64) {
        lock_queue(&self.commands).retain(|command| command.id != id);
    }
}

#[derive(Clone, Copy)]
struct SweepAtoms {
    wake: Atom,
    stop: Atom,
    tick: Atom,
    supervisor_stop: Atom,
}

impl SweepAtoms {
    fn new() -> Self {
        let atoms = AtomTable::with_common_atoms();
        Self {
            wake: atoms.intern(WAKE_ATOM_NAME),
            stop: atoms.intern(STOP_ATOM_NAME),
            tick: atoms.intern(TICK_ATOM_NAME),
            supervisor_stop: atoms.intern(SUPERVISOR_STOP_ATOM_NAME),
        }
    }
}

struct SweepSpec {
    store_dir: std::path::PathBuf,
    wal_path: std::path::PathBuf,
    shard: ShardHandle,
    interval: Duration,
    command_timeout: Duration,
    commands: CommandQueue,
    atoms: SweepAtoms,
}

impl SweepSpec {
    fn factory(self: &Arc<Self>) -> beamr::native::native_process::NativeHandlerFactory {
        let spec = Arc::clone(self);
        Box::new(move || Box::new(SweepNativeHandler::new(Arc::clone(&spec))))
    }
}

struct SweepSupervisor {
    spec: Arc<SweepSpec>,
    child_pid: Arc<Mutex<Option<u64>>>,
    started: bool,
}

impl SweepSupervisor {
    const fn new(spec: Arc<SweepSpec>, child_pid: Arc<Mutex<Option<u64>>>) -> Self {
        Self {
            spec,
            child_pid,
            started: false,
        }
    }

    fn spawn_child(&self, ctx: &mut NativeContext<'_>) {
        match ctx.spawn_native(self.spec.factory(), Some(ctx.self_pid())) {
            Ok(pid) => {
                *lock_child_pid(&self.child_pid) = Some(pid);
            }
            Err(error) => {
                log::debug!("failed to spawn sweep child: {error}");
            }
        }
    }
}

impl NativeHandler for SweepSupervisor {
    fn handle(&mut self, ctx: &mut NativeContext<'_>) -> NativeOutcome {
        if !self.started {
            ctx.set_trap_exit(true);
            self.spawn_child(ctx);
            self.started = true;
            return NativeOutcome::Wait;
        }
        while let Some(message) = ctx.recv() {
            if message.as_atom() == Some(self.spec.atoms.supervisor_stop) {
                return NativeOutcome::Stop(ExitReason::Normal);
            }
            let Some(tuple) = Tuple::new(message) else {
                continue;
            };
            if tuple.arity() == 3 && tuple.get(0) == Some(Term::atom(Atom::EXIT)) {
                let reason = tuple.get(2).and_then(Term::as_atom);
                *lock_child_pid(&self.child_pid) = None;
                if reason != Some(Atom::NORMAL) {
                    self.spawn_child(ctx);
                }
            }
        }
        NativeOutcome::Wait
    }
}

struct SweepNativeHandler {
    spec: Arc<SweepSpec>,
    last_sweep: Option<std::time::Instant>,
    armed: bool,
}

impl SweepNativeHandler {
    const fn new(spec: Arc<SweepSpec>) -> Self {
        Self {
            spec,
            last_sweep: None,
            armed: false,
        }
    }

    fn run_sweep(&self) -> Result<SweepStats, SweepError> {
        let (store, root, buffer) = recover_view(&self.spec.store_dir, &self.spec.wal_path)?;
        let mut merged = BTreeMap::new();
        if let Some(root) = root {
            collect_tree(&store, root, &mut merged)?;
        }
        for mutation in &buffer {
            match mutation {
                Mutation::Put { key, value } => {
                    merged.insert(key.clone(), value.clone());
                }
                Mutation::Delete { key } => {
                    merged.remove(key);
                }
            }
        }
        let mut stats = SweepStats {
            scanned: merged.len(),
            expired: 0,
            deleted: 0,
        };
        let now = crate::current_timestamp();
        let expired_keys = merged
            .into_iter()
            .filter_map(|(key, value)| match is_expired_at(&value, now) {
                Ok(true) => Some(Ok(key)),
                Ok(false) => None,
                Err(error) => Some(Err(SweepError::Store(error.to_string()))),
            })
            .collect::<Result<Vec<_>, SweepError>>()?;
        stats.expired = expired_keys.len();
        for key in expired_keys {
            // Re-check expiry atomically in the actor: a key refreshed by a
            // concurrent write between the snapshot above and this delete must
            // NOT be removed. `delete_if_expired` reports whether it deleted, so
            // `deleted` counts physical removals, not just candidates.
            let removed = self
                .spec
                .shard
                .delete_if_expired(key, self.spec.command_timeout)
                .map_err(|error| SweepError::Shard(error.to_string()))?;
            if removed {
                stats.deleted = stats.deleted.saturating_add(1);
            }
        }
        Ok(stats)
    }

    fn should_sweep(&self) -> bool {
        self.last_sweep
            .is_none_or(|last| last.elapsed() >= self.spec.interval)
    }

    /// Arm a single delayed self-tick at the configured interval.
    ///
    /// `ctx.schedule` (beamr 0.8.2) hands the scheduler's timer wheel a
    /// `Deliver` timer that pushes `tick` into THIS process's own mailbox after
    /// `interval`, then wakes it — so the tick arrives as an ordinary mailbox
    /// message on a later scheduler slice, with no host thread and no busy
    /// spin. The returned `TimerRef` is intentionally dropped: the period is
    /// re-armed afresh after every tick (see [`Self::on_tick`]), so there is no
    /// outstanding timer to cancel.
    fn schedule_next_tick(&self, ctx: &mut NativeContext<'_>) {
        let _: Option<beamr::timer::TimerRef> =
            ctx.schedule(self.spec.interval, Term::atom(self.spec.atoms.tick));
    }

    /// Handle one periodic tick: sweep when due, then ALWAYS re-arm so the
    /// period continues even on a tick that decided not to sweep.
    fn on_tick(&mut self, ctx: &mut NativeContext<'_>) {
        if self.should_sweep() {
            if let Err(error) = self.run_sweep() {
                log::debug!("ttl sweep pass failed: {error}");
            }
            self.last_sweep = Some(std::time::Instant::now());
        }
        self.schedule_next_tick(ctx);
    }

    fn drain_command(&mut self) -> Option<NativeOutcome> {
        let command = lock_queue(&self.spec.commands).pop()?;
        match command.kind {
            SweepCommandKind::Run { reply } => {
                let result = self.run_sweep();
                if result.is_ok() {
                    self.last_sweep = Some(std::time::Instant::now());
                }
                drop(reply.send(result));
                None
            }
            SweepCommandKind::Shutdown { reply } => {
                drop(reply.send(Ok(())));
                Some(NativeOutcome::Stop(ExitReason::Normal))
            }
        }
    }
}

impl NativeHandler for SweepNativeHandler {
    fn handle(&mut self, ctx: &mut NativeContext<'_>) -> NativeOutcome {
        if !self.armed {
            // First scheduler slice: start the periodic self-tick so a `tick`
            // atom lands every `interval` from now on. Without this the actor
            // would only ever sweep on an explicit host `Run` command.
            self.schedule_next_tick(ctx);
            self.armed = true;
        }
        while let Some(message) = ctx.recv() {
            match message.as_atom() {
                Some(atom) if atom == self.spec.atoms.stop => {
                    return NativeOutcome::Stop(ExitReason::Normal);
                }
                Some(atom) if atom == self.spec.atoms.tick => {
                    self.on_tick(ctx);
                }
                _ => {
                    if let Some(outcome) = self.drain_command() {
                        return outcome;
                    }
                }
            }
        }
        NativeOutcome::Wait
    }
}

fn wait_for_child_pid(
    child_pid: &Arc<Mutex<Option<u64>>>,
    supervisor_pid: u64,
    timeout: Duration,
) -> Result<u64, SweepError> {
    let started = std::time::Instant::now();
    loop {
        let current = *lock_child_pid(child_pid);
        if let Some(pid) = current {
            return Ok(pid);
        }
        if started.elapsed() >= timeout {
            return Err(SweepError::ReplyTimeout {
                pid: supervisor_pid,
            });
        }
        std::thread::yield_now();
    }
}

fn lock_queue(commands: &CommandQueue) -> MutexGuard<'_, Vec<SweepCommand>> {
    commands
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_child_pid(child_pid: &Mutex<Option<u64>>) -> MutexGuard<'_, Option<u64>> {
    child_pid
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn recv<T>(response: &mpsc::Receiver<T>, pid: u64, timeout: Duration) -> Result<T, SweepError> {
    response.recv_timeout(timeout).map_err(|error| match error {
        RecvTimeoutError::Timeout => SweepError::ReplyTimeout { pid },
        RecvTimeoutError::Disconnected => SweepError::ReplyDisconnected { pid },
    })
}

#[cfg(test)]
mod tests;
