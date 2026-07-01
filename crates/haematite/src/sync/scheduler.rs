//! Periodic sync scheduling for distributed databases.
//!
//! This module owns timing and topology-derived target selection only. It does
//! not walk trees, transfer nodes, open sockets, or merge roots; those remain in
//! the sync protocol and merge layers. A scheduled operation calls an injected
//! [`SyncPullTrigger`] once for each `(partner, shard)` so production wiring can
//! delegate to beamr distribution send helpers while tests can record calls.
//!
//! The scheduler is a supervised beamr native process. The supervisor traps child
//! exits and restarts non-normal exits; the child arms a beamr timer and re-arms
//! it after every tick. Because the scheduler stores only a shard count and never
//! owns shard handles, it does not serialize shard actor writes while periodic
//! sync work is triggered.

use std::collections::VecDeque;
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

use crate::branch::ShardId;

use super::topology::{SyncNodeId, SyncTopology, TopologyError};

const WAKE_ATOM_NAME: &str = "haematite_sync_scheduler_wake";
const TICK_ATOM_NAME: &str = "haematite_sync_scheduler_tick";
const SUPERVISOR_STOP_ATOM_NAME: &str = "haematite_sync_scheduler_supervisor_stop";

mod error;
pub use error::SyncSchedulerError;

/// Callback boundary between scheduling and the actual sync protocol.
pub trait SyncPullTrigger: Send + Sync + 'static {
    /// Trigger one target-initiated pull from `partner` for `shard_id`.
    ///
    /// Implementations should enqueue work through beamr distribution/protocol
    /// helpers and return promptly; the scheduler must not reimplement transport
    /// or wait on shard write-path operations.
    fn trigger_pull(&self, partner: &SyncNodeId, shard_id: ShardId) -> Result<(), String>;
}

/// Trigger implementation used when protocol wiring is supplied by a later layer.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopSyncPullTrigger;

impl SyncPullTrigger for NoopSyncPullTrigger {
    fn trigger_pull(&self, partner: &SyncNodeId, shard_id: ShardId) -> Result<(), String> {
        log::trace!("noop sync pull trigger skipped partner `{partner}` shard {shard_id}");
        Ok(())
    }
}

/// The set of shards a sync pass should visit on each tick.
///
/// LAZY SHARD MATERIALISATION: an un-materialised shard holds no data, so
/// triggering a pull for it is pure waste. The scheduler queries this each tick
/// instead of blindly fanning across `0..shard_count`, so a very high shard count
/// costs nothing to sync until shards are actually materialised. The default
/// [`DenseShardSource`] preserves the old dense behaviour for callers that have
/// no lazy router (e.g. the scheduler's own unit tests).
pub trait SyncShardSource: Send + Sync + 'static {
    /// Shard ids to sync on this tick, ascending. MUST be a subset of
    /// `0..shard_count`.
    fn shards_to_sync(&self) -> Vec<ShardId>;
}

/// Dense source: every shard id `0..shard_count` (the pre-lazy behaviour).
#[derive(Debug, Clone, Copy)]
pub struct DenseShardSource {
    pub shard_count: usize,
}

impl SyncShardSource for DenseShardSource {
    fn shards_to_sync(&self) -> Vec<ShardId> {
        (0..self.shard_count).collect()
    }
}

/// Configuration for one local sync scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncSchedulerConfig {
    pub local_node: SyncNodeId,
    pub nodes: Vec<SyncNodeId>,
    pub topology: SyncTopology,
    pub shard_count: usize,
    pub interval: Duration,
}

impl SyncSchedulerConfig {
    pub fn new(
        local_node: impl Into<SyncNodeId>,
        nodes: Vec<SyncNodeId>,
        topology: SyncTopology,
        shard_count: usize,
        interval: Duration,
    ) -> Self {
        Self {
            local_node: local_node.into(),
            nodes,
            topology,
            shard_count,
            interval,
        }
    }

    pub fn validate(&self) -> Result<(), SyncSchedulerError> {
        if self.shard_count == 0 {
            return Err(SyncSchedulerError::InvalidShardCount);
        }
        if self.interval.is_zero() {
            return Err(SyncSchedulerError::InvalidInterval);
        }
        self.topology
            .partners_for(&self.local_node, &self.nodes)
            .map(drop)?;
        Ok(())
    }

    pub fn partners(&self) -> Result<Vec<SyncNodeId>, TopologyError> {
        self.topology.partners_for(&self.local_node, &self.nodes)
    }
}

/// Statistics for one scheduler tick or explicit run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncSchedulerStats {
    pub partners: usize,
    pub shards: usize,
    pub operations_triggered: usize,
}

type SchedulerReply = SyncSender<Result<SyncSchedulerStats, SyncSchedulerError>>;
type UnitReply = SyncSender<Result<(), SyncSchedulerError>>;

enum SyncSchedulerCommandKind {
    RunOnce {
        reply: SchedulerReply,
    },
    Shutdown {
        reply: UnitReply,
    },
    #[cfg(test)]
    CrashChild,
}

struct SyncSchedulerCommand {
    id: u64,
    kind: SyncSchedulerCommandKind,
}

type CommandQueue = Arc<Mutex<VecDeque<SyncSchedulerCommand>>>;

/// Host-side handle to a supervised periodic sync scheduler.
#[derive(Clone)]
pub struct SyncSchedulerHandle {
    child_pid: Arc<Mutex<Option<u64>>>,
    scheduler: Arc<Scheduler>,
    commands: CommandQueue,
    wake_atom: Atom,
    supervisor_pid: u64,
    supervisor_stop_atom: Atom,
    next_command_id: Arc<AtomicU64>,
}

impl fmt::Debug for SyncSchedulerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SyncSchedulerHandle")
            .field("pid", &self.pid())
            .field("supervisor_pid", &self.supervisor_pid)
            .finish_non_exhaustive()
    }
}

impl SyncSchedulerHandle {
    /// Spawn a supervised sync scheduler that fans across every shard id
    /// `0..shard_count` on each tick (the dense, pre-lazy behaviour).
    pub fn spawn(
        scheduler: Arc<Scheduler>,
        config: SyncSchedulerConfig,
        trigger: Arc<dyn SyncPullTrigger>,
        command_timeout: Duration,
    ) -> Result<Self, SyncSchedulerError> {
        let shard_source = Arc::new(DenseShardSource {
            shard_count: config.shard_count,
        });
        Self::spawn_with_shard_source(scheduler, config, trigger, shard_source, command_timeout)
    }

    /// Spawn a supervised sync scheduler that queries `shard_source` each tick for
    /// the shards to sync — the lazy-materialisation seam: a router-backed source
    /// syncs ONLY materialised shards, so an un-materialised shard (which holds no
    /// data) is never pulled.
    pub fn spawn_with_shard_source(
        scheduler: Arc<Scheduler>,
        config: SyncSchedulerConfig,
        trigger: Arc<dyn SyncPullTrigger>,
        shard_source: Arc<dyn SyncShardSource>,
        command_timeout: Duration,
    ) -> Result<Self, SyncSchedulerError> {
        config.validate()?;
        let commands = Arc::new(Mutex::new(VecDeque::new()));
        let atoms = SyncSchedulerAtoms::new();
        let child_pid = Arc::new(Mutex::new(None));
        let spec = Arc::new(SyncSchedulerSpec {
            config,
            trigger,
            shard_source,
            commands: Arc::clone(&commands),
            atoms,
        });
        let child_pid_for_factory = Arc::clone(&child_pid);
        let spec_for_factory = Arc::clone(&spec);
        let supervisor_pid = scheduler
            .spawn_native(Box::new(move || {
                Box::new(SyncSchedulerSupervisor::new(
                    Arc::clone(&spec_for_factory),
                    Arc::clone(&child_pid_for_factory),
                ))
            }))
            .map_err(|error| SyncSchedulerError::Spawn(format!("{error:?}")))?;
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

    /// Beamr pid of the current scheduler child process, if it has started.
    pub fn pid(&self) -> Option<u64> {
        *lock_child_pid(&self.child_pid)
    }

    /// Beamr pid of the supervisor that restarts the scheduler child.
    pub const fn supervisor_pid(&self) -> u64 {
        self.supervisor_pid
    }

    /// Trigger one scheduler pass immediately.
    pub fn run_once(&self, timeout: Duration) -> Result<SyncSchedulerStats, SyncSchedulerError> {
        let pid = self.current_pid()?;
        let (reply, response) = mpsc::sync_channel(1);
        self.enqueue(pid, SyncSchedulerCommandKind::RunOnce { reply })?;
        recv(&response, pid, timeout)?
    }

    /// Stop the scheduler child and its supervisor.
    pub(crate) fn shutdown(&self, timeout: Duration) -> Result<(), SyncSchedulerError> {
        if let Some(pid) = self.pid() {
            let (reply, response) = mpsc::sync_channel(1);
            self.enqueue(pid, SyncSchedulerCommandKind::Shutdown { reply })?;
            recv(&response, pid, timeout)??;
        }
        if self
            .scheduler
            .enqueue_atom_message(self.supervisor_pid, self.supervisor_stop_atom)
        {
            Ok(())
        } else {
            Err(SyncSchedulerError::ActorUnavailable {
                pid: self.supervisor_pid,
            })
        }
    }

    #[cfg(test)]
    fn crash_child_for_test(&self) -> Result<u64, SyncSchedulerError> {
        let pid = self.current_pid()?;
        self.enqueue(pid, SyncSchedulerCommandKind::CrashChild)?;
        Ok(pid)
    }

    fn current_pid(&self) -> Result<u64, SyncSchedulerError> {
        self.pid().ok_or(SyncSchedulerError::ActorUnavailable {
            pid: self.supervisor_pid,
        })
    }

    fn enqueue(&self, pid: u64, kind: SyncSchedulerCommandKind) -> Result<(), SyncSchedulerError> {
        let id = self.next_command_id.fetch_add(1, Ordering::Relaxed);
        lock_queue(&self.commands).push_back(SyncSchedulerCommand { id, kind });
        if self.scheduler.enqueue_atom_message(pid, self.wake_atom) {
            Ok(())
        } else {
            self.remove_command(id);
            Err(SyncSchedulerError::ActorUnavailable { pid })
        }
    }

    fn remove_command(&self, id: u64) {
        lock_queue(&self.commands).retain(|command| command.id != id);
    }
}

#[derive(Clone, Copy)]
struct SyncSchedulerAtoms {
    wake: Atom,
    tick: Atom,
    supervisor_stop: Atom,
}

impl SyncSchedulerAtoms {
    fn new() -> Self {
        let atoms = AtomTable::with_common_atoms();
        Self {
            wake: atoms.intern(WAKE_ATOM_NAME),
            tick: atoms.intern(TICK_ATOM_NAME),
            supervisor_stop: atoms.intern(SUPERVISOR_STOP_ATOM_NAME),
        }
    }
}

struct SyncSchedulerSpec {
    config: SyncSchedulerConfig,
    trigger: Arc<dyn SyncPullTrigger>,
    shard_source: Arc<dyn SyncShardSource>,
    commands: CommandQueue,
    atoms: SyncSchedulerAtoms,
}

impl SyncSchedulerSpec {
    fn factory(self: &Arc<Self>) -> beamr::native::native_process::NativeHandlerFactory {
        let spec = Arc::clone(self);
        Box::new(move || Box::new(SyncSchedulerNativeHandler::new(Arc::clone(&spec))))
    }
}

struct SyncSchedulerSupervisor {
    spec: Arc<SyncSchedulerSpec>,
    child_pid: Arc<Mutex<Option<u64>>>,
    started: bool,
}

impl SyncSchedulerSupervisor {
    const fn new(spec: Arc<SyncSchedulerSpec>, child_pid: Arc<Mutex<Option<u64>>>) -> Self {
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
                log::debug!("failed to spawn sync scheduler child: {error}");
            }
        }
    }
}

impl NativeHandler for SyncSchedulerSupervisor {
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

struct SyncSchedulerNativeHandler {
    spec: Arc<SyncSchedulerSpec>,
    armed: bool,
}

impl SyncSchedulerNativeHandler {
    const fn new(spec: Arc<SyncSchedulerSpec>) -> Self {
        Self { spec, armed: false }
    }

    fn schedule_next_tick(&self, ctx: &mut NativeContext<'_>) {
        let _: Option<beamr::timer::TimerRef> =
            ctx.schedule(self.spec.config.interval, Term::atom(self.spec.atoms.tick));
    }

    fn run_scheduled_syncs(&self) -> Result<SyncSchedulerStats, SyncSchedulerError> {
        let partners = self.spec.config.partners()?;
        // LAZY: only shards the source reports (materialised shards, under the
        // router-backed source) are pulled — an un-materialised shard holds no
        // data, so syncing it is pure waste. The dense source reproduces the old
        // `0..shard_count` fan-out exactly.
        let shards = self.spec.shard_source.shards_to_sync();
        let mut stats = SyncSchedulerStats {
            partners: partners.len(),
            shards: shards.len(),
            operations_triggered: 0,
        };

        for partner in partners {
            for &shard_id in &shards {
                self.spec
                    .trigger
                    .trigger_pull(&partner, shard_id)
                    .map_err(|message| SyncSchedulerError::Trigger {
                        partner: partner.clone(),
                        shard_id,
                        message,
                    })?;
                stats.operations_triggered = stats.operations_triggered.saturating_add(1);
            }
        }

        Ok(stats)
    }

    fn on_tick(&self, ctx: &mut NativeContext<'_>) {
        if let Err(error) = self.run_scheduled_syncs() {
            log::debug!("scheduled sync pass failed: {error}");
        }
        self.schedule_next_tick(ctx);
    }

    fn drain_command(&self) -> Option<NativeOutcome> {
        let command = lock_queue(&self.spec.commands).pop_front()?;
        match command.kind {
            SyncSchedulerCommandKind::RunOnce { reply } => {
                drop(reply.send(self.run_scheduled_syncs()));
                None
            }
            SyncSchedulerCommandKind::Shutdown { reply } => {
                drop(reply.send(Ok(())));
                Some(NativeOutcome::Stop(ExitReason::Normal))
            }
            #[cfg(test)]
            SyncSchedulerCommandKind::CrashChild => Some(NativeOutcome::Stop(ExitReason::Kill)),
        }
    }
}

impl NativeHandler for SyncSchedulerNativeHandler {
    fn handle(&mut self, ctx: &mut NativeContext<'_>) -> NativeOutcome {
        if !self.armed {
            self.schedule_next_tick(ctx);
            self.armed = true;
        }

        while let Some(message) = ctx.recv() {
            match message.as_atom() {
                Some(atom) if atom == self.spec.atoms.tick => {
                    self.on_tick(ctx);
                }
                Some(atom) if atom == self.spec.atoms.wake => {
                    if let Some(outcome) = self.drain_command() {
                        return outcome;
                    }
                }
                _ => {}
            }
        }
        NativeOutcome::Wait
    }
}

fn wait_for_child_pid(
    child_pid: &Arc<Mutex<Option<u64>>>,
    supervisor_pid: u64,
    timeout: Duration,
) -> Result<u64, SyncSchedulerError> {
    let started = std::time::Instant::now();
    loop {
        let current = *lock_child_pid(child_pid);
        if let Some(pid) = current {
            return Ok(pid);
        }
        if started.elapsed() >= timeout {
            return Err(SyncSchedulerError::ReplyTimeout {
                pid: supervisor_pid,
            });
        }
        std::thread::yield_now();
    }
}

fn lock_queue(commands: &CommandQueue) -> MutexGuard<'_, VecDeque<SyncSchedulerCommand>> {
    commands
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_child_pid(child_pid: &Mutex<Option<u64>>) -> MutexGuard<'_, Option<u64>> {
    child_pid
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn recv<T>(
    response: &mpsc::Receiver<T>,
    pid: u64,
    timeout: Duration,
) -> Result<T, SyncSchedulerError> {
    response.recv_timeout(timeout).map_err(|error| match error {
        RecvTimeoutError::Timeout => SyncSchedulerError::ReplyTimeout { pid },
        RecvTimeoutError::Disconnected => SyncSchedulerError::ReplyDisconnected { pid },
    })
}

#[cfg(test)]
#[path = "scheduler_tests.rs"]
mod tests;
