//! WR-9a: a REAL haematite shard actor running on beamr's cooperative
//! (single-threaded / wasm) [`WasmScheduler`].
//!
//! This is the native, deterministic proof that the SAME shard process body
//! production uses — [`ShardNativeHandler`](super::native), built through the
//! identical `make_factory` seam the threaded [`ShardHandle`](super::handle)
//! spawns — also runs on the cooperative scheduler that backs the wasm runtime
//! ([`beamr::scheduler::WasmScheduler`]), with NO OS threads, no tokio, and no
//! browser in the execution path.
//!
//! ## Why this lives inside `shard::actor`
//!
//! The threaded host ([`ShardHandle::spawn`](super::handle)) is hard-wired to
//! the threaded [`beamr::scheduler::Scheduler`] (it calls `spawn_native` +
//! `enqueue_atom_message`). The cooperative scheduler has a different surface
//! (`spawn_native_root` + `send_owned` + `run_native_until_idle`), so this smoke
//! drives the shard through the lower-level seams the handle is built on:
//! [`ShardNativeHandler::make_factory`](super::native) (the SAME factory the
//! threaded path spawns) plus the shared [`CommandQueue`]. Both are `pub(super)`,
//! so the test must be a child of `shard::actor` to reach them — it does NOT
//! widen any production visibility.
//!
//! ## How a put/get round-trips cooperatively
//!
//! The shard handler counts one mailbox token per queued command: a received
//! token (`ctx.recv().is_some()`) simply means "drain one command" — the handler
//! NEVER inspects the token's value (see `native.rs`). So a command travels as a
//! real Rust value on the shared [`CommandQueue`] (the binary never crosses a
//! beamr term), and the WAKE is any owned mailbox message delivered via
//! [`WasmScheduler::send_owned`]. The reply travels back over the command's own
//! [`std::sync::mpsc`] channel. The test therefore: pushes a command, wakes the
//! shard with one token, and pumps cooperative turns until the reply arrives.
//!
//! The backing store is the REAL [`DiskStore`](crate::store) over a temp dir and
//! the REAL durable WAL — the native, on-disk store the threaded shard uses (no
//! OPFS/IndexedDB; that is Wave-3). The shard logic exercised is identical; only
//! the scheduler differs.

use std::collections::VecDeque;
use std::error::Error;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::{Arc, Mutex};

use beamr::atom::{Atom, AtomTable};
use beamr::ets::OwnedTerm;
use beamr::module::ModuleRegistry;
use beamr::native::BifRegistryImpl;
use beamr::process::ExitReason;
use beamr::scheduler::WasmScheduler;
use beamr::term::Term;

use super::handle::{CommandQueue, ShardCommand, ShardCommandKind, ShardError};
use super::native::{self, ShardNativeHandler};

/// A boxed test error so every fallible step uses `?` instead of `unwrap`/`expect`
/// (the codebase convention — see the sibling `tests.rs` / `native.rs` tests).
type TestResult = Result<(), Box<dyn Error>>;

/// Build a cooperative [`WasmScheduler`] with fresh, common-seeded facilities —
/// the same construction beamr's own cooperative native tests use.
fn cooperative_scheduler() -> WasmScheduler {
    let atom_table = Arc::new(AtomTable::with_common_atoms());
    let modules = Arc::new(ModuleRegistry::new());
    let bifs = Arc::new(BifRegistryImpl::new());
    WasmScheduler::new(atom_table, modules, bifs)
}

/// An owned wake token. Its VALUE is irrelevant — the shard handler treats any
/// delivered mailbox message as "drain one command" — so a bare atom suffices.
fn wake() -> OwnedTerm {
    OwnedTerm::immediate(Term::atom(Atom::OK))
}

/// Maximum cooperative turns to pump while waiting for one reply. The shard
/// handler replies within the slice the wake token is delivered, so a small
/// budget is ample; it only bounds the loop so a regression fails fast rather
/// than hanging.
const MAX_TURNS_PER_REPLY: usize = 16;

/// Push one command onto the shared queue, wake the shard with one token, then
/// pump cooperative turns until the command's reply lands on `reply_rx`.
///
/// This is the cooperative analogue of [`ShardHandle::enqueue`](super::handle)
/// followed by a blocking `recv`: the command is a real Rust value on the queue,
/// the wake is a mailbox token, and the reply returns over the command's own
/// mpsc channel — driven by pumping `run_native_until_idle` rather than blocking
/// a thread on the channel.
fn enqueue_wake_and_pump<T>(
    scheduler: &mut WasmScheduler,
    pid: u64,
    commands: &CommandQueue,
    command: ShardCommand,
    reply_rx: &Receiver<T>,
) -> Result<T, Box<dyn Error>> {
    native::lock_queue(commands).push_back(command);
    scheduler.send_owned(pid, &wake())?;
    for _ in 0..MAX_TURNS_PER_REPLY {
        let _exited = scheduler.run_native_until_idle();
        match reply_rx.try_recv() {
            Ok(value) => return Ok(value),
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                return Err("shard reply channel disconnected before a reply".into());
            }
        }
    }
    Err("shard produced no reply within the cooperative turn budget".into())
}

/// A real shard `NativeHandler` runs cooperatively on `WasmScheduler` and a
/// `put` followed by a `get` round-trips the stored value through the actor.
///
/// Flow, all single-threaded / cooperative:
/// 1. Build the SAME factory the threaded `ShardHandle` spawns
///    ([`ShardNativeHandler::make_factory`]) over a real temp [`DiskStore`] +
///    durable WAL, and `spawn_native_root` it on the cooperative scheduler.
/// 2. Pump one turn so the shard boots (opens store + WAL, runs recovery) and
///    parks waiting for mail.
/// 3. `put(k, v)`: queue the command, wake with one token, pump until the
///    `Ok(())` ack returns over the command's channel.
/// 4. `get(k)`: same path; assert the returned value equals what was put.
#[test]
fn shard_actor_put_get_round_trip_on_cooperative_scheduler() -> TestResult {
    let mut scheduler = cooperative_scheduler();

    let dir = tempfile::tempdir()?;
    let store_dir = dir.path().join("store");
    let wal_path = dir.path().join("shard.wal");

    // The SAME shared command queue + restart-capable factory the threaded host
    // uses; only the scheduler it is spawned onto differs.
    let commands: CommandQueue = Arc::new(Mutex::new(VecDeque::new()));
    let factory = ShardNativeHandler::make_factory(store_dir, wal_path, Arc::clone(&commands));
    let pid = scheduler.spawn_native_root(factory);

    // Boot turn: the shard opens its store + WAL, runs recovery, finds no mail,
    // and parks. It must NOT have exited (no startup error, no shutdown).
    let exited = scheduler.run_native_until_idle();
    assert!(
        !exited.contains(&pid),
        "the freshly booted shard must park, not exit, before any command"
    );
    assert_eq!(
        scheduler.native_exit_reason(pid),
        None,
        "the shard is live (no native exit) after booting"
    );

    let key = b"wr-9a/key".to_vec();
    let value = b"cooperative-shard-value".to_vec();

    // --- put -------------------------------------------------------------
    let (put_tx, put_rx) = mpsc::sync_channel(1);
    let put_command = ShardCommand {
        id: 1,
        kind: ShardCommandKind::Put {
            key: key.clone(),
            value: value.clone(),
            ttl: None,
            reply: put_tx,
        },
    };
    let put_result: Result<(), ShardError> =
        enqueue_wake_and_pump(&mut scheduler, pid, &commands, put_command, &put_rx)?;
    put_result?;

    // The put must NOT have stopped the shard: it stays live for the get.
    assert_eq!(
        scheduler.native_exit_reason(pid),
        None,
        "the shard stays live after the put"
    );

    // --- get -------------------------------------------------------------
    let (get_tx, get_rx) = mpsc::sync_channel(1);
    let get_command = ShardCommand {
        id: 2,
        kind: ShardCommandKind::Get { key, reply: get_tx },
    };
    let read_back: Result<Option<Vec<u8>>, ShardError> =
        enqueue_wake_and_pump(&mut scheduler, pid, &commands, get_command, &get_rx)?;

    assert_eq!(
        read_back?,
        Some(value),
        "the value put through the cooperative shard reads back equal"
    );

    // --- shutdown (clean exit) ------------------------------------------
    // Drive the shard to a clean stop to prove the Shutdown command path also
    // works cooperatively and the process exits Normal (not a crash/leak).
    let (stop_tx, stop_rx) = mpsc::sync_channel(1);
    let stop_command = ShardCommand {
        id: 3,
        kind: ShardCommandKind::Shutdown { reply: stop_tx },
    };
    let stop_result: Result<(), ShardError> =
        enqueue_wake_and_pump(&mut scheduler, pid, &commands, stop_command, &stop_rx)?;
    stop_result?;

    // Pump until the shard's Stop is observed by the scheduler.
    for _ in 0..MAX_TURNS_PER_REPLY {
        if scheduler.native_exit_reason(pid) == Some(ExitReason::Normal) {
            break;
        }
        let _exited = scheduler.run_native_until_idle();
    }
    assert_eq!(
        scheduler.native_exit_reason(pid),
        Some(ExitReason::Normal),
        "the shard stopped cleanly (Normal) after the shutdown command"
    );

    Ok(())
}
