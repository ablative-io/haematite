# CORE-007 shard actor → real beamr 0.8.0 native process — implementation spec

> Status: spec ready, 2026-06-23. Branch `core-007-shard-actor-native` off main (557f397).
> Authored by a code-architect cross-read (beamr native API + liminal conversation-actor bridge
> + haematite main ShardActor + CORE-007 fake reference), reviewed by Claude. Full agent transcript
> recoverable via SendMessage to agent `ae73f9214384fb091`. This file is the durable digest.

## Decision: REWRITE FRESH off main (not rebase)
CORE-007's branch (45de590) is a beamr-0.6.4 FAKE (`extern crate self as beamr; pub type Pid=u64;` +
std::thread + mpsc + global `OnceLock<Runtime>` + catch_unwind). Main's `shard/actor.rs` meanwhile became
the PERSIST-003 write-boundary+recovery component (`ShardActor::new/from_recovered/get/commit/buffer`,
`committed_root`, 316L, ZERO ShardMessage/spawn/supervise). Rebase = ~100+L semantic conflict AND drags the
unusable shim through. Instead: keep main's `ShardActor` storage type AS-IS (wrap, don't reimplement), add a
real native-process wrapper. Port CORE-007's MESSAGE PROTOCOL (Get/Put/Delete/Commit/Range, ShardMessage/
ShardReply) + the 5 test cases as REFERENCE; discard its mechanism entirely.

## Architecture: NativeHandler directly + mpsc side-channel bridge (the conversation-actor pattern)
- Use `beamr::NativeHandler` directly, NOT the `Actor` facade — Get/Put carry `Vec<u8>`; the facade's
  encode/decode is immediates/tuples only. **Binary NEVER crosses the term boundary.**
- The beamr process is a REAL scheduler-managed native process (real pid, mailbox, links, factory restart).
  The term send carries ONLY a wake atom. Vec<u8> payloads travel as real Rust values through a side channel.
  This is exactly liminal's conversation actor (`crates/liminal/src/conversation/actor.rs:299-353`): an
  `Arc<Mutex<VecDeque<Command>>>` queue + per-command `mpsc::SyncSender` reply + wake via atom.
- This honest distinction from the CORE-007 fake: the fake WAS a std::thread (no scheduler, no real
  supervision); this IS a beamr process whose payload bridge happens to be mpsc.

## Resolved mechanics (real beamr API refs)
- **Wake primitive:** `Scheduler::enqueue_atom_message(target_pid: u64, atom: Atom) -> bool`
  (beamr `scheduler/mod.rs:1084`). Pushes `Term::atom` into the mailbox (Present) / pending_io (Executing) /
  returns `false` if Absent (dead pid). Then wakes the process. Confirmed public.
- **Spawn:** `Scheduler::spawn_native(factory: NativeHandlerFactory) -> Result<u64, ExecError>`
  (beamr `scheduler/spawning.rs:214`, public). `NativeHandlerFactory = Box<dyn Fn() -> Box<dyn NativeHandler>
  + Send + Sync>` (native_process.rs:33). Scheduler stores the factory and re-invokes it on restart.
- **Handler loop:** `impl NativeHandler::handle(&mut self, ctx) -> NativeOutcome` —
  `while ctx.recv().is_some() { pop_and_execute() }` then `NativeOutcome::Wait`. One mailbox token ≈ one queued
  command; QueueEmpty = spurious wake (skip). Never return `Continue` in steady state (drain fully per slice).
  Storage ops are fast (WAL append + in-mem buffer). (Range over huge trees → future reduction-budget follow-up.)
- **Send-ness:** `NativeHandler: Send + 'static` (NOT Sync). `DiskStore.cache: RefCell<LruCache>` — RefCell IS
  Send when contents are Send (only !Sync), so DiskStore should already be Send → NO store change expected.
  Pre-flight: `static_assertions::assert_impl_all!(DiskStore: Send)`. Only if it fails, change cache to Mutex.

## File plan (all <500 lines)
- `shard/actor.rs` (MOD ~160): keep main's `ShardActor` storage type unchanged; add `pub mod native; pub mod
  handle;` + `#[cfg(test)] mod tests;`. Re-export `ShardHandle`, `ShardError`.
- `shard/actor/native.rs` (NEW ~180): `ShardNativeHandler { actor: ShardActor, store: DiskStore, commands:
  Arc<Mutex<VecDeque<ShardCommand>>>, startup_error: Option<ShardError> }`; `build(store_dir, wal_path,
  commands)` (opens DiskStore, `WalRecovery::recover_path`, `DurableWal::new(CommitOnly)`, `ShardActor::
  from_recovered`/`new`; on any err → sentinel that returns `Stop(ExitReason::Error)` on first slice);
  `NativeHandler impl`; `pop_and_execute()`; `make_factory(...) -> NativeHandlerFactory`; `merge_range` free fn
  (port from CORE-007 `send_merged_range`).
- `shard/actor/handle.rs` (NEW ~170): `ShardHandle { pid, scheduler: Arc<Scheduler>, commands, wake_atom,
  next_command_id }` (Clone); `ShardCommand{id,kind}`; `ShardCommandKind::{Get,Put,Delete,Commit,Range}` each
  carrying owned bytes + `mpsc::SyncSender<Result<..,ShardError>>`; `RangeItem::{Entry,Done}`; `ShardError`
  (+Display/Error/From<WalError>). `ShardHandle::spawn/get/put/delete/commit/range/pid`; `enqueue()` rolls back
  the command if `enqueue_atom_message`→false. Callers block on `recv_timeout(timeout)`.
- `shard/actor/tests.rs` (NEW ~280): `test_scheduler()` (thread_count=1), `TestShard`, port the 5 cases.

## Invariants (each must be preserved / checkpointed)
- WAL-before-buffer — enforced INSIDE `ShardActor::put/delete` (don't replicate); test 2 inspects WAL pre-get.
- Committed-root marker AFTER tree commit — inside `ShardActor::commit`; test 3.
- History-independence — `batch_mutate`; test 4 (two shards, different order, equal root).
- Per-shard write serialization — FREE under the scheduler (one `handle()` at a time per pid); no lock on actor.
- No unwrap/expect/panic in non-test (Cargo lints deny); no file >500.
- **Lock discipline (Landmine 4):** hold `commands.lock()` ONLY for `pop_front()` in a tight block; release
  BEFORE any storage op. Never hold across `NativeOutcome` return.

## Landmines
1. ETF closure gap across Executing — N/A by construction (wake is a plain atom, payload via mpsc).
2. **PID changes on restart (CRITICAL):** `spawn_native` gives a NEW pid on restart; `ShardHandle` caches pid →
   post-restart `enqueue_atom_message(old_pid)` returns false → `ActorUnavailable`. DECISION: `ShardHandle` is a
   single-spawn fixed-pid handle; re-spawn/reconnect is the ROUTER's job (CORE-008). Test 5 models crash + manual
   re-spawn against the SAME paths (WAL recovery picks up state) — does NOT rely on auto-restart/supervisor.
3. Commands queued during the crash window — rolled back via `remove_command` on the false path; callers treat
   `ActorUnavailable`/`ReplyDisconnected` as retryable. WAL fsync-per-append bounds loss to the in-flight op.
4. MutexGuard across scheduling boundary — see lock discipline above.
5. DiskStore Send — verified non-issue (RefCell is Send); assert in pre-flight, fix to Mutex only if it fails.

## Scope boundary
CORE-007 = the shard actor itself (single-spawn handle + native process + factory wired for future restart).
Supervisor TREE + reconnect-on-dead-pid = CORE-008 (router). Do not build a supervisor here; just wire the
factory so a future supervisor CAN restart. Test 5 verifies WAL-recovery-on-respawn + sibling isolation.

## Implementation checklist (ordered)
0. Pre-flight: assert `DiskStore: Send`; confirm `beamr::atom::{AtomTable,Atom}` + `intern` are public.
1. handle.rs types (ShardCommand/Kind, ShardError, RangeItem, ShardHandle struct).
2. native.rs (ShardNativeHandler, build, pop_and_execute, NativeHandler impl, make_factory, merge_range). `cargo check`.
3. handle.rs spawn/enqueue/remove_command + get/put/delete/commit/range. `cargo clippy --all-targets -D warnings`.
4. tests.rs: 5 ported cases (merge/shadow; WAL-before-tree; commit-marker+idempotent; history-independence;
   supervised-restart-replays-WAL + sibling-running). `cargo test -p haematite`.
5. Cleanup: grep-confirm no `extern crate self as beamr`/`Pid = u64`/`OnceLock<Runtime>`; fmt; `wc -l` <500 all.
   Adversarial review: lock-drop discipline, WAL-before-buffer, sentinel Stop path, NO re-faking.
