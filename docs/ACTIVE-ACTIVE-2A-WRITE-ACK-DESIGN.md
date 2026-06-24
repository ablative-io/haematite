# Step 2a — Synchronous write-ack replication transport (haematite)

Status: **DESIGN, REVISED post-adversarial-review.** This is the foundation step for Aion
active-active (see `aion/docs/AION-DISTRIBUTION-DESIGN.md` step 2a). It is the one piece the
entire active-active stack stands on: without a real cross-instance ack transport, quorum-on-write
cannot be enforced end-to-end, so the epoch fence (step 3) cannot be made safe.

Grounded in three read-only ground-truth scans of the live code (wire transport,
quorum/ack seam, membership/liveness) and the two committed characterization spikes
(`spike_fencing.rs` E1/E2/E3, `spike_quorum.rs` Q1/Q2/Q3). Every claim below carries a
`file:line` anchor so the review can check it against the code, not against prose.

## ⚠️ VERIFIED SCOPE CORRECTION (post-review — read this first)

Two adversarial reviews + a direct code check overturned two load-bearing assumptions in the
first draft. The corrected scope is materially larger and is recorded here so it is never
re-glossed:

- **🔴 A — There is NO live cross-instance transport in haematite today (VERIFIED).** `Database`
  (db.rs:43–50) holds only `{config, scheduler, router, sweeps, sync_schedulers, timeout}` — no
  `NetKernel`, no `ConnectionManager`, no tokio runtime. The wire.rs transport helpers
  (`send_*_via_beamr`, `register_beamr_sync_handler`) appear ONLY as parameters/re-exports and
  have **no production caller**. The production sync trigger is `NoopSyncPullTrigger` (db.rs:378) —
  the sync scheduler fires into a **no-op**. haematite's distribution has been built and tested at
  the protocol/merge/quorum-arithmetic layer but has **never run over a real network between two
  live Database instances** (both spikes substitute an in-test transport for exactly this reason).
  ⟹ A whole substrate — **2a-0** below — must be built and SPIKED before the write-ack path.
- **🔴 B — `ack-implies-durable` is FALSE as the WAL is configured (VERIFIED).** The production
  shard WAL uses `FsyncPolicy::CommitOnly` (shard/actor/native.rs:94); `put_with_ttl` appends to
  the OS page cache and **does not fsync** (durable.rs:108–113, 161–165) — fsync happens only at
  the `commit()` tree-flush boundary. So a `WriteAck{Applied}` sent "after put returns" attests a
  **page-cache write, not durability**, silently defeating quorum-on-write. ⟹ strong writes need a
  forced-sync apply path (the WAL has an unused `PerWrite` mode, durable.rs:163).
- **🔴 C — A blind "idempotent" receiver put reopens E3 split-brain (correctness review).** The
  receiver apply MUST be a conditional **CAS**, not a blind put: under a heal-mid-write window a
  blind re-apply of a partitioned node's stale proposal gets acked → both partitions reach quorum →
  two owners. The spike used `cas` for precisely this reason. A CAS-reject is a **vote-against**
  (tallied toward "cannot reach quorum → fenced"), NOT a transport failure that poisons the whole
  quorum (the current `AckFailed` short-circuit at consistency.rs:266 is too blunt).
- **🟠 D — write_id reuse across writer restart yields a false quorum.** Fold the origin
  incarnation into the id: `write_id = (origin, creation, counter)`, so a prior-incarnation ack
  cannot satisfy a post-restart write reusing the same counter.
- **🟠 E — the liveness fast-fail (first draft) is unsafe + an over-claim was corrected.** A
  `reachable < quorum` fast-fail can abort a *majority* write during a transient blip ("majority
  must win" violated); DROPPED — liveness picks send-targets only, the existing static `possible`
  short-circuit stays. And: 2a is **necessary but NOT sufficient** for the epoch fence — step 3's
  epoch *read* needs a read quorum (or quorum-acked CAS) or a stale local read re-opens E3.

The sections below are the corrected design. Where the original draft is now wrong, it has been
rewritten in place; the build sequence at the end reflects the true (larger) scope.

## What the spikes already proved (do not re-litigate)

- **E3 (fencing spike):** local `cas` is INSUFFICIENT — two partitioned nodes each cas-bump
  their own epoch copy and BOTH acquire; the clean merge hides the split-brain. The fix MUST
  be quorum on the epoch write.
- **Q2 (quorum spike):** a quorum-gated write fed by a real `wait_for_quorum`, with
  `total_nodes` derived from FULL membership, fences the minority and lets exactly one side
  acquire. The quorum *arithmetic* is real and correct.
- **Q1/Q3:** the ONLY substituted piece in the spike was the ack **source** — an in-test
  iterator standing in for a cross-instance ack transport that does not exist. `total_nodes`
  MUST be the full membership count, never the reachable subset (else a minority self-quorums).

**2a builds exactly that missing ack source — the real transport — and nothing more.** It does
NOT build the epoch fence, AcquireShard, or the union event-merge (those are steps 2b/3/4).

## The seam we are filling (the gap, precisely)

`api/kv.rs::wait_for_consistency` (kv.rs:141–157) already does the right thing on the *receiver*
side: for a `Strong` write it creates `std::sync::mpsc::channel::<Ack<usize>>()` and calls
`wait_for_quorum_from_receiver(strong, &receiver)` (consistency.rs:277–341) — then **drops the
sender** (kv.rs:155) because no producer exists. So:

- `total_nodes = 1` → quorum 1 → local WAL ack satisfies it → OK.
- `total_nodes ≥ 2` → quorum ≥ 2 → no remote acks ever arrive → `QuorumTimeout`.

**The quorum *arithmetic* on the receiver side is complete and correct; the *transport* on both
sides is unbuilt (Fix A).** What 2a adds is: (i) the live distribution substrate that lets two
Databases talk at all (2a-0), (ii) a real cross-instance ack producer that, when a remote node has
*conditionally + durably* applied the write, delivers a vote into that sender before the timeout,
and (iii) the membership binding feeding `total_nodes` from the full cluster. The first draft's
"2a is entirely a producer / the receiver side is structurally complete" was an over-statement —
corrected here.

## Design

### Node identity (decision: generalize `usize` → real node id)

The ack channel is `Ack<usize>` (kv.rs:151) — `usize` was a spike placeholder. Real haematite
node identity is `SyncNodeId(String)` (topology.rs:28), and beamr addresses connections by
`Atom` (wire.rs send path takes `remote: Atom`). Decision:

- The quorum/ack layer keys on **`SyncNodeId`** (what `DistributedDatabaseConfig` already knows —
  config.rs:22–30). Channel becomes `Ack<SyncNodeId>`; `wait_for_quorum_from_receiver` is already
  generic over `NodeId` so this is a type-parameter change at the call site, not a primitive change.
- At send time, `SyncNodeId` → `Atom` by interning the name (beamr atoms are interned strings);
  `ConnectionManager::get_connection(atom)` (connection.rs:434) resolves the link.
- **Incarnation:** beamr `Node{name, creation: u32}` (node.rs:8–11) already carries a per-restart
  incarnation. 2a threads `creation` through the `WriteAck` payload so a stale-incarnation ack
  (from a peer that restarted mid-write) can be discarded. The quorum DENOMINATOR stays the static
  full-membership name set (a restart is the same logical node); `creation` only gates ack validity
  and stale-connection discard. Full `SyncNodeId{name, creation}` upgrade is a step-3 (fencing)
  concern; 2a only adds `creation` to the wire payload so we don't re-cut the frame later.

### Wire: two new `SyncMessage` variants (wire.rs:27–34)

```
WriteId = { origin: SyncNodeId, origin_creation: u32, counter: u64 }   // incarnation-safe id

WriteProposal { write_id: WriteId,
                key: KvKey,
                expected: Option<Hash>,   // CAS precondition: prior value hash (None = create)
                value: KvValue, ttl: Option<Duration> }                  // tag 7
WriteAck      { write_id: WriteId,
                acker: SyncNodeId, acker_creation: u32,
                outcome: AckOutcome /* Applied | Rejected(CasMismatch|ApplyError) */ }  // tag 8
```

- `write_id` is the **explicit correlation id** scout 1 flagged as net-new: the current protocol
  has NO request→reply correlation (it correlates implicitly by shard_id). An ack must route to the
  *right* in-flight write's sender, so `write_id` is mandatory. **Fix D:** the id embeds the origin
  incarnation — `(origin, origin_creation, counter)` — so a slow ack for a *prior* writer
  incarnation cannot satisfy a *post-restart* write that reused the same in-memory `counter`. The
  router additionally rejects any ack whose `write_id.origin_creation ≠ my current incarnation`.
- **Fix C — the proposal carries a CAS precondition (`expected`), not just a value.** The receiver
  applies conditionally (compare current value-hash to `expected`) and only acks `Applied` if it
  matches; otherwise `Rejected(CasMismatch)`. This is what makes a heal-mid-write proposal from a
  stale partition get *rejected* by an already-advanced replica instead of blindly re-applied and
  acked. `AckOutcome::Rejected` is a **vote-against**, tallied so the writer can conclude "cannot
  reach quorum of accepts → I am fenced" — it is NOT a transport `AckFailed` that poisons the whole
  quorum (see the quorum-tally change below).
- Encode/decode arms follow the existing byte-tagged pattern (`encode_sync_message` kv path
  wire.rs:37–72, `decode_sync_message` wire.rs:75+); add `MESSAGE_WRITE_PROPOSAL=7`,
  `MESSAGE_WRITE_ACK=8`. Round-trip codec test per existing convention.
- Send helpers `send_write_proposal_via_beamr` / `send_write_ack_via_beamr` wrap the existing
  `send_sync_message_via_beamr(manager, remote, msg, write_frame)` (wire.rs:154–168) →
  `DistConnection::write_raw` (connection.rs:182). No transport changes.

### Inbound handlers (attach to the single registered handler — wire.rs:273–282)

The one `register_beamr_sync_handler` closure already receives `Result<SyncMessage, SyncError>`.
Add two match arms:

- **`WriteProposal` → conditional-durable-apply-then-ack (Fix C + Fix B).** The receiver
  CAS-applies: read the current value-hash for `key`, compare to `expected`; on **mismatch** send
  `WriteAck{Rejected(CasMismatch)}` and apply nothing (this is what fences a stale heal-mid-write
  proposal). On **match**, apply via a **force-sync** shard path (a new "fsync-now" command / the
  WAL's unused `PerWrite` mode, durable.rs:163), and **only after `sync_all` returns** send
  `WriteAck{Applied}`. Under the production `CommitOnly` WAL a plain `put_with_ttl` would ack a
  page-cache write (Fix B) — so the force-sync is mandatory for `ack-implies-durable`. On a genuine
  apply error → `WriteAck{Rejected(ApplyError)}`. (Event-stream append / union-merge semantics are
  explicitly 2b; 2a's strong path carries the control-plane CAS write.)
- **`WriteAck` → route to the correlation registry (Fix D).** Look up `write_id` in the writer-side
  `DashMap<WriteId, Sender<AckVote<SyncNodeId>>>`. **Reject the ack unless `write_id.origin_creation`
  equals my current incarnation** (closes the restart-reuse false-quorum). Then forward the vote:
  `Applied → AckVote::accept(acker)`, `Rejected → AckVote::reject(acker)`. Unknown/expired
  `write_id` (already-quorate or already-timed-out) → drop. Duplicate votes from one node are
  absorbed by the tally's unique-node dedup.

**Quorum tally change (Fix C, in the consistency primitive).** Today `wait_for_quorum*` treats any
`Ack::Failed` as a hard `AckFailed` error (consistency.rs:266) — too blunt for CAS, where a reject
is a legitimate "this replica is ahead, you lost" signal, not a transport fault. 2a adds a CAS-aware
tally: count **accepts** toward `required`; on a **reject**, decrement the remaining *possible
accepts* ceiling, and if `possible_accepts < required` return a distinct **`Fenced`** outcome
(deterministic loss, not an error) instead of waiting out the timeout. Transport faults remain
`AckFailed`. This keeps the existing static `possible` short-circuit (Fix E) and adds the reject path.

### Writer-side coordinator + the sync/async bridge (the load-bearing subtlety)

`wait_for_quorum_from_receiver` is **blocking** `std::sync::mpsc` + `recv_timeout` (consistency.rs:314),
but the beamr transport is **async** (tokio). The bridge:

1. The Strong write path (`put_with_ttl_and_consistency`, kv.rs:87–99) — after the local durable
   WAL write (kv.rs:95–97) — registers `(write_id → Sender)` in the correlation registry, then
   **fire-and-forget spawns** the proposal sends onto the beamr runtime via a held
   `tokio::runtime::Handle`: `handle.spawn(async move { conn.write_raw(frame).await })`, one per
   reachable peer, with at-least-once retry/backoff outside the quorum deadline.
2. It then **blocks the calling thread** on `wait_for_quorum_from_receiver` (unchanged primitive).
3. Inbound `WriteAck` handlers (running on the beamr read loop, async) call the non-blocking
   `Sender::send` — which is safe to call from async — feeding the blocked writer.
4. On quorum reached / timeout, the writer deregisters `write_id` (drops the Sender).

**Threading contract (must be explicit + enforced):** the Strong-write call BLOCKS a thread, so it
must NOT execute on a beamr runtime worker thread (it would consume a worker while parked and can
deadlock the runtime under load). Strong writes run on a dedicated blocking pool / `spawn_blocking`.
This contract already implicitly exists for `wait_for_quorum_from_receiver`; 2a makes it real and
documents it at the public `put_with_consistency` boundary.

**Resolved (Fix A): the substrate is absent and is 2a-0, NOT a sub-task here.** Verified: `Database`
holds no NetKernel/ConnectionManager/runtime (db.rs:43–50) and nothing registers the handler; the
sync trigger is `NoopSyncPullTrigger` (db.rs:378). So the runtime `Handle` + `ConnectionManager` the
bridge spawns onto are *produced by 2a-0*, which stands up beamr distribution in Database. 2a-3
consumes them; it does not create them. (beamr's NetKernel owns its own runtime + manager —
distribution/mod.rs:71/89 — at `worker_threads(1)`, mod.rs:74, which sharpens the blocking-thread
contract: a Strong write must never block the lone beamr worker.)

### Membership + liveness (feeds `total_nodes` and fast-fail)

- **Denominator = FULL membership.** `total_nodes` = `DistributedDatabaseConfig::nodes.len()`
  (config.rs:24) — the static authoritative full set. This is the Q3 invariant: full membership,
  never the reachable subset. No code binds this today; 2a adds the binding.
- **Liveness = beamr `connected_nodes()`** (connection.rs:443) + the reactive `ConnectionDownHook`
  (connection.rs:81–134). **Fix E — used for exactly ONE thing: which peers to send proposals to.**
  It does NOT drive any abort/fast-fail predicate. (The first draft proposed a
  `local + reachable < quorum → QuorumUnavailable` fast-fail; it is UNSAFE — a transient link blip
  would spuriously abort a write the *majority* should win, violating spike Q2's "majority must win"
  invariant. The existing static `possible = local + (total_nodes-1)` short-circuit
  (consistency.rs:285–289) is kept and is the ONLY availability short-circuit; a real but slow peer
  still gets to ack within the timeout.) Liveness NEVER changes the denominator.
- Incarnation (`creation`) from beamr `Node` (node.rs) gates stale-ack discard and is carried in
  the wire payload; the `ConnectionDownHook` discards a peer's in-flight expectation on disconnect.

## Build sequence — SIX independently-verifiable increments (revised)

Each increment compiles + its own tests pass before the next starts. Each is built by a subagent
and then **independently adversarially verified** (never trust a green report). 2a-0 is net-new,
surfaced by the feasibility review, and is the substrate everything else rides on.

- **2a-0 — Live distribution substrate in Database.** The Database has no
  NetKernel/ConnectionManager/runtime today (Fix A). **✅ SPIKE GATE PASSED** (2026-06-24,
  `tests/spike_distribution_transport.rs`): a haematite `SyncMessage` (`RootExchangeRequest`)
  round-trips byte-for-byte between two real beamr 0.9.0 endpoints over loopback TCP, real OTP
  handshake, zero transport mocking. **Empirically-confirmed integration model (use this, do NOT
  re-derive):**
  - Use the **bare `ConnectionManager`**, NOT `NetKernel`. `ConnectionManager::new(Arc<AtomTable>,
    resolver: Arc<dyn NodeResolver>, cookie, local_node_name, local_creation: u32)` (connection.rs:336)
    + `listen(addr) -> AcceptHandle` (connection.rs:494) + `connect(name) -> Arc<DistConnection>`
    (connection.rs:518). Do NOT use `ConnectionManager::start()` — it hides its own `AtomTable`.
    `NetKernel` is just a sync-over-async wrapper with a `worker_threads(1)` runtime; skip it.
  - **Shared `AtomTable` is MANDATORY** — an `Atom` is an index into one specific table; the sender
    must address a peer by the atom for the peer's *advertised handshake name* interned in the SAME
    table the connection is keyed by. This is THE load-bearing wiring detail.
  - Inbound: `register_control_frame_handler` (connection.rs:414); haematite's `encode_beamr_sync_frame`
    (wire.rs:115) already emits exactly beamr's 8-byte `control_len||payload_len` + `control||payload`
    framing — co-designed, and the spike confirms they agree on the wire.
  - **Database is INJECTED a distribution-endpoint bundle** `{Arc<AtomTable>, ConnectionManager,
    AcceptHandle, runtime Handle}` (`Database::with_distribution(endpoint)`), one per node, shared
    across all shard actors so peer-atom resolution stays consistent. The endpoint owns a small
    dedicated multi-thread runtime (beamr's `NetKernel::Drop` moves the runtime-drop onto a std::thread
    because dropping a tokio runtime in async context panics — mod.rs:57-68; our endpoint follows the
    same discipline). The `write_frame` closure (sync-shaped `FnOnce`) bridges to async `write_raw`
    via `handle.block_on(...)`. Inbound frames drain into a channel feeding the merge path, replacing
    `NoopSyncPullTrigger` (db.rs:378).
  - **Build NOW:** the production `Database::with_distribution` wiring + real send trigger + inbound
    drain. *Verify:* two real `Database` instances on loopback exchange an existing `SyncMessage`
    end-to-end through the Database API (not just raw ConnectionManagers); the blocking-thread
    contract holds (no Strong write wedges the endpoint runtime). Gets its own adversarial review.
- **2a-1 — Wire variants + codec.** Add `WriteProposal`/`WriteAck` (with the `WriteId{origin,
  origin_creation, counter}`, `expected: Option<Hash>` CAS field, and `AckOutcome`) to `SyncMessage`,
  encode/decode arms, send helpers. *Verify:* byte round-trip tests (encode→decode identity) incl.
  empty/large value, `None`/`Some` ttl + `expected`, every `AckOutcome`; unknown-tag handling
  unchanged. Pure serialization — fully isolated.
- **2a-2 — Node identity + membership binding + CAS-aware tally.** Generalize the consistency call
  site `Ack<usize>` → `Ack<SyncNodeId>`; bind `total_nodes` to `config.nodes.len()` (FULL membership);
  add the liveness view (connected_nodes + down-hook) for **send-target selection only** (Fix E, NO
  fast-fail); extend the quorum tally for CAS rejects → `Fenced` (Fix C). *Verify:* fake-membership
  unit tests — full-membership denominator; a reject-majority yields `Fenced` (not `AckFailed`, not
  timeout); reachable subset is NEVER the denominator (re-assert Q3 against the real binding); a
  transient "down" peer does NOT abort a write that still reaches quorum (Fix E regression test).
- **2a-3 — Writer-side coordinator + bridge.** Correlation registry `DashMap<WriteId, Sender>` with
  incarnation-gated routing (Fix D), spawned proposal sends + retry onto the beamr runtime, block on
  the receiver, deregister on quorum/`Fenced`/timeout. *Verify:* writer with a stubbed in-process ack
  producer reaches quorum; **threading-contract test** (a Strong write must not run on / wedge the
  single-worker beamr runtime); duplicate-vote dedup; late-vote-after-quorum dropped; **restart-reuse
  test** (a prior-incarnation ack for a reused counter is rejected — Fix D).
- **2a-4 — Receiver-side conditional-durable-apply-then-ack.** Inbound `WriteProposal` → CAS compare
  vs `expected` → on match **force-sync** apply (new fsync-now path / `PerWrite`) → `WriteAck{Applied}`
  only after `sync_all`; on mismatch → `WriteAck{Rejected(CasMismatch)}` applying nothing. *Verify:*
  **ack-implies-durable kill-test** (kill between apply and ack → on restart the value IS present —
  this test FAILS under `CommitOnly`, so it is the proof the force-sync path is real, Fix B);
  CAS-mismatch rejects without applying (Fix C); apply-error path.
- **2a-5 — Real three-node end-to-end (the empirical proof).** Real in-process `Database` instances
  over real beamr loopback TCP (built on 2a-0); Strong CAS write on a 3-node membership. *Verify:*
  majority **COMMITS via the real transport** (replaces the spike's in-test ack source); partitioned
  minority is **FENCED**; under the E3 split + a **heal-mid-write** injection exactly one side
  acquires (the Fix C scenario — the test the correctness review warned a steady-state test would
  miss). This is the moment `spike_quorum` Q2's assert becomes true against the *real* CAS ack path.
  Then add an `#[ignore]` soak.

## Explicit non-goals (scope fence — these are later steps, NOT 2a)

- The monotonic epoch fence + `AcquireShard` handoff (step 3) — 2a only makes quorum-on-write
  *possible*; it does not write the epoch protocol.
- Non-LWW union event-stream merge (step 2b) — 2a's strong path carries control-plane puts; the
  data-plane append keyspace and its idempotency/union semantics are 2b, with their own spike.
- Snapshot/trim, the aion backend adapter (steps 6/7).
- Full `SyncNodeId{name, creation}` identity refactor — 2a carries `creation` on the wire but does
  not restructure `SyncNodeId`; that lands with fencing.

## Risk register (for the review to attack)

1. **Sync/async bridge deadlock** if a Strong write runs on a runtime worker. Mitigation: the
   documented + enforced blocking-thread contract (2a-3). *Review: is enforcement real or just doc?*
2. **Correlation registry leak** — a `write_id` whose acks never arrive and whose writer died.
   Mitigation: writer always deregisters on quorum/timeout; entries are bounded by in-flight writes.
   *Review: any path that registers but never deregisters?*
3. **`ack-implies-durable` violated** if the receiver acks before WAL fsync. Mitigation: apply path
   ordering (2a-4) + the kill-test. *Review: is the fsync boundary actually before the ack send?*
4. **Denominator drift** — any path that lets `total_nodes` track reachable rather than full
   membership reintroduces the Q3 self-quorum bug. *Review: is `nodes.len()` the ONLY denominator?*
5. **Idempotency assumption** — 2a claims control-plane puts are idempotent under retry. *Review:
   is the strong path EVER used for a non-idempotent write before 2b lands? If so, guard it.*
6. **Incarnation gating correctness** — does discarding stale-creation acks ever drop a VALID ack
   and stall quorum? *Review: the creation-match logic under a same-tick restart.*
</content>
</invoke>
