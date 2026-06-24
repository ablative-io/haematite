# Step 3 — Per-shard ownership election + epoch fence (haematite)

Status: **DESIGN, REVISED post-adversarial-review, pre-build.** This is the step that closes the
**concurrent-proposer** window that step 2a (`ACTIVE-ACTIVE-2A-WRITE-ACK-DESIGN.md`) deliberately
left open. 2a delivered SEQUENTIAL conflicting-write safety on a real quorum-CAS transport. Step 3
makes the system safe under CONCURRENT writers by guaranteeing **at most one proposer per shard at
a time**, established by a real majority election and enforced by an epoch fence on every data
write.

Grounded in three read-only ground-truth scans of the live code (shard ownership/routing, beamr
node identity, liminal global-name surface). Every load-bearing claim carries a `file:line` anchor
so the review checks it against code, not prose.

---

## 0. The one-paragraph thesis

There is exactly one correct minimal primitive here, and the grounding forces us onto it. We elect
an owner per shard with a **single-decree Paxos Prepare/Promise majority round** using **unique,
monotonic, durably-persisted ballots that ARE the epoch**. Once elected, the owner is the sole
proposer for that shard, so 2a's data-write path is driven single-writer (the regime in which 2a is
already proven safe). Every data write is stamped with the owner's epoch; receivers reject any write
whose epoch is below their persisted `promised[s]`. A deposed or partitioned ex-owner is fenced
because a majority has already promised a higher epoch. No external coordinator, no new consensus
dependency, no multi-decree log — data still rides the 2a quorum-CAS path. The single-binary thesis
holds.

---

## ⚠️ POST-ADVERSARIAL-REVISION (read this second)

One adversarial review (tasked solely to break the safety core, code-cites verified against the
live tree) confirmed the core property — **single live owner + fenced ex-owner** — survives every
interleaving it could construct, and surfaced five seams at the 2a↔step-3 boundary. All five are
folded into the sections below; recorded here so they are never re-glossed:

- **R5 (was flagged 🔴, adjudicated 🟠) — "committed" must mean CLIENT-ACKED.** A write reaching
  2a's *phantom*-quorum is NOT yet committed: `replicate_write` returns `Ok` to the client only
  AFTER the proposer's own local durable apply (`db/receiver.rs:100`), so **client-acked-success ⟹
  proposer-durable + (quorum−1) peers-durable = a full durable majority**, which every Promise
  majority intersects (§4). The state-sync premise holds *for acknowledged writes*. What remains is
  **in-doubt** writes (proposer crashed before local apply, or `LocalCommitFailed`) leaving a
  divergent tail on a minority node — handled by handoff reconciliation (§2.4), not by pretending it
  cannot happen. "Committed" is defined crisply in §2.0.
- **R2 — a data write must NOT raise `promised`.** Only Prepare raises `promised[s]`; a data write
  is *accepted* iff `epoch ≥ promised` but never advances it (standard Paxos acceptor semantics).
  This removes a liveness-poison vector (a rogue/un-elected writer spraying a high epoch to fence
  the true owner) while keeping the §4 fence intact. §2.3 rewritten accordingly.
- **R4 — persist the max-minted ballot counter before Prepare.** Mirrors 2a Fix-D. A candidate that
  minted a ballot, crashed before any Promise, and restarted could otherwise regress/reuse it. Mint
  floor = `max(promised[s], owner_epoch[s], persisted_max_minted[s]).counter + 1`, fsync'd before
  sending any Prepare (§2.2, §3).
- **R8/R7 — `promised[s]` is ACTOR-LOCAL; Prepare and the fence share the shard-actor slice.** The
  fence-check + `epoch` ride on the `ApplyDurable` command and execute inside the actor slice; a
  Prepare for shard `s` is processed by (or synchronously visible to) the same actor. NEVER a
  `DashMap` consulted by `replicate_write` before enqueuing — that reopens a TOCTOU. This also fences
  a prior-owner's in-flight write that lands after a Promise (re-checked at apply time, same FIFO
  slice). §3/§6 mandate it.
- **R1 — membership is FIXED for a shard's epoch lineage.** The §4 intersection proof assumes one
  node-set; two majorities of *different* sets need not intersect. Reconfiguration requires joint
  consensus and is **out of scope** for the safety core (§8).

## ⚠️ The trap: why the "obvious" design is WRONG (read this first)

The obvious step-3 is: *"AcquireShard = a `replicate_write` quorum-CAS on a reserved epoch key
`__epoch/<shard>`; whoever wins the CAS to `epoch+1` is the new owner."* **This is unsafe. It
inherits 2a's documented concurrent-proposer hole and reproduces split-brain in the acquisition
itself.**

Concrete trace, 3-node cluster `{A, B, C}`, `quorum = 3/2+1 = 2`, epoch key currently hash `H0`:

1. A and C concurrently try to acquire: both `replicate_write(__epoch, expected=H0, →epoch1)`.
2. 2a counts a **phantom local ack** for the proposer *before* it applies locally
   (`db/receiver.rs:46-48`, the documented 2a-5 boundary), and each proposer also serves as a
   *receiver* for the other's proposal while still un-applied.
3. A sends its proposal to B and C. C sends its proposal to A and B.
4. At A's shard actor, C's proposal arrives: A has **not yet applied its own** write, so current
   hash is still `H0`, the CAS matches → A applies C's value and acks `Applied` to C.
5. Symmetrically B applies A's proposal first and acks A; later rejects C (hash now ≠ H0).
6. Tally: **A** = phantom(1) + B-ack(1) = 2 = quorum. **C** = phantom(1) + A-ack(1) = 2 = quorum.
   **Both "win" epoch 1. Two owners.** Exactly the split-brain step 3 exists to prevent — now
   inside the ownership protocol.

This is not a bug in 2a; it is 2a's explicitly-documented scope boundary
(`db/receiver.rs:57-69`: "does NOT by itself close the CONCURRENT-proposer window"). The lesson:
**a symmetric compare-and-swap from a shared expected value is not an election.** Two proposers
share the same precondition (`expected=H0`) and there is no step that forces a *majority to commit to
one of them before the other can be accepted anywhere*. Closing this requires the Prepare/Promise
phase below; nothing weaker suffices.

---

## 1. Grounding (verified, with cites)

**G1 — No ownership exists today; clean slate, clean fence point.**
Key→shard routing is `blake3(key) % shard_count`, fixed count, and **every node holds every shard
locally** (`shard/router.rs:20-28`, `db/config.rs:13`). There is no shard→node map, lease, or epoch
anywhere (`db.rs:49-63` lists only `{config, scheduler, router, sweeps, sync_schedulers,
distribution, timeout}`). The conditional-durable apply is a single uninterruptible actor slice:
read current value-hash → CAS-compare vs `expected` → `put` → `commit`(fsync), rollback on fault
(`shard/actor.rs:320-345`). **The fence check slots in immediately before the CAS read at
`shard/actor.rs:331`**, in the *same* actor slice — so there is no TOCTOU between fence-check and
write, just as there is none between CAS-compare and write today.

**G2 — beamr `creation` is unusable as the fencing incarnation; the epoch must be
haematite-persisted.** `creation` is a caller-supplied `u32` defaulting to `0`, NOT auto-incremented
on restart (`beamr scheduler/mod.rs:626`, `distribution/node.rs:10`). Worse, the
`ConnectionManager` **discards a peer's creation after the handshake** — `DistConnection` has no
field for it (`beamr distribution/connection.rs:552`), so a node has *no way to learn a peer's
incarnation* to fence it. Peer-creation-based fencing is not even expressible on the current
transport. ⟹ The epoch is a **haematite-owned, durably-persisted, per-shard counter** that travels
*inside the write path* and is self-describing; fencing never depends on the transport surfacing
peer identity (it can't).

**G3 — liminal has NO global-name registry; it cannot be the coordinator.** liminal provides only
per-node channel registries; clustering is beamr-process-group pub/sub with "no custom consensus,
gossip, or failure detector" (`liminal-server/src/cluster/mod.rs:1-9`). The "global-name singleton
coordinator" is explicitly *unbuilt aspirational* work (`aion/docs/AION-DISTRIBUTION-DESIGN.md:102-
106`, build item 6). ⟹ We do **not** depend on it. The election is run by haematite itself over the
beamr control-frame transport 2a already uses. (A liminal singleton, if it ever ships, is at most a
*liveness optimization* — picking which node *attempts* acquisition — never a *safety* gate. Safety
is the majority election alone.)

**G4 — control-frame plumbing for new message types is straightforward.** New `SyncMessage`
variants register/send through the exact `register_control_frame_handler` + `send` path 2a uses
(`sync/endpoint.rs:221,317,547-564`; `sync/protocol/wire.rs:166-205`); the 8-byte
control-len/payload-len frame header already accommodates a new control tag
(`beamr connection.rs:590-629`).

---

## 2. The protocol

### 2.0 Definition of "committed" (R5 — load-bearing for §2.4/§4)

A write is **committed** iff `replicate_write` returned `Ok` to its caller. By construction
(`db/receiver.rs:91-100`) that happens only after BOTH (i) 2a peer-quorum and (ii) the proposer's
own local durable apply — so a committed write is durable on a **full quorum** of nodes (proposer +
`quorum−1` peers). A write that merely reached 2a's *phantom*-quorum but whose proposer has not yet
locally applied is **in-doubt**, not committed: its caller has not been told success. In-doubt writes
may exist on as few as `quorum−1` nodes (a minority) and may be lost or diverge across a crash; that
is acceptable (a crash-before-ack is always indeterminate) and is reconciled at handoff (§2.4). Every
correctness claim below quantifies over **committed** writes under this definition.

### 2.1 Identifiers

- **Ballot / epoch** `b = (counter: u64, node: SyncNodeId)`, ordered lexicographically by
  `(counter, node)`. This makes every ballot **globally unique** (no two nodes share one) and
  **monotonic**. The `counter` is the per-shard epoch number; the `node` tiebreak guarantees
  uniqueness so two candidates can never collide on the same ballot — the single property 2a's
  symmetric CAS lacked.
- **`promised[s]`** — per node, per shard `s`: the highest ballot this node has promised in a
  Prepare. Durably persisted (§3). Initial value `(0, "")` (below every real ballot).
- **`owner_epoch[s]`** — the ballot under which the current owner was elected; carried on every data
  write as `write.epoch`.

### 2.2 AcquireShard — Phase 1 (Prepare / Promise), the ONLY safety-critical round

A candidate `C` that wants ownership of shard `s`:

1. Picks `b = (mint_floor[s] + 1, C)` where `mint_floor[s] = max(promised[s].counter,
   owner_epoch[s].counter, persisted_max_minted[s])`, then **durably fsyncs
   `persisted_max_minted[s] = b.counter` BEFORE sending any Prepare** (R4). This guarantees a
   restarted candidate can never re-mint or regress a ballot it already emitted — the analogue of
   2a's Fix-D incarnation fold. Without this fsync a candidate that crashed between minting and the
   first Promise could reuse a ballot with a different meaning, breaking ballot uniqueness.
2. Sends `Prepare{s, b}` to **all** nodes in full membership (`WriteMembership.total_nodes`, the
   quorum denominator — `sync/membership.rs:24-36`).
3. Each node `n`, on `Prepare{s, b}`:
   - If `b > promised[s]`: **durably set `promised[s] = b` (fsync before replying)**, reply
     `Promise{s, b, accepted_epoch[s], last_committed_root[s]}` — the last field lets the new owner
     state-sync (§2.4).
   - Else: reply `Nack{s, promised[s]}` (so `C` learns a higher ballot exists and can retry above
     it, or back off).
4. `C` becomes owner of `s` **iff it collects Promises from a strict majority** (`total/2 + 1`) of
   full membership. On majority: `owner_epoch[s] = b`, `C` may now serve writes. On timeout / only a
   minority / any Nack-driven loss: `C` is NOT owner, applies nothing, retries or yields.

There is **no separate Phase-2 accept round for ownership.** Once `C` holds a majority of Promises at
`b`, the *first data write* (and every subsequent one) carries `epoch = b` and self-certifies through
the fence (§2.3). Folding accept into the data path is safe because the Promise majority already
established that no ballot `≥ b` was promised elsewhere at the time of election (proof in §4).

### 2.3 Data writes — epoch fence on the 2a path

The owner writes via the existing `Database::replicate_write` (`db/receiver.rs:76-109`), with two
additions:

- The `WriteProposal` gains an `epoch: Ballot` field = `owner_epoch[s]`.
- `ShardActor::apply_durable` gains a fence check **before** the CAS read (`shard/actor.rs:331`):
  - If `write.epoch < promised[s]`: **reject `Fenced`** — apply nothing. (A stale owner.)
  - If `write.epoch ≥ promised[s]`: run the existing CAS-compare → put → commit. **The data write
    does NOT raise `promised[s]`** (R2 — standard Paxos acceptor semantics: `promised` is advanced
    ONLY by a Prepare, §2.2). Accepting a write whose epoch exceeds `promised` is admitting a
    legitimate newer owner whose Prepare this node happened to miss; but because the write does not
    *raise* `promised`, an un-elected writer that sprays a high epoch cannot poison this node's
    `promised` and thereby fence the true owner. The safety burden is carried entirely by *rejecting
    `<`* (§4), and a rogue writer still cannot win an accept-majority (it never won a Promise
    majority), so dropping the raise costs no safety.

The fence and the CAS run in the **same actor slice** (R8), and a `Prepare` for shard `s` is
processed through the SAME actor, so `promised[s]` is actor-local state and the ordering "checked
`promised`, then this exact write committed" is atomic — no interleave between fence and write, and a
prior-owner's in-flight write that was queued before a Prepare but applies after it is re-checked
against the now-higher `promised` at apply time and correctly fenced (R7). `promised[s]` must NEVER be
read from a `DashMap` outside the slice (e.g. by `replicate_write` before enqueuing) — that reopens a
TOCTOU between the check and the apply.

`replicate_write` itself is otherwise unchanged: drive to peer-quorum, then locally durable-apply on
success. Because there is now a single owner per shard, the concurrent-proposer interleaving that
made the phantom-local-ack dangerous in 2a **cannot arise** — the regime collapses to the
single-writer case 2a already proves.

### 2.4 Handoff reconciliation — REVISED (causal commit stamp + union merge)

> ⚠️ **CORRECTION (supersedes the original "adopt the most-advanced committed_root").** The original
> §2.4 assumed the promise-majority committed states are **totally ordered**, so a new owner could
> just adopt the single most-advanced `committed_root`. **That assumption is false**, and a
> single-root adoption can silently drop a *committed, client-acked* write. The corrected design
> below merges the promise-majority states using a causal commit stamp. (Found in 3-4 verification;
> the original is preserved only as the cautionary header.)

**Why a single root is insufficient — forks arise even under single ownership.** Committed state
across the promise majority can be **incomparable**, not totally ordered, because *per-key follower
lag forks it under one owner*. Shard `s`, keys `k2,k3`, cluster `{A,B,C}`, quorum 2:

1. Owner `A` commits `k2=v2` to majority `{A,C}` (B lagged) — client-acked (§2.0).
2. `A` commits `k3=v3` to majority `{A,B}` (C lagged) — client-acked. (Different keys ⟹ no CAS chain
   ties them to the same followers; the two writes legitimately reach different quorums.)
3. Now `B` holds `{k3}`, `C` holds `{k2}` — each holds a committed write the other lacks. `A` is
   removed; the only promise majority is `{B,C}`. **No single committed_root contains both**, so
   adopting either promiser's root drops a committed write. The new owner must take the **union**.

**Why there is nonetheless NO genuine same-key conflict to "resolve".** For a *single key*, the
epoch fence (§2.3) guarantees the committed values across the cluster form a **chain**
`v1 → v2 → v3 …` along the CAS precondition, never an incomparable fork. A fork would require two
*concurrent committed writers* to that key from the same base — exactly what single-owner + the fence
prevent (the lower-epoch owner is fenced; at most one of the racing writes ever commits/acks). So the
only thing handoff lacks is the **ability to order two values of one key** — content-addressed value
bytes do not encode their CAS predecessor, so `v1` and `v2` are indistinguishable by content alone.

**The primitive: a causal commit stamp.** Every committed write carries
`stamp = (epoch: Ballot, seq: u64)`:
- `epoch` — the owner's ballot under which the write was made (already threaded on the write path by
  §2.3 / increment 3-3). Globally unique and monotonic across owners.
- `seq` — the owner's monotonic write counter for **its current serving epoch**, advanced once per
  committed write. Orders writes *within* one epoch (needed: two sequential same-key writes by one
  owner share an epoch and must still be ordered).

**`seq` is IN-MEMORY, reset to 0 per won election — it is NOT persisted (and must not be).** Persisting
`seq` would mean an fsync on the hot write path; it is unnecessary because of the **serve-authority
invariant**: a node may serve (write as owner) ONLY under an epoch it won via a *live* election in its
current process lifetime. A recovered `owner_epoch` from disk does **not** authorize serving — a
crashed/restarted owner must **re-acquire** (§2.2) before serving again, and re-acquisition yields a
ballot strictly greater than any it ever minted (guaranteed by the already-durable
`persisted_max_minted`, R4/§3). Therefore the new serving epoch `e''` strictly exceeds the pre-crash
`e'`, so `(e'', 0)` cannot collide with any pre-crash `(e', k)` even though `seq` restarts at 0.
Epoch-first ordering also means a fresh owner's very first write `(e'', 0)` dominates *every* prior
write to a key — correct, since it CAS-derives from (caught up to) the prior tip. Net: `(epoch, seq)`
is globally unique and totally ordered **with `seq` purely in-memory**, and — because the fence forces
same-key commits into a CAS chain — the higher stamp on a key is always the chain *descendant*. This
is the total order a replicated log would give, without rebuilding replication as a log.

**Deletes and TTL are UNIFIED into the one fenced/stamped path (no second write path).** A `delete`
is modelled as a **stamped tombstone** written through `replicate_write` exactly like a put: fenced
(carries the owner epoch), CAS'd (`expected` = hash of the value being deleted), quorum-replicated,
and **persisted in the tree as a stamped entry that reads as absent**. The CAS/`current_value_hash`
stays over the *logical value* (a tombstone reads as `None`, so create-if-absent still matches) — so
3-3's fence semantics are unchanged; the stamp + tombstone-kind are merge metadata stored alongside,
NOT part of the CAS identity. The old local-only `delete` path (unfenced, unreplicated, bare
key-removal) is **removed**. TTL-expiry needs no replicated op: expiry is deterministic and an expired
entry reads as `None` everywhere; the merge operates on *stored stamped entries* (visibility applied
at read), so a refresh-before-expiry (a higher-`seq` put) always wins over a stale value via the
stamp, and physical sweep is local GC. This closes the review's delete/sweep gap at the root:
**exactly one write path, every committed mutation stamped + fenced + replicated.**

**Handoff = union + per-key max-stamp merge.** The new owner, after winning (§2.2) and before
serving:

1. Fetches the committed state of each promiser in its majority (reuse the existing pull/transfer
   primitive — `find_missing_nodes` + the content-addressed apply path; that plumbing is sound, only
   the *adoption* logic changes).
2. **Merges** them with a **purpose-built, ancestor-free, 2-way union merge** (NOT
   `branch::merge_with_report`, which is a *three-way* merge whose resolver only fires on mutual
   divergence from a common ancestor and sees only value bytes — it cannot host this). The merge
   reuses the lower-level prolly-tree node/range-iteration primitives but its own resolver: a key
   present on one side only is **kept** (union); a key on both sides resolves to the entry with the
   **max `(epoch, seq)`** (the chain tip), tombstones included (a tombstone can be the winning tip).
   `max` over the total order `(epoch, seq)` is a commutative, associative, idempotent semilattice
   join, so iterating the merge over >2 promisers is **order-independent** (no order-dependent root —
   the trap haematite's prolly history was once bitten by). Adopt the merged root as the committed
   baseline, fsync'd.
3. Begin serving only AFTER the merge completes ("elected but not live" until then). `seq` starts at
   0 for the new epoch (per the serve-authority invariant above — no recovery from merged data).

**Why this loses no committed write (R5).** Each committed write (put OR delete) is durable on a
quorum; the promise majority intersects every such quorum; so for every committed key the chain-tip
entry — value or tombstone — is present on at least one promiser. Per-key max-stamp selects exactly
that tip, and cross-key union keeps every key, so the merged state dominates **every** committed
mutation across the majority, forks and deletes included. Quantifies over **committed** writes (§2.0);
never depends on roots being totally ordered.

**In-doubt tails.** In-doubt writes (un-acked, §2.0) live in the WAL buffer *beyond* the committed
root, so they are not part of any promiser's committed state and never enter the merge. They converge
via the new owner's subsequent epoch-stamped writes (CAS+fence make the majority uniform) and ordinary
anti-entropy. Since no client was told they succeeded, losing or converging them is always permitted.

**Reused vs. discarded from the first 3-4 attempt.** The pull/transport plumbing
(`ShardSyncRequest`/`PushResponse`, `export_reachable`, the catch-up coordinator) is correct and kept.
The **single-root `import_synced`/`become_live` adoption is discarded** for the union merge above.

**2b lands cleanly on top.** "Don't clobber run-history events" (2b) becomes a *per-keyspace resolver
swap*: for the run-history keyspace the merge **unions the values** instead of taking max-stamp. Same
machinery; the general kv path stays max-stamp-correct regardless.

### 2.5 Epoch-key exemption (avoid regress)

`promised[s]` and ownership are coordination metadata, not data keys, so they are governed by the
Prepare/Promise round, **not** by the data fence. If we also persist them via a reserved key, that
key is exempt from the §2.3 fence (it has no owner-epoch of its own). Stated as an invariant:
**ownership/promise state is changed only by the Prepare round and the fence-adopt rule, never by a
fenced data write.** This prevents the "epoch key is itself gated by an epoch" infinite regress.

---

## 3. Durable state additions

`promised[s]` (and `accepted_epoch[s]`, `owner_epoch[s]`) must survive crash and be fsync'd before
they are *acted on* (before replying Promise; before serving as owner). Candidate homes, to be
decided in 3-0 spike:

- **(pref) A reserved per-shard metadata frame in the shard WAL.** The WAL already fsyncs a
  committed-root marker at `commit()` (`shard/actor/native.rs:94`, `wal/recovery.rs:93-116`); add a
  `PromiseRecord{shard, ballot}` frame type recovered on boot. Keeps ownership durability in the
  same fsync domain as data, single file per shard, no new store.
- (alt) A reserved KV key `__promise/<s>` written through the *forced-sync* apply path. Simpler to
  prototype, but routes through the tree and muddies the data/metadata boundary (§2.5) — prefer the
  WAL frame.

Three values are persisted per shard, each fsync'd **before it is acted on**:
1. `promised[s]` — **fsync BEFORE replying Promise** (explorer risk a). A crash between grant and
   persist must leave the node *not* promised, never silently double-promising a lower ballot.
2. `owner_epoch[s]` — **fsync before the owner's first served write**. A crash between election win
   and persist must leave the node *not* owning, never silently double-owning.
3. `persisted_max_minted[s]` — **fsync BEFORE sending any Prepare** (R4). Guarantees a restarted
   candidate's next ballot strictly exceeds every ballot it ever minted, preserving ballot
   uniqueness/monotonicity across restart. Missing this entry was an R4 finding against the first
   draft.

All three live in the same fsync domain as the committed-root marker (the WAL `PromiseRecord` frame),
so ownership durability shares the data durability path — no second store, no cross-store ordering
hazard. `promised[s]` is also the actor-local state the fence reads in-slice (R8): the shard actor
owns it; Prepare mutates it through the actor; the fence reads it through the actor.

---

## 4. Safety argument (majority intersection)

**Claim: at most one node can get data writes committed to a majority for shard `s` at any time
("single live owner"), and a superseded owner is fenced.** (Quantified over **committed** writes,
§2.0; in-doubt writes are a convergence concern handled at §2.4, not a safety one. The fence below
rejects `<` and — per R2 — a data write never *raises* `promised`, so this argument depends only on
`promised` values set by Prepare.)

Let owner `X` be elected at ballot `b_X` (majority promise set `M_X`) and `Y` at `b_Y > b_X`
(majority set `M_Y`). Any two majorities of the same membership intersect: `M_X ∩ M_Y ≠ ∅`.

- Take `n ∈ M_X ∩ M_Y`. `n` promised `b_X`, then (monotonic, `b_Y > b_X`) promised `b_Y`, so now
  `promised_n[s] = b_Y`.
- `X`'s data write carries `epoch = b_X`. At every `n ∈ M_Y`, `b_X < promised_n[s] = b_Y` ⟹ **Fenced,
  rejected.** So `X` can be accepted by at most the `N − |M_Y|` nodes outside `M_Y`, which is **a
  minority** (since `M_Y` is a majority). `X` can never reach write-quorum again. **X is fenced. ✓**
- `Y`'s data write carries `b_Y`; every `n ∈ M_Y` has `promised_n[s] = b_Y`, `b_Y ≥ b_Y` ⟹ accept ⟹
  majority ⟹ commits. Single live owner is `Y`. ✓

**Why the Prepare phase is necessary (the §"trap" reproduced formally):** without it, two candidates
write data with incomparable provenance but *ordered* ballots `(1, X)` and `(1, Y)`. A receiver
starting at `promised=(0,"")` accepts whichever arrives, then accepts the other iff its ballot is
greater — so ordering across receivers can split acceptance and both reach a majority. The Prepare
round forces a *majority to promise one ballot first*; majority-intersection then guarantees the
loser cannot also collect a majority. The data fence alone (no Prepare) is **insufficient**; the
Prepare round is load-bearing.

**Why folding accept into the first data write is safe:** the Paxos value-adoption rule exists so a
new leader does not erase a value that *may already be chosen*. Here the "value" being agreed is
ownership, and we *want* `Y` to supersede `X`; data is a separate concern carried on the CAS path and
preserved across handoff by the §2.4 state-sync (which reads from the same promise-majority that
intersects any committed-write majority). So no committed *data* is lost, and ownership is precisely
the monotonic ballot. A distinct Phase-2 ownership round would add a network round for no additional
safety.

**Liveness boundary (not a safety claim):** duelling proposers can livelock (each Nacks the other and
retries higher) — classic Paxos. Mitigation is a liveness concern only: randomized backoff, and/or
the (optional, unbuilt) liminal singleton picking *who attempts* acquisition. Under partition, the
minority side simply cannot elect (no majority) and is correctly unavailable for writes — the CAP
choice we want (CP for a given shard).

---

## 5. Wire protocol additions

New `SyncMessage` variants (encode/decode in `sync/protocol/wire.rs`, new control tags alongside
`SYNC`):

- `Prepare { shard: u32, ballot: Ballot }`
- `Promise { shard: u32, ballot: Ballot, accepted_epoch: Ballot, committed_root: Option<Hash> }`
- `Nack { shard: u32, promised: Ballot }`
- `WriteProposal` gains `epoch: Ballot` (extends the 2a variant).
- `WriteAck` gains an `outcome` arm `Rejected(RejectReason::Fenced)` (extends 2a `AckOutcome`).

`Ballot` codec: `u64` counter (big-endian) + length-prefixed `SyncNodeId` bytes, bounds-checked like
the existing wire reads (`wire.rs` `read_exact` via `checked_add` + `get(pos..end)`).

---

## 6. Build increments (each: build → I independently re-verify → merge `--no-ff`)

Same discipline as 2a. NEVER trust a green without my own adversarial read + re-run.

- **3-0 — Durable promise state (spike-first).** Add `Ballot`, per-shard `promised`/`owner_epoch`,
  the WAL `PromiseRecord` frame + recovery, fsync-before-act. **Spike:** kill a node mid-Prepare,
  prove it recovers `promised` and never double-promises a lower ballot. *Gate: a crash-injection
  test, not just a unit test.*
- **3-1 — Prepare/Promise/Nack wire + codec** (round-trip + bounds tests, mirrors 2a-1).
- **3-2 — Election coordinator** (`AcquireShard`): send Prepare to full membership, tally a majority
  of Promises with unique-ballot logic, Nack-driven retry/backoff. Reuse the 2a quorum tally shape.
- **3-3 — Epoch fence in `apply_durable`**: the `< promised → Fenced` / `≥ → adopt+CAS` rule at
  `shard/actor.rs:331`, plumb `epoch` through `WriteProposal` and `replicate_write`.
- **3-4 — Unified stamped write path + handoff merge (REVISED, §2.4).** Sub-increments:
  - **3-4a — Commit stamp.** `(epoch, seq)` on every committed write; `seq` in-memory per won epoch
    (NOT persisted — serve-authority invariant: re-acquire to serve after crash). Stamp stored in the
    value envelope; CAS stays over the logical value (stamp is NOT in the CAS hash). 3-3 fence
    unchanged.
  - **3-4b — Deletes as stamped tombstones through `replicate_write`** (fenced + quorum-replicated +
    persisted as read-as-absent stamped entries). Remove the local-only `delete` path.
  - **3-4c — Ancestor-free 2-way union + max-stamp merge** (own resolver over prolly-tree iteration
    primitives; NOT `merge_with_report`). Property test: ≥3 forked states, permuted merge order →
    identical root (commutative/associative).
  - **3-4d — Handoff wiring:** `become_live` merges the promise-majority committed states before
    serving (keep the pull plumbing). *Gates: (i) FORKED-committed-state failover — owner commits k2
    to {A,C}, k3 to {A,B}; new owner over {B,C} must serve BOTH (single-root adoption fails this);
    (ii) committed DELETE survives failover — a tombstone is not resurrected.*
- **3-5 — End-to-end concurrent-proposer proof.** The adversarial counterpart to 2a-5: spin a
  cluster, drive **two nodes to acquire the same shard concurrently**, assert **exactly one** wins,
  the loser is `Fenced` (explicitly, not `QuorumTimeout`), and a deposed owner that keeps writing is
  rejected. Then partition mid-write and assert the minority cannot elect.

---

## 7. Explicit "try to break this" list for adversarial review

1. **Trap reproduction:** can ANY concurrent-acquisition interleaving still yield two owners at the
   same or different epochs? Attack the §4 intersection argument directly.
2. **Fence-adopt `≥` rule:** does adopting `promised[s] = write.epoch` on a write whose Prepare this
   node missed ever let an *un-elected* writer (one that never got a majority of Promises) advance a
   node's `promised` and thereby fence the *true* owner? (Intended answer: an un-elected writer can
   touch only a minority before the true owner's majority fences it — verify rigorously.)
3. **Crash windows:** between Promise-fsync and reply; between election win and `owner_epoch` fsync;
   between data CAS-commit and ack. Does any crash double-own or lose a committed write?
4. **Handoff merge correctness (REVISED §2.4 — attack this hardest):** (a) Can the **union + per-key
   max-stamp merge** ever drop a committed write, given forked per-key committed states? (b) Is the
   claim "same-key committed values are always a CAS chain, never an incomparable fork" actually
   airtight under the fence — find a committed same-key fork if one exists. (c) Can two committed
   writes share a `(epoch, seq)` stamp (a tie would make max-stamp ambiguous)? (d) Does `seq`
   survive owner change + crash monotonically (recovered as `max(seq)` over merged state)? (e) Can a
   new owner serve before the merge completes and roll back a committed write?
5. **Epoch-key regress / metadata-vs-data boundary:** any path where ownership state is mutated by a
   fenced data write, or a data write escapes the fence?
6. **Ballot monotonicity under restart:** can a restarted node reuse or regress a `counter` and mint
   a ballot ≤ one it already used, breaking uniqueness/monotonicity?
7. **Liveness vs safety conflation:** is anything in §2 relying on the (optional) liminal singleton
   or on timeouts for *safety* rather than liveness?
8. **2a interaction:** does single-ownership actually neutralise the 2a phantom-local-ack hole in
   ALL cases, including owner-change racing an in-flight 2a write from the prior owner?

---

## 8. Scope discipline (do not over- or under-build)

- We build **single-decree** ownership election (per ownership change, rare/failover), NOT a
  multi-decree Raft log. Data stays on the 2a quorum-CAS path.
- Anything weaker than the Prepare/Promise majority round has the §"trap" split-brain — not
  optional.
- The liminal singleton and failure-detector-driven *automatic* failover are **liveness** features,
  explicitly out of scope for the safety core; they can land later without changing the safety
  argument.
- **Membership is FIXED for the life of a shard's epoch lineage (R1).** The §4 intersection proof
  assumes a single node-set: two majorities of the *same* membership always intersect, but two
  majorities of *different* sets need not (N={A,B,C} vs N'={C,D,E} ⟹ {A,B} and {D,E} are disjoint
  majorities ⟹ two un-fenced owners). Online reconfiguration (add/remove a node) therefore MUST go
  through a joint-consensus / overlapping-quorum step and is **out of scope** for the step-3 safety
  core. The denominator is `config.nodes` (`sync/membership.rs:58`), static for the cluster's life;
  this invariant must be asserted, not assumed.
