<!-- STATUS: DRAFT design blueprint (2026-07-01). Source-grounded synthesis of five
perspective passes (routing/range-map, tree split-merge mechanics, global-root invariant,
split/merge coordination under failover, within-shard order + migration). Every seam cites
file:line in crates/haematite (and crates/aion-store-haematite where the store rides along).
NOT approved to build — SPIKE-FIRST. §7 gates the whole pipeline on a proptest that must pass
before any coordination code is written. Carries OPEN DECISIONS for Tom (§8). Review first. -->

# Elastic Resharding — hash-RANGE ownership with structural split/merge over the prolly tree

> Implementation blueprint. Makes haematite's `shard_count` **elastic** instead of an immutable
> ceiling, by routing on hash **RANGE** ownership and realising a shard split/merge as a
> **structural** prolly-tree fork/merge. Grounds every seam in verified source (file:line).
> Composes with the quorum write path (`replicate_write`), the step-3 epoch fence, and the
> handoff union-merge — reusing them rather than inventing a parallel consensus. This is
> CockroachDB/Bigtable range-splitting done over the **BLAKE3 hash space** (so distribution
> stays uniform, no hotspots), which is uniquely natural here because the tree is
> content-addressed, structurally shared, and history-independent-merge.

---

## 1. Problem & goal

### 1.1 The immutable `shard_count` ceiling

Sharding today is a fixed **modulo** map. `ShardRouter::shard_for` takes the first 8 bytes of
`BLAKE3(key)` as a big-endian `u64` and returns `value % self.handles.len()`
(`shard/router.rs:20-28`). The divisor is `config.shard_count`, a **required, no-default**
durable field (`db/config.rs:11-18`) serialized to `CONFIG_FILE` at create (`config.rs:32-37`)
and read back verbatim on open (`config.rs:39-42`); `validate_shard_count` rejects only 0
(`config.rs:51-57`). Startup spawns exactly `0..shard_count` actors, each into a **positional**
`shard-{index}` directory with its own `store` + `shard.wal` (`db/startup.rs:34,83,235-237`),
and `Database::shard_count()` is a `const fn` returning that field (`db.rs:460-462`).

Nothing reconciles a changed count with data on disk. Because routing is `hash % N`, changing
`N` **re-homes ~every key** under the new modulus and orphans the old `shard-{index}` dirs. So
`shard_count` is a hard ceiling chosen once at create time — the documented "shard-count
immutability trap." The current mitigation is to raise the *default* count so the ceiling is
high; that is a workaround, not elasticity.

This ceiling blocks the control-plane strategy: a self-contained, multi-tenant control plane
that **places and scales compute** wants a data substrate that grows a namespace's shard set
on load and shrinks it when idle, without a global rehash or a stop-the-world migration.

### 1.2 The fork/merge insight

The prolly tree already has three properties that make **range** splitting cheap and safe where
it would be expensive in a B-tree engine:

1. **History-independent root.** The root hash is a pure function of the final key→value SET,
   never of operation order — `finish_root`/`build_spine` rebuild the spine level-by-level from
   the leaf set only (`tree/mutate.rs:433-480`), grouped at every level by the same
   content-defined boundary detector `is_boundary = BLAKE3(key) % target_size == 0`
   (`tree/boundary.rs:17-43`). The order-dependent-root regression (positional internal split)
   is **fixed and proptest-guarded** (project memory: haematite history-independence bug
   RESOLVED, landed 7197cf6).
2. **O(1) structural fork.** `fork(root)` records the root hash and allocates an empty WAL
   buffer; it takes **no node store** and cannot read or copy tree nodes — work is constant in
   entry count (`branch/fork.rs:6-15`). `fork_shards` gives one independent root per shard
   (`fork.rs:28-38`).
3. **History-independent union merge.** `merge_committed_union` (the handoff reconciler) is a
   commutative/associative/idempotent max-`(epoch,seq)` semilattice join over reachable
   committed nodes (`sync/handoff_merge.rs:26-32,107-147`; `db/receiver.rs:900-989`), already
   used by `acquire_shard_and_serve`/`become_live` before a new owner serves.

The insight: **a shard SPLIT at the range median is a structural tree split (a fork with
subtree sharing, ~O(log n)), and a shard MERGE of adjacent ranges is a structural union of two
disjoint hash-ranges (history-independent, already hardened).** `shard_count` stops being a
ceiling: start at **1 range covering the whole hash space**, split as data/load grows, merge to
shrink; a split touches **only the one shard**, no global rehash.

### 1.3 The one tension that shapes the whole design

The elegance above is only *fully* realised if the tree is ordered so a hash-range is a
contiguous tree interval. But the tree today is ordered by **raw key bytes**
(`tree/node.rs:230-245` compares `previous_key.cmp(key)`; `tree/mutate.rs:216,288`;
`tree/cursor.rs:272-285`), and **Aion's event-stream read path depends on that raw-key order**
(§6). Re-ordering the tree by hash-prefix would give a clean O(log n) median split but would
scatter each workflow's events, breaking history reconstruction. This tension — hash-order for
cheap splits vs. raw-key-order for contiguous stream reads — is the central feasibility
question and is resolved explicitly in §2.3 and §6, not hand-waved.

---

## 2. Model: hash-range routing + the durable range-map + within-shard ordering

Three separable layers. Change the first two; **do not** change the third.

### 2.1 Routing becomes range-lookup, not modulo

Replace `handles: Vec<ShardHandle>` indexed by `hash % len` (`router.rs:6-9,20-28`) with an
ordered range map resolved by binary search:

```rust
struct ShardRouter {
    ranges:  BTreeMap<[u8; N] /* lo (BLAKE3 prefix) */, ShardId>, // [lo, next_lo) → owner
    handles: HashMap<ShardId, ShardHandle>,
    version: RangeMapVersion, // monotone; see §5
}
```

`shard_for(key)` takes the BLAKE3 prefix as a routing **point** and finds the owning range with
`ranges.range(..=point).next_back()` — the shard whose `[lo, hi)` contains it (rewrites
`router.rs:20-28`). `handle_for_shard` stays an id→handle lookup, but **over a map, not a Vec
index** (`router.rs:37-39`). Start with a single range `[0, 2^256)` → one shard, exactly the
today-behaviour for a 1-shard DB.

`ShardId` keeps its type (`ids.rs:14 type ShardId = usize`) but **loses its "= Vec position"
semantics**: after a split the id set is **sparse**, so every positional use — `handles.get(id)`
(`router.rs:38`), the `shard-{index}` dir (`startup.rs:235`), `0..shard_count` loops
(`sync/scheduler.rs:373`; aion `store.rs:1164,1211`), `ordered_hashes`' `vec![None; N]`
(`db/helpers.rs:36-61`) — must become set/map lookups. Ids are minted from a durable counter.

### 2.2 The range-map is durable, replicated cluster state — not a config scalar

`shard_count` in `config.rs:13` is a node-local scalar. A split is a quorum-replicated,
CAS-guarded, failover-surviving state change — **exactly the thing haematite's tree already
replicates.** Do **not** put the range-map in a side file. Store it in a **reserved
well-known metadata shard** (ShardId 0, fixed whole-space range at bootstrap) whose tree holds
rows `rangemap:<lo> → { hi, owner_shard_id, map_version, epoch }`. This shard is acquired,
owned, and merged by the **same** `acquire_shard_and_serve` + `become_live` union path as any
data shard (`receiver.rs:900-989`), so the range-map inherits quorum, epoch-fence, and
history-independent merge for free.

A split then becomes a **CAS write into the range-map shard** (replace one `[lo,hi)` row with
two — `[lo,mid)`, `[mid,hi)`) driven through `replicate_write` (`receiver.rs:84-147`), with the
CAS `expected` = the old row's committed hash. That gives the atomic "split happened exactly
once" guard the append counter already demonstrates (`receiver.rs:430-466`): a concurrent second
splitter sees a hash mismatch and loses. `config.shard_count` stops being the source of truth —
it degrades to at most an **initial-range seed** — and `Database::shard_count()` (`db.rs:460`)
becomes "count of live ranges in the map."

### 2.3 Within-shard ordering stays RAW KEY (the resolution of §1.3)

**Decision (recommended): keep the prolly tree keyed by RAW user key. Do not hash-prefix the
tree key.** Elasticity comes entirely from the **routing** layer (range map) plus **fork/merge
of whole shard trees**, not from re-ordering entries by hash-prefix.

This choice is forced by the event-stream read path: events are stored at
`stream_key || 0x00 || seq.to_be_bytes()` so a raw-key range over one stream yields events **in
sequence order**, and `read_event_entries_from` issues **one contiguous raw-key range per
stream and trusts the returned order** (`api/event_store.rs:11-20,178-198,269-280`;
`db.rs:146-160`). Hash-prefixing the tree key would scatter a stream's events by per-record
hash, silently returning wrong/partial history to `read_events_from`
(`aion store.rs:1238-1258`). See §6 for the full order-dependency audit.

**The cost of this decision is paid in §3:** with raw-key ordering, a *hash*-range does **not**
correspond to a contiguous tree interval — modulo-style, a hash-range's keys are **scattered**
across the raw-key-ordered tree. So the "split = O(log n) structural median cut" headline is
**not free**; §3.2 defines what a split actually costs under raw-key ordering and why it is
still the right trade. The hash-order alternative (composite `hash || key`) is kept alive only
as an OPEN DECISION (§8) with its honest blast radius.

### 2.4 Aion rides along with almost no shape change

Aion already models the owned set as a **sparse** `owned_shards: Arc<RwLock<Option<BTreeSet<usize>>>>`
(`aion store.rs:204`), with `extend_owned_shards` for failover adoption (`store.rs:439`) and
`scan_prefix_scoped` scoping enumeration to it (`store.rs:1185-1222`). A post-split node owning
`{0, 7}` needs **no** shape change. The only edits: every enumeration that fans across
`0..shard_count()` (`store.rs:1164,1211`) iterates the **live ShardId set from the range-map**
instead of a dense range; and shard identity in `publish_shard_owner`/`shard_owner_key`
(`keyspace.rs:161-185`) becomes a **stable range-id**, not a dense `usize`, so a split does not
renumber survivors. The scan/claim paths themselves are order-agnostic and need **no** change —
every caller already re-sorts (`store.rs:1158-1159`; §6.1).

---

## 3. Split & merge mechanics

### 3.1 What "structural" buys, and where

Two regimes, decided by §2.3:

- **Raw-key ordering (recommended):** a *hash*-range is a **scattered** subset of the raw-key
  tree. A split therefore cannot be a single structural median cut; it is a **partition walk**
  (§3.2). The O(log n) structural-fork elegance is **retained only for the MERGE-neutral
  primitives** (`fork`, content-addressed subtree sharing) that make the *data movement* cheap,
  not the *cut* cheap.
- **Hash-key ordering (deferred alternative, §8):** a hash-range *is* a contiguous tree
  interval; a split *is* the O(log n) median cut described below. This is the elegant path, but
  it costs the event-stream re-encode (§6) and is not recommended for v1.

The mechanics below describe both, flagged.

### 3.2 SPLIT of `[lo, hi)` at `mid`

1. **Choose `mid`.** A streaming quantile of the routing points (BLAKE3 prefixes) of the keys in
   the shard, targeting the median so both halves get ~half the load. See §4 for why `mid` may
   be **snapped to a content-defined boundary**, not the exact arithmetic median.
2. **Mint a stable ShardId** for the new half from the durable counter (`ids.rs:14` value, not
   position).
3. **Spawn the new physical shard at runtime** — dir/WAL/store (`startup.rs:157-176`). Today
   `spawn_one_shard` runs **only at boot** into a fixed `shard-{index}` dir; a split needs a
   **live** spawn into a dir named by the **stable id**, not the position. Runtime spawn is
   currently untested (§8 risk).
4. **Produce the new half's baseline root:**
   - *Hash-key ordering:* descend the spine choosing the child straddling `mid`, recurse **only
     down that one path**, reuse (by hash) every subtree wholly on one side of the cut, and
     re-chunk the straddling leaf/internal node via `split_after_boundaries` + `build_spine`
     (`mutate.rs:385-480`). ~O(log n) node reads/rewrites — same shape as `cursor` descent
     (`cursor.rs:103-153`) and `diff`'s range recursion (`diff.rs:99-144`). **This primitive
     does not exist today** and is net-new tree code (§3.4).
   - *Raw-key ordering:* walk the shard, route each key by its BLAKE3 prefix into the half it
     now belongs to, and `batch_mutate` the moved keys into the new shard's tree. O(n) in the
     moved half. In-process with a **shared content-addressed NodeStore**, the *data* is
     structurally shared (near-free); the O(n) is the routing walk + spine rebuild, not a byte
     copy. Cross-node, it is a bulk content-addressed node transfer (sync already ships nodes).
5. **Legitimize ownership.** The splitting node is the live owner (holds `live_epoch` via
   `owner_stamps`). Run `acquire_shard_and_serve(new_id)` on the **same node**
   (`receiver.rs:900`) — a self-quorum election that mints `owner_epoch` (fsync) + in-memory
   `live_epoch`, then `become_live` union-merges the promise majority's roots (here trivial,
   since the node handed the new shard its own subtree).
6. **Publish the range-map CAS** (§2.2), `expected` = old row hash. **This is the linearization
   point** — routing flips here and only here (§5).

### 3.3 MERGE of adjacent `[lo, mid)`, `[mid, hi)`

The inverse, and the part that is genuinely on solid ground:

1. Union-merge the two trees with `merge_committed_union` — the **already-hardened**
   history-independent, order-and-multiplicity-independent join (`receiver.rs:944-989`;
   `handoff_merge.rs:26-32`). Because the two ranges are **disjoint** in the hash space, this is
   a clean concatenation of key sets with no per-key conflict (the union simply keeps every
   key; the max-`(epoch,seq)` rule only matters for the rare key present in both, which cannot
   happen for disjoint ranges — a useful invariant to assert).
2. `acquire_shard_and_serve` the survivor over **both** old sub-roots so it adopts a lossless
   baseline before serving.
3. Retire one ShardId; CAS the two range rows back into one.

Note the union merge is a **per-key reconciler**, not a spine concatenation. A cheaper
*structural* concat (splice two spines, re-chunk only the seam via `build_spine`) is possible
under hash-key ordering but is **net-new code** with its own proptests; v1 uses the existing
union merge, which is correct if not maximally cheap.

### 3.4 New tree primitives required (hash-key path only)

There is **no** tree-level split-at-midpoint, concat-adjacent, or bulk-build-from-sorted
primitive anywhere; the only splitter is `split_after_boundaries` chunking a flat list
(`mutate.rs:385`). If the hash-key path is ever chosen, `SPLIT(root, mid) -> (lo_root, hi_root)`
and `CONCAT(lo_root, hi_root) -> root` are net-new `tree/` primitives, each with its own
history-independence proptest (the existing harness `mutate_history_independence_tests.rs`
does **not** cover split/concat). Under the recommended raw-key path, **no new tree primitive is
needed** — split is a routing-walk + `batch_mutate`, merge is `merge_committed_union`.

---

## 4. The global-root invariant under a dynamic shard set (the DEEP gate)

### 4.1 There is no global root today — one must be built first

The terminal committed surface is `Database::commit()`, which fans a `Commit` to every shard and
returns a **positional vector** `ShardRoots = BTreeMap<usize, Hash>` reassembled by
`ordered_hashes(results, shard_count)` — `vec![None; shard_count]`, drop each `Ok(hash)` into
slot `index`, error on any missing slot (`api/kv.rs:39,244-250`; `db/helpers.rs:36-61`). There
is **no** fold/concat/Merkle-combine into one digest anywhere. So there is no existing global
root to *regress* — but there is also nothing to *compare* two topologies with, and a dynamic
range set has **no dense `0..N`** for `ordered_hashes` to reassemble. **A real global root must
be built as part of this work**, and it must be computed over **ranges, not indices.**

### 4.2 The global root as a fold over ranges

Model the range-map as an ordered `(split-point → range-subtree-root)` list and feed it through
the **same** `finish_root`/`build_spine` machinery (`mutate.rs:438-480`). The global root then
inherits the already-hardened history-independence: it is a pure function of the set of
`(split-point, subtree-root)` pairs, **independent of the order splits/merges happened**. An
**empty range must contribute the canonical empty-leaf hash** (`store_empty_leaf`, referenced
`mutate.rs:442-443`) so that a range with no keys folds identically whether it is present-empty
or absent-but-covered — otherwise a shard appearing/disappearing perturbs the global root.

### 4.3 The load-bearing invariant: split/merge must preserve the root

**GATE 1 — split-preserves-root.** A split of `[lo,hi)` at `mid` into `[lo,mid)+[mid,hi)` must
**not change the global root**, and `MERGE(SPLIT(t)) == t` at the root-hash level. This ties
directly to history-independence: it holds **iff** the global root is a canonical Merkle fold
over the leaf data ordered by the split key, with split points being mere internal cut positions
that carry **no identity of their own**.

The subtlety (and it is the whole gate): `union(left, right)` hashes **identically** to `whole`
only if the cut lands on a **content-defined boundary** — a key where `is_boundary` is true
(`boundary.rs:17-43`). Because `build_spine` groups by content-defined boundaries that depend
**only on key content**, the concatenation `subtree(lo,mid) ++ subtree(mid,hi)` re-folds to
exactly the spine of `subtree(lo,hi)` **when `mid` sits on a boundary** — the cut point is
invisible to the fold. If `mid` is an **arbitrary** hash-space median, the left half's last leaf
and the right half's first leaf do **not** sit on a natural boundary; re-chunking them yields
**new** nodes (new hashes) that did not exist pre-split, and round-trip hash-identity does
**not** hold for free.

**Resolution: snap `mid` to the nearest content-defined boundary to the target median, not the
exact median.** This makes split positions **data-dependent, not arithmetic**, and slightly
non-uniform in the hash space — a real, bounded weakening of the anti-hotspot guarantee that
motivated hash-ranges. The tension (perfect load balance vs. structural determinism) is genuine;
the boundary spacing (~`target_size` keys) bounds the drift, so with a reasonable `target_size`
the median error is small. **This must be proven by proptest before anything downstream is
built (§7 spike).**

Under the **recommended raw-key ordering**, GATE 1 is subtler still: the tree is *not* ordered
by the split key (hash), so "split preserves the root" is not even the right frame for a single
shard's tree — the *global* root is a fold over per-range roots, and moving a scattered key set
out of one shard's raw-key tree into another's genuinely changes both shard roots. What must be
preserved is the **fold over ranges of the underlying key→value SET**: the union of the two
post-split shards' contents equals the pre-split shard's contents, and the range-fold of the two
new roots equals the range-fold with the one old root replaced. This is a **set-equality +
deterministic-fold** obligation, provable but distinct from the boundary-snap argument, and is
GATE 1's raw-key form. The spike must state which regime it is proving.

---

## 5. Split/merge coordination protocol

Composes with — does not replace — the quorum write path (`replicate_write`,
`receiver.rs:84-147`), the step-3 epoch fence (`stamp.epoch < promised[shard]` both fences and
votes-against, `receiver.rs:494-575`), and the handoff union-merge (`handoff_merge.rs:107-147`).

### 5.1 Ordering (the safe sequence)

A split must commit in this order, and **only** this order:

1. **New shard live + baseline durable FIRST.** Steps §3.2(3)-(5): spawn, produce baseline root,
   `acquire_shard_and_serve(new_id)` so it has `owner_epoch` (fsync) + `live_epoch` and a
   union-merged baseline **before it can serve** (`receiver.rs:892-960` fail-closed:
   elected-but-not-live never stamps).
2. **Range-map CAS flips routing LAST** (§3.2(6)). `expected` = old range-row hash. This is the
   atomic publish / linearization point.

Rationale: if the map CAS committed first, routing would send writes to a shard with no live
owner (they fence forever) or to two shards claiming overlapping ranges (split-brain over a
key). "New shard live before routing flips" closes both.

### 5.2 Naming the three correctness gates

- **GATE 1 — empty/range root synthesis & split-preserves-root** (§4.3). The global root must
  fold deterministically over a dynamic range set, treat an empty range as the canonical
  empty-leaf, and satisfy `MERGE(SPLIT(t)) == t` at the root-hash level (boundary-snap under
  hash ordering; set-equality + deterministic-fold under raw-key ordering). **This is the spike
  gate — nothing else is built until a proptest proves it (§7).**
- **GATE 2 — atomic range-map publish under concurrency.** The range-map CAS
  (`expected` = old row hash, mirroring the append-counter CAS at `receiver.rs:430-466`) must
  serialize concurrent splitters and split-vs-merge races so exactly one wins; and the map
  **version must be monotone**, with every data write carrying and fencing on the version it
  assumed — a **new range-epoch fence dimension** analogous to the existing owner epoch-fence
  (`receiver.rs:564-575`). Without this, a split committing mid-flight routes a write to a
  retired shard (W3/W6 below).
- **GATE 3 — acquire/recover-before-serve during handoff.** The new (split) or surviving (merge)
  owner must run `acquire_shard_and_serve` → `become_live` union-merge over its promise majority
  **before serving** (`receiver.rs:900-960`). Elected-but-not-live must **fail closed**
  (`receiver.rs:892`): no `live_epoch` → no stamping → the next election re-runs the merge. This
  gate is **reused as-is** from the epoch-fence work; the split path must not weaken it.

### 5.3 Failure windows (honest assessment)

- **W1 — die after fork, before publish.** `fork` is in-memory/non-durable and store-less
  (`fork.rs:6-15`), so if nothing was published the old map still routes all of `[lo,hi)` to the
  original shard and the sub-roots are simply lost. **Benign and self-closing IFF the design is
  publish-or-nothing** (zero externally-visible effect before the CAS). Caveat: if step §3.2(4)
  *persists* sub-root nodes into the shared store before publish, those nodes must be
  **registered as live roots** (`branch::registry`, `fork.rs:22-26`) or a background prune could
  delete nodes a crash-retry needs. Prefer: no pre-publish store writes that aren't
  registry-pinned.
- **W2 — die during the range-map quorum write (partial acks).** Reduces to the existing
  quorum-write in-doubt window, reconciled by the union merge on the next election
  (`handoff_merge.rs:107-147`). BUT the range-map is not a normal value — it changes **routing**,
  so a split that reached quorum-minus-one can leave some replicas routing `k` to the old shard
  and others to the new. This is exactly why GATE 2's **monotone version + per-write version
  fence** is mandatory: a stale-version write is fenced like a stale-epoch write.
- **W3 — concurrent write on `[lo,hi)` during the split window.** `replicate_write` routes then
  stamps an **explicit** `shard_id` the receiver trusts blindly (`receiver.rs:109,524,531`). A
  split committing between routing and apply names a shard that no longer owns the sub-range.
  The **only** sound resolution: route under the range-map version — writes stamped *before* the
  new map version go to the old shard, *after* go to the new; the new owner's `merge_adopt`
  reconciles the overlap. Re-forking at latest-committed-root in a racy loop is rejected.
- **W4 — two nodes split the same range (or split-vs-merge).** The range-map CAS-on-prior-version
  serializes them: one wins, one sees a mismatch and aborts (`receiver.rs:494`). Sound **iff the
  range-map is a single CAS-guarded object**; non-adjacent concurrent splits need multi-key
  atomicity (batch proposal, `receiver.rs:576-609`) or they can interleave into an inconsistent
  map.
- **W5 — new owner dies after publish, before `become_live`.** Routing already points at the new
  shard, but it is elected-but-not-live and **must not serve** (fail-closed, `receiver.rs:892`).
  GATE 3 covers this exactly as the epoch-fence work already does; next election re-runs the
  merge. Sound, reused as-is.
- **W6 — child epoch/seq derivation.** `live_epoch`/`seq` are per-shard, in-memory, and set
  **only** by a live win this process (never seeded from disk-recovered `owner_epoch`,
  `db.rs:356-368`) — the R-LE anti-duplicate-stamp gate, whose dominance argument assumes a
  **single lineage** (`receiver.rs:934-936`), not a fork of the id itself. Splitting one shard
  into two needs a principled derivation of the children's epochs/seqs so the child's first
  write dominates inherited entries. Running a **fresh** `acquire_shard_and_serve` for the child
  (§3.2(5)) supplies a clean new epoch and sidesteps inheriting a stale seq — this is the
  recommended derivation and must be asserted in the merge proptest.

### 5.4 Interaction with the open kill-9 failover bug

The split failover path adds **more** adopted-shard membership writes, and the memory-flagged
**kill-9 failover quorum bug** (CRITICAL #157: adopted-shard `WriteMembership` keeps the dead
owner, `required 2 acknowledged 1`) lives on exactly that path. **Elastic resharding is BLOCKED
on that fix** and would likely amplify the required-vs-acknowledged gap. §8 lists it as a hard
dependency, not a footnote.

---

## 6. Within-shard key-order dependencies + migration/compat

### 6.1 The order-dependency audit (why §2.3 keeps raw-key order)

Two classes of within-shard read:

- **Order-AGNOSTIC — the KV prefix scans** (timers `t:`, outbox `o:`, packages, routes,
  namespaces, event-stream *enumeration*). Every `scan_prefix`/`scan_prefix_scoped` fans
  `range_per_shard(shard, prefix, upper)` across shards, concatenates in **arbitrary**
  cross-shard order, and **every caller re-sorts** — outbox by `(visible_after, dispatch_key)`
  (`store.rs:1613-1679`), timers by `(fire_at, workflow_id, timer_id)` (`store.rs:2254-2263`),
  packages, routes, namespaces (`store.rs:2300-2408`). The code says so explicitly: "the
  cross-shard concatenation order is arbitrary, but every caller re-sorts" (`store.rs:1158-1159`).
  **This class does not constrain the tree's ordering at all.**
- **Order-DEPENDENT — the event-stream read path** (workflow history). `read_event_entries_from`
  issues **one contiguous raw-key range per stream and trusts the returned order**
  (`db.rs:146-160`; `event_store.rs:178-198,269-280`), relying on (a) all events of a stream
  staying **contiguous** and (b) ascending by `seq` — both guaranteed by the
  `stream_key || 0x00 || seq` key layout under **raw-key** ordering. `read_events_from` consumes
  in returned order to rebuild history (`aion store.rs:1238-1258`).

The load-bearing conclusion: the *only* within-shard order dependency is the event stream, and
it **requires raw-key order**. Therefore §2.3 keeps the tree raw-key-ordered; elasticity lives
in routing. (OPEN, §8: confirm `read_event_entries_from` is the *sole* order-trusting `db.range`
consumer via a repo-wide grep — verified for aion event history + `scan_sequences`; benches/other
crates unchecked.)

### 6.2 Migration & compat from fixed-modulo DBs

A modulo shard holds keys with `hash % N == i` — a **STRIDED**, not contiguous, subset of the
hash space. So there is **no cheap aliasing** of an existing modulo DB into hash-ranges;
pretending otherwise corrupts routing. Two honest paths:

- **Default — version flag, new DBs are ranges.** Add a `routing_mode` tag to `DatabaseConfig`
  (`config.rs:11-18`): `Modulo { shard_count }` (existing, unchanged) vs
  `Ranges { manifest_seed }`. `read_config` dispatches on the tag (`db.rs:92-97`;
  `config.rs:39-42`). Existing modulo DBs keep working byte-identically; new DBs mint as
  `Ranges` with one whole-space range. **No in-place conversion.** Recommended for the roadmap.
- **Explicit conversion (opt-in, offline).** Freeze/quiesce, create a fresh `Ranges` DB with one
  range, stream every `(key, value)` via `scan_sequence_keys`/`range_per_shard` and re-put
  (router re-routes each key). O(total entries); structural sharing on the write side keeps it
  cheap-ish but it must be offline. Only build this if a customer needs to convert a live modulo
  DB.

### 6.3 The `config.shard_count` blast radius

`shard_count` is a no-default field threaded widely: `config.rs:11-18`, `sync/scheduler.rs:66`,
aion `create_with_shard_count` (`store.rs:237-243`), and `Database::shard_count()` as a `const
fn` (`db.rs:460-461`). Making it elastic means it is **no longer constant**; every caller
assuming a fixed count (aion `0..shard_count` at `store.rs:1211`; sync scheduler partner×shard
fan-out at `scheduler.rs:373`) must **re-read live membership from the range-map** after a split,
not cache a boot-time constant. Also: is `CONFIG_FILE` node-local or replicated today? If
node-local, the range-manifest needs a replication path (it lives in the metadata shard, §2.2)
before cross-node splits are safe — this folds into the haematite-as-cluster-source-of-truth
direction.

---

## 7. Slice pipeline (spike-first)

Epoch-fence-style incremental slices, but **gated on a spike**: the split-preserves-root proof
(GATE 1) is cheap to attempt and, if it fails or forces an unacceptable uniformity loss, kills
or re-shapes the whole project. **Build nothing downstream until S0 is green.**

- **S0 — SPIKE / GATE 1 (blocking).** A standalone proptest, no coordination code. Prove: (i) an
  empty range folds to the canonical empty-leaf and folds identically to absent-but-covered
  (`mutate.rs:442-443`); (ii) `MERGE(SPLIT(t)) == t` at the root-hash level for the chosen
  ordering regime — under hash-key ordering, snap `mid` to the nearest boundary
  (`boundary.rs:17-43`) and measure the median drift vs. uniformity; under raw-key ordering,
  prove set-equality + deterministic range-fold. Extend `mutate_history_independence_tests.rs`.
  **Exit criterion: proptest green + a written verdict on the uniformity cost.** If red, STOP.
- **S1 — range-map data model + router.** Introduce the `Ranges` config tag (§6.2), the range-map
  rows in the metadata shard (§2.2), and the range-lookup `shard_for` (§2.1) behind the tag.
  Single-node, one whole-space range → **byte-identical to a 1-shard modulo DB**. Sever the
  first tranche of positional-id assumptions (`router.rs:38`, `ordered_hashes`).
- **S2 — sparse ShardId sweep.** Move every `0..shard_count` loop and `handles.get(index)` to
  live-set iteration (`scheduler.rs:373`; aion `store.rs:1164,1211`; `startup.rs` dir naming to
  stable-id). Mechanical but wide; no behaviour change (still one range).
- **S3 — global root fold.** Replace `ordered_hashes` (`helpers.rs:36-61`) with the range-fold
  (§4.2). Wire the empty-range canonical hash. Now two topologies with the same data have the
  same global root — the artefact GATE 1 protects.
- **S4 — range-map version fence (GATE 2).** Monotone map version; every data write carries and
  fences on it (`receiver.rs:564-575` analogue). CAS-guarded map publish (`receiver.rs:430-466`
  pattern). No split yet — just the fence machinery, testable by forcing stale-version writes.
- **S5 — runtime shard spawn.** Make `spawn_one_shard` (`startup.rs:157-176`) safe to call live,
  into a stable-id dir, against the scheduler. Prove a spawned-then-`acquire_shard_and_serve`d
  shard serves correctly (GATE 3 reused).
- **S6 — SPLIT, single-node.** Compose S4+S5+§3.2: choose `mid`, produce baseline, acquire+serve
  child, publish CAS. Prove data conservation + global-root preservation on a live single node.
- **S7 — MERGE, single-node.** §3.3 via `merge_committed_union`; assert disjoint-range no-conflict.
- **S8 — cross-node split/merge under failover.** Only after the kill-9 quorum bug (#157) is
  fixed (§5.4). Full W1-W6 window testing, kill-9 mid-split, verify no lost/double-routed key.
- **S9 — Aion ride-along.** Range-id identity for `owned_shards`/`publish_shard_owner`
  (`keyspace.rs:161-185`); live-set enumeration; a cross-node **failover-during-split** demo on
  the real Aion-on-haematite substrate (the visceral kill-it-watch-it-recover demo Tom wants).

---

## 8. Open decisions + honest risks

**Open decisions (for Tom):**

1. **Tree ordering — raw-key (recommended) vs. hash-key composite.** §2.3 recommends raw-key
   (keeps event-stream contiguity, elasticity via routing; split is a partition walk, not an
   O(log n) cut). The hash-key `hash || key` alternative gives the elegant O(log n) median split
   but requires re-encoding event keys as `hash(stream_key) || stream_key || seq` to keep a
   stream contiguous, +32 bytes/key on disk, and a rewrite of every reader/scan/stream-index —
   large Aion blast radius. **Recommend raw-key for v1; keep hash-key as a future optimization
   only if profiling proves the partition-walk split too slow.**
2. **Where the range-map lives.** Recommended: reserved well-known ShardId 0 metadata shard
   (reuses acquire/merge/epoch for free) — accept it is a routing hot-spot that must be cached
   and invalidated on split, and a bootstrap chicken-and-egg (fixed whole-space seed range
   solves lookup). Alternative: a separate replicated structure. **Recommend the metadata
   shard.**
3. **Split point — boundary-snap vs. exact median.** GATE 1 forces boundary-snap under hash
   ordering; the S0 spike must quantify the uniformity cost before this is locked.
4. **Migration — version-flag-only vs. build in-place conversion.** Recommend version-flag-only
   for the roadmap (§6.2); build conversion only on demand.

**Honest risks:**

- **R1 (highest) — GATE 1 may not hold cheaply.** If `MERGE(SPLIT(t)) == t` requires boundary-
  snapping that drifts the median enough to reintroduce hotspots, the headline "uniform
  hash-range" guarantee weakens. The S0 spike exists to find this **before** any coordination
  code. This is the biggest single risk.
- **R2 — routing-vs-split race (W3/GATE 2) is the deepest correctness hazard.** The receiver
  trusts an explicit stamped `shard_id` (`receiver.rs:531`); the epoch fence protects a deposed
  **owner** but **not** a stale **range** assignment. A brand-new **range-epoch fence dimension**
  must be designed and threaded — it does not exist today.
- **R3 — blocked on the kill-9 quorum bug (#157).** Split failover adds adopted-shard membership
  writes on exactly the broken path; cross-node resharding (S8) cannot land until #157 is fixed,
  and likely amplifies it (§5.4).
- **R4 — pervasive positional-id blast radius.** `shard-{index}` dirs, `0..shard_count` loops,
  `handles.get(index)`, `vec![None; N]`, per-shard promise WAL, and all of sync/election keyed by
  a dense `shard_id` (`sync_codec/message/root.rs`, `election.rs:36`) assume index == identity.
  Sparse ids are mechanical but wide, and two nodes at **different split generations** cannot
  line up `shard_id N` against `shard_id N` — root-exchange, pull, and election catch-up
  (`receiver.rs:697-976`) need a **range-addressed** wire identity (lower-bound split point), not
  a positional id.
- **R5 — runtime shard spawn is untested.** `spawn_one_shard` is boot-only today; live spawn
  against the scheduler (S5) is new surface.
- **R6 — child epoch/seq derivation** (W6) must be proven not to resurrect a stale inherited
  entry; the recommended fresh-`acquire_shard_and_serve` derivation needs a merge proptest.

---

## 9. Relationship to the interim (fixed 4096 + lazy materialization)

The interim mitigation — **raise the default `shard_count` high (e.g. 4096) and lazily
materialize shards** — is a workaround for the immutability trap, not a fix. It picks a large
ceiling up front and defers the physical cost of unused shards. The range model **subsumes** it
cleanly:

- **The interim is the degenerate range map.** A fixed-4096 modulo DB is exactly 4096 equal
  hash-ranges that can **never** split or merge. The range model generalizes this to an
  **arbitrary, mutable** set of ranges — so the interim is the special case "4096 immutable
  ranges," and the range model is "start at 1, split/merge on demand."
- **Lazy materialization becomes runtime spawn (S5).** The interim's "don't spawn a shard until
  it's needed" is precisely the runtime `spawn_one_shard` the split path requires — the same
  primitive, generalized from "spawn one of 4096 fixed slots" to "spawn a freshly-minted range
  owner."
- **Migration path.** A DB born under the interim (fixed 4096) is a `Modulo` DB in §6.2 terms; it
  keeps working under the version flag and, if elasticity is later wanted, converts via the
  offline re-put (§6.2) — no data model conflict, because both partition the same BLAKE3 space.
- **Sequencing.** Ship the interim now (it removes the immediate ceiling pain with minimal risk),
  and treat this document as the **forward** design. Nothing in the interim is thrown away: the
  high default is a sane starting range count, and lazy spawn is a down-payment on S5. The range
  model turns "4096 was hopefully enough" into "grows and shrinks with load, no ceiling."

---

<!-- Correctness gates recap (must be cited by every downstream brief):
GATE 1 — empty/range root synthesis & split-preserves-root (§4.3, spike-gated S0).
GATE 2 — atomic range-map publish + monotone version fence under concurrency (§5.2, S4).
GATE 3 — acquire/recover-before-serve during handoff, fail-closed (§5.2, reused from epoch-fence).
Hard dependency: kill-9 failover quorum bug #157 must land before cross-node S8. -->
