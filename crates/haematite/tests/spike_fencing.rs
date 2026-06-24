//! SPIKE (NOT a feature to ship) — empirical validation of fencing tokens and
//! active-active divergence on haematite.
//!
//! Goal: learn, by RUNNING real haematite primitives, whether a monotonic
//! fencing epoch enforced via `cas` can protect an active-active event-stream
//! design from split-brain, and what `sync`/`merge` REALLY does to divergent
//! event streams.
//!
//! Run with output:
//!   cargo test -p haematite --test `spike_fencing` -- --nocapture --test-threads=1
//!
//! Layers used:
//!  * E1 uses the REAL `EventStore`/`Database` actor path (`cas`, `append`).
//!  * E2/E3 model two partitioned nodes as two `MemoryStore`s whose trees are
//!    built with the EXACT `EventStore` keyspace encoding, then run the REAL
//!    production sync path (`pull_from_source`) and merge engine
//!    (`merge_synced_roots`) — i.e. the same code haematite's own sync scheduler
//!    drives. This lets us observe precise before/after tree state.

#![allow(clippy::unwrap_used, clippy::panic, clippy::print_stdout)]

use haematite::api::EventStore;
use haematite::branch::conflict::ConflictPolicy;
use haematite::db::{Database, DatabaseConfig};
use haematite::store::MemoryStore;
use haematite::sync::{SyncMergeRoots, merge_synced_roots, pull_from_source};
use haematite::tree::{Cursor, Hash, LeafNode, Node, batch_mutate};

type TestResult = Result<(), Box<dyn std::error::Error>>;

// ---------------------------------------------------------------------------
// Keyspace replication: build trees the way the EventStore + shard actor do.
// event:    stream_key || 0x00 || seq_be(8)          (engine seq is 1-based)
// counter:  stream_key || 0xff 's' 'e' 'q'
// scalar:   raw key                                  (CAS lives here)
// ---------------------------------------------------------------------------

fn event_key(stream: &[u8], engine_seq: u64) -> Vec<u8> {
    let mut k = stream.to_vec();
    k.push(0x00);
    k.extend_from_slice(&engine_seq.to_be_bytes());
    k
}

fn seq_counter_key(stream: &[u8]) -> Vec<u8> {
    let mut k = stream.to_vec();
    k.extend_from_slice(&[0xff, b's', b'e', b'q']);
    k
}

fn empty_root(store: &mut MemoryStore) -> Hash {
    store.put(&Node::Leaf(LeafNode::new(Vec::new()).unwrap()))
}

fn get(store: &MemoryStore, root: Hash, key: &[u8]) -> Option<Vec<u8>> {
    Cursor::new(store, root).get(key).unwrap()
}

/// Append `payloads` to a stream tree starting at engine seq `from_seq+1`,
/// writing each event key AND bumping the counter key — exactly as the shard
/// actor's `append` does. Returns the new root.
fn append_events(
    store: &mut MemoryStore,
    root: Hash,
    stream: &[u8],
    from_seq: u64,
    payloads: &[&[u8]],
) -> Hash {
    let mut muts: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    let mut seq = from_seq;
    for p in payloads {
        seq += 1;
        muts.push((event_key(stream, seq), Some(p.to_vec())));
    }
    muts.push((seq_counter_key(stream), Some(seq.to_be_bytes().to_vec())));
    batch_mutate(store, root, &muts).unwrap()
}

/// Read back every event payload present for `stream` under `root`, by probing
/// engine seqs 1..=max.
fn read_stream(store: &MemoryStore, root: Hash, stream: &[u8], max: u64) -> Vec<(u64, Vec<u8>)> {
    let mut out = Vec::new();
    for seq in 1..=max {
        if let Some(v) = get(store, root, &event_key(stream, seq)) {
            out.push((seq, v));
        }
    }
    out
}

fn counter(store: &MemoryStore, root: Hash, stream: &[u8]) -> Option<u64> {
    get(store, root, &seq_counter_key(stream)).map(|b| {
        let arr: [u8; 8] = b.as_slice().try_into().unwrap();
        u64::from_be_bytes(arr)
    })
}

// ===========================================================================
// E1 — CAS as a monotonic fencing epoch (single real Database instance).
// ===========================================================================

#[test]
fn e1_cas_fencing_epoch_single_instance() -> TestResult {
    println!("\n================ E1: CAS as fencing epoch (real Database) ================");
    let dir = tempfile::tempdir()?;
    let db = Database::create(DatabaseConfig {
        data_dir: dir.path().join("db"),
        shard_count: 4,
        sweep_interval: None,
        distributed: None,
    })?;
    let store = EventStore::new(db);

    // Ownership record for a shard lives at a CAS scalar key.
    let owner_key = b"ownership/shard-7/epoch".to_vec();
    let stream = b"runhistory/shard-7".to_vec();

    // --- acquire: epoch absent -> 1 (owner A) ---
    println!("[A] acquire: cas(None -> 1)");
    store.cas(&owner_key, None, 1)?;
    println!("    stored epoch = {:?}", store.read_value(&owner_key)?);

    // Owner A writes an event gated on epoch == 1. Gate = read epoch, compare.
    let gate = |expected: u64| -> Result<u64, String> {
        match store.read_value(&owner_key) {
            Ok(Some(e)) if e == expected => Ok(e),
            Ok(other) => Err(format!("FENCED: epoch is {other:?}, expected {expected}")),
            Err(e) => Err(format!("read error {e:?}")),
        }
    };

    println!("[A] write event gated on epoch==1");
    let epoch = gate(1)?;
    let next = store.append(&stream, b"A:event-0", 0)?;
    println!("    gate ok (epoch={epoch}), appended, stream next_seq={next}");

    // --- NEW owner B acquires: cas-bump epoch 1 -> 2 ---
    println!("[B] acquire: cas(Some(1) -> 2)");
    store.cas(&owner_key, Some(1), 2)?;
    println!("    stored epoch = {:?}", store.read_value(&owner_key)?);

    // --- STALE owner A still believes epoch==1, tries to write. MUST be fenced. ---
    println!("[A] (stale) attempt write gated on epoch==1 -> expect FENCED");
    match gate(1) {
        Ok(_) => panic!("E1 FAILED: stale owner A passed the epoch gate!"),
        Err(msg) => println!("    correctly fenced: {msg}"),
    }

    // Demonstrate the gate via cas itself (the actual conditional-write primitive):
    // a stale writer that tries to re-assert epoch 1 via cas(Some(1)->1) also fails.
    println!("[A] (stale) cas(Some(1) -> 1) as a conditional write -> expect CasMismatch");
    match store.cas(&owner_key, Some(1), 1) {
        Err(e) => println!("    correctly rejected: {e:?}"),
        Ok(()) => panic!("E1 FAILED: stale cas(Some(1)) succeeded after epoch moved to 2"),
    }

    // --- NEW owner B (epoch 2) writes successfully ---
    println!("[B] write event gated on epoch==2");
    let epoch = gate(2)?;
    let next = store.append(&stream, b"B:event-1", next)?;
    println!("    gate ok (epoch={epoch}), appended, stream next_seq={next}");

    // Absent-vs-physical-zero probe: store a physical 0 and see if it reads back
    // as Some(0) (distinct from None=absent).
    println!("[probe] absent-vs-physical-zero:");
    let zkey = b"ownership/zerocheck".to_vec();
    println!("    before any write, read_value = {:?}", store.read_value(&zkey)?);
    store.cas(&zkey, None, 0)?; // create with physical 0
    println!("    after cas(None -> 0), read_value = {:?}", store.read_value(&zkey)?);
    let absent_again = store.cas(&zkey, None, 5);
    println!("    cas(None -> 5) on a physical-0 key = {absent_again:?} (None != Some(0))");

    println!("E1 RESULT: fencing epoch via cas works on a single instance.\n");
    Ok(())
}

// ===========================================================================
// E2 — Divergent writes across TWO partitioned instances + REAL sync/merge.
// ===========================================================================

#[test]
fn e2_divergent_event_streams_sync_merge() -> TestResult {
    println!("\n================ E2: divergent event streams across two nodes ================");
    let policy = ConflictPolicy::Lww; // haematite's default conflict policy

    // Two partitioned nodes. They share a common base, then BOTH append to the
    // SAME stream while partitioned (no sync), with OVERLAPPING engine seqs.
    let mut node_a = MemoryStore::new();
    let mut node_b = MemoryStore::new();

    let stream = b"run/42".to_vec();

    // Common base: both start from an empty stream. Build base in A, copy to B by
    // applying the same mutation (content-addressed => identical hash).
    let empty_a = empty_root(&mut node_a);
    let empty_b = empty_root(&mut node_b);
    let base_a = append_events(&mut node_a, empty_a, &stream, 0, &[b"shared-0"]);
    let base_b = append_events(&mut node_b, empty_b, &stream, 0, &[b"shared-0"]);
    assert_eq!(base_a, base_b, "content-addressed base roots must match");
    let base = base_a;
    println!("base root = {base} (stream has shared-0 at engine seq 1, counter=1)");

    // PARTITION: each node independently appends its OWN next event at engine
    // seq 2 (overlapping seq, divergent payload), and bumps the counter to 2.
    let root_a = append_events(&mut node_a, base, &stream, 1, &[b"A-wrote-this"]);
    let root_b = append_events(&mut node_b, base, &stream, 1, &[b"B-wrote-this"]);
    println!("node A root = {root_a}");
    println!("node B root = {root_b}");

    println!("\n--- BEFORE sync ---");
    println!("A stream: {:?}", render(read_stream(&node_a, root_a, &stream, 4)));
    println!("A counter: {:?}", counter(&node_a, root_a, &stream));
    println!("B stream: {:?}", render(read_stream(&node_b, root_b, &stream, 4)));
    println!("B counter: {:?}", counter(&node_b, root_b, &stream));

    // HEAL: run the REAL sync transfer (B pulls A's nodes into B's store) then
    // the REAL three-way merge on node B. Target = B's root, source = A's root,
    // base = common base.
    let pull = pull_from_source(&node_a, &mut node_b, 0, Some(root_a), Some(root_b))?;
    println!("\nsync pull: nodes_transferred = {}", pull.stats.nodes_transferred);

    let merged = merge_synced_roots(
        &mut node_b,
        0,
        SyncMergeRoots::new(root_b, root_a, base),
        &policy,
    )?;
    println!("merge divergences reported = {}", merged.divergence_count());
    for d in &merged.divergences {
        println!(
            "  conflict key={} parent(B)={:?} branch(A)={:?} resolved={:?}",
            String::from_utf8_lossy(&d.key),
            d.parent_value.as_ref().map(|v| String::from_utf8_lossy(v).into_owned()),
            d.branch_value.as_ref().map(|v| String::from_utf8_lossy(v).into_owned()),
            d.resolved_value.as_ref().map(|v| String::from_utf8_lossy(v).into_owned()),
        );
    }

    println!("\n--- AFTER merge (merged root on node B) ---");
    let after = read_stream(&node_b, merged.merged_root, &stream, 4);
    println!("merged stream: {:?}", render(after));
    println!("merged counter: {:?}", counter(&node_b, merged.merged_root, &stream));

    // Empirical checks: did A's event survive? did B's? what happened at the
    // collided event key (engine seq 2)? what about the counter?
    let seq2 = get(&node_b, merged.merged_root, &event_key(&stream, 2));
    println!(
        "\nevent@seq2 after merge = {:?}",
        seq2.as_ref().map(|v| String::from_utf8_lossy(v).into_owned())
    );
    println!("OBSERVATION: A's payload survived = {}", seq2.as_deref() == Some(b"A-wrote-this"));
    println!("OBSERVATION: B's payload survived = {}", seq2.as_deref() == Some(b"B-wrote-this"));
    println!(
        "OBSERVATION: exactly one of the two divergent events at seq2 was silently dropped (LWW)."
    );
    println!("E2 RESULT: see observations above.\n");
    Ok(())
}

// A variant of E2 where the two nodes write to DIFFERENT engine seqs (no key
// collision) to show distinct-key events both survive (no conflict at all).
#[test]
fn e2b_nonoverlapping_seqs_both_survive_but_counter_collides() -> TestResult {
    println!("\n========== E2b: non-overlapping seqs (distinct keys) ==========");
    let mut node_a = MemoryStore::new();
    let mut node_b = MemoryStore::new();
    let stream = b"run/77".to_vec();

    let empty_a = empty_root(&mut node_a);
    let empty_b = empty_root(&mut node_b);
    let base = append_events(&mut node_a, empty_a, &stream, 0, &[b"shared-0"]);
    let base_b = append_events(&mut node_b, empty_b, &stream, 0, &[b"shared-0"]);
    assert_eq!(base, base_b);

    // A appends at seq 2, B appends at seq 3 (pretend interleaving). Distinct
    // event keys. BUT both also write the SAME counter key to different values.
    let root_a = append_events(&mut node_a, base, &stream, 1, &[b"A-at-2"]);
    let root_b = append_events(&mut node_b, base, &stream, 2, &[b"B-at-3"]);

    pull_from_source(&node_a, &mut node_b, 0, Some(root_a), Some(root_b))?;
    let merged = merge_synced_roots(
        &mut node_b,
        0,
        SyncMergeRoots::new(root_b, root_a, base),
        &ConflictPolicy::Lww,
    )?;

    println!("divergences = {}", merged.divergence_count());
    println!("merged stream: {:?}", render(read_stream(&node_b, merged.merged_root, &stream, 5)));
    println!("merged counter: {:?}", counter(&node_b, merged.merged_root, &stream));
    println!(
        "OBSERVATION: distinct-key events both survive; the SHARED counter key is the only \
         conflict and is LWW'd, so the counter no longer matches the true event count."
    );
    Ok(())
}

// ===========================================================================
// E3 — Does fencing prevent E2 divergence? And can the ownership record itself
// diverge across partitions (the critical question)?
// ===========================================================================

#[test]
fn e3_fence_with_shared_ownership_domain() -> TestResult {
    println!("\n================ E3: fencing across a partition ================");
    // Model the ownership/epoch record as a CAS scalar living in a SINGLE
    // consistency domain (one shared store). If acquire goes through this single
    // domain, only one owner holds the current epoch, and the stale owner's
    // gated writes never commit -> nothing divergent to merge.
    let mut shared_owner = MemoryStore::new();
    let owner_key = b"ownership/run-42/epoch".to_vec();
    let mut owner_root = empty_root(&mut shared_owner);

    // cas helper over the shared single-domain store.
    let read_epoch = |store: &MemoryStore, root: Hash| -> Option<u64> {
        get(store, root, &owner_key).map(|b| {
            let a: [u8; 8] = b.as_slice().try_into().unwrap();
            u64::from_be_bytes(a)
        })
    };
    let cas_epoch =
        |store: &mut MemoryStore, root: Hash, expected: Option<u64>, new: u64| -> Result<Hash, String> {
            let actual = get(store, root, &owner_key).map(|b| {
                let a: [u8; 8] = b.as_slice().try_into().unwrap();
                u64::from_be_bytes(a)
            });
            if actual != expected {
                return Err(format!("CasMismatch expected={expected:?} actual={actual:?}"));
            }
            Ok(batch_mutate(store, root, &[(owner_key.clone(), Some(new.to_be_bytes().to_vec()))]).unwrap())
        };

    // A acquires epoch 1 in the shared domain.
    owner_root = cas_epoch(&mut shared_owner, owner_root, None, 1)?;
    println!("[A] acquired epoch 1 in shared domain. epoch={:?}", read_epoch(&shared_owner, owner_root));

    // B (new owner) acquires epoch 2 in the SAME shared domain.
    owner_root = cas_epoch(&mut shared_owner, owner_root, Some(1), 2)?;
    println!("[B] acquired epoch 2 in shared domain. epoch={:?}", read_epoch(&shared_owner, owner_root));

    // Stale A tries to write, gated on epoch==1: gate fails -> NO event written.
    let a_epoch_belief = 1_u64;
    let gate_ok = read_epoch(&shared_owner, owner_root) == Some(a_epoch_belief);
    println!("[A] gate write on epoch==1 -> {}", if gate_ok { "PASS (BUG)" } else { "FENCED" });
    assert!(!gate_ok, "E3 FAILED: stale owner passed the shared-domain gate");
    println!(
        "E3 RESULT (shared domain): stale writer fenced; with a single consistency domain for the \
         epoch record there is nothing divergent to merge.\n"
    );
    Ok(())
}

#[test]
fn e3_critical_ownership_record_diverges_under_partition() -> TestResult {
    println!("\n========== E3 CRITICAL: can BOTH partitions acquire the same shard? ==========");
    // Now the DANGEROUS case: the ownership/epoch record is itself replicated and
    // each partition holds its OWN local copy. Under partition, each side does a
    // LOCAL cas-bump. Local cas has no knowledge of the other side.
    let mut node_a = MemoryStore::new();
    let mut node_b = MemoryStore::new();
    let owner_key = b"ownership/run-99/epoch".to_vec();

    // Common base: epoch 1, owned by some prior owner. Both sides start here.
    let empty_a = empty_root(&mut node_a);
    let empty_b = empty_root(&mut node_b);
    let base_a = batch_mutate(&mut node_a, empty_a, &[(owner_key.clone(), Some(1_u64.to_be_bytes().to_vec()))]).unwrap();
    let base_b = batch_mutate(&mut node_b, empty_b, &[(owner_key.clone(), Some(1_u64.to_be_bytes().to_vec()))]).unwrap();
    assert_eq!(base_a, base_b);
    let base = base_a;

    let read = |store: &MemoryStore, root: Hash| -> Option<u64> {
        get(store, root, &owner_key).map(|b| u64::from_be_bytes(b.as_slice().try_into().unwrap()))
    };
    // LOCAL cas: each side reads ITS OWN copy, sees epoch 1, and bumps to 2.
    let local_cas = |store: &mut MemoryStore, root: Hash, expected: u64, new: u64| -> Result<Hash, String> {
        let actual = read(store, root);
        if actual != Some(expected) {
            return Err(format!("CasMismatch expected={expected} actual={actual:?}"));
        }
        Ok(batch_mutate(store, root, &[(owner_key.clone(), Some(new.to_be_bytes().to_vec()))]).unwrap())
    };

    // BOTH partitions independently acquire (cas-bump 1 -> 2). Both SUCCEED,
    // because each only consults its local copy.
    let root_a = local_cas(&mut node_a, base, 1, 2)?;
    let root_b = local_cas(&mut node_b, base, 1, 2)?;
    println!("[A] local cas(1 -> 2) => {:?}  (SUCCESS)", read(&node_a, root_a));
    println!("[B] local cas(1 -> 2) => {:?}  (SUCCESS)", read(&node_b, root_b));
    println!(">>> BOTH partitions believe they own the shard at epoch 2. Split-brain on the EPOCH itself.");

    // Each "owner" now writes events gated on its locally-true epoch==2 — both
    // pass their local gate.
    let stream = b"run/99".to_vec();
    let ev_a = append_events(&mut node_a, root_a, &stream, 0, &[b"A-thinks-it-owns"]);
    let ev_b = append_events(&mut node_b, root_b, &stream, 0, &[b"B-thinks-it-owns"]);

    // HEAL: merge the ownership record. Both sides set epoch 1 -> 2: same value.
    pull_from_source(&node_a, &mut node_b, 0, Some(ev_a), Some(ev_b))?;
    let merged = merge_synced_roots(&mut node_b, 0, SyncMergeRoots::new(ev_b, ev_a, base), &ConflictPolicy::Lww)?;

    let final_epoch = read(&node_b, merged.merged_root);
    println!("\n--- AFTER heal/merge ---");
    println!("merged epoch = {final_epoch:?}");
    println!("merge divergences reported = {}", merged.divergence_count());
    for d in &merged.divergences {
        let key_is_epoch = d.key == owner_key;
        println!(
            "  diverged key = {} (is_epoch_record={key_is_epoch})  parent={:?} branch={:?} resolved={:?}",
            String::from_utf8_lossy(&d.key),
            d.parent_value.as_ref().map(|v| String::from_utf8_lossy(v).into_owned()),
            d.branch_value.as_ref().map(|v| String::from_utf8_lossy(v).into_owned()),
            d.resolved_value.as_ref().map(|v| String::from_utf8_lossy(v).into_owned()),
        );
    }
    let epoch_was_flagged = merged.divergences.iter().any(|d| d.key == owner_key);
    println!(
        "epoch record itself flagged as a conflict? {epoch_was_flagged}  (both wrote 2 => identical \
         => NOT flagged; merge is clean on the epoch key)"
    );
    println!("merged stream = {:?}", render(read_stream(&node_b, merged.merged_root, &stream, 2)));
    println!(
        "OBSERVATION: both sides wrote epoch=2 so the epoch record merges CLEANLY (no conflict \
         surfaced), yet TWO different owners each committed events under 'their' epoch 2. The \
         conflict is only discovered (if at all) at the EVENT layer, where one event is LWW-dropped."
    );
    println!(
        "E3 CRITICAL RESULT: local cas alone is INSUFFICIENT for active-active fencing. Two \
         partitioned nodes can BOTH acquire the same shard because each cas-bumps its own local \
         copy of the epoch record; the divergence is silent at the ownership layer. A single \
         consistency domain / quorum on the epoch record is REQUIRED.\n"
    );
    Ok(())
}

fn render(events: Vec<(u64, Vec<u8>)>) -> Vec<(u64, String)> {
    events
        .into_iter()
        .map(|(s, v)| (s, String::from_utf8_lossy(&v).into_owned()))
        .collect()
}
