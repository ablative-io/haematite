//! AA-A1c end-to-end: REPLICATED multi-key atomic STREAM APPEND over a real
//! 3-node cluster {A,B,C}, quorum 2, REAL beamr loopback transport.
//!
//! `Database::replicate_append` proposes a whole stream-append's entries (N event
//! puts + the sequence-counter put) as ONE all-or-nothing `BatchWriteProposal` to a
//! membership quorum, then durably applies the IDENTICAL batch + stamp locally. The
//! point of A1c (vs the actor-level A1a / wire-level A1b) is to prove the WHOLE
//! batch replicates and survives a real failover — not just a local append.
//!
//! GATE 1 — REPLICATED BATCH: owner A appends `[e1,e2,e3]` to a majority {B,C}.
//! ALL THREE nodes read back `[e1,e2,e3]` in order with next-seq 3 (each node's
//! local store read directly) — the whole batch replicated, not just appended at A.
//!
//! GATE 2 — FAILOVER SERVES THE FULL BATCH (the headline): same setup; A is
//! partitioned away; a surviving node is elected + `become_live`-merges and serves
//! the FULL `[e1,e2,e3]` with next-seq 3 — nothing orphaned or partial. The
//! falsifiability control proves the new owner LACKED the batch before failover, so
//! the data came through the replicate+merge path, not the test setup.
//!
//! GATE 3 — DEPOSED OWNER FENCED: after an ownership change, a `replicate_append`
//! from the deposed (stale-epoch) owner fails (the quorum fences it) and writes
//! NOTHING on the majority.
//!
//! GATE 4 — SEQUENCE CONFLICT: a `replicate_append` with a stale `expected_seq`
//! returns `SequenceConflict` and proposes nothing (no partial replication).
//!
//! Tests return `Result` and use `?` (the crate denies `expect`/`unwrap`/`panic`).

// `doc_lazy_continuation`: the GATE-N module-doc list uses lazy continuation
// prose; `tuple_array_conversions`: a `[node_a, node_b, node_c]` iteration array of
// three same-typed node refs trips a false "tuple→array" suggestion.
#![allow(
    clippy::panic,
    clippy::doc_markdown,
    clippy::doc_lazy_continuation,
    clippy::tuple_array_conversions
)]

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use haematite::db::respond_to_inbound_writes;
use haematite::sync::membership::WriteMembership;
use haematite::sync::{DistributionEndpoint, SyncNodeId};
use haematite::{Database, DatabaseConfig, DatabaseError};

type TestResult = Result<(), Box<dyn Error>>;

const NODE_A: &str = "node-a@127.0.0.1";
const NODE_B: &str = "node-b@127.0.0.1";
const NODE_C: &str = "node-c@127.0.0.1";

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const OP_TIMEOUT: Duration = Duration::from_secs(5);

const SHARD: usize = 0;

/// Width of the big-endian timestamp header `encode_event_value` prepends to each
/// stored event value (`timestamp.to_be_bytes() || payload`).
const TS_WIDTH: usize = 8;

fn loopback() -> Result<SocketAddr, Box<dyn Error>> {
    Ok("127.0.0.1:0".parse()?)
}

fn config_for_shards(path: &Path, shard_count: usize) -> DatabaseConfig {
    DatabaseConfig {
        data_dir: path.to_path_buf(),
        shard_count,
        sweep_interval: None,
        distributed: None,
    }
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if predicate() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// One node: a live `Database` with an attached endpoint plus a background
/// responder draining + answering inbound `Prepare`s, `WriteProposal`s,
/// `BatchWriteProposal`s, and `ShardSyncRequest`s.
struct Node {
    db: Arc<Database>,
    addr: SocketAddr,
    name: &'static str,
    responder: Option<JoinHandle<()>>,
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl Node {
    fn spawn_sharded(name: &'static str, dir: &Path, shard_count: usize) -> Result<Self, Box<dyn Error>> {
        let endpoint = DistributionEndpoint::bind(name, loopback()?, 1, None)?;
        let addr = endpoint.local_addr();
        let db = Arc::new(
            Database::create(config_for_shards(dir.join("db").as_path(), shard_count))?
                .with_distribution(endpoint),
        );

        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let responder_db = Arc::clone(&db);
        let responder_running = Arc::clone(&running);
        let responder = std::thread::spawn(move || {
            while responder_running.load(std::sync::atomic::Ordering::Relaxed) {
                drop(respond_to_inbound_writes(
                    &responder_db,
                    Duration::from_millis(50),
                ));
            }
        });

        Ok(Self {
            db,
            addr,
            name,
            responder: Some(responder),
            running,
        })
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);
        if let Some(handle) = self.responder.take() {
            drop(handle.join());
        }
    }
}

fn link(from: &Node, to: &Node) -> TestResult {
    let endpoint = from.db.distribution().ok_or("dialing node has no endpoint")?;
    endpoint.add_peer(to.name, to.addr);
    endpoint.connect(to.name)?;
    if !wait_until(HANDSHAKE_TIMEOUT, || endpoint.is_connected(to.name)) {
        return Err(format!("{} never registered a link to {}", from.name, to.name).into());
    }
    Ok(())
}

fn link_both(a: &Node, b: &Node) -> TestResult {
    link(a, b)?;
    link(b, a)?;
    Ok(())
}

fn membership(total_nodes: usize, send_targets: &[&str]) -> WriteMembership {
    WriteMembership {
        total_nodes,
        send_targets: send_targets.iter().map(|n| SyncNodeId::from(*n)).collect(),
    }
}

/// A full {A,B,C} mesh plus the three `TempDir` guards (kept alive by the caller so
/// the data dirs outlive the nodes).
struct Mesh {
    node_a: Node,
    node_b: Node,
    node_c: Node,
    _dirs: [tempfile::TempDir; 3],
}

fn spawn_mesh() -> Result<Mesh, Box<dyn Error>> {
    spawn_mesh_sharded(1)
}

fn spawn_mesh_sharded(shard_count: usize) -> Result<Mesh, Box<dyn Error>> {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;
    let node_a = Node::spawn_sharded(NODE_A, dir_a.path(), shard_count)?;
    let node_b = Node::spawn_sharded(NODE_B, dir_b.path(), shard_count)?;
    let node_c = Node::spawn_sharded(NODE_C, dir_c.path(), shard_count)?;
    link_both(&node_a, &node_b)?;
    link_both(&node_a, &node_c)?;
    link_both(&node_b, &node_c)?;
    Ok(Mesh {
        node_a,
        node_b,
        node_c,
        _dirs: [dir_a, dir_b, dir_c],
    })
}

/// Find a stream key that routes (whole-key BLAKE3 % shard_count) to `shard`.
fn stream_for_shard(db: &Database, shard: usize) -> Vec<u8> {
    for i in 0..1_000_000_u64 {
        let key = format!("wf-{i:06}").into_bytes();
        if db.shard_for(&key) == shard {
            return key;
        }
    }
    panic!("no stream key found routing to shard {shard}");
}

/// Read a node's local event stream, stripping the per-event timestamp header to
/// recover the raw payloads in sequence order — exactly the logical values
/// `EventStore::read` would decode (the engine strips the stamp/TTL envelope on the
/// read path; we strip the timestamp prefix `encode_event_value` adds).
fn read_payloads(node: &Node, stream: &[u8]) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
    let raw = node.db.read_events(stream)?;
    let mut out = Vec::with_capacity(raw.len());
    for value in raw {
        if value.len() < TS_WIDTH {
            return Err(format!("event value shorter than timestamp header: {value:?}").into());
        }
        out.push(value[TS_WIDTH..].to_vec());
    }
    Ok(out)
}

fn next_seq(node: &Node, stream: &[u8]) -> Result<Option<u64>, Box<dyn Error>> {
    Ok(node.db.read_stream_next_seq(stream)?)
}

// ===========================================================================
// GATE 1 — REPLICATED BATCH lands on ALL THREE nodes.
// ===========================================================================

/// Owner A appends `[e1,e2,e3]` to a quorum {B,C}; assert all three nodes read the
/// full batch in order with next-seq 3. Proves the WHOLE batch replicated (not just
/// locally appended): B and C each ran the receiver `apply_durable_batch` and hold
/// every event + the counter.
#[test]
fn replicated_batch_lands_on_all_nodes() -> TestResult {
    let mesh = spawn_mesh()?;
    let (node_a, node_b, node_c) = (&mesh.node_a, &mesh.node_b, &mesh.node_c);
    let stream = b"stream-1".to_vec();
    let e1 = b"event-one".to_vec();
    let e2 = b"event-two".to_vec();
    let e3 = b"event-three".to_vec();

    node_a
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B, NODE_C]), OP_TIMEOUT)?;

    let returned = node_a.db.replicate_append(
        &stream,
        &[e1.clone(), e2.clone(), e3.clone()],
        0,
        &membership(3, &[NODE_B, NODE_C]),
        OP_TIMEOUT,
    )?;
    assert_eq!(returned, 3, "replicate_append returns the new next-seq");

    let expected = vec![e1, e2, e3];
    for node in [node_a, node_b, node_c] {
        assert_eq!(
            read_payloads(node, &stream)?,
            expected,
            "node {} must hold the full replicated batch in order",
            node.name
        );
        assert_eq!(
            next_seq(node, &stream)?,
            Some(3),
            "node {} must hold next-seq 3 after the batch",
            node.name
        );
    }
    Ok(())
}

/// REGRESSION: `scan_sequence_keys` must enumerate a stream whose sequence counter
/// was written by `replicate_append` — i.e. a STAMPED value. A locally-appended
/// counter is stored as a bare eight-byte value, but the replicated apply path
/// stores the counter inside a stamp envelope (`HMSTMP01 || .. || seq`). The
/// full-keyspace scan must strip that envelope exactly as `get` /
/// `read_stream_next_seq` do; decoding the raw stamped bytes as a bare sequence
/// failed with "invalid sequence metadata" and broke any `EventStore::scan` /
/// workflow enumeration over a replicated stream.
#[test]
fn scan_sequence_keys_enumerates_stamped_counter() -> TestResult {
    let mesh = spawn_mesh()?;
    let node_a = &mesh.node_a;
    let stream = b"stream-scan".to_vec();

    node_a
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B, NODE_C]), OP_TIMEOUT)?;
    node_a.db.replicate_append(
        &stream,
        &[b"e1".to_vec(), b"e2".to_vec()],
        0,
        &membership(3, &[NODE_B, NODE_C]),
        OP_TIMEOUT,
    )?;

    // The headline: this walks the whole keyspace and decodes the STAMPED counter.
    // Before the fix it returned Err("invalid sequence metadata").
    let streams = node_a.db.scan_sequence_keys()?;
    let found = streams.iter().find(|(key, _seq)| key == &stream);
    assert_eq!(
        found.map(|(_, seq)| *seq),
        Some(2),
        "scan_sequence_keys must surface the replicated stream with the stamped next-seq 2"
    );
    Ok(())
}

/// SPIKE / REGRESSION: distribution × multi-shard. Three nodes, shard_count=3, each
/// node OWNS a DIFFERENT shard (A=0, B=1, C=2) and is a follower for the other two.
/// A `replicate_append` routed to shard S is driven by S's owner and replicated to
/// the full quorum, so every node can read every shard's stream. Proves per-shard
/// election + `replicate_append` routing COMPOSE under shard_count>1 — the foundation
/// the active-active multi-shard demo (different workflows owned by different nodes)
/// stands on.
#[test]
fn multi_shard_each_node_owns_a_distinct_shard_and_replicates() -> TestResult {
    let mesh = spawn_mesh_sharded(3)?;
    let (node_a, node_b, node_c) = (&mesh.node_a, &mesh.node_b, &mesh.node_c);

    // Each node acquires ONE distinct shard; the other two promise/follow it.
    node_a
        .db
        .acquire_shard_and_serve(0, &membership(3, &[NODE_B, NODE_C]), OP_TIMEOUT)?;
    node_b
        .db
        .acquire_shard_and_serve(1, &membership(3, &[NODE_A, NODE_C]), OP_TIMEOUT)?;
    node_c
        .db
        .acquire_shard_and_serve(2, &membership(3, &[NODE_A, NODE_B]), OP_TIMEOUT)?;

    // One stream per shard, each driven by THAT shard's owner.
    let plan: [(&Node, usize, &[&str]); 3] = [
        (node_a, 0, &[NODE_B, NODE_C]),
        (node_b, 1, &[NODE_A, NODE_C]),
        (node_c, 2, &[NODE_A, NODE_B]),
    ];
    let mut streams = Vec::new();
    for (owner, shard, targets) in plan {
        let stream = stream_for_shard(&owner.db, shard);
        assert_eq!(owner.db.shard_for(&stream), shard);
        let payload = format!("event-on-shard-{shard}").into_bytes();
        let next = owner.db.replicate_append(
            &stream,
            std::slice::from_ref(&payload),
            0,
            &membership(3, targets),
            OP_TIMEOUT,
        )?;
        assert_eq!(next, 1, "shard {shard} owner append returns next-seq 1");
        streams.push((shard, stream, payload));
    }

    // Every node holds every shard's stream — full replication across the quorum,
    // with three different shards owned by three different nodes simultaneously.
    for (shard, stream, payload) in &streams {
        for node in [&node_a, &node_b, &node_c] {
            assert_eq!(
                read_payloads(node, stream)?,
                vec![payload.clone()],
                "node {} must hold shard {shard}'s replicated event",
                node.name
            );
            assert_eq!(
                next_seq(node, stream)?,
                Some(1),
                "node {} must hold next-seq 1 for shard {shard}",
                node.name
            );
        }
    }
    Ok(())
}

// ===========================================================================
// GATE 2 — FAILOVER serves the FULL batch (the headline).
// ===========================================================================

/// Owner A replicates `[e1,e2,e3]` to a quorum {B} ONLY (so the batch commits on
/// {A,B} but C LAGS the whole batch), then A is partitioned away. C is elected the
/// new owner over {C,B} and `become_live`-merges: it must RECOVER and serve the FULL
/// `[e1,e2,e3]` with next-seq 3 — nothing orphaned or partial. This is strictly
/// non-vacuous: C did NOT hold any of the batch before failover (asserted), so every
/// event could ONLY have arrived via the merge pull from B. The companion control
/// `bare_acquire_without_merge_lacks_batch` proves the SAME setup WITHOUT the merge
/// recovers nothing.
#[test]
fn failover_serves_full_batch() -> TestResult {
    let mesh = spawn_mesh()?;
    let (node_a, node_c) = (&mesh.node_a, &mesh.node_c);
    let stream = b"stream-failover".to_vec();
    let full = vec![b"f-one".to_vec(), b"f-two".to_vec(), b"f-three".to_vec()];

    node_a
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B, NODE_C]), OP_TIMEOUT)?;
    // Replicate to {B} ONLY: quorum {A,B} reached; C never receives the batch.
    node_a.db.replicate_append(
        &stream,
        &full,
        0,
        &membership(3, &[NODE_B]),
        OP_TIMEOUT,
    )?;

    // C lags the WHOLE batch before failover (load-bearing): whatever it serves
    // after becoming live can ONLY have come from the merge pull, not the setup.
    assert!(
        read_payloads(node_c, &stream)?.is_empty(),
        "C must lag the batch before failover (load-bearing)"
    );

    // FAILOVER: A partitioned (not a send target). C is elected over {C,B} and
    // become_live UNION-merges B's committed tree (which holds the batch) into C's.
    node_c
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B]), OP_TIMEOUT)?;

    assert_eq!(
        read_payloads(node_c, &stream)?,
        full,
        "the new owner must RECOVER and serve the FULL replicated batch after failover \
         — every event arrived via the merge pull from B"
    );
    assert_eq!(
        next_seq(node_c, &stream)?,
        Some(3),
        "the new owner must serve next-seq 3 after failover — nothing partial"
    );
    Ok(())
}

/// Falsifiability control for GATE 2: a node that NEVER received the batch and only
/// runs a BARE `acquire_shard` (no `become_live` merge) does NOT serve the events —
/// proving the data in `failover_serves_full_batch` came through the replicate+merge
/// path, not the harness. Here A replicates to {B} ONLY (C is excluded), C lags the
/// whole batch, then C bare-acquires over {C, B} (B advertises the root but no merge
/// pulls it) and STILL lacks the events.
#[test]
fn bare_acquire_without_merge_lacks_batch() -> TestResult {
    let mesh = spawn_mesh()?;
    let (node_a, node_c) = (&mesh.node_a, &mesh.node_c);
    let stream = b"stream-control".to_vec();
    let e1 = b"c-one".to_vec();
    let e2 = b"c-two".to_vec();

    node_a
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B, NODE_C]), OP_TIMEOUT)?;
    // Replicate to {B} ONLY: quorum {A,B} reached, C never receives the batch.
    node_a.db.replicate_append(
        &stream,
        &[e1, e2],
        0,
        &membership(3, &[NODE_B]),
        OP_TIMEOUT,
    )?;

    // C lags the WHOLE batch (load-bearing: it had nothing before failover).
    assert!(
        read_payloads(node_c, &stream)?.is_empty(),
        "C must lag the batch before failover (load-bearing control)"
    );

    // BARE acquire — election ONLY, no become_live merge. C never pulls/unions B.
    node_c
        .db
        .acquire_shard(SHARD, &membership(3, &[NODE_B]), OP_TIMEOUT)?;

    assert!(
        read_payloads(node_c, &stream)?.is_empty(),
        "WITHOUT become_live's merge the new owner does NOT serve the batch — proves \
         the data comes from the replicate+merge path, not the test setup"
    );
    assert_eq!(
        next_seq(node_c, &stream)?,
        None,
        "without the merge the new owner has no sequence counter for the stream either"
    );
    Ok(())
}

// ===========================================================================
// GATE 3 — DEPOSED OWNER is FENCED (writes nothing on the majority).
// ===========================================================================

/// A owns the shard and replicates a batch; C is then elected over {B}, deposing A
/// (B and C now promise C's strictly-higher ballot). A `replicate_append` from the
/// DEPOSED owner A stamps its STALE live_epoch; the intersection peers fence it,
/// eroding possible-accepts below quorum → a `ConsistencyError` naming the fence,
/// and the stale batch lands on NEITHER other node.
#[test]
fn deposed_owner_append_is_fenced() -> TestResult {
    let mesh = spawn_mesh()?;
    let (node_a, node_b, node_c) = (&mesh.node_a, &mesh.node_b, &mesh.node_c);
    let stream = b"stream-fence".to_vec();

    node_a
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B, NODE_C]), OP_TIMEOUT)?;
    node_a.db.replicate_append(
        &stream,
        &[b"pre".to_vec()],
        0,
        &membership(3, &[NODE_B, NODE_C]),
        OP_TIMEOUT,
    )?;

    // OWNERSHIP CHANGE: C is elected over {C, B}. B and C now promise C's higher
    // ballot, so A is deposed at the {B,C} intersection.
    node_c
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B]), OP_TIMEOUT)?;

    // The DEPOSED owner A appends. Its stamp carries the STALE (lower) epoch; the
    // fence rejects it on the majority. NEVER Ok.
    let deposed = node_a.db.replicate_append(
        &stream,
        &[b"stale-batch-event".to_vec()],
        1,
        &membership(3, &[NODE_B, NODE_C]),
        OP_TIMEOUT,
    );
    match &deposed {
        Err(DatabaseError::ConsistencyError(message)) => assert!(
            message.contains("fenced"),
            "the deposed owner's append must fail as a FENCE, got: {message}"
        ),
        other => panic!("deposed owner's append must be fenced (ConsistencyError), got {other:?}"),
    }

    // The stale batch's event must NOT have landed on the other two nodes: their
    // streams still end at the single pre-failover event (next-seq 1), with no
    // "stale-batch-event".
    for node in [node_b, node_c] {
        let payloads = read_payloads(node, &stream)?;
        assert!(
            !payloads.iter().any(|p| p.as_slice() == b"stale-batch-event"),
            "node {} must NOT hold the deposed owner's fenced batch event",
            node.name
        );
        assert_eq!(
            next_seq(node, &stream)?,
            Some(1),
            "node {} sequence counter must NOT advance from the fenced append",
            node.name
        );
    }
    Ok(())
}

// ===========================================================================
// GATE 4 — SEQUENCE CONFLICT proposes nothing (no partial replication).
// ===========================================================================

/// A stream already holds one batch (next-seq 3). A `replicate_append` with a STALE
/// `expected_seq = 0` must return `SequenceConflict` from the owner-local OCC
/// pre-check WITHOUT proposing anything: no node's stream changes (still next-seq 3,
/// no extra events).
#[test]
fn stale_expected_seq_conflicts_and_proposes_nothing() -> TestResult {
    let mesh = spawn_mesh()?;
    let (node_a, node_b, node_c) = (&mesh.node_a, &mesh.node_b, &mesh.node_c);
    let stream = b"stream-occ".to_vec();
    let original = vec![b"o-one".to_vec(), b"o-two".to_vec(), b"o-three".to_vec()];

    node_a
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B, NODE_C]), OP_TIMEOUT)?;
    node_a.db.replicate_append(
        &stream,
        &original,
        0,
        &membership(3, &[NODE_B, NODE_C]),
        OP_TIMEOUT,
    )?;

    // STALE expected_seq = 0 (the stream is already at 3).
    let conflict = node_a.db.replicate_append(
        &stream,
        &[b"should-not-land".to_vec()],
        0,
        &membership(3, &[NODE_B, NODE_C]),
        OP_TIMEOUT,
    );
    match &conflict {
        Err(DatabaseError::SequenceConflict { expected, actual }) => {
            assert_eq!(*expected, 0, "conflict echoes the stale expected_seq");
            assert_eq!(*actual, 3, "conflict reports the true current next-seq");
        }
        other => panic!("stale expected_seq must be a SequenceConflict, got {other:?}"),
    }

    // NOTHING was proposed: every node's stream is unchanged (still the original
    // three events, next-seq 3, no "should-not-land").
    for node in [node_a, node_b, node_c] {
        assert_eq!(
            read_payloads(node, &stream)?,
            original,
            "node {} stream must be unchanged after the conflict (no partial replication)",
            node.name
        );
        assert_eq!(
            next_seq(node, &stream)?,
            Some(3),
            "node {} next-seq must be unchanged after the conflict",
            node.name
        );
    }
    Ok(())
}
