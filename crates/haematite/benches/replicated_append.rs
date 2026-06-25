//! `replicated_append` — the cost of a quorum-replicated atomic batch append
//! (`replicate_append`) versus a plain local `append_batch` of the same N events.
//!
//! ## What this measures
//!
//! An N-event stream append done two ways, head to head:
//!
//! * **`replicate_append` (quorum)** — propose the WHOLE batch (N event puts + the
//!   sequence-counter put) as ONE all-or-nothing `BatchWriteProposal` to a membership
//!   quorum over the loopback transport, wait for a majority of durable acks, THEN
//!   durably apply the proposer's own identical batch. One stamp, one fsync per node,
//!   for the whole batch.
//!
//! * **`append_batch` (local baseline)** — the SAME N events appended atomically on a
//!   standalone single-node store (`Database::append`), one local fsync, no network.
//!
//! The comparison isolates the quorum + cross-node-fsync tax of the replicated atomic
//! append from the raw cost of writing N events locally. Because BOTH paths commit the
//! whole batch under ONE fsync-per-node, the per-event amortization is similar; the
//! delta is the replication coordination (transport round-trip + peer fsyncs).
//!
//! ## Honesty notes
//!
//! * Loopback cluster (all nodes in-process over 127.0.0.1): real network latency
//!   would widen the replicated-vs-local gap. The ratio here is a lower bound on the
//!   replication tax.
//! * The mesh + election happen ONCE; only the append call is timed.
//! * Each iteration appends to a FRESH stream (expected_seq 0) so every measured op is
//!   a clean N-event batch and never a sequence conflict or an ever-growing stream.
//! * `BATCH_N` is the events-per-append; the reported time is per WHOLE batch, not per
//!   event. Absolute numbers are MACHINE-DEPENDENT (fsync + loopback scheduling).

#![allow(
    clippy::print_stdout,
    clippy::doc_markdown,
    clippy::missing_panics_doc,
    clippy::doc_lazy_continuation,
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::significant_drop_tightening
)]

use std::hint::black_box;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use haematite::db::respond_to_inbound_writes;
use haematite::sync::membership::WriteMembership;
use haematite::sync::{DistributionEndpoint, SyncNodeId};
use haematite::{Database, DatabaseConfig};

const NODE_A: &str = "bench-a@127.0.0.1";
const NODE_B: &str = "bench-b@127.0.0.1";
const NODE_C: &str = "bench-c@127.0.0.1";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const OP_TIMEOUT: Duration = Duration::from_secs(5);
const SHARD: usize = 0;

/// Events per appended batch (both the replicated and local paths use the same N).
const BATCH_N: usize = 8;

fn payloads() -> Vec<Vec<u8>> {
    (0..BATCH_N).map(|i| format!("event-{i:04}").into_bytes()).collect()
}

fn bench_replicated_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("replicated_append");

    // -- baseline: a local atomic append_batch of N events, no cluster ---------
    group.bench_function("local_append_batch", |b| {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::create(DatabaseConfig {
            data_dir: dir.path().join("db"),
            shard_count: 1,
            sweep_interval: None,
            distributed: None,
        })
        .expect("create db");
        let batch = payloads();
        let mut counter = 0_u64;
        b.iter(|| {
            // Fresh stream each iteration → expected_seq 0, a clean N-event batch.
            let stream = format!("s:{counter:016}").into_bytes();
            counter += 1;
            let next = db
                .append(black_box(stream), black_box(batch.clone()), 0)
                .expect("append_batch");
            black_box(next);
        });
        drop(dir);
    });

    // -- replicate_append of N events to a quorum {A,B,C} over loopback --------
    group.bench_function("replicate_append_quorum", |b| {
        let cluster = Cluster::spawn().expect("spawn cluster");
        cluster
            .node_a
            .db
            .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B, NODE_C]), OP_TIMEOUT)
            .expect("acquire");
        let batch = payloads();
        let mut counter = 0_u64;
        b.iter(|| {
            let stream = format!("ra:{counter:016}").into_bytes();
            counter += 1;
            let next = cluster
                .node_a
                .db
                .replicate_append(
                    black_box(stream),
                    black_box(batch.clone()),
                    0,
                    &membership(3, &[NODE_B, NODE_C]),
                    OP_TIMEOUT,
                )
                .expect("replicate_append");
            black_box(next);
        });
        cluster.shutdown();
    });

    group.finish();
}

// --- Minimal 3-node loopback cluster harness (as in the e2e tests) ----------

struct Node {
    db: Arc<Database>,
    addr: SocketAddr,
    name: &'static str,
    responder: Option<JoinHandle<()>>,
    running: Arc<AtomicBool>,
}

impl Node {
    fn spawn(name: &'static str, dir: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let endpoint = DistributionEndpoint::bind(name, "127.0.0.1:0".parse()?, 1, None)?;
        let addr = endpoint.local_addr();
        let db = Arc::new(
            Database::create(DatabaseConfig {
                data_dir: dir.join("db"),
                shard_count: 1,
                sweep_interval: None,
                distributed: None,
            })?
            .with_distribution(endpoint),
        );
        let running = Arc::new(AtomicBool::new(true));
        let responder_db = Arc::clone(&db);
        let responder_running = Arc::clone(&running);
        let responder = std::thread::spawn(move || {
            while responder_running.load(Ordering::Relaxed) {
                drop(respond_to_inbound_writes(&responder_db, Duration::from_millis(20)));
            }
        });
        Ok(Self { db, addr, name, responder: Some(responder), running })
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(handle) = self.responder.take() {
            drop(handle.join());
        }
    }
}

struct Cluster {
    node_a: Node,
    // node_b/node_c are held only to keep their responders + endpoints alive for the
    // duration of the bench; they are answered over the wire, not read directly.
    #[allow(dead_code)]
    node_b: Node,
    #[allow(dead_code)]
    node_c: Node,
    _dirs: [tempfile::TempDir; 3],
}

impl Cluster {
    fn spawn() -> Result<Self, Box<dyn std::error::Error>> {
        let dir_a = tempfile::tempdir()?;
        let dir_b = tempfile::tempdir()?;
        let dir_c = tempfile::tempdir()?;
        let node_a = Node::spawn(NODE_A, dir_a.path())?;
        let node_b = Node::spawn(NODE_B, dir_b.path())?;
        let node_c = Node::spawn(NODE_C, dir_c.path())?;
        link_both(&node_a, &node_b)?;
        link_both(&node_a, &node_c)?;
        link_both(&node_b, &node_c)?;
        Ok(Self { node_a, node_b, node_c, _dirs: [dir_a, dir_b, dir_c] })
    }

    fn shutdown(self) {
        drop(self);
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

fn link(from: &Node, to: &Node) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = from.db.distribution().ok_or("dialing node has no endpoint")?;
    endpoint.add_peer(to.name, to.addr);
    endpoint.connect(to.name)?;
    if !wait_until(HANDSHAKE_TIMEOUT, || endpoint.is_connected(to.name)) {
        return Err(format!("{} never linked to {}", from.name, to.name).into());
    }
    Ok(())
}

fn link_both(a: &Node, b: &Node) -> Result<(), Box<dyn std::error::Error>> {
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

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(20);
    targets = bench_replicated_append
}
criterion_main!(benches);
