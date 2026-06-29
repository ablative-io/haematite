//! `replicated_write` — the cost of a quorum-replicated `replicate_write` versus a
//! plain local committed write, on an in-process cluster over the real beamr
//! loopback transport.
//!
//! ## What this measures
//!
//! Two write paths for ONE key=value, head to head:
//!
//! * **`replicate_write` (quorum)** — propose the write to a membership quorum over
//!   the loopback transport, wait for a majority of durable acks, THEN durably apply
//!   the proposer's own copy. This is the active-active committed-write path: it pays
//!   for (a) the network round-trip to the peers, (b) each acking peer's durable
//!   fsync, and (c) the proposer's own fsync. The whole thing is serialised through
//!   the single shard owner.
//!
//! * **`local_commit` (baseline)** — the SAME logical write done with a plain
//!   buffered `put` + `commit` on a standalone single-node store (one local fsync, no
//!   network, no peers). This is the floor the replicated path is compared against.
//!
//! The ratio (replicate_write / local_commit) is the price of replication +
//! single-owner coordination on THIS machine's loopback. Expect it to be several×
//! the local commit: a quorum write fsyncs on multiple nodes and crosses the
//! transport, where the local write fsyncs once with no network.
//!
//! ## Honesty notes
//!
//! * This is a LOOPBACK cluster: all three "nodes" are in one process talking over
//!   127.0.0.1. Real network latency would make the replicated path relatively MORE
//!   expensive; loopback understates the gap. Treat the ratio as a lower bound on the
//!   replication tax, not an upper bound.
//! * The mesh is built ONCE and reused across iterations (standing up 3 endpoints +
//!   an election per iteration would swamp the measured op). Only the `replicate_write`
//!   / `commit` call itself is timed.
//! * Each iteration writes a UNIQUE key (create-if-absent), so no iteration degrades
//!   into a CAS mismatch and the owner's stream of writes is monotone.
//! * Absolute numbers are MACHINE-DEPENDENT (fsync + loopback scheduling).

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

fn bench_replicated_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("replicated_write");

    // -- baseline: a plain local committed write (put + commit), no cluster ----
    group.bench_function("local_commit", |b| {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::create(DatabaseConfig {
            data_dir: dir.path().join("db"),
            shard_count: 1,
            sweep_interval: None,
            distributed: None,
        })
        .expect("create db");
        let mut counter = 0_u64;
        b.iter(|| {
            let key = format!("k:{counter:016}").into_bytes();
            counter += 1;
            db.put(black_box(key), black_box(b"value".to_vec()))
                .expect("put");
            db.commit().expect("commit");
        });
        drop(dir);
    });

    // -- replicate_write to a quorum {A,B,C} over loopback ---------------------
    group.bench_function("replicate_write_quorum", |b| {
        let cluster = Cluster::spawn().expect("spawn cluster");
        cluster
            .node_a
            .db
            .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B, NODE_C]), OP_TIMEOUT)
            .expect("acquire");
        let mut counter = 0_u64;
        b.iter(|| {
            let key = format!("rw:{counter:016}").into_bytes();
            counter += 1;
            cluster
                .node_a
                .db
                .replicate_write(
                    black_box(key),
                    None,
                    black_box(b"value".to_vec()),
                    None,
                    &membership(3, &[NODE_B, NODE_C]),
                    OP_TIMEOUT,
                )
                .expect("replicate_write");
        });
        // Keep the cluster alive until the bench finishes.
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
                drop(respond_to_inbound_writes(
                    &responder_db,
                    Duration::from_millis(20),
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
        Ok(Self {
            node_a,
            node_b,
            node_c,
            _dirs: [dir_a, dir_b, dir_c],
        })
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
    let endpoint = from
        .db
        .distribution()
        .ok_or("dialing node has no endpoint")?;
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
    targets = bench_replicated_write
}
criterion_main!(benches);
