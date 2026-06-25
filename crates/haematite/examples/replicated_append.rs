//! `replicated_append` — a focused look at `Database::replicate_append`: one
//! all-or-nothing multi-event batch, replicated to a quorum, lands on every node.
//!
//! Run with:
//!
//! ```text
//! cargo run -p haematite --example replicated_append
//! ```
//!
//! `replicate_append` proposes a whole stream-append (N event puts + the
//! sequence-counter put) as ONE all-or-nothing batch to a membership quorum, then
//! durably applies the IDENTICAL batch locally. This example stands up a 3-node
//! in-process cluster {A, B, C}, has the owner replicate a 4-event batch to the
//! quorum {A, B, C}, and shows ALL THREE nodes then hold the full batch in order
//! with the correct next-seq — and that the same call is idempotent under the OCC
//! sequence guard (a re-send at the stale seq is rejected, not double-applied).

#![allow(
    clippy::print_stdout,
    clippy::doc_markdown,
    clippy::doc_lazy_continuation,
    clippy::uninlined_format_args
)]

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use haematite::db::respond_to_inbound_writes;
use haematite::sync::membership::WriteMembership;
use haematite::sync::{DistributionEndpoint, SyncNodeId};
use haematite::{Database, DatabaseConfig, DatabaseError};

type Result3<T> = Result<T, Box<dyn Error>>;

const NODE_A: &str = "node-a@127.0.0.1";
const NODE_B: &str = "node-b@127.0.0.1";
const NODE_C: &str = "node-c@127.0.0.1";

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const OP_TIMEOUT: Duration = Duration::from_secs(5);
const SHARD: usize = 0;
const TS_WIDTH: usize = 8;

fn main() -> Result3<()> {
    println!("== replicated_append: one atomic batch -> a quorum -> every node holds it ==\n");

    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;
    let node_a = Node::spawn(NODE_A, dir_a.path())?;
    let node_b = Node::spawn(NODE_B, dir_b.path())?;
    let node_c = Node::spawn(NODE_C, dir_c.path())?;
    link_both(&node_a, &node_b)?;
    link_both(&node_a, &node_c)?;
    link_both(&node_b, &node_c)?;
    println!("3-node cluster {{A, B, C}} up (quorum = 2 of 3)\n");

    // Owner A takes the shard.
    node_a
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B, NODE_C]), OP_TIMEOUT)?;
    println!("A is the live owner of shard {SHARD}\n");

    let stream = b"metrics:ingest";
    let batch: Vec<Vec<u8>> = vec![
        b"sample-1".to_vec(),
        b"sample-2".to_vec(),
        b"sample-3".to_vec(),
        b"sample-4".to_vec(),
    ];

    // -- Replicate the whole batch to the full quorum {A, B, C} -----------------
    println!("-- A replicate_appends a 4-event batch to quorum {{A, B, C}} --");
    for (i, e) in batch.iter().enumerate() {
        println!("    event {i}: {}", String::from_utf8_lossy(e));
    }
    let next = node_a.db.replicate_append(
        stream,
        &batch,
        0,
        &membership(3, &[NODE_B, NODE_C]),
        OP_TIMEOUT,
    )?;
    println!("  committed atomically at quorum; new next-seq = {next}\n");

    // -- Every node holds the full batch in order -------------------------------
    println!("-- every node holds the full batch in order with next-seq 4 --");
    for node in [&node_a, &node_b, &node_c] {
        let payloads = read_payloads(node, stream)?;
        let seq = node.db.read_stream_next_seq(stream)?;
        println!("  {}: events = {:?}, next-seq = {seq:?}", node.name, as_strs(&payloads));
        assert_eq!(payloads, batch, "{} must hold the full batch", node.name);
        assert_eq!(seq, Some(4), "{} must hold next-seq 4", node.name);
    }
    println!();

    // -- The OCC guard: a re-send at the stale seq is rejected, not doubled ------
    println!("-- a re-send at the now-stale expected_seq 0 is rejected (no double-apply) --");
    match node_a.db.replicate_append(
        stream,
        &[b"should-not-land".to_vec()],
        0,
        &membership(3, &[NODE_B, NODE_C]),
        OP_TIMEOUT,
    ) {
        Err(DatabaseError::SequenceConflict { expected, actual }) => {
            println!("  rejected: SequenceConflict {{ expected: {expected}, actual: {actual} }}");
            println!("  (the owner-local OCC pre-check proposed NOTHING — no partial replication)");
        }
        other => return Err(format!("expected a SequenceConflict, got {other:?}").into()),
    }
    for node in [&node_a, &node_b, &node_c] {
        let payloads = read_payloads(node, stream)?;
        assert_eq!(payloads, batch, "{} stream unchanged after the conflict", node.name);
    }
    println!("  all nodes still hold exactly the original 4 events\n");

    println!("== done: replicate_append delivered one atomic batch to the whole quorum ==");

    node_a.stop_responder();
    node_b.stop_responder();
    node_c.stop_responder();
    Ok(())
}

// --- Harness (same shape as the e2e tests) ---------------------------------

struct Node {
    db: Arc<Database>,
    addr: SocketAddr,
    name: &'static str,
    responder: Option<JoinHandle<()>>,
    running: Arc<AtomicBool>,
}

impl Node {
    fn spawn(name: &'static str, dir: &Path) -> Result3<Self> {
        let endpoint = DistributionEndpoint::bind(name, loopback()?, 1, None)?;
        let addr = endpoint.local_addr();
        let db = Arc::new(
            Database::create(config_for(dir.join("db").as_path()))?.with_distribution(endpoint),
        );
        let running = Arc::new(AtomicBool::new(true));
        let responder_db = Arc::clone(&db);
        let responder_running = Arc::clone(&running);
        let responder = std::thread::spawn(move || {
            while responder_running.load(Ordering::Relaxed) {
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

    fn stop_responder(&self) {
        self.running.store(false, Ordering::Relaxed);
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

fn loopback() -> Result3<SocketAddr> {
    Ok("127.0.0.1:0".parse()?)
}

fn config_for(path: &Path) -> DatabaseConfig {
    DatabaseConfig {
        data_dir: path.to_path_buf(),
        shard_count: 1,
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

fn link(from: &Node, to: &Node) -> Result3<()> {
    let endpoint = from.db.distribution().ok_or("dialing node has no endpoint")?;
    endpoint.add_peer(to.name, to.addr);
    endpoint.connect(to.name)?;
    if !wait_until(HANDSHAKE_TIMEOUT, || endpoint.is_connected(to.name)) {
        return Err(format!("{} never registered a link to {}", from.name, to.name).into());
    }
    Ok(())
}

fn link_both(a: &Node, b: &Node) -> Result3<()> {
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

fn read_payloads(node: &Node, stream: &[u8]) -> Result3<Vec<Vec<u8>>> {
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

fn as_strs(payloads: &[Vec<u8>]) -> Vec<String> {
    payloads
        .iter()
        .map(|p| String::from_utf8_lossy(p).into_owned())
        .collect()
}
