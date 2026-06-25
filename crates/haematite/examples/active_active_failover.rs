//! `active_active_failover` — THE HEADLINE: workflow state survives the death of
//! the node that owns it.
//!
//! Run with:
//!
//! ```text
//! cargo run -p haematite --example active_active_failover
//! ```
//!
//! This stands up a real 3-node in-process haematite cluster {A, B, C} over the
//! REAL beamr loopback transport (the same harness the `handoff_merge_e2e` /
//! `replicated_append_e2e` integration tests use) and demonstrates the step-3
//! active-active failover end to end:
//!
//!   Act 1  Owner elected      — node A wins the shard via `acquire_shard_and_serve`
//!                               (Phase-1 election + union-merge handoff).
//!   Act 2  Data committed      — A `replicate_append`s a batch of workflow events
//!          + replicated         and `replicate_write`s a state key to a quorum that
//!                               INCLUDES the survivor B (so B durably holds them).
//!   Act 3  Owner killed        — A is partitioned by MEMBERSHIP EXCLUSION: it is
//!                               simply never named as a send target again, modelling
//!                               an owner that has died / been cut off.
//!   Act 4  New owner elected   — survivor B runs `acquire_shard_and_serve` over the
//!          + merged             remaining quorum, which fences the dead owner with a
//!                               strictly-higher epoch and runs `become_live`'s
//!                               union-merge handoff from the promise majority.
//!   Act 5  Data recovered      — B, the NEW owner, serves the FULL committed event
//!          + served             stream (correct next-seq) and the committed state
//!                               key. The workflow survived its owner's death.
//!
//! To make the recovery non-vacuous, Act 2 replicates the workflow data to a quorum
//! that EXCLUDES the eventual new owner B for the events — wait, no: we deliberately
//! exclude C from one write and include B, so that B holds the data and we can prove
//! the failover serves it. See the inline narration for exactly which node holds what
//! and why each assertion is load-bearing.

// This example narrates a distributed protocol with println! and panics loudly on
// any unexpected protocol outcome (it is a demo, not library code).
#![allow(
    clippy::print_stdout,
    clippy::doc_markdown,
    clippy::doc_lazy_continuation,
    clippy::doc_overindented_list_items,
    clippy::ref_option,
    clippy::option_if_let_else,
    clippy::uninlined_format_args,
    clippy::too_many_lines
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
use haematite::{Database, DatabaseConfig};

type Result3<T> = Result<T, Box<dyn Error>>;

const NODE_A: &str = "node-a@127.0.0.1";
const NODE_B: &str = "node-b@127.0.0.1";
const NODE_C: &str = "node-c@127.0.0.1";

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const OP_TIMEOUT: Duration = Duration::from_secs(5);

/// All workflow state in this demo lives on one shard (shard 0). A real deployment
/// shards by key; the failover protocol is per-shard, so one shard tells the story.
const SHARD: usize = 0;

/// Width of the big-endian timestamp header `replicate_append` prepends to each
/// stored event value (`timestamp.to_be_bytes() || payload`); we strip it to recover
/// the raw payload bytes for display.
const TS_WIDTH: usize = 8;

fn main() -> Result3<()> {
    println!("== active_active_failover: workflow state survives owner death ==\n");

    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let dir_c = tempfile::tempdir()?;

    println!("standing up a 3-node in-process cluster over real beamr loopback...");
    let node_a = Node::spawn(NODE_A, dir_a.path())?;
    let node_b = Node::spawn(NODE_B, dir_b.path())?;
    let node_c = Node::spawn(NODE_C, dir_c.path())?;

    // Full mesh: every node can talk to every other node.
    link_both(&node_a, &node_b)?;
    link_both(&node_a, &node_c)?;
    link_both(&node_b, &node_c)?;
    println!("  nodes A, B, C are up and fully meshed (quorum = 2 of 3)\n");

    let stream = b"workflow:order-1001";
    let state_key = b"workflow:order-1001:status";

    // =======================================================================
    // ACT 1 — OWNER ELECTED
    // =======================================================================
    println!("-- Act 1: OWNER ELECTED --");
    println!("  node A runs acquire_shard_and_serve(shard {SHARD})...");
    node_a
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B, NODE_C]), OP_TIMEOUT)?;
    println!("  A won the Phase-1 election (majority of promises) and ran the");
    println!("  union-merge handoff — A is now the LIVE owner of shard {SHARD}.\n");

    // =======================================================================
    // ACT 2 — DATA COMMITTED + REPLICATED
    // =======================================================================
    println!("-- Act 2: DATA COMMITTED + REPLICATED (to a quorum that includes survivor B) --");

    // Replicate the workflow's event stream to a quorum {A, B}. We send to B ONLY
    // (so the batch commits on {A, B}) and deliberately EXCLUDE C: that makes C lag
    // the data, so when B later takes over we are proving B's OWN committed copy is
    // served, and the merge is exercised for real (C contributes nothing).
    let events: Vec<Vec<u8>> = vec![
        b"OrderPlaced".to_vec(),
        b"PaymentAuthorized".to_vec(),
        b"InventoryReserved".to_vec(),
    ];
    println!("  A replicate_appends the workflow event stream to quorum {{A, B}}:");
    for (i, event) in events.iter().enumerate() {
        println!("      event {i}: {}", String::from_utf8_lossy(event));
    }
    let new_seq = node_a.db.replicate_append(
        stream,
        &events,
        0,
        &membership(3, &[NODE_B]), // send to B; quorum {A, B} reached; C excluded
        OP_TIMEOUT,
    )?;
    println!("  replicate_append committed at quorum; new next-seq = {new_seq}");

    // Replicate a committed STATE key (a single value) to the same quorum {A, B}.
    println!("  A replicate_writes the workflow status key to quorum {{A, B}}:");
    println!("      {} = AWAITING_SHIPMENT", b2s(state_key));
    node_a.db.replicate_write(
        state_key.to_vec(),
        None, // create-if-absent
        b"AWAITING_SHIPMENT".to_vec(),
        None,
        &membership(3, &[NODE_B]),
        OP_TIMEOUT,
    )?;

    // Prove the committed data is on the SURVIVOR B and that C lagged it (so the
    // recovery in Act 5 is genuinely from B's committed copy + the merge handoff).
    let b_events = read_payloads(&node_b, stream)?;
    let b_status = node_b.db.get(state_key)?;
    println!("  survivor B durably holds:");
    println!("      events  = {:?}", as_strs(&b_events));
    println!("      status  = {}", show(&b_status));
    assert_eq!(b_events, events, "B must hold the full committed event batch");
    assert_eq!(
        b_status,
        Some(b"AWAITING_SHIPMENT".to_vec()),
        "B must hold the committed status"
    );

    let c_events = read_payloads(&node_c, stream)?;
    println!("  laggard  C holds events = {:?}  (deliberately excluded — load-bearing)", as_strs(&c_events));
    assert!(
        c_events.is_empty(),
        "C must lag the events so the failover recovery is non-vacuous"
    );
    println!();

    // =======================================================================
    // ACT 3 — OWNER KILLED
    // =======================================================================
    println!("-- Act 3: OWNER KILLED (node A partitioned by membership exclusion) --");
    // We model A's death by simply never naming A as a send/quorum target again, AND
    // stopping A's responder so it can no longer answer the protocol. From the rest
    // of the cluster's point of view, A is gone.
    node_a.stop_responder();
    println!("  node A's responder is stopped and A is excluded from all future");
    println!("  memberships — the owner is, for all protocol purposes, DEAD.");
    println!("  the workflow's only live committed copy now lives on survivors {{B, C}}.\n");

    // =======================================================================
    // ACT 4 — NEW OWNER ELECTED + MERGED
    // =======================================================================
    println!("-- Act 4: NEW OWNER ELECTED + MERGED --");
    println!("  survivor B runs acquire_shard_and_serve(shard {SHARD}) over quorum {{B, C}}...");
    // B elects itself over {B, C}. The election mints a strictly-higher epoch than
    // A ever held, FENCING the dead owner; become_live then UNION-merges the promise
    // majority's committed state (B's own + C's empty) into a lossless baseline.
    node_b
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_C]), OP_TIMEOUT)?;
    println!("  B won a strictly-higher epoch (the dead owner A is now fenced) and");
    println!("  ran become_live's union-merge handoff — B is the NEW LIVE owner.\n");

    // =======================================================================
    // ACT 5 — DATA RECOVERED + SERVED
    // =======================================================================
    println!("-- Act 5: DATA RECOVERED + SERVED (by the new owner B) --");
    let recovered_events = read_payloads(&node_b, stream)?;
    let recovered_next_seq = node_b.db.read_stream_next_seq(stream)?;
    let recovered_status = node_b.db.get(state_key)?;

    println!("  the NEW owner B serves the workflow:");
    println!("      events   = {:?}", as_strs(&recovered_events));
    println!("      next-seq = {recovered_next_seq:?}");
    println!("      status   = {}", show(&recovered_status));

    // THE PROPERTY: the full committed workflow survived its owner's death.
    assert_eq!(
        recovered_events, events,
        "the new owner must serve the FULL committed event stream after failover"
    );
    assert_eq!(
        recovered_next_seq,
        Some(3),
        "the new owner must serve the correct next-seq (nothing partial/orphaned)"
    );
    assert_eq!(
        recovered_status,
        Some(b"AWAITING_SHIPMENT".to_vec()),
        "the new owner must serve the committed status key after failover"
    );

    // The new owner can CONTINUE the workflow: append the next event onto the
    // recovered stream, under the recovered next-seq. This proves B is not just a
    // read replica — it is the LIVE owner and can advance the stream from exactly
    // where the dead owner left off.
    //
    // We use a local owner append here (not a quorum replicate_append). Why: C
    // legitimately MISSED the original batch (it was deliberately excluded in Act 2),
    // so its sequence counter is absent and a replicated append at seq 3 would be
    // correctly REJECTED by C until C catches up — that rejection is the OCC guard
    // working, not a failover failure. The point of this act is liveness of the new
    // owner over the RECOVERED stream, which the local owner append shows cleanly.
    println!("  B, as the live owner, CONTINUES the recovered workflow:");
    println!("      replicate_append OrderShipped onto the recovered stream, to quorum {{B, C}}...");
    // Quorum {B, C}: total_nodes 3 ⇒ quorum 2. C missed the original batch, so it
    // would reject a stream append at seq 3 (correct OCC). To extend the recovered
    // stream we therefore first bring C current via its own become_live merge (it
    // pulls B's committed tree), THEN the quorum append commits on {B, C}.
    node_c
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_B]), OP_TIMEOUT)?;
    // C is now caught up; B re-acquires (higher epoch than C) so B is the live owner
    // again and its live_epoch can stamp the continuation.
    node_b
        .db
        .acquire_shard_and_serve(SHARD, &membership(3, &[NODE_C]), OP_TIMEOUT)?;
    let after = node_b.db.replicate_append(
        stream,
        &[b"OrderShipped".to_vec()],
        3,
        &membership(3, &[NODE_C]),
        OP_TIMEOUT,
    )?;
    println!("      replicate_append OrderShipped at recovered seq 3 -> new next-seq = {after}");
    assert_eq!(after, 4, "the new owner can advance the recovered stream");
    let final_events = read_payloads(&node_b, stream)?;
    println!("      B's stream is now: {:?}", as_strs(&final_events));
    assert_eq!(
        final_events,
        vec![
            b"OrderPlaced".to_vec(),
            b"PaymentAuthorized".to_vec(),
            b"InventoryReserved".to_vec(),
            b"OrderShipped".to_vec(),
        ],
        "the appended event extends the recovered stream"
    );
    println!();

    println!("== done: owner elected -> data committed+replicated -> owner killed ==");
    println!("==       -> new owner elected+merged -> data recovered and served.    ==");
    println!("== the workflow state survived the death of the node that owned it.    ==");

    // Tidy: stop the survivors' responders before their Databases drop.
    node_b.stop_responder();
    node_c.stop_responder();
    Ok(())
}

// ===========================================================================
// Harness — lifted from the crate's handoff_merge_e2e / replicated_append_e2e
// integration tests: a Node is a live Database + endpoint + a background thread
// draining and answering inbound protocol messages.
// ===========================================================================

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
                // Drain + answer inbound Prepare/WriteProposal/BatchWriteProposal/
                // ShardSyncRequest. A plain timeout just loops again.
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

    /// Stop this node's background responder (models the node dying / detaching).
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

/// Read a node's local event stream, stripping the per-event timestamp header to
/// recover the raw payloads in sequence order.
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

fn show(value: &Option<Vec<u8>>) -> String {
    match value {
        Some(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        None => "<absent>".to_owned(),
    }
}

fn b2s(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}
