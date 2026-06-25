//! `event_store` — a narrated tour of haematite's typed `EventStore` facade.
//!
//! Run with:
//!
//! ```text
//! cargo run -p haematite --example event_store
//! ```
//!
//! The `EventStore` is an append-only, optimistically-concurrent log over a real
//! [`Database`]. This example demonstrates:
//!
//! * `append`          — append one event under an `expected_seq` (OCC) guard.
//! * `read`            — read a stream's full event history in sequence order.
//! * `read_from`       — read only the tail of a stream (events `>= from_seq`).
//! * `append_batch`    — append many events atomically as one commit.
//! * the sequence-conflict guard — a deliberate stale-`expected_seq` append that is
//!                       rejected with [`ApiError::SequenceConflict`], leaving the
//!                       stream untouched.
//!
//! The store is the substrate Aion uses for durable per-workflow event streams.

#![allow(
    clippy::print_stdout,
    clippy::doc_markdown,
    clippy::doc_overindented_list_items,
    clippy::uninlined_format_args
)]

use std::error::Error;

use haematite::{ApiError, Database, DatabaseConfig, Event, EventStore};

fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let db = Database::create(DatabaseConfig {
        data_dir: dir.path().join("events"),
        shard_count: 4,
        sweep_interval: None,
        distributed: None,
    })?;
    let store = EventStore::new(db);

    println!("== event_store: an append-only typed log ==");
    println!("created an EventStore at {}\n", dir.path().display());

    let stream = b"workflow:checkout-42";

    // -- Act 1: append events one at a time, advancing the sequence -------------
    println!("-- Act 1: append events under optimistic-concurrency (expected_seq) --");
    // A fresh stream's next-seq is 0. Each append returns the NEW next-seq.
    let next = store.append(stream, b"OrderPlaced", 0)?;
    println!("  append seq 0  OrderPlaced       -> next_seq = {next}");
    let next = store.append(stream, b"PaymentAuthorized", next)?;
    println!("  append seq 1  PaymentAuthorized -> next_seq = {next}");
    let next = store.append(stream, b"InventoryReserved", next)?;
    println!("  append seq 2  InventoryReserved -> next_seq = {next}");
    println!();

    // -- Act 2: read the full stream in order -----------------------------------
    println!("-- Act 2: read the full stream in sequence order --");
    let events = store.read(stream)?;
    print_events("  ", &events);
    assert_eq!(events.len(), 3);
    println!();

    // -- Act 3: read_from the tail ----------------------------------------------
    println!("-- Act 3: read only the tail (read_from skips earlier events) --");
    let tail = store.read_from(stream, 1)?;
    println!("  read_from(seq >= 1):");
    print_events("    ", &tail);
    assert_eq!(tail.len(), 2);
    assert_eq!(tail[0].seq, 1);
    println!();

    // -- Act 4: append_batch atomically -----------------------------------------
    println!("-- Act 4: append a BATCH atomically (one commit, all-or-nothing) --");
    let batch: [&[u8]; 2] = [b"OrderShipped", b"OrderDelivered"];
    let next = store.append_batch(stream, &batch, next)?;
    println!("  append_batch [OrderShipped, OrderDelivered] -> next_seq = {next}");
    let events = store.read(stream)?;
    println!("  the stream now holds {} events:", events.len());
    print_events("    ", &events);
    assert_eq!(events.len(), 5);
    println!();

    // -- Act 5: the sequence-conflict guard (the deliberate one) ----------------
    println!("-- Act 5: a deliberate sequence conflict --");
    println!("  a stale writer still thinks the stream is at seq 0 and appends there...");
    match store.append(stream, b"DuplicateOrderPlaced", 0) {
        Ok(_) => println!("  UNEXPECTED: the stale append succeeded"),
        Err(ApiError::SequenceConflict(conflict)) => {
            println!(
                "  append seq 0  REJECTED: SequenceConflict {{ expected: {}, actual: {} }}",
                conflict.expected, conflict.actual
            );
            println!("  (the stream is really at seq {}, so nothing was appended)", conflict.actual);
        }
        Err(other) => println!("  UNEXPECTED error: {other}"),
    }
    let after = store.read(stream)?;
    println!("  the stream is unchanged — still {} events (the guard protected it)", after.len());
    assert_eq!(after.len(), 5);
    println!();

    println!("== done: append, read, read_from, append_batch, sequence-conflict guard ==");
    Ok(())
}

/// Print events as `seq: payload` lines.
fn print_events(indent: &str, events: &[Event]) {
    for event in events {
        println!(
            "{indent}seq {}: {}",
            event.seq,
            String::from_utf8_lossy(&event.payload)
        );
    }
}
