//! `persistence_recovery` — durability across a process restart.
//!
//! Run with:
//!
//! ```text
//! cargo run -p haematite --example persistence_recovery
//! ```
//!
//! This is the durability story in one short program:
//!
//! 1. Create a store at a FIXED path (not a delete-on-drop temp dir).
//! 2. Write KV values, an event stream, and a CAS counter, then `commit`.
//! 3. DROP the `Database` entirely — the in-memory handles, shard actors, and
//!    caches all go away (this models the process exiting / the host restarting).
//! 4. REOPEN from the SAME path with `Database::open` and read everything back.
//!
//! The reopened store recovers committed state from each shard's durable WAL +
//! committed prolly-tree root, so the data survives the restart byte-for-byte.

#![allow(
    clippy::print_stdout,
    clippy::doc_markdown,
    clippy::doc_overindented_list_items,
    clippy::ref_option,
    clippy::option_if_let_else,
    clippy::uninlined_format_args
)]

use std::error::Error;

use haematite::{Database, DatabaseConfig, EventStore};

fn main() -> Result<(), Box<dyn Error>> {
    // A persistent directory we control the lifetime of. We keep the TempDir guard
    // alive for the WHOLE program so the path is not deleted between "restarts";
    // the point is that we reopen the SAME path, not that it is temporary.
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("durable-db");

    println!("== persistence_recovery: durability across a restart ==");
    println!("data directory: {}\n", data_dir.display());

    // -- Phase 1: write + commit, then DROP the database ------------------------
    println!("-- Phase 1: write, commit, then drop the Database (simulated crash/exit) --");
    {
        let db = Database::create(DatabaseConfig {
            data_dir: data_dir.clone(),
            shard_count: 4,
            sweep_interval: None,
            distributed: None,
        })?;

        db.put(b"config:region".to_vec(), b"ap-southeast-2".to_vec())?;
        db.put(b"config:mode".to_vec(), b"active-active".to_vec())?;
        println!("  put  config:region = ap-southeast-2");
        println!("  put  config:mode   = active-active");

        // A scalar CAS counter.
        db.cas(b"seq:requests".to_vec(), None, 41)?;
        db.cas(b"seq:requests".to_vec(), Some(41), 42)?;
        println!("  cas  seq:requests  = 42");

        // An event stream via the typed facade over the SAME database handle.
        let store = EventStore::new(db);
        store.append(b"audit:log", b"NodeStarted", 0)?;
        store.append(b"audit:log", b"ConfigLoaded", 1)?;
        println!("  append audit:log [NodeStarted, ConfigLoaded]");

        // Flush every shard's buffer to its durable tree, then drop EVERYTHING.
        store.flush()?;
        let db = store.into_database();
        let roots = db.commit()?;
        println!("  commit -> {} shard roots persisted", roots.len());

        println!("  ...dropping the Database (all in-memory state is gone)\n");
        drop(db);
    }

    // -- Phase 2: reopen from the same path and read everything back ------------
    println!("-- Phase 2: reopen from the SAME path and read it all back --");
    let reopened = Database::open(&data_dir)?;
    println!("  Database::open({}) succeeded", data_dir.display());

    let region = reopened.get(b"config:region")?;
    let mode = reopened.get(b"config:mode")?;
    let seq = reopened.read_value(b"seq:requests")?;
    println!("  get  config:region -> {}", show(&region));
    println!("  get  config:mode   -> {}", show(&mode));
    println!("  read seq:requests  -> {seq:?}");

    assert_eq!(region, Some(b"ap-southeast-2".to_vec()));
    assert_eq!(mode, Some(b"active-active".to_vec()));
    assert_eq!(seq, Some(42));

    // The event stream survived too — wrap the reopened db as an EventStore.
    let store = EventStore::new(reopened);
    let events = store.read(b"audit:log")?;
    println!("  read audit:log -> {} events:", events.len());
    for event in &events {
        println!(
            "    seq {}: {}",
            event.seq,
            String::from_utf8_lossy(&event.payload)
        );
    }
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].payload, b"NodeStarted");
    assert_eq!(events[1].payload, b"ConfigLoaded");
    println!();

    println!("== done: every committed value, counter, and event survived the restart ==");
    Ok(())
}

fn show(value: &Option<Vec<u8>>) -> String {
    match value {
        Some(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        None => "<absent>".to_owned(),
    }
}
