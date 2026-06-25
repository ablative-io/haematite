//! `kv_basics` — a narrated tour of haematite's single-node key-value API.
//!
//! Run with:
//!
//! ```text
//! cargo run -p haematite --example kv_basics
//! ```
//!
//! This walks the public [`Database`] KV surface end to end against a real
//! on-disk store in a throwaway temp directory:
//!
//! * `put` / `get`            — buffered single-key write, read-your-writes.
//! * `commit`                 — flush every shard's WAL buffer to its prolly tree
//!                              and return the per-shard root hashes.
//! * `range`                  — a shard-local `[from, to)` scan in key order.
//! * `cas` (compare-and-swap) — the scalar `u64` optimistic-concurrency primitive,
//!                              including a deliberate CAS *conflict*.
//! * `delete`                 — a stamped tombstone that reads as absent.
//!
//! Everything prints a human-readable timeline so you can follow what the store is
//! doing act by act. It uses only the crate's real public API.

// Examples narrate with println! and may unwrap-via-`?`; the crate's strict
// lints are for library code, not this illustrative binary.
#![allow(
    clippy::print_stdout,
    clippy::doc_markdown,
    clippy::doc_overindented_list_items,
    clippy::ref_option,
    clippy::option_if_let_else,
    clippy::uninlined_format_args
)]

use std::error::Error;

use haematite::{Database, DatabaseConfig};

/// All KV operations here target one logical shard. We use a 4-shard store to show
/// `commit` returns a root per shard, but keep the demo keys easy to read.
const SHARD_COUNT: usize = 4;

fn main() -> Result<(), Box<dyn Error>> {
    // A throwaway data directory; dropped (and deleted) when `dir` goes out of scope.
    let dir = tempfile::tempdir()?;
    let db = Database::create(DatabaseConfig {
        data_dir: dir.path().join("kv"),
        shard_count: SHARD_COUNT,
        sweep_interval: None,
        distributed: None,
    })?;

    println!("== kv_basics: a single-node key-value tour ==");
    println!("created a {SHARD_COUNT}-shard store at {}\n", dir.path().display());

    // -- Act 1: put + read-your-writes ------------------------------------------
    println!("-- Act 1: put a key, read it back (read-your-writes) --");
    db.put(b"user:alice".to_vec(), b"online".to_vec())?;
    println!("  put  user:alice = online");
    let alice = db.get(b"user:alice")?;
    println!("  get  user:alice -> {}", show(&alice));
    assert_eq!(alice, Some(b"online".to_vec()));
    println!("  (the value is visible immediately, before any commit)\n");

    // -- Act 2: commit -> per-shard roots ---------------------------------------
    println!("-- Act 2: commit flushes the WAL buffer into the prolly trees --");
    let roots = db.commit()?;
    println!("  commit returned a root hash for each of {} shards:", roots.len());
    for (shard, root) in &roots {
        println!("    shard {shard}: {}", short_hash(&format!("{root:?}")));
    }
    println!("  the committed value survives a future reopen (see persistence_recovery)\n");

    // -- Act 3: range scan -------------------------------------------------------
    println!("-- Act 3: a shard-local range scan in key order --");
    // Pick keys that all hash to one shard so the [from, to) scan is meaningful.
    let from = b"room:";
    let to = b"room;"; // ';' is one byte past ':' so [from, to) covers "room:*"
    let keys = shard_local_keys(&db, 3, from);
    for (i, key) in keys.iter().enumerate() {
        let value = format!("occupant-{i}").into_bytes();
        db.put(key.clone(), value)?;
        println!("  put  {} = occupant-{i}", String::from_utf8_lossy(key));
    }
    db.commit()?;
    let scanned = db.range(from, to)?;
    println!(
        "  range [{}, {}) returned {} entries in key order:",
        b2s(from),
        b2s(to),
        scanned.len()
    );
    for (key, value) in &scanned {
        println!(
            "    {} = {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value)
        );
    }
    println!();

    // -- Act 4: cas success ------------------------------------------------------
    println!("-- Act 4: compare-and-swap on a scalar u64 counter --");
    // `expected = None` means "the key must currently be unset" (create-if-absent).
    db.cas(b"seq:orders".to_vec(), None, 1)?;
    println!("  cas  seq:orders: None -> 1   (created)");
    db.cas(b"seq:orders".to_vec(), Some(1), 2)?;
    println!("  cas  seq:orders: 1 -> 2      (matched, applied)");
    let current = db.read_value(b"seq:orders")?;
    println!("  read seq:orders -> {current:?}");
    assert_eq!(current, Some(2));
    println!();

    // -- Act 5: cas CONFLICT (the deliberate one) -------------------------------
    println!("-- Act 5: a deliberate CAS conflict --");
    println!("  someone still thinks seq:orders == 1, and tries 1 -> 99...");
    match db.cas(b"seq:orders".to_vec(), Some(1), 99) {
        Ok(()) => println!("  UNEXPECTED: the stale CAS applied"),
        Err(error) => {
            println!("  cas  seq:orders: 1 -> 99    REJECTED: {error}");
            println!("  (the current value is 2, not the expected 1, so nothing was written)");
        }
    }
    let after = db.read_value(b"seq:orders")?;
    println!("  read seq:orders -> {after:?}   (unchanged — the conflict protected it)");
    assert_eq!(after, Some(2));
    println!();

    // -- Act 6: delete -----------------------------------------------------------
    println!("-- Act 6: delete writes a stamped tombstone that reads as absent --");
    println!("  user:alice currently = {}", show(&db.get(b"user:alice")?));
    db.delete(b"user:alice".to_vec())?;
    let gone = db.get(b"user:alice")?;
    println!("  delete user:alice");
    println!(
        "  get    user:alice -> {}   (absent — the tombstone shadows the old value)",
        show(&gone)
    );
    assert_eq!(gone, None);
    println!();

    println!("== done: put/get, commit, range, cas (+conflict), delete all demonstrated ==");
    Ok(())
}

/// Render an optional value for display.
fn show(value: &Option<Vec<u8>>) -> String {
    match value {
        Some(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        None => "<absent>".to_owned(),
    }
}

fn b2s(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Show only the first chunk of a long debug hash so the timeline stays readable.
fn short_hash(full: &str) -> String {
    let trimmed: String = full.chars().take(20).collect();
    format!("{trimmed}…")
}

/// Find `count` keys that all route to the SAME shard as `anchor`, so a `[from, to)`
/// range over them is shard-local (range is a shard-local query in haematite).
fn shard_local_keys(db: &Database, count: usize, anchor: &[u8]) -> Vec<Vec<u8>> {
    let target = db.shard_for(anchor);
    let mut keys = Vec::with_capacity(count);
    let mut candidate = 0_u64;
    while keys.len() < count {
        let key = format!("room:{candidate:04}").into_bytes();
        if db.shard_for(&key) == target {
            keys.push(key);
        }
        candidate += 1;
    }
    keys.sort();
    keys
}
