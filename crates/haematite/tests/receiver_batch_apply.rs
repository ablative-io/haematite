//! Integration tests for the active-active A1b receiver-side ATOMIC multi-key
//! batch apply-then-ack (`handle_inbound_batch_write` / `apply_batch_write_proposal`).
//!
//! These are the batch analogue of `receiver_apply.rs`. They exercise the WIRE +
//! RECEIVER half of a replicated multi-key append: a `BatchWriteProposal` of N
//! entries is fed to the receiver handler on a shard, and the SINGLE verdict
//! (`BatchWriteAck`) plus the all-or-nothing on-disk effect are asserted. The
//! headline property is ATOMICITY: a fence or any single CAS mismatch must reject
//! the WHOLE batch and write NOTHING — never a partial application, never a false
//! accept.
//!
//! Tests return `Result` and use `?` (the crate denies `expect`/`unwrap`/`panic`).

use std::error::Error;
use std::path::Path;

use haematite::sync::SyncNodeId;
use haematite::sync::ballot::{Ballot, Stamp};
use haematite::sync::protocol::{AckOutcome, RejectReason, WriteId};
use haematite::sync::protocol::{BatchWriteEntry, BatchWriteProposal};
use haematite::tree::Hash;
use haematite::{Database, DatabaseConfig};

type TestResult = Result<(), Box<dyn Error>>;

fn config_for(path: &Path, shard_count: usize) -> DatabaseConfig {
    DatabaseConfig {
        data_dir: path.to_path_buf(),
        shard_count,
        sweep_interval: None,
        distributed: None,
    }
}

fn entry(key: &[u8], expected: Option<Hash>, value: &[u8]) -> BatchWriteEntry {
    BatchWriteEntry {
        key: key.to_vec(),
        expected,
        value: value.to_vec(),
        ttl: None,
    }
}

/// Build a `BatchWriteProposal` for `shard_id` carrying `entries` under one shared
/// `stamp`, with a fixed correlation id.
fn batch(shard_id: usize, entries: Vec<BatchWriteEntry>, stamp: Stamp) -> BatchWriteProposal {
    BatchWriteProposal {
        write_id: WriteId::new(SyncNodeId::from("node-a@127.0.0.1"), 1, 0),
        shard_id,
        entries,
        stamp,
    }
}

/// ATOMIC APPLY: an N-entry batch on a compatible shard applies EVERY key durably
/// under the shared stamp, and the ack is ACCEPT.
///
/// Force a single shard (so every key routes to it), feed a 3-entry create-if-absent
/// batch with `promised = bottom` (compatible — the proposal's bottom epoch is not
/// below it), and assert: ack Applied, all three keys readable with their values,
/// and every key carries the IDENTICAL shared stamp. The third assertion is what
/// proves the apply ran the real stamped batch path (not a vacuous put), and that
/// it was the ONE shared stamp.
#[test]
fn batch_applies_all_entries_atomically() -> TestResult {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 1))?;
    let shard_id = db.shard_for(b"any-key");

    let stamp = Stamp::new(Ballot::new(5, SyncNodeId::from("owner-node")), 11);
    let entries = vec![
        entry(b"batch/k1", None, b"v1"),
        entry(b"batch/k2", None, b"v2"),
        entry(b"batch/k3", None, b"v3"),
    ];
    let ack = db.apply_batch_write_proposal(&batch(shard_id, entries, stamp.clone()));
    assert_eq!(
        ack.outcome,
        AckOutcome::Applied,
        "a fence-compatible batch with matching CAS preconditions must be ACCEPTED"
    );

    // EVERY key is readable with its value AND carries the ONE shared stamp.
    for (key, value) in [
        (b"batch/k1".as_slice(), b"v1".as_slice()),
        (b"batch/k2".as_slice(), b"v2".as_slice()),
        (b"batch/k3".as_slice(), b"v3".as_slice()),
    ] {
        assert_eq!(
            db.get(key)?,
            Some(value.to_vec()),
            "every applied key must be readable with its value"
        );
        assert_eq!(
            db.stored_stamp_for_test(key).as_ref(),
            Some(&stamp),
            "every applied key must carry the IDENTICAL shared batch stamp"
        );
    }
    Ok(())
}

/// FENCE: a stale-epoch batch (the shard's `promised` strictly ABOVE the batch's
/// epoch) applies NOTHING and is REJECTED(Fenced).
///
/// Advance the shard's `promised` to a high ballot, then feed a batch whose shared
/// stamp's epoch is BELOW it. The handler must reject the whole batch as Fenced and
/// leave EVERY key absent. The "every key absent" assertion is what proves nothing
/// was partially written; it cannot pass vacuously because the positive control
/// (`batch_applies_all_entries_atomically`) shows the same keys DO get written when
/// not fenced.
#[test]
fn stale_epoch_batch_is_fenced_and_writes_nothing() -> TestResult {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 1))?;
    let shard_id = db.shard_for(b"any-key");

    // Advance promised to (10, "winner"); a batch stamped at epoch (3, ...) is below
    // it, so the receiver must fence the whole batch.
    let promised = Ballot::new(10, SyncNodeId::from("winner"));
    assert!(
        db.record_promise_for_test(shard_id, promised),
        "the test must successfully advance promised so the batch is genuinely stale"
    );

    let stale = Stamp::new(Ballot::new(3, SyncNodeId::from("deposed")), 0);
    let entries = vec![
        entry(b"fenced/k1", None, b"v1"),
        entry(b"fenced/k2", None, b"v2"),
        entry(b"fenced/k3", None, b"v3"),
    ];
    let ack = db.apply_batch_write_proposal(&batch(shard_id, entries, stale));
    assert_eq!(
        ack.outcome,
        AckOutcome::Rejected(RejectReason::Fenced),
        "a batch whose epoch is below the shard's promised must be REJECTED(Fenced)"
    );

    // NOTHING was written: every key is absent.
    for key in [b"fenced/k1".as_slice(), b"fenced/k2", b"fenced/k3"] {
        assert_eq!(
            db.get(key)?,
            None,
            "a fenced batch must apply NOTHING — every key must be absent"
        );
    }
    Ok(())
}

/// CAS-MISMATCH ATOMICITY: one entry's `expected` mismatches → the WHOLE batch is
/// rejected and NO entry is written (not even the entries whose CAS would match).
///
/// Seed one key with a value. Feed a batch where two NEW keys would create cleanly
/// but a third entry targets the seeded key with a WRONG `expected`. The handler
/// must reject(CasMismatch) and leave the two new keys ABSENT and the seeded key
/// UNCHANGED. The "two new keys absent" assertion is the atomicity proof: under a
/// non-atomic per-key apply they would have been written before the mismatch.
#[test]
fn one_cas_mismatch_rejects_whole_batch() -> TestResult {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 1))?;
    let shard_id = db.shard_for(b"any-key");

    // Seed + commit a value so the receiver's CAS read sees it.
    let seeded = b"cas/existing".to_vec();
    db.put(seeded.clone(), b"original".to_vec())?;
    db.commit()?;

    let stamp = Stamp::new(Ballot::new(2, SyncNodeId::from("owner")), 4);
    let entries = vec![
        // Two clean create-if-absent entries that WOULD apply on their own.
        entry(b"cas/new1", None, b"n1"),
        entry(b"cas/new2", None, b"n2"),
        // A wrong CAS precondition on the seeded key (empty-value hash != hash of
        // "original"): this single mismatch must reject the WHOLE batch.
        entry(&seeded, Some(Hash::of(b"")), b"intruder"),
    ];
    let ack = db.apply_batch_write_proposal(&batch(shard_id, entries, stamp));
    assert_eq!(
        ack.outcome,
        AckOutcome::Rejected(RejectReason::CasMismatch),
        "any single CAS mismatch must reject the WHOLE batch as a vote-against"
    );

    // ATOMICITY: the two would-be-clean new keys were NOT written.
    assert_eq!(
        db.get(b"cas/new1")?,
        None,
        "a CAS-rejected batch must apply NOTHING — clean entries must be absent too"
    );
    assert_eq!(db.get(b"cas/new2")?, None, "the second clean entry too");
    // The seeded key is unchanged.
    assert_eq!(
        db.get(&seeded)?,
        Some(b"original".to_vec()),
        "the mismatching entry must not mutate the existing value"
    );
    Ok(())
}
