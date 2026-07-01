//! Integration tests for the active-active "2a-4" receiver-side
//! conditional-durable-apply-then-ack.
//!
//! These exercise [`Database::apply_write_proposal`] — the receiver half of
//! quorum-on-write. The headline is the **ack-implies-durable kill-test**: an
//! applied write must be on STABLE storage (committed tree + fsynced WAL marker)
//! before the ack, so it survives a crash that drops the process WITHOUT a clean
//! tree-commit. A plain `CommitOnly` put would leave the value only in the
//! (non-fsynced, un-truncated) WAL buffer and NO committed-root marker — the
//! falsifiable difference this test pins down.
//!
//! Tests return `Result` and use `?` (the crate denies `expect`/`unwrap`/`panic`).

use std::error::Error;
use std::path::Path;

use haematite::sync::SyncNodeId;
use haematite::sync::ballot::Ballot;
use haematite::sync::protocol::{AckOutcome, RejectReason, WriteId, WriteProposal};
use haematite::tree::Hash;
use haematite::wal::DurableWal;
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

/// Path to a shard's WAL file (mirrors the database's internal layout).
fn wal_path(data_dir: &Path, shard_id: usize) -> std::path::PathBuf {
    data_dir.join(format!("shard-{shard_id}")).join("shard.wal")
}

/// Build a `WriteProposal` with a fixed correlation id for `key`/`value`.
fn proposal(key: &[u8], expected: Option<Hash>, value: &[u8]) -> WriteProposal {
    WriteProposal {
        write_id: WriteId::new(SyncNodeId::from("node-a@127.0.0.1"), 1, 0),
        // These receiver tests run a single-shard database, so every key routes to
        // shard 0; the receiver now routes the proposal by this explicit `shard_id`.
        shard_id: 0,
        key: key.to_vec(),
        expected,
        value: value.to_vec(),
        ttl: None,
        // No election in these 2a receiver tests: stamp bottom, which `>= promised`
        // (also bottom) so the fence is a no-op and 2a semantics are unchanged.
        epoch: Ballot::bottom(),
        seq: 0,
        tombstone: false,
    }
}

/// THE KILL-TEST: an applied write is durable BEFORE the ack.
///
/// Apply a `WriteProposal` through the force-sync receiver path, then SIMULATE A
/// CRASH by dropping the `Database` WITHOUT any clean tree-commit, and assert the
/// value IS present on reopen.
///
/// Why this proves the force-sync is real (Fix B): the receiver apply routes
/// through the shard's `apply_durable` command, which does `put` + `commit` in one
/// slice. `commit` (a) persists the value into the prolly TREE on the `DiskStore`
/// (each node file fsynced) and (b) writes a fsynced committed-root MARKER into the
/// WAL, truncating the replay entries. So after the apply the WAL file carries a
/// `committed_root` marker (asserted below) and the value is reachable from the
/// committed tree alone. A plain `CommitOnly` `put` (the trap) does NEITHER: it
/// appends a non-fsynced WAL frame and leaves `committed_root()` as `None`. This
/// test therefore FAILS if the apply uses a normal put — the marker assertion is
/// exactly that proof.
#[test]
fn ack_implies_durable_kill_test() -> TestResult {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 1))?;
    let key = b"replicated-key".to_vec();
    let shard_id = db.shard_for(&key);

    // Apply a create-if-absent proposal (expected = None) through the force-sync
    // receiver path. The ack is Applied only AFTER the durable commit returns.
    let ack = db.apply_write_proposal(&proposal(&key, None, b"durable-value"));
    assert_eq!(ack.outcome, AckOutcome::Applied, "apply must succeed");

    // PROOF OF FORCE-SYNC: the WAL now holds a committed-root marker. A plain
    // CommitOnly put would leave this `None` (see the kv.rs buffer test), so this
    // single assertion is what distinguishes a durable apply from a page-cache put.
    let contents = DurableWal::read_file(wal_path(&data_dir, shard_id))?;
    assert!(
        contents.committed_root().is_some(),
        "durable apply must have committed a root marker (force-sync proof)"
    );

    // SIMULATE CRASH: drop the database with NO clean tree-commit issued by the
    // test. (`Database::drop` only stops the shard actors; it does not commit.)
    drop(db);

    // REOPEN and assert the value survived — recovered from the committed tree the
    // durable apply flushed, not from a fortunate page-cache replay of a buffer.
    let reopened = Database::open(&data_dir)?;
    assert_eq!(
        reopened.get(&key)?,
        Some(b"durable-value".to_vec()),
        "the applied value must survive a crash + reopen"
    );

    // And the recovered shard is itself already committed (empty buffer, root
    // marker present): the value lives in the durable tree.
    let after = DurableWal::read_file(wal_path(&data_dir, shard_id))?;
    assert!(after.committed_root().is_some(), "root marker persists");
    assert!(
        after.entries().is_empty(),
        "no un-committed replay entries remain — the value is in the tree"
    );
    Ok(())
}

/// CAS mismatch rejects WITHOUT applying (Fix C).
///
/// Seed `key` with a value, then propose a write whose `expected` hash is WRONG.
/// The apply must reject (`CasMismatch`) and the stored value must be unchanged.
#[test]
fn cas_mismatch_rejects_without_applying() -> TestResult {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 1))?;
    let key = b"contended-key".to_vec();

    // Seed an initial value and commit it, so the receiver's CAS read sees it.
    db.put(key.clone(), b"original".to_vec())?;
    db.commit()?;

    // Propose with a deliberately wrong `expected` (the empty-value hash, which is
    // not the hash of "original"). The replica is "ahead": it must vote against.
    let wrong_expected = Some(Hash::of(b""));
    let ack = db.apply_write_proposal(&proposal(&key, wrong_expected, b"intruder"));
    assert_eq!(
        ack.outcome,
        AckOutcome::Rejected(RejectReason::CasMismatch),
        "a wrong CAS precondition must be rejected as a vote-against"
    );

    // NOTHING was applied: the stored value is still the original.
    assert_eq!(
        db.get(&key)?,
        Some(b"original".to_vec()),
        "a rejected proposal must not mutate the stored value"
    );
    Ok(())
}

/// CAS MATCH applies (the positive control for the mismatch test): proposing with
/// the CORRECT current-value hash succeeds and overwrites the value durably.
#[test]
fn cas_match_applies_over_existing_value() -> TestResult {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 1))?;
    let key = b"contended-key".to_vec();

    db.put(key.clone(), b"original".to_vec())?;
    db.commit()?;

    // The correct precondition is the hash of the current visible value bytes.
    let correct_expected = Some(Hash::of(b"original"));
    let ack = db.apply_write_proposal(&proposal(&key, correct_expected, b"successor"));
    assert_eq!(ack.outcome, AckOutcome::Applied, "matching CAS applies");
    assert_eq!(db.get(&key)?, Some(b"successor".to_vec()), "value advanced");
    Ok(())
}

/// `ApplyError` path: a genuine apply fault (NOT a CAS mismatch) surfaces as
/// `Rejected(ApplyError)`. We force the fault by shutting the owning shard down so
/// the durable-apply command cannot be served, then proposing to it.
#[test]
fn apply_error_when_shard_unavailable() -> TestResult {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 1))?;
    let key = b"unreachable-key".to_vec();

    // Under LAZY materialisation a shard is not spawned until first touch, so
    // materialise the owning shard (a read is enough) BEFORE shutting it down —
    // otherwise `shutdown_shards_for_test` finds nothing and the subsequent apply
    // would simply re-materialise a fresh, working shard.
    db.get(&key)?;

    // Stop every materialised shard actor so a subsequent apply cannot be served.
    // The reply channel disconnects / times out -> a non-CAS ShardError ->
    // ApplyError.
    db.shutdown_shards_for_test();

    let ack = db.apply_write_proposal(&proposal(&key, None, b"value"));
    assert_eq!(
        ack.outcome,
        AckOutcome::Rejected(RejectReason::ApplyError),
        "a non-CAS apply fault must be an ApplyError, not a CAS vote-against"
    );
    Ok(())
}
