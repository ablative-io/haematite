//! Integration tests for the SINGLE-KEY active-active receiver apply path
//! (`apply_write_proposal` / `apply_proposal_durably`) covering the two gaps the
//! existing `receiver_apply.rs` (CAS mismatch / CAS match / `ApplyError` / durable
//! kill-test) and `receiver_batch_apply.rs` (batch fence / batch atomicity) do
//! NOT cover:
//!
//! 1. **Epoch fence below the live promised** — a single-key proposal stamped at
//!    an epoch BELOW the shard's durably `promised` ballot must be REJECTED(Fenced)
//!    and persist NOTHING, even across a crash + reopen. (The batch fence is
//!    tested; the single-key fence was not.)
//! 2. **Idempotent duplicate** — re-applying the IDENTICAL stamped proposal is a
//!    no-op accept: the value and its stored commit stamp are unchanged, never
//!    duplicated or rolled back.
//!
//! Tests return `Result` and use `?` (the crate denies `expect`/`unwrap`/`panic`).

use std::error::Error;
use std::path::Path;

use haematite::sync::SyncNodeId;
use haematite::sync::ballot::Ballot;
use haematite::sync::protocol::{AckOutcome, RejectReason, WriteId, WriteProposal};
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

/// A single-key proposal for `key`/`value` at an explicit `epoch`/`seq`, routed to
/// `shard_id`, with a fixed correlation id.
fn proposal_at(
    shard_id: usize,
    key: &[u8],
    expected: Option<Hash>,
    value: &[u8],
    epoch: Ballot,
    seq: u64,
) -> WriteProposal {
    WriteProposal {
        write_id: WriteId::new(SyncNodeId::from("node-a@127.0.0.1"), 1, 0),
        shard_id,
        key: key.to_vec(),
        expected,
        value: value.to_vec(),
        ttl: None,
        epoch,
        seq,
        tombstone: false,
    }
}

/// FENCE (single key): a proposal whose stamp epoch is BELOW the shard's durably
/// `promised` ballot must be REJECTED(Fenced) and write NOTHING — proven durable
/// by a crash (drop) + reopen showing the key absent.
///
/// This is the single-key analogue of the batch `stale_epoch_batch_is_fenced`
/// test, with the extra crash-survival assertion: a fenced write must not leave
/// any durable state behind, so the reopened database cannot resurrect it.
#[test]
fn stale_epoch_single_key_is_fenced_and_persists_nothing() -> TestResult {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 1))?;
    let key = b"fenced-key".to_vec();
    let shard_id = db.shard_for(&key);

    // Durably advance promised to (10, "winner"); a proposal stamped at epoch
    // (3, "deposed") is strictly below it, so the receiver must fence it.
    let promised = Ballot::new(10, SyncNodeId::from("winner"));
    assert!(
        db.record_promise_for_test(shard_id, promised),
        "the test must advance promised so the proposal is genuinely stale"
    );

    let stale_epoch = Ballot::new(3, SyncNodeId::from("deposed"));
    let ack = db.apply_write_proposal(&proposal_at(
        shard_id,
        &key,
        None,
        b"stale-value",
        stale_epoch,
        0,
    ));
    assert_eq!(
        ack.outcome,
        AckOutcome::Rejected(RejectReason::Fenced),
        "a proposal whose epoch is below promised must be REJECTED(Fenced)"
    );

    // Nothing was applied in-process.
    assert_eq!(
        db.get(&key)?,
        None,
        "a fenced single-key proposal must apply NOTHING"
    );

    // CRASH + REOPEN: a fenced write must leave NO durable state, so the value
    // cannot reappear after recovery.
    drop(db);
    let reopened = Database::open(&data_dir)?;
    assert_eq!(
        reopened.get(&key)?,
        None,
        "a fenced write must not be recoverable — it was never made durable"
    );
    Ok(())
}

/// IDEMPOTENCY: a duplicate create-if-absent proposal does not corrupt or
/// double-apply durable state.
///
/// The receiver CAS is over the current LOGICAL value hash: `expected = None`
/// means "expect absent". After the first apply the key holds a value, so a
/// REPLAY of the identical `expected = None` proposal no longer matches (the key
/// is present) and is correctly rejected as a vote-against (`CasMismatch`). The
/// load-bearing property is that the replay leaves the original value AND its
/// stored commit stamp EXACTLY as they were — never duplicated, mutated, or
/// rolled back. (A blind re-apply path would re-stamp or overwrite; this proves
/// the CAS fence makes replays safe.)
#[test]
fn duplicate_create_if_absent_proposal_does_not_corrupt_state() -> TestResult {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 1))?;
    let key = b"dup-key".to_vec();
    let shard_id = db.shard_for(&key);

    // A concrete stamp (epoch (5, "owner"), seq 7). With promised at bottom this
    // is fence-compatible, so the first apply accepts.
    let epoch = Ballot::new(5, SyncNodeId::from("owner"));
    let first = db.apply_write_proposal(&proposal_at(shard_id, &key, None, b"v", epoch.clone(), 7));
    assert_eq!(
        first.outcome,
        AckOutcome::Applied,
        "first apply must succeed"
    );
    assert_eq!(db.get(&key)?, Some(b"v".to_vec()));
    let stamp_after_first = db.stored_stamp_for_test(&key);
    assert!(
        stamp_after_first.is_some(),
        "the applied write must carry a stored commit stamp"
    );

    // REPLAY the identical create-if-absent proposal. The key is now present, so
    // the create-if-absent CAS no longer matches: the replay is a vote-against,
    // NOT a second application.
    let replay = db.apply_write_proposal(&proposal_at(shard_id, &key, None, b"v", epoch, 7));
    assert_eq!(
        replay.outcome,
        AckOutcome::Rejected(RejectReason::CasMismatch),
        "a create-if-absent replay onto a now-present key is a CAS vote-against"
    );
    assert_eq!(
        db.get(&key)?,
        Some(b"v".to_vec()),
        "the value must be unchanged by the rejected replay"
    );
    assert_eq!(
        db.stored_stamp_for_test(&key),
        stamp_after_first,
        "the stored commit stamp must be untouched by the rejected replay"
    );
    Ok(())
}

/// IDEMPOTENCY (matched-CAS replay): re-applying the SAME write with a CAS that
/// MATCHES the current value commits the IDENTICAL content, so the durable result
/// is unchanged.
///
/// This is the complementary case: a re-delivery that carries the correct
/// `expected` (the hash of the value being overwritten) and the SAME stamp + bytes
/// re-commits the byte-identical stamped envelope. Because content is identical,
/// the re-applied stamp equals the original — the apply is convergent, not a
/// state change.
#[test]
fn matched_cas_replay_is_convergent() -> TestResult {
    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("db");
    let db = Database::create(config_for(&data_dir, 1))?;
    let key = b"conv-key".to_vec();
    let shard_id = db.shard_for(&key);

    let epoch = Ballot::new(5, SyncNodeId::from("owner"));
    let first =
        db.apply_write_proposal(&proposal_at(shard_id, &key, None, b"v1", epoch.clone(), 1));
    assert_eq!(first.outcome, AckOutcome::Applied);
    let stamp_after_first = db.stored_stamp_for_test(&key);

    // Re-apply the SAME stamped write but with the matching CAS precondition (the
    // hash of "v1") and the SAME stamp + value: it commits identical content.
    let cas = Some(Hash::of(b"v1"));
    let replay = db.apply_write_proposal(&proposal_at(shard_id, &key, cas, b"v1", epoch, 1));
    assert_eq!(
        replay.outcome,
        AckOutcome::Applied,
        "a matched-CAS replay of identical content must apply"
    );
    assert_eq!(db.get(&key)?, Some(b"v1".to_vec()), "value unchanged");
    assert_eq!(
        db.stored_stamp_for_test(&key),
        stamp_after_first,
        "identical content + identical stamp ⇒ the stored stamp is unchanged"
    );
    Ok(())
}
