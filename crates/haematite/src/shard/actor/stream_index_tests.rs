use super::{ShardActor, ShardError, sequence_key};
use crate::store::MemoryStore;
use crate::sync::ballot::{Ballot, Stamp};
use crate::sync::topology::SyncNodeId;
use crate::wal::{DurableWal, FsyncPolicy, WalError, WalRecovery};
use std::path::{Path, PathBuf};

#[derive(Debug)]
struct TempWal {
    dir: tempfile::TempDir,
    path: PathBuf,
}

impl TempWal {
    fn path(&self) -> &Path {
        debug_assert!(self.path.starts_with(self.dir.path()));
        &self.path
    }
}

fn temp_path(name: &str) -> Result<TempWal, WalError> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join(name);
    Ok(TempWal { dir, path })
}

fn stamped_seq_value(next_seq: u64, stamp_seq: u64) -> Result<Vec<u8>, WalError> {
    crate::ttl::entry::encode_stamped_optional_ttl(
        next_seq.to_be_bytes().to_vec(),
        Stamp::new(Ballot::new(9, SyncNodeId::new("stream-index")), stamp_seq),
        None,
    )
    .map_err(|error| WalError::TreeError(error.to_string()))
}

fn scan_error(error: &ShardError) -> WalError {
    WalError::TreeError(error.to_string())
}

#[test]
fn stream_index_matches_full_walk_after_commit_recovery_and_merge_adopt() -> Result<(), WalError> {
    let source = temp_path("actor-stream-index-source.wal")?;
    let target = temp_path("actor-stream-index-target.wal")?;
    let mut source_store = MemoryStore::new();
    let mut target_store = MemoryStore::new();

    let source_wal = DurableWal::new(source.path(), FsyncPolicy::CommitOnly)?;
    let mut source_actor = ShardActor::new(source_wal);
    source_actor.put(sequence_key(b"alpha"), stamped_seq_value(2, 0)?)?;
    source_actor.put(sequence_key(b"beta"), stamped_seq_value(3, 1)?)?;
    source_actor.commit(&mut source_store)?;
    assert_eq!(
        source_actor
            .scan_sequences()
            .map_err(|error| scan_error(&error))?,
        source_actor
            .scan_sequences_full_walk(&source_store)
            .map_err(|error| scan_error(&error))?,
        "index must match a full walk after normal commit"
    );

    drop(source_actor);
    let recovered = WalRecovery::recover_path(source.path(), &source_store)?;
    let source_wal = DurableWal::new(source.path(), FsyncPolicy::CommitOnly)?;
    let mut recovered_actor = ShardActor::from_recovered(source_wal, recovered, &source_store)?;
    assert_eq!(
        recovered_actor
            .scan_sequences()
            .map_err(|error| scan_error(&error))?,
        recovered_actor
            .scan_sequences_full_walk(&source_store)
            .map_err(|error| scan_error(&error))?,
        "index must be rebuilt from the committed root on recovery"
    );

    recovered_actor.put(sequence_key(b"gamma"), stamped_seq_value(5, 2)?)?;
    recovered_actor.commit(&mut source_store)?;
    let export = recovered_actor.export_reachable(0, &source_store)?;

    let target_wal = DurableWal::new(target.path(), FsyncPolicy::CommitOnly)?;
    let mut target_actor = ShardActor::new(target_wal);
    target_actor.put(sequence_key(b"local"), stamped_seq_value(1, 3)?)?;
    target_actor.commit(&mut target_store)?;
    target_actor.merge_adopt(&[export], &mut target_store)?;
    assert_eq!(
        target_actor
            .scan_sequences()
            .map_err(|error| scan_error(&error))?,
        target_actor
            .scan_sequences_full_walk(&target_store)
            .map_err(|error| scan_error(&error))?,
        "index must be rebuilt when merge_adopt swaps the committed root"
    );
    assert_eq!(
        target_actor
            .scan_sequences()
            .map_err(|error| scan_error(&error))?,
        vec![
            (b"alpha".to_vec(), 2),
            (b"beta".to_vec(), 3),
            (b"gamma".to_vec(), 5),
            (b"local".to_vec(), 1),
        ]
    );
    Ok(())
}
