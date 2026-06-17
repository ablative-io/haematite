/// The top-level database handle.
///
/// TODO(CORE-009): `Database::commit` is not implemented in this crate yet — it
/// lands with CORE-009 R5. When it does, every successful commit must append the
/// newly committed composite root hash to the
/// [`CommitLog`](crate::branch::CommitLog) exactly once, stamped with
/// [`current_timestamp`](crate::branch::current_timestamp); a failed commit must
/// append nothing. The commit log (BRANCH-003 R2) is built and tested in
/// isolation precisely so this is the only wiring CORE-009 needs to add. Until
/// then the log has no producer and `CommitLog::list()` is empty.
#[derive(Debug)]
pub struct Database;
