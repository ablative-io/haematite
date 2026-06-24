//! Boot-failure reply fan-out for a shard native process.
//!
//! When a shard fails to boot (bad store/WAL path, failed recovery) its
//! sentinel slice drains every queued command with a [`ShardError::Spawn`] so
//! callers fail fast instead of hanging. Each command kind carries a distinctly
//! typed reply channel, so the fan-out needs one tiny sender per kind.

use std::sync::mpsc::SyncSender;

use crate::tree::Hash;

use super::handle::{RangeItem, ScanReply, ShardCommand, ShardCommandKind, ShardError};
use super::{PromiseState, RecordPromiseOutcome};

/// Reply to a queued command with a startup error so its caller fails fast.
pub(super) fn reply_startup_error(command: ShardCommand, message: &str) {
    let error = ShardError::Spawn(message.to_owned());
    match command.kind {
        ShardCommandKind::Get { reply, .. } | ShardCommandKind::GetRaw { reply, .. } => {
            send_get(&reply, error);
        }
        ShardCommandKind::DeleteIfExpired { reply, .. } => send_bool(&reply, error),
        ShardCommandKind::Put { reply, .. }
        | ShardCommandKind::Delete { reply, .. }
        | ShardCommandKind::Cas { reply, .. }
        | ShardCommandKind::ApplyDurable { reply, .. }
        | ShardCommandKind::RecordOwnerEpoch { reply, .. }
        | ShardCommandKind::Shutdown { reply } => send_unit(&reply, error),
        ShardCommandKind::Commit { reply } => send_commit(&reply, error),
        ShardCommandKind::Range { reply, .. } => send_range(&reply, error),
        ShardCommandKind::Append { reply, .. } => send_append(&reply, error),
        ShardCommandKind::ReadValue { reply, .. } => send_read_value(&reply, error),
        ShardCommandKind::RecordPromise { reply, .. } => send_promise(&reply, error),
        ShardCommandKind::ReserveMinted { reply, .. } => send_reserved(&reply, error),
        ShardCommandKind::ReadPromiseState { reply } => send_promise_state(&reply, error),
        ShardCommandKind::ScanSequences { reply } => send_scan(&reply, error),
    }
}

fn send_get(reply: &SyncSender<Result<Option<Vec<u8>>, ShardError>>, error: ShardError) {
    drop(reply.send(Err(error)));
}

fn send_unit(reply: &SyncSender<Result<(), ShardError>>, error: ShardError) {
    drop(reply.send(Err(error)));
}

fn send_bool(reply: &SyncSender<Result<bool, ShardError>>, error: ShardError) {
    drop(reply.send(Err(error)));
}

fn send_commit(reply: &SyncSender<Result<Hash, ShardError>>, error: ShardError) {
    drop(reply.send(Err(error)));
}

fn send_range(reply: &SyncSender<Result<Vec<RangeItem>, ShardError>>, error: ShardError) {
    drop(reply.send(Err(error)));
}

fn send_append(reply: &SyncSender<Result<u64, ShardError>>, error: ShardError) {
    drop(reply.send(Err(error)));
}

fn send_read_value(reply: &SyncSender<Result<Option<u64>, ShardError>>, error: ShardError) {
    drop(reply.send(Err(error)));
}

fn send_scan(reply: &ScanReply, error: ShardError) {
    drop(reply.send(Err(error)));
}

fn send_promise(
    reply: &SyncSender<Result<RecordPromiseOutcome, ShardError>>,
    error: ShardError,
) {
    drop(reply.send(Err(error)));
}

fn send_reserved(reply: &SyncSender<Result<u64, ShardError>>, error: ShardError) {
    drop(reply.send(Err(error)));
}

fn send_promise_state(reply: &SyncSender<Result<PromiseState, ShardError>>, error: ShardError) {
    drop(reply.send(Err(error)));
}
