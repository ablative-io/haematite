//! Native beamr transport glue for the sync protocol.
//!
//! These helpers frame a [`SyncMessage`] with the platform-neutral
//! [`crate::sync_codec`] codec and hand the bytes to beamr's existing
//! distribution connection manager. They are the native-only half of the
//! protocol (they depend on `beamr::atom::Atom` and
//! `beamr::distribution::*`), so they stay in `crate::sync` and are gated out of
//! the wasm build.

use std::sync::Arc;

use beamr::atom::Atom;
use beamr::distribution::ConnectionManager;
use beamr::distribution::connection::DistConnection;

use crate::sync_codec::wire::{
    SYNC_CONTROL_FRAME, encode_beamr_push_response_frame, encode_beamr_sync_frame,
};
use crate::sync_codec::{
    BatchWriteAck, BatchWriteProposal, Nack, Prepare, Promise, PullRequest, PushResponse,
    RootExchangeRequest, RootExchangeResponse, ShardSyncRequest, SyncError, SyncMessage,
    TargetNodeRequest, TargetNodeResponse, WriteAck, WriteProposal, decode_sync_message,
};

/// Send a sync message through beamr's existing distribution connection manager.
///
/// This function never opens its own socket. It requires an already-established
/// beamr distribution connection and hands that connection plus the encoded
/// frame to `write_frame`, allowing the caller's runtime to drive
/// `DistConnection::write_raw` without haematite creating a separate transport.
pub fn send_sync_message_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    message: &SyncMessage,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    let connection = manager
        .get_connection(remote)
        .ok_or(SyncError::TransportConnectionUnavailable)?;
    let frame = encode_beamr_sync_frame(message)?;
    write_frame(connection, frame)
}

pub fn send_root_exchange_request_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    request: RootExchangeRequest,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::RootRequest(request),
        write_frame,
    )
}

pub fn send_root_exchange_response_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    response: RootExchangeResponse,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::RootResponse(response),
        write_frame,
    )
}

pub fn send_pull_request_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    request: PullRequest,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::PullRequest(request),
        write_frame,
    )
}

pub fn send_push_response_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    response: &PushResponse,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    // Encode the (potentially large) PushResponse by reference rather than
    // cloning it into an owned `SyncMessage` just to feed the generic path.
    let connection = manager
        .get_connection(remote)
        .ok_or(SyncError::TransportConnectionUnavailable)?;
    let frame = encode_beamr_push_response_frame(response)?;
    write_frame(connection, frame)
}

pub fn send_shard_sync_request_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    request: ShardSyncRequest,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::ShardSyncRequest(request),
        write_frame,
    )
}

pub fn send_target_node_request_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    request: TargetNodeRequest,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::TargetNodeRequest(request),
        write_frame,
    )
}

pub fn send_target_node_response_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    response: &TargetNodeResponse,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::TargetNodeResponse(response.clone()),
        write_frame,
    )
}

pub fn send_write_proposal_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    proposal: &WriteProposal,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::WriteProposal(proposal.clone()),
        write_frame,
    )
}

pub fn send_write_ack_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    ack: &WriteAck,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::WriteAck(ack.clone()),
        write_frame,
    )
}

pub fn send_batch_write_proposal_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    proposal: &BatchWriteProposal,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::BatchWriteProposal(proposal.clone()),
        write_frame,
    )
}

pub fn send_batch_write_ack_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    ack: &BatchWriteAck,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::BatchWriteAck(ack.clone()),
        write_frame,
    )
}

pub fn send_prepare_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    prepare: &Prepare,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::Prepare(prepare.clone()),
        write_frame,
    )
}

pub fn send_promise_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    promise: &Promise,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::Promise(promise.clone()),
        write_frame,
    )
}

pub fn send_nack_via_beamr<F>(
    manager: &ConnectionManager,
    remote: Atom,
    nack: &Nack,
    write_frame: F,
) -> Result<(), SyncError>
where
    F: FnOnce(Arc<DistConnection>, Vec<u8>) -> Result<(), SyncError>,
{
    send_sync_message_via_beamr(
        manager,
        remote,
        &SyncMessage::Nack(nack.clone()),
        write_frame,
    )
}

/// Register a beamr control-frame handler for haematite sync messages.
pub fn register_beamr_sync_handler<F>(manager: &ConnectionManager, handler: F)
where
    F: Fn(Result<SyncMessage, SyncError>) + Send + Sync + 'static,
{
    manager.register_control_frame_handler(move |control, payload| {
        if control == SYNC_CONTROL_FRAME {
            handler(decode_sync_message(payload));
        }
    });
}
