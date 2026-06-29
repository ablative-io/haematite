//! Unit tests for `route_ack_outcome`: the ONE boundary where the wire
//! [`RejectReason`] is mapped onto a typed [`CasVote`] / [`RejectKind`]. Before
//! the typed-fence split this function collapsed `CasMismatch` and `Fenced` into a
//! single untyped `Reject`; these tests pin the now-distinct classification.

use std::sync::mpsc;

use dashmap::DashMap;

use super::{
    AckOutcome, CasVote, RejectKind, RejectReason, SyncNodeId, WriteId, WriteRegistry,
    route_ack_outcome,
};

const LOCAL_CREATION: u32 = 7;

fn fixture() -> (
    WriteRegistry,
    WriteId,
    SyncNodeId,
    mpsc::Receiver<CasVote<SyncNodeId>>,
) {
    let registry: WriteRegistry = std::sync::Arc::new(DashMap::new());
    let write_id = WriteId::new(SyncNodeId::new("origin"), LOCAL_CREATION, 1);
    let (sender, receiver) = mpsc::channel();
    registry.insert(write_id.clone(), sender);
    (registry, write_id, SyncNodeId::new("acker"), receiver)
}

/// Drive one ack through `route_ack_outcome` and return the routed vote, or `None`
/// if nothing was routed (e.g. a dropped stale-incarnation ack).
fn route(outcome: AckOutcome) -> Option<CasVote<SyncNodeId>> {
    let (registry, write_id, acker, receiver) = fixture();
    route_ack_outcome(&registry, LOCAL_CREATION, &write_id, &acker, outcome);
    receiver.recv().ok()
}

#[test]
fn applied_routes_to_accept() {
    let acker = SyncNodeId::new("acker");
    assert_eq!(route(AckOutcome::Applied), Some(CasVote::Accept(acker)));
}

#[test]
fn fenced_reject_routes_to_epoch_fence_reject() {
    let acker = SyncNodeId::new("acker");
    assert_eq!(
        route(AckOutcome::Rejected(RejectReason::Fenced)),
        Some(CasVote::Reject(acker, RejectKind::EpochFence)),
        "a wire Fenced reject must classify as the stronger EpochFence kind"
    );
}

#[test]
fn cas_mismatch_reject_routes_to_cas_mismatch_reject() {
    let acker = SyncNodeId::new("acker");
    assert_eq!(
        route(AckOutcome::Rejected(RejectReason::CasMismatch)),
        Some(CasVote::Reject(acker, RejectKind::CasMismatch)),
        "a wire CasMismatch reject must classify as the weaker CasMismatch kind"
    );
}

#[test]
fn apply_error_reject_routes_to_fault() {
    let acker = SyncNodeId::new("acker");
    assert_eq!(
        route(AckOutcome::Rejected(RejectReason::ApplyError)),
        Some(CasVote::Fault(acker)),
        "an apply error remains a retryable transport-style Fault"
    );
}

#[test]
fn ack_for_prior_incarnation_is_dropped() {
    // Fix D incarnation gate: an ack minted for a different origin_creation is
    // discarded entirely (no vote routed), regardless of reason classification.
    let (registry, write_id, acker, receiver) = fixture();
    route_ack_outcome(
        &registry,
        LOCAL_CREATION + 1,
        &write_id,
        &acker,
        AckOutcome::Rejected(RejectReason::Fenced),
    );
    assert!(
        receiver.try_recv().is_err(),
        "stale-incarnation ack must be dropped"
    );
}
