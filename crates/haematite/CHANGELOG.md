# Changelog

All notable changes to the `haematite` crate are documented here. The format is
loosely based on [Keep a Changelog](https://keepachangelog.com/), and this crate
follows semantic versioning.

## 0.3.0

### Added

- **Typed CAS-reject classification (`RejectKind`).** The CAS-aware quorum tally
  now distinguishes an *epoch fence* (a conflicting higher-ballot owner deposed
  us) from a benign *value-CAS mismatch* race instead of collapsing both into one
  reject bucket. `sync::CasVote::Reject` now carries a `sync::RejectKind`
  (`EpochFence` | `CasMismatch`); the wire `RejectReason` is mapped onto it at the
  endpoint boundary (`route_ack_outcome`), the one place the reason was previously
  discarded.
- **`ConsistencyError::CasConflict { required, possible_accepts }`** — a
  deterministic CAS loss to value mismatches alone (still the live owner; the
  precondition merely lost a write ordering, so the caller may re-read and retry).
  Distinct from the stronger `Fenced` (must re-resolve ownership) and from the
  retryable transport `AckFailed`.
- **`DatabaseError::CasConflict { required, possible_accepts }`** — the typed twin
  of the above, with the `From<ConsistencyError>` mapping preserving the fields.
- **`Database::current_owner_epoch(shard_id) -> sync::Ballot`** and
  **`Database::is_current_owner(shard_id) -> bool`** — point-in-time advisory
  readers of the in-memory live serve-authority (R-LE). They report the SAME
  `live_epoch` that authorizes every stamped write, so a node that recovered
  `owner_epoch` from disk after a crash but did not re-acquire correctly reports
  `is_current_owner == false`.
- `OwnerStamps::live_epoch` is now a supported reader (was `#[doc(hidden)]`
  test-support); signature unchanged.

### Changed

- The CAS quorum tally precedence on a provable loss is now: any epoch fence ⇒
  `Fenced` (the stronger signal wins, even alongside a value-CAS mismatch);
  value-CAS mismatches only ⇒ `CasConflict`; faults only ⇒ `AckFailed`. This is
  fixed on BOTH the iterator (`wait_for_cas_quorum`) and the live receiver
  (`wait_for_cas_quorum_from_receiver`) paths; the latter previously inlined
  reject → `Fenced` and bypassed the shared classifier. All pre-existing
  Fenced-vs-AckFailed classification tests pass unchanged (a bare
  `CasVote::reject` still defaults to `EpochFence`).
