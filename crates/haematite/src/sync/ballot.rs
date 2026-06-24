//! Step-3 epoch-fence ballots (single-decree Paxos).
//!
//! A [`Ballot`] is the unique, monotonic, durably-persisted epoch that drives
//! per-shard ownership election (see `docs/ACTIVE-ACTIVE-STEP3-EPOCH-FENCE-DESIGN.md`
//! §2.1). It is ordered lexicographically by `(counter, node)`:
//!
//! - The `counter` is the per-shard epoch number and dominates ordering.
//! - The `node` tiebreak guarantees GLOBAL UNIQUENESS — no two nodes can ever
//!   mint the same ballot — which is the single property 2a's symmetric CAS
//!   lacked. Two candidates that pick the same `counter` are still ordered by
//!   their distinct node ids, so they can never collide on one ballot.
//!
//! The derived [`Ord`]/[`PartialOrd`] follow Rust's field-declaration order, so
//! `counter` is compared first and `node` only breaks ties. This ordering is
//! pinned by [`tests`] so a field reorder can never silently invert it.

use crate::sync::topology::SyncNodeId;

/// A unique, monotonic election ballot = the per-shard epoch (§2.1).
///
/// Ordering is lexicographic by `(counter, node)`. The bottom value
/// [`Ballot::BOTTOM`]/[`Ballot::bottom`] is `(0, "")`, which is strictly less
/// than every real ballot (a real candidate's [`SyncNodeId`] is never empty and
/// a minted counter is at least `1` — see the §2.2 mint floor).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Ballot {
    /// Per-shard epoch number; dominates ordering.
    pub counter: u64,
    /// Minting node; breaks ties so every ballot is globally unique.
    pub node: SyncNodeId,
}

impl Ballot {
    /// The bottom ballot `(0, "")`, below every real ballot (§2.1 initial value).
    ///
    /// Provided as a function because [`SyncNodeId`] holds a `String` and so is
    /// not `const`-constructible; this is the canonical default seeded into a
    /// freshly-recovered shard with no persisted promise state.
    #[must_use]
    pub fn bottom() -> Self {
        Self {
            counter: 0,
            node: SyncNodeId::new(""),
        }
    }

    /// Build a ballot from a counter and minting node.
    #[must_use]
    pub const fn new(counter: u64, node: SyncNodeId) -> Self {
        Self { counter, node }
    }
}

/// A causal commit stamp `(epoch, seq)` carried by every committed write (§2.4).
///
/// The total order is lexicographic by `(epoch, seq)`:
///
/// - `epoch` — the owner's ballot the write was made under. Globally unique and
///   monotonic ACROSS owners (a re-acquired owner's ballot strictly exceeds the
///   prior one), so it dominates ordering.
/// - `seq` — the owner's monotonic per-(shard, live-epoch) write counter, advanced
///   once per committed write, ordering writes WITHIN one epoch.
///
/// `max` over this total order is the commutative/associative/idempotent
/// semilattice join the §2.4 handoff merge (3-4c) uses; the per-key chain tip is
/// always the higher stamp. The derived [`Ord`]/[`PartialOrd`] follow
/// field-declaration order (epoch first, seq second); [`tests`] pins it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Stamp {
    /// The owner ballot the write was made under; dominates ordering.
    pub epoch: Ballot,
    /// The owner's monotonic per-epoch write counter; orders within one epoch.
    pub seq: u64,
}

impl Stamp {
    /// Build a commit stamp from an epoch ballot and a per-epoch sequence number.
    #[must_use]
    pub const fn new(epoch: Ballot, seq: u64) -> Self {
        Self { epoch, seq }
    }

    /// The bottom stamp `((0,""), 0)` — below every real stamp. The stamp a
    /// 2a-compatible (un-elected, `live_epoch = bottom`) write carries.
    #[must_use]
    pub fn bottom() -> Self {
        Self {
            epoch: Ballot::bottom(),
            seq: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Ballot;
    use crate::sync::topology::SyncNodeId;

    fn ballot(counter: u64, node: &str) -> Ballot {
        Ballot::new(counter, SyncNodeId::from(node))
    }

    #[test]
    fn counter_dominates_node_in_ordering() {
        // (1,"z") < (2,"a"): a higher counter wins even with a "larger" node, so
        // counter is compared FIRST. Falsifiable — node-first ordering inverts it.
        assert!(ballot(1, "z") < ballot(2, "a"));
    }

    #[test]
    fn node_breaks_ties_at_equal_counter() {
        // (1,"a") < (1,"b"): equal counters fall back to node order.
        assert!(ballot(1, "a") < ballot(1, "b"));
    }

    #[test]
    fn bottom_is_below_every_real_ballot() {
        assert!(Ballot::bottom() < ballot(1, ""));
        assert!(Ballot::bottom() < ballot(1, "a"));
        assert!(Ballot::bottom() < ballot(0, "a"));
        assert_eq!(Ballot::bottom(), ballot(0, ""));
    }

    #[test]
    fn stamp_epoch_dominates_seq_in_ordering() {
        use super::Stamp;
        // A higher epoch wins even with a lower seq: epoch is compared FIRST.
        let lower_epoch_high_seq = Stamp::new(ballot(1, "z"), u64::MAX);
        let higher_epoch_zero_seq = Stamp::new(ballot(2, "a"), 0);
        assert!(lower_epoch_high_seq < higher_epoch_zero_seq);
    }

    #[test]
    fn stamp_seq_breaks_ties_at_equal_epoch() {
        use super::Stamp;
        // Equal epoch falls back to seq order (orders two writes by one owner).
        assert!(Stamp::new(ballot(3, "a"), 4) < Stamp::new(ballot(3, "a"), 5));
    }

    #[test]
    fn stamp_bottom_is_below_every_real_stamp() {
        use super::Stamp;
        assert!(Stamp::bottom() < Stamp::new(ballot(1, "a"), 0));
        assert!(Stamp::bottom() < Stamp::new(Ballot::bottom(), 1));
        assert_eq!(Stamp::bottom(), Stamp::new(Ballot::bottom(), 0));
    }

    #[test]
    fn ballot_is_clone_eq_hash_debug() {
        let original = ballot(7, "node-a");
        let copy = original.clone();
        assert_eq!(original, copy);

        let mut set = std::collections::HashSet::new();
        set.insert(original);
        assert!(set.contains(&copy));

        assert!(format!("{copy:?}").contains("node-a"));
    }
}
