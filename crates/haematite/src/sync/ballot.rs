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
