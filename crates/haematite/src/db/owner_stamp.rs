//! AA-3-4a R-LE / R-SEQ: the in-memory per-shard serve-authority state.
//!
//! Two enforcement invariants from the REVISED §2.4 live here:
//!
//! - **R-LE (live-epoch serve authority).** A node stamps committed writes from
//!   an IN-MEMORY `live_epoch`, set ONLY by a successful `acquire_shard` in THIS
//!   process lifetime — NEVER from the disk-recovered `owner_epoch` (which exists
//!   only for the fence / promise logic). A crashed owner that recovered
//!   `owner_epoch = e'` from disk but did NOT re-acquire this lifetime must stamp
//!   `Ballot::bottom()` (2a-compat), never `e'`. Re-acquisition yields a strictly
//!   higher ballot (R4 `persisted_max_minted`), so the new serving epoch `e'' >
//!   e'` and `(e'', 0)` cannot collide with any pre-crash `(e', k)` even though
//!   `seq` restarts at 0.
//!
//! - **R-SEQ (owner-assigned, atomic).** The per-(shard, live-epoch) `seq` is
//!   drawn by ONE atomic `fetch_add` per write — no read-modify-write at the call
//!   site (no TOCTOU, cf. R8). `seq` resets to 0 when `live_epoch` changes; gaps
//!   from fenced/timed-out writes are fine. The drawn `seq` is then carried in the
//!   `WriteProposal` so every replica stores the identical `(epoch, seq)`.
//!
//! `seq` is purely in-memory and intentionally NOT persisted (persisting it would
//! force an fsync on the hot write path); the serve-authority invariant above is
//! what makes that safe.

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

use crate::branch::ShardId;
use crate::sync::ballot::{Ballot, Stamp};

/// The in-memory serve-authority for ONE shard: its live serving epoch and the
/// atomic per-epoch sequence counter.
#[derive(Debug)]
struct ShardOwnerStamp {
    /// The epoch this node may serve (stamp) writes under, set ONLY by a live
    /// `acquire_shard`. `Ballot::bottom()` means "no live election this lifetime"
    /// (2a-compat).
    live_epoch: Ballot,
    /// The next `seq` to assign under `live_epoch`. Reset to 0 whenever
    /// `live_epoch` changes.
    seq: AtomicU64,
}

impl ShardOwnerStamp {
    fn bottom() -> Self {
        Self {
            live_epoch: Ballot::bottom(),
            seq: AtomicU64::new(0),
        }
    }
}

/// Per-shard in-memory serve-authority map (R-LE / R-SEQ). Default for every
/// shard is `{ live_epoch: bottom, seq: 0 }`.
#[derive(Debug, Default)]
pub struct OwnerStamps {
    shards: DashMap<ShardId, ShardOwnerStamp>,
}

impl OwnerStamps {
    /// R-LE: record that THIS node won `won_ballot` for `shard` via a live
    /// election in this process lifetime. This is the ONLY writer of `live_epoch`.
    /// Resets the shard's `seq` to 0 for the new epoch.
    ///
    /// Idempotency note: a repeated win at the SAME ballot would reset `seq`, but
    /// `acquire_shard` always mints a STRICTLY higher ballot per attempt, so a
    /// genuine re-acquire is always a new epoch and the reset is correct.
    pub fn record_won(&self, shard: ShardId, won_ballot: Ballot) {
        self.shards.insert(
            shard,
            ShardOwnerStamp {
                live_epoch: won_ballot,
                seq: AtomicU64::new(0),
            },
        );
    }

    /// R-LE + R-SEQ: draw the next commit stamp for a write to `shard`. The epoch
    /// is the shard's in-memory `live_epoch` (NOT the disk `owner_epoch`); the
    /// `seq` is a SINGLE atomic `fetch_add` (no TOCTOU). With no live election the
    /// stamp is `(bottom, seq)` — 2a-compat.
    pub fn next_stamp(&self, shard: ShardId) -> Stamp {
        let entry = self
            .shards
            .entry(shard)
            .or_insert_with(ShardOwnerStamp::bottom);
        let seq = entry.seq.fetch_add(1, Ordering::Relaxed);
        Stamp::new(entry.live_epoch.clone(), seq)
    }

    /// Test-support: read a shard's current in-memory `live_epoch`.
    #[doc(hidden)]
    pub fn live_epoch(&self, shard: ShardId) -> Ballot {
        self.shards
            .get(&shard)
            .map_or_else(Ballot::bottom, |entry| entry.live_epoch.clone())
    }

    /// Test-support: the stamp the NEXT draw WOULD produce, WITHOUT advancing the
    /// counter (a non-mutating peek of `(live_epoch, seq)`).
    #[doc(hidden)]
    pub fn peek_stamp(&self, shard: ShardId) -> Stamp {
        self.shards.get(&shard).map_or_else(Stamp::bottom, |entry| {
            Stamp::new(entry.live_epoch.clone(), entry.seq.load(Ordering::Relaxed))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::sync::ballot::Ballot;
    use crate::sync::topology::SyncNodeId;

    use super::OwnerStamps;

    fn ballot(counter: u64, node: &str) -> Ballot {
        Ballot::new(counter, SyncNodeId::new(node))
    }

    #[test]
    fn default_stamp_is_bottom_epoch_with_monotonic_seq() {
        // With no live election a shard stamps `(bottom, seq)` and the seq is a
        // plain per-draw monotonic counter from 0 (2a-compat).
        let stamps = OwnerStamps::default();
        assert_eq!(stamps.live_epoch(0), Ballot::bottom());
        let first = stamps.next_stamp(0);
        let second = stamps.next_stamp(0);
        assert_eq!(first.epoch, Ballot::bottom());
        assert_eq!(first.seq, 0);
        assert_eq!(second.seq, 1);
    }

    #[test]
    fn record_won_sets_live_epoch_and_resets_seq() {
        let stamps = OwnerStamps::default();
        // Draw a couple of bottom-epoch stamps (seq advances).
        assert_eq!(stamps.next_stamp(0).seq, 0);
        assert_eq!(stamps.next_stamp(0).seq, 1);

        // A live win sets live_epoch and RESETS seq to 0 for the new epoch.
        stamps.record_won(0, ballot(5, "A"));
        assert_eq!(stamps.live_epoch(0), ballot(5, "A"));
        let s0 = stamps.next_stamp(0);
        let s1 = stamps.next_stamp(0);
        assert_eq!(s0.epoch, ballot(5, "A"));
        assert_eq!(s0.seq, 0, "seq resets to 0 on a new live epoch");
        assert_eq!(s1.seq, 1);

        // A higher re-acquire resets again under the new epoch.
        stamps.record_won(0, ballot(6, "A"));
        let s = stamps.next_stamp(0);
        assert_eq!(s.epoch, ballot(6, "A"));
        assert_eq!(s.seq, 0);
    }

    /// CONCURRENT-SEQ GATE: two concurrent `next_stamp` draws on the same shard
    /// must get DISTINCT seq values (atomic `fetch_add`, no TOCTOU). We hammer the
    /// counter from many threads and assert every drawn seq is unique with no gaps.
    #[test]
    fn concurrent_draws_get_distinct_seq_no_toctou() {
        const THREADS: usize = 8;
        const PER_THREAD: u64 = 1000;
        let stamps = Arc::new(OwnerStamps::default());
        stamps.record_won(3, ballot(1, "owner"));

        let mut handles = Vec::new();
        for _ in 0..THREADS {
            let stamps = Arc::clone(&stamps);
            handles.push(std::thread::spawn(move || {
                let mut seqs = Vec::with_capacity(PER_THREAD as usize);
                for _ in 0..PER_THREAD {
                    let drawn = stamps.next_stamp(3);
                    assert_eq!(drawn.epoch, ballot(1, "owner"));
                    seqs.push(drawn.seq);
                }
                seqs
            }));
        }

        let mut all = HashSet::new();
        for handle in handles {
            let seqs = handle.join().unwrap_or_default();
            assert!(!seqs.is_empty(), "worker thread produced no seqs (panicked?)");
            for seq in seqs {
                assert!(all.insert(seq), "duplicate seq {seq} — TOCTOU / non-atomic draw");
            }
        }
        // Exactly `THREADS * PER_THREAD` distinct seqs in `0..N`, contiguous.
        let total = THREADS as u64 * PER_THREAD;
        assert_eq!(all.len() as u64, total);
        for expected in 0..total {
            assert!(all.contains(&expected), "missing seq {expected}");
        }
    }
}
