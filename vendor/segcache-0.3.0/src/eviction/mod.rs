//! Eviction selects which segment to reclaim when the cache is full.
//!
//! The [`Eviction`] struct ranks segments according to the configured
//! [`Policy`] and returns them for eviction. For policies that need
//! ranking (Fifo, Cte, Util), segments are sorted periodically. For
//! stateless policies (Random, RandomFifo), ranking is skipped.

use core::cmp::{max, Ordering};
use core::num::NonZeroU32;

use ::rand::RngExt;

use crate::rng;
use crate::segments::*;
use crate::Random;
use crate::*;

mod ghost;
mod policy;

pub(crate) use ghost::GhostQueue;
pub use policy::Policy;

/// Ranks and returns segments for eviction according to the configured
/// [`Policy`].
pub struct Eviction {
    policy: Policy,
    last_update_time: Instant,
    ranked_segs: Box<[Option<NonZeroU32>]>,
    index: usize,
    rng: Box<Random>,
    /// Ghost queue for S3-FIFO (empty for other policies)
    pub(crate) ghost: GhostQueue,
}

impl Eviction {
    /// Creates a new `Eviction` which will handle up to `nseg` segments
    /// using the specified eviction policy.
    pub fn new(nseg: usize, policy: Policy) -> Self {
        let ranked_segs = vec![None; nseg].into_boxed_slice();

        // For S3-FIFO, size the ghost queue proportionally
        // (approximating the number of items in the admission pool)
        let ghost_capacity = if matches!(policy, Policy::S3Fifo { .. }) {
            std::cmp::max(1024, nseg * 64)
        } else {
            0
        };

        Self {
            policy,
            last_update_time: Instant::now(),
            ranked_segs,
            index: 0,
            rng: Box::new(rng()),
            ghost: GhostQueue::new(ghost_capacity),
        }
    }

    #[inline]
    pub fn policy(&self) -> Policy {
        self.policy
    }

    /// Returns the segment id of the least valuable segment.
    pub fn least_valuable_seg(&mut self) -> Option<NonZeroU32> {
        let index = self.index;
        self.index += 1;
        self.ranked_segs.get(index).copied().flatten()
    }

    /// Returns a random u32
    #[inline]
    pub fn random(&mut self) -> u32 {
        self.rng.random()
    }

    pub fn should_rerank(&mut self) -> bool {
        match self.policy {
            Policy::None
            | Policy::Random
            | Policy::RandomFifo
            | Policy::Merge { .. }
            | Policy::S3Fifo { .. } => false,
            Policy::Fifo | Policy::Cte | Policy::Util => {
                let now = Instant::now();
                if self.ranked_segs[0].is_none()
                    || (now - self.last_update_time).as_secs() > 1
                    || self.ranked_segs.len() < (self.index + 8)
                {
                    self.last_update_time = now;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Sort segments by the active policy's comparator.
    pub fn rerank(&mut self, headers: &[SegmentHeader]) {
        let cmp: fn(&SegmentHeader, &SegmentHeader) -> Ordering = match self.policy {
            Policy::Fifo => Self::compare_fifo,
            Policy::Cte => Self::compare_cte,
            Policy::Util => Self::compare_util,
            _ => return,
        };

        let mut ids: Vec<NonZeroU32> = headers.iter().map(|h| h.id()).collect();
        ids.sort_by(|a, b| {
            cmp(
                &headers[a.get() as usize - 1],
                &headers[b.get() as usize - 1],
            )
        });

        for (slot, id) in self.ranked_segs.iter_mut().zip(ids.iter()) {
            *slot = Some(*id);
        }
        self.index = 0;
    }

    // -- Comparators --

    fn compare_fifo(lhs: &SegmentHeader, rhs: &SegmentHeader) -> Ordering {
        if !lhs.can_evict() {
            Ordering::Greater
        } else if !rhs.can_evict() {
            Ordering::Less
        } else {
            let lhs_age = max(lhs.create_at(), lhs.merge_at());
            let rhs_age = max(rhs.create_at(), rhs.merge_at());
            lhs_age.cmp(&rhs_age)
        }
    }

    fn compare_cte(lhs: &SegmentHeader, rhs: &SegmentHeader) -> Ordering {
        if !lhs.can_evict() {
            Ordering::Greater
        } else if !rhs.can_evict() {
            Ordering::Less
        } else {
            let lhs_expire = lhs.create_at() + lhs.ttl();
            let rhs_expire = rhs.create_at() + rhs.ttl();
            lhs_expire.cmp(&rhs_expire)
        }
    }

    fn compare_util(lhs: &SegmentHeader, rhs: &SegmentHeader) -> Ordering {
        if !lhs.can_evict() {
            Ordering::Greater
        } else if !rhs.can_evict() {
            Ordering::Less
        } else {
            lhs.live_bytes().cmp(&rhs.live_bytes())
        }
    }

    // -- Merge parameters --

    /// Returns the maximum number of segments which can be merged during a
    /// single merge operation.
    #[inline]
    pub fn max_merge(&self) -> usize {
        if let Policy::Merge { max, .. } = self.policy {
            max
        } else {
            8
        }
    }

    /// Returns the number of segments to combine during an eviction merge.
    #[inline]
    pub fn n_merge(&self) -> usize {
        if let Policy::Merge { merge, .. } = self.policy {
            merge
        } else {
            4
        }
    }

    /// Returns the number of segments to combine during a compaction merge.
    #[inline]
    pub fn n_compact(&self) -> usize {
        if let Policy::Merge { compact, .. } = self.policy {
            compact
        } else {
            2
        }
    }

    /// The compact ratio serves as a low watermark for triggering compaction.
    #[inline]
    pub fn compact_ratio(&self) -> f64 {
        if self.n_compact() == 0 {
            0.0
        } else {
            1.0 / self.n_compact() as f64
        }
    }

    /// The target ratio represents the desired occupancy of a segment after
    /// eviction-based merge pruning.
    #[inline]
    pub fn target_ratio(&self) -> f64 {
        1.0 / self.n_merge() as f64
    }

    /// The stop ratio is a high watermark that causes a merge pass to stop
    /// when the target segment exceeds this occupancy.
    #[inline]
    pub fn stop_ratio(&self) -> f64 {
        self.target_ratio() * (self.n_merge() - 1) as f64 + 0.05
    }
}
