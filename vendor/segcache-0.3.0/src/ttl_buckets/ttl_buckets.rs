//! Collection of TTL buckets covering the full TTL range.
//!
//! 1024 buckets organized in 4 logarithmic tiers:
//!
//! | Tier | TTL range          | Bucket width | Buckets |
//! |------|--------------------|--------------|---------|
//! | 1    | 1s – 2048s         | 8s           | 256     |
//! | 2    | 2048s – 32,768s    | 128s         | 256     |
//! | 3    | 32,768s – 524,288s | 2,048s       | 256     |
//! | 4    | 524,288s – 8.4Ms   | 32,768s      | 256     |
//!
//! TTL of 0 (no expiry) and TTLs beyond ~97 days map to the last bucket.

use crate::*;

const BUCKETS_PER_TIER: usize = 256;
const TIER_COUNT: usize = 4;
const TOTAL_BUCKETS: usize = BUCKETS_PER_TIER * TIER_COUNT;

// Tier widths as bit shifts (each tier is 4x wider than the previous).
const TIER_1_SHIFT: usize = 3; //   8s
const TIER_2_SHIFT: usize = 7; // 128s
const TIER_3_SHIFT: usize = 11; // 2048s
const TIER_4_SHIFT: usize = 15; // 32768s

// Tier boundaries: the max TTL (exclusive) that fits in each tier.
const TIER_1_MAX: i32 = 1 << (TIER_1_SHIFT + 8); //   2,048
const TIER_2_MAX: i32 = 1 << (TIER_2_SHIFT + 8); //  32,768
const TIER_3_MAX: i32 = 1 << (TIER_3_SHIFT + 8); // 524,288

/// The full collection of TTL buckets.
pub struct TtlBuckets {
    pub(crate) buckets: Box<[TtlBucket]>,
    pub(crate) last_expired: Instant,
}

impl TtlBuckets {
    /// Create a new set of 1024 TTL buckets covering the full TTL range.
    pub fn new() -> Self {
        let widths = [
            1 << TIER_1_SHIFT,
            1 << TIER_2_SHIFT,
            1 << TIER_3_SHIFT,
            1 << TIER_4_SHIFT,
        ];

        let mut buckets = Vec::with_capacity(TOTAL_BUCKETS);
        for width in &widths {
            for j in 0..BUCKETS_PER_TIER {
                let ttl = width * j + 1;
                buckets.push(TtlBucket::new(ttl as i32));
            }
        }

        Self {
            buckets: buckets.into_boxed_slice(),
            last_expired: Instant::now(),
        }
    }

    /// Map a TTL duration to its bucket index (0–1023).
    pub(crate) fn get_bucket_index(&self, ttl: Duration) -> usize {
        let secs = ttl.as_secs() as i32;
        if secs <= 0 {
            self.buckets.len() - 1
        } else if secs & !(TIER_1_MAX - 1) == 0 {
            (secs >> TIER_1_SHIFT) as usize
        } else if secs & !(TIER_2_MAX - 1) == 0 {
            (secs >> TIER_2_SHIFT) as usize + BUCKETS_PER_TIER
        } else if secs & !(TIER_3_MAX - 1) == 0 {
            (secs >> TIER_3_SHIFT) as usize + BUCKETS_PER_TIER * 2
        } else {
            let idx = (secs >> TIER_4_SHIFT) as usize + BUCKETS_PER_TIER * 3;
            idx.min(TOTAL_BUCKETS - 1)
        }
    }

    /// Get a mutable reference to the bucket for the given TTL.
    pub(crate) fn get_mut_bucket(&mut self, ttl: Duration) -> &mut TtlBucket {
        let index = self.get_bucket_index(ttl);
        // SAFETY: get_bucket_index always returns a valid index.
        unsafe { self.buckets.get_unchecked_mut(index) }
    }

    /// Run eager expiration across all buckets. Returns total segments expired.
    pub(crate) fn expire(
        &mut self,
        hashtable: &MultiChoiceHashtable,
        segments: &mut Segments,
    ) -> usize {
        let now = Instant::now();
        if now == self.last_expired {
            return 0;
        }
        self.last_expired = now;

        let start = Instant::now();
        let mut expired = 0;
        for bucket in self.buckets.iter_mut() {
            expired += bucket.expire(hashtable, segments);
        }
        let duration = start.elapsed();
        debug!("expired: {expired} segments in {duration:?}");

        #[cfg(feature = "metrics")]
        EXPIRE_TIME.add(duration.as_nanos() as _);

        expired
    }

    /// Clear all segments across all buckets. Returns total segments cleared.
    pub(crate) fn clear(
        &mut self,
        hashtable: &MultiChoiceHashtable,
        segments: &mut Segments,
    ) -> usize {
        let start = Instant::now();
        let mut cleared = 0;
        for bucket in self.buckets.iter_mut() {
            cleared += bucket.clear(hashtable, segments);
        }
        let duration = start.elapsed();
        debug!("cleared: {cleared} segments in {duration:?}");

        #[cfg(feature = "metrics")]
        CLEAR_TIME.add(duration.as_nanos() as _);

        cleared
    }
}

impl Default for TtlBuckets {
    fn default() -> Self {
        Self::new()
    }
}
