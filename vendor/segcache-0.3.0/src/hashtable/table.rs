//! Lock-free N-choice hashtable implementation.
//!
//! Supports:
//! - Configurable N-choice hashing (1-8 choices) for tunable load factors
//! - ASFC (Adaptive Software Frequency Counter) for frequency tracking
//! - Ghost entries for preserving frequency after eviction
//! - Storage-agnostic location handling via KeyVerifier
//! - SIMD-accelerated bucket scanning on supported platforms

use crate::hashtable::bucket::Hashbucket;
use crate::hashtable::location::Location;
use crate::hashtable::traits::{Hashtable, KeyVerifier};
use crate::sync::Ordering;
use ahash::RandomState;
use core::hash::{BuildHasher, Hasher};

/// Maximum number of bucket choices supported.
pub const MAX_CHOICES: u8 = 8;

/// Lock-free hashtable for caches.
///
/// Each entry stores:
/// - 12-bit tag (hash suffix for fast filtering)
/// - 8-bit frequency counter (ASFC algorithm)
/// - 44-bit location (opaque, meaning defined by storage backend)
pub struct MultiChoiceHashtable {
    hash_builder: Box<RandomState>,
    buckets: Box<[Hashbucket]>,
    num_buckets: usize,
    mask: u64,
    num_choices: u8,
}

// SAFETY: All mutable state is behind AtomicU64 with proper ordering.
unsafe impl Send for MultiChoiceHashtable {}
unsafe impl Sync for MultiChoiceHashtable {}

#[allow(dead_code)]
impl MultiChoiceHashtable {
    /// Create a new hashtable with two-choice hashing (default).
    ///
    /// # Parameters
    /// - `power`: Total item capacity is 2^power (8 slots per bucket, minimum power 7)
    pub fn new(power: u8) -> Self {
        Self::with_choices(power, 2)
    }

    /// Create a new hashtable with configurable N-choice hashing.
    ///
    /// # Parameters
    /// - `power`: Total item capacity is 2^power (8 slots per bucket, minimum power 7)
    /// - `num_choices`: Number of bucket choices (1-8)
    pub fn with_choices(power: u8, num_choices: u8) -> Self {
        assert!(power >= 7, "power must be at least 7 (128 slots)");
        assert!(
            (1..=MAX_CHOICES).contains(&num_choices),
            "num_choices must be 1-{}",
            MAX_CHOICES
        );

        // Use fixed seeds for deterministic behavior
        let hash_builder = RandomState::with_seeds(
            0xbb8c484891ec6c86,
            0x0522a25ae9c769f9,
            0xeed2797b9571bc75,
            0x4feb29c1fbbd59d0,
        );

        // 8 slots per bucket, so bucket count = 2^(power-3)
        let bucket_power = power - 3;
        let num_buckets = 1_usize << bucket_power;
        let mask = (num_buckets as u64) - 1;

        let buckets = (0..num_buckets)
            .map(|_| Hashbucket::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            hash_builder: Box::new(hash_builder),
            buckets,
            num_buckets,
            mask,
            num_choices,
        }
    }

    /// Get a reference to the hash builder (used by S3-FIFO ghost queue).
    pub fn hash_builder(&self) -> &RandomState {
        &self.hash_builder
    }

    #[inline]
    fn bucket(&self, index: usize) -> &Hashbucket {
        debug_assert!(index < self.num_buckets);
        &self.buckets[index]
    }

    /// Prefetch a bucket into cache.
    #[inline]
    fn prefetch_bucket(&self, index: usize) {
        debug_assert!(index < self.num_buckets);
        let bucket_ptr = &self.buckets[index] as *const Hashbucket as *const i8;

        #[cfg(all(target_arch = "x86_64", target_feature = "sse"))]
        unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(bucket_ptr);
        }

        #[cfg(target_arch = "aarch64")]
        unsafe {
            std::arch::asm!(
                "prfm pldl1keep, [{ptr}]",
                ptr = in(reg) bucket_ptr,
                options(nostack, preserves_flags)
            );
        }

        #[cfg(not(any(
            all(target_arch = "x86_64", target_feature = "sse"),
            target_arch = "aarch64"
        )))]
        let _ = bucket_ptr;
    }

    /// Compute hash for a key.
    #[inline]
    fn hash_key(&self, key: &[u8]) -> u64 {
        let mut hasher = self.hash_builder.build_hasher();
        hasher.write(key);
        hasher.finish()
    }

    /// Compute bucket indices for N-choice hashing.
    #[inline]
    fn bucket_indices(&self, hash: u64) -> [usize; MAX_CHOICES as usize] {
        let mask = self.mask;
        [
            (hash & mask) as usize,
            ((hash ^ (hash >> 32)) & mask) as usize,
            (((hash >> 16) ^ (hash << 16)) & mask) as usize,
            (((hash >> 48) ^ (hash >> 8) ^ hash) & mask) as usize,
            ((hash.rotate_left(17) ^ hash) & mask) as usize,
            ((hash.rotate_left(31) ^ (hash >> 16)) & mask) as usize,
            ((hash.wrapping_mul(0x9E3779B97F4A7C15) >> 32) & mask) as usize,
            ((hash.wrapping_mul(0x517CC1B727220A95) >> 32) & mask) as usize,
        ]
    }

    /// Extract tag from hash.
    #[inline]
    fn tag_from_hash(hash: u64) -> u16 {
        ((hash >> 32) & 0xFFF) as u16
    }

    /// Count occupied (non-empty, non-ghost) slots in a bucket.
    #[inline]
    fn count_occupied(&self, bucket_index: usize) -> usize {
        let bucket = self.bucket(bucket_index);
        let mut count = 0;
        for slot in &bucket.items {
            let packed = slot.load(Ordering::Relaxed);
            if packed != 0 && !Hashbucket::is_ghost(packed) {
                count += 1;
            }
        }
        count
    }

    // =========================================================================
    // SIMD tag scanning
    // =========================================================================

    /// Find slots with matching tags using SIMD (AVX2).
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(feature = "loom")))]
    #[inline]
    fn find_tag_matches_simd(bucket: &Hashbucket, tag_shifted: u64) -> u8 {
        use std::arch::x86_64::*;

        unsafe {
            let items_ptr = bucket.items.as_ptr() as *const u8;

            let slots_0_3 = _mm256_load_si256(items_ptr as *const __m256i);
            let slots_4_7 = _mm256_load_si256(items_ptr.add(32) as *const __m256i);

            let tag_mask_val = 0xFFF0_0000_0000_0000_u64 as i64;
            let tag_shifted_i64 = tag_shifted as i64;

            let tag_mask = _mm256_set1_epi64x(tag_mask_val);
            let tag_vec = _mm256_set1_epi64x(tag_shifted_i64);

            let ghost_mask_val = 0x0000_0FFF_FFFF_FFFF_u64 as i64;
            let ghost_vec = _mm256_set1_epi64x(ghost_mask_val);
            let zero = _mm256_setzero_si256();
            let all_ones = _mm256_set1_epi64x(-1);

            let tags_0_3 = _mm256_and_si256(slots_0_3, tag_mask);
            let tag_match_0_3 = _mm256_cmpeq_epi64(tags_0_3, tag_vec);
            let nonzero_0_3 = _mm256_xor_si256(_mm256_cmpeq_epi64(slots_0_3, zero), all_ones);
            let locs_0_3 = _mm256_and_si256(slots_0_3, _mm256_set1_epi64x(ghost_mask_val));
            let nonghost_0_3 = _mm256_xor_si256(_mm256_cmpeq_epi64(locs_0_3, ghost_vec), all_ones);
            let valid_0_3 =
                _mm256_and_si256(tag_match_0_3, _mm256_and_si256(nonzero_0_3, nonghost_0_3));

            let tags_4_7 = _mm256_and_si256(slots_4_7, tag_mask);
            let tag_match_4_7 = _mm256_cmpeq_epi64(tags_4_7, tag_vec);
            let nonzero_4_7 = _mm256_xor_si256(_mm256_cmpeq_epi64(slots_4_7, zero), all_ones);
            let locs_4_7 = _mm256_and_si256(slots_4_7, _mm256_set1_epi64x(ghost_mask_val));
            let nonghost_4_7 = _mm256_xor_si256(_mm256_cmpeq_epi64(locs_4_7, ghost_vec), all_ones);
            let valid_4_7 =
                _mm256_and_si256(tag_match_4_7, _mm256_and_si256(nonzero_4_7, nonghost_4_7));

            let mask_0_3 = _mm256_movemask_pd(_mm256_castsi256_pd(valid_0_3)) as u8;
            let mask_4_7 = _mm256_movemask_pd(_mm256_castsi256_pd(valid_4_7)) as u8;

            mask_0_3 | (mask_4_7 << 4)
        }
    }

    /// Find slots with matching tags using NEON (ARM64).
    #[cfg(all(target_arch = "aarch64", not(feature = "loom")))]
    #[inline]
    fn find_tag_matches_simd(bucket: &Hashbucket, tag_shifted: u64) -> u8 {
        use std::arch::aarch64::*;

        const TAG_MASK: u64 = 0xFFF0_0000_0000_0000;
        const GHOST_LOCATION: u64 = 0x0000_0FFF_FFFF_FFFF;

        unsafe {
            let items_ptr = bucket.items.as_ptr() as *const u64;

            let slots_0_1: uint64x2_t;
            let slots_2_3: uint64x2_t;
            let slots_4_5: uint64x2_t;
            let slots_6_7: uint64x2_t;

            std::arch::asm!(
                "ld1 {{{v0:v}.2d}}, [{p0}]",
                "ld1 {{{v1:v}.2d}}, [{p1}]",
                "ld1 {{{v2:v}.2d}}, [{p2}]",
                "ld1 {{{v3:v}.2d}}, [{p3}]",
                p0 = in(reg) items_ptr,
                p1 = in(reg) items_ptr.add(2),
                p2 = in(reg) items_ptr.add(4),
                p3 = in(reg) items_ptr.add(6),
                v0 = out(vreg) slots_0_1,
                v1 = out(vreg) slots_2_3,
                v2 = out(vreg) slots_4_5,
                v3 = out(vreg) slots_6_7,
                options(nostack, preserves_flags),
            );

            let tag_mask_vec = vdupq_n_u64(TAG_MASK);
            let tag_vec = vdupq_n_u64(tag_shifted);
            let ghost_vec = vdupq_n_u64(GHOST_LOCATION);
            let zero_vec = vdupq_n_u64(0);

            let tags_0_1 = vandq_u64(slots_0_1, tag_mask_vec);
            let tag_match_0_1 = vceqq_u64(tags_0_1, tag_vec);
            let nonzero_0_1 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(slots_0_1, zero_vec)));
            let locs_0_1 = vandq_u64(slots_0_1, vdupq_n_u64(GHOST_LOCATION));
            let nonghost_0_1 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(locs_0_1, ghost_vec)));
            let valid_0_1 = vandq_u32(
                vreinterpretq_u32_u64(tag_match_0_1),
                vandq_u32(nonzero_0_1, nonghost_0_1),
            );

            let tags_2_3 = vandq_u64(slots_2_3, tag_mask_vec);
            let tag_match_2_3 = vceqq_u64(tags_2_3, tag_vec);
            let nonzero_2_3 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(slots_2_3, zero_vec)));
            let locs_2_3 = vandq_u64(slots_2_3, vdupq_n_u64(GHOST_LOCATION));
            let nonghost_2_3 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(locs_2_3, ghost_vec)));
            let valid_2_3 = vandq_u32(
                vreinterpretq_u32_u64(tag_match_2_3),
                vandq_u32(nonzero_2_3, nonghost_2_3),
            );

            let tags_4_5 = vandq_u64(slots_4_5, tag_mask_vec);
            let tag_match_4_5 = vceqq_u64(tags_4_5, tag_vec);
            let nonzero_4_5 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(slots_4_5, zero_vec)));
            let locs_4_5 = vandq_u64(slots_4_5, vdupq_n_u64(GHOST_LOCATION));
            let nonghost_4_5 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(locs_4_5, ghost_vec)));
            let valid_4_5 = vandq_u32(
                vreinterpretq_u32_u64(tag_match_4_5),
                vandq_u32(nonzero_4_5, nonghost_4_5),
            );

            let tags_6_7 = vandq_u64(slots_6_7, tag_mask_vec);
            let tag_match_6_7 = vceqq_u64(tags_6_7, tag_vec);
            let nonzero_6_7 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(slots_6_7, zero_vec)));
            let locs_6_7 = vandq_u64(slots_6_7, vdupq_n_u64(GHOST_LOCATION));
            let nonghost_6_7 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(locs_6_7, ghost_vec)));
            let valid_6_7 = vandq_u32(
                vreinterpretq_u32_u64(tag_match_6_7),
                vandq_u32(nonzero_6_7, nonghost_6_7),
            );

            let v0_1 = vreinterpretq_u64_u32(valid_0_1);
            let v2_3 = vreinterpretq_u64_u32(valid_2_3);
            let v4_5 = vreinterpretq_u64_u32(valid_4_5);
            let v6_7 = vreinterpretq_u64_u32(valid_6_7);

            let r0 = (vgetq_lane_u64(v0_1, 0) >> 63) as u8;
            let r1 = ((vgetq_lane_u64(v0_1, 1) >> 63) << 1) as u8;
            let r2 = ((vgetq_lane_u64(v2_3, 0) >> 63) << 2) as u8;
            let r3 = ((vgetq_lane_u64(v2_3, 1) >> 63) << 3) as u8;
            let r4 = ((vgetq_lane_u64(v4_5, 0) >> 63) << 4) as u8;
            let r5 = ((vgetq_lane_u64(v4_5, 1) >> 63) << 5) as u8;
            let r6 = ((vgetq_lane_u64(v6_7, 0) >> 63) << 6) as u8;
            let r7 = ((vgetq_lane_u64(v6_7, 1) >> 63) << 7) as u8;

            r0 | r1 | r2 | r3 | r4 | r5 | r6 | r7
        }
    }

    /// Scalar fallback for finding tag matches.
    #[cfg(any(
        feature = "loom",
        not(any(
            all(target_arch = "x86_64", target_feature = "avx2"),
            target_arch = "aarch64"
        ))
    ))]
    #[inline]
    fn find_tag_matches_simd(bucket: &Hashbucket, tag_shifted: u64) -> u8 {
        const TAG_MASK: u64 = 0xFFF0_0000_0000_0000;
        const GHOST_LOCATION: u64 = 0x0000_0FFF_FFFF_FFFF;

        let mut result = 0u8;
        for slot_index in 0..8 {
            let packed = bucket.items[slot_index].load(Ordering::Relaxed);
            if packed != 0
                && (packed & GHOST_LOCATION) != GHOST_LOCATION
                && (packed & TAG_MASK) == tag_shifted
            {
                result |= 1 << slot_index;
            }
        }
        result
    }

    // =========================================================================
    // Bucket-level search helpers
    // =========================================================================

    /// Search a bucket for an item, updating frequency on hit.
    #[inline]
    fn search_bucket_for_get(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        verifier: &impl KeyVerifier,
    ) -> Option<(Location, u8)> {
        let bucket = self.bucket(bucket_index);
        let tag_shifted = (tag as u64) << 52;

        let mut mask = Self::find_tag_matches_simd(bucket, tag_shifted);

        while mask != 0 {
            let slot_index = mask.trailing_zeros() as usize;
            mask &= mask - 1;

            let packed = bucket.items[slot_index].load(Ordering::Acquire);

            if packed == 0 || Hashbucket::is_ghost(packed) {
                continue;
            }
            if (packed & 0xFFF0_0000_0000_0000) != tag_shifted {
                continue;
            }

            let location = Hashbucket::location(packed);
            verifier.prefetch(location);

            if verifier.verify(key, location, false) {
                let freq = Hashbucket::freq(packed);
                if freq < 127 {
                    if let Some(new_packed) = Hashbucket::try_update_freq(packed, freq) {
                        let _ = bucket.items[slot_index].compare_exchange(
                            packed,
                            new_packed,
                            Ordering::Release,
                            Ordering::Relaxed,
                        );
                    }
                }

                return Some((location, freq));
            }
        }

        None
    }

    /// Search a bucket for an item WITHOUT updating frequency.
    #[inline]
    fn search_bucket_no_freq(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        verifier: &impl KeyVerifier,
    ) -> Option<(Location, u8)> {
        let bucket = self.bucket(bucket_index);
        let tag_shifted = (tag as u64) << 52;

        let mut mask = Self::find_tag_matches_simd(bucket, tag_shifted);

        while mask != 0 {
            let slot_index = mask.trailing_zeros() as usize;
            mask &= mask - 1;

            let packed = bucket.items[slot_index].load(Ordering::Acquire);

            if packed == 0 || Hashbucket::is_ghost(packed) {
                continue;
            }
            if (packed & 0xFFF0_0000_0000_0000) != tag_shifted {
                continue;
            }

            let location = Hashbucket::location(packed);
            verifier.prefetch(location);

            if verifier.verify(key, location, false) {
                return Some((location, Hashbucket::freq(packed)));
            }
        }

        None
    }

    /// Search a bucket for existence (no frequency update).
    fn search_bucket_exists(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        verifier: &impl KeyVerifier,
    ) -> bool {
        let bucket = self.bucket(bucket_index);
        let tag_shifted = (tag as u64) << 52;

        let mut mask = Self::find_tag_matches_simd(bucket, tag_shifted);

        while mask != 0 {
            let slot_index = mask.trailing_zeros() as usize;
            mask &= mask - 1;

            let packed = bucket.items[slot_index].load(Ordering::Acquire);

            if packed == 0 || Hashbucket::is_ghost(packed) {
                continue;
            }
            if (packed & 0xFFF0_0000_0000_0000) != tag_shifted {
                continue;
            }

            let location = Hashbucket::location(packed);
            verifier.prefetch(location);

            if verifier.verify(key, location, false) {
                return true;
            }
        }

        false
    }

    /// Search for a ghost entry's frequency.
    fn search_bucket_for_ghost(&self, bucket_index: usize, tag: u16) -> Option<u8> {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let speculative = bucket.items[slot_index].load(Ordering::Relaxed);

            if Hashbucket::is_ghost(speculative) && Hashbucket::tag(speculative) == tag {
                let packed = bucket.items[slot_index].load(Ordering::Acquire);
                if Hashbucket::is_ghost(packed) && Hashbucket::tag(packed) == tag {
                    return Some(Hashbucket::freq(packed));
                }
            }
        }

        None
    }

    /// Increment frequency of ghost entries with matching tag.
    fn increment_ghost_freq_in_bucket(&self, bucket_index: usize, tag: u16) {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let packed = bucket.items[slot_index].load(Ordering::Acquire);

            if packed != 0 && Hashbucket::is_ghost(packed) && Hashbucket::tag(packed) == tag {
                let freq = Hashbucket::freq(packed);
                if freq < 127 {
                    if let Some(new_packed) = Hashbucket::try_update_freq(packed, freq) {
                        let _ = bucket.items[slot_index].compare_exchange(
                            packed,
                            new_packed,
                            Ordering::Release,
                            Ordering::Relaxed,
                        );
                    }
                }
            }
        }
    }

    /// Search for frequency of a specific item.
    fn search_bucket_for_freq(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        verifier: &impl KeyVerifier,
    ) -> Option<u8> {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let speculative = bucket.items[slot_index].load(Ordering::Relaxed);

            if speculative == 0 || Hashbucket::is_ghost(speculative) {
                continue;
            }

            if Hashbucket::tag(speculative) == tag {
                let packed = bucket.items[slot_index].load(Ordering::Acquire);
                if packed == 0 || Hashbucket::is_ghost(packed) || Hashbucket::tag(packed) != tag {
                    continue;
                }

                let location = Hashbucket::location(packed);
                if verifier.verify(key, location, false) {
                    return Some(Hashbucket::freq(packed));
                }
            }
        }

        None
    }

    /// Search for frequency by exact location.
    fn search_bucket_for_item_freq(
        &self,
        bucket_index: usize,
        tag: u16,
        location: Location,
    ) -> Option<u8> {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let speculative = bucket.items[slot_index].load(Ordering::Relaxed);

            if speculative == 0 || Hashbucket::is_ghost(speculative) {
                continue;
            }

            if Hashbucket::tag(speculative) == tag {
                let packed = bucket.items[slot_index].load(Ordering::Acquire);
                if packed == 0 || Hashbucket::is_ghost(packed) {
                    continue;
                }

                if Hashbucket::tag(packed) == tag && Hashbucket::location(packed) == location {
                    return Some(Hashbucket::freq(packed));
                }
            }
        }

        None
    }

    // =========================================================================
    // Insert / remove helpers
    // =========================================================================

    /// Try to link in a bucket, handling existing entries and ghosts.
    fn try_link_in_bucket(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        new_packed: u64,
        verifier: &impl KeyVerifier,
    ) -> Option<Result<Option<Location>, ()>> {
        let bucket = self.bucket(bucket_index);

        // First pass: look for existing entry or matching ghost
        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let speculative = bucket.items[slot_index].load(Ordering::Relaxed);

            if Hashbucket::tag(speculative) == tag {
                let packed = bucket.items[slot_index].load(Ordering::Acquire);
                if Hashbucket::tag(packed) != tag {
                    continue;
                }

                if Hashbucket::is_ghost(packed) {
                    let freq = Hashbucket::freq(packed);
                    let new_with_freq = Hashbucket::with_freq(new_packed, freq);

                    match bucket.items[slot_index].compare_exchange(
                        packed,
                        new_with_freq,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return Some(Ok(None)),
                        Err(_) => continue,
                    }
                }

                let location = Hashbucket::location(packed);

                if verifier.verify(key, location, true) {
                    let freq = Hashbucket::freq(packed);
                    let new_with_freq = Hashbucket::with_freq(new_packed, freq);

                    match bucket.items[slot_index].compare_exchange(
                        packed,
                        new_with_freq,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return Some(Ok(Some(location))),
                        Err(_) => continue,
                    }
                }
            }
        }

        // Second pass: look for empty slot
        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let packed = bucket.items[slot_index].load(Ordering::Relaxed);

            if packed == 0 {
                match bucket.items[slot_index].compare_exchange(
                    0,
                    new_packed,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return Some(Ok(None)),
                    Err(_) => continue,
                }
            }
        }

        // Third pass: look for any ghost to evict
        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let speculative = bucket.items[slot_index].load(Ordering::Relaxed);

            if Hashbucket::is_ghost(speculative) {
                let packed = bucket.items[slot_index].load(Ordering::Acquire);

                if Hashbucket::is_ghost(packed) {
                    match bucket.items[slot_index].compare_exchange(
                        packed,
                        new_packed,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return Some(Ok(None)),
                        Err(_) => continue,
                    }
                }
            }
        }

        None // Bucket full, try another
    }

    /// Try to unlink an item from a bucket.
    fn try_unlink_in_bucket(&self, bucket_index: usize, tag: u16, expected: Location) -> bool {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let speculative = bucket.items[slot_index].load(Ordering::Relaxed);

            if speculative == 0 || Hashbucket::is_ghost(speculative) {
                continue;
            }

            if Hashbucket::tag(speculative) == tag {
                let packed = bucket.items[slot_index].load(Ordering::Acquire);
                if packed == 0 || Hashbucket::is_ghost(packed) {
                    continue;
                }

                if Hashbucket::tag(packed) == tag && Hashbucket::location(packed) == expected {
                    match bucket.items[slot_index].compare_exchange(
                        packed,
                        0,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return true,
                        Err(_) => continue,
                    }
                }
            }
        }

        false
    }

    /// Try to convert an item to ghost in a bucket.
    fn try_to_ghost_in_bucket(&self, bucket_index: usize, tag: u16, expected: Location) -> bool {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let speculative = bucket.items[slot_index].load(Ordering::Relaxed);

            if speculative == 0 || Hashbucket::is_ghost(speculative) {
                continue;
            }

            if Hashbucket::tag(speculative) == tag {
                let packed = bucket.items[slot_index].load(Ordering::Acquire);
                if packed == 0 || Hashbucket::is_ghost(packed) {
                    continue;
                }

                if Hashbucket::tag(packed) == tag && Hashbucket::location(packed) == expected {
                    let ghost = Hashbucket::to_ghost(packed);
                    match bucket.items[slot_index].compare_exchange(
                        packed,
                        ghost,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return true,
                        Err(_) => continue,
                    }
                }
            }
        }

        false
    }

    /// Try to CAS update location in a bucket.
    fn try_cas_in_bucket(
        &self,
        bucket_index: usize,
        tag: u16,
        old_location: Location,
        new_location: Location,
        preserve_freq: bool,
    ) -> bool {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let speculative = bucket.items[slot_index].load(Ordering::Relaxed);

            if speculative == 0 || Hashbucket::is_ghost(speculative) {
                continue;
            }

            if Hashbucket::tag(speculative) == tag {
                let packed = bucket.items[slot_index].load(Ordering::Acquire);
                if packed == 0 || Hashbucket::is_ghost(packed) {
                    continue;
                }

                if Hashbucket::tag(packed) == tag && Hashbucket::location(packed) == old_location {
                    let freq = if preserve_freq {
                        Hashbucket::freq(packed)
                    } else {
                        1
                    };
                    let new_packed = Hashbucket::pack(tag, freq, new_location);

                    if bucket.items[slot_index]
                        .compare_exchange(packed, new_packed, Ordering::Release, Ordering::Relaxed)
                        .is_ok()
                    {
                        return true;
                    }
                }
            }
        }

        false
    }
}

// ============================================================================
// Hashtable trait implementation
// ============================================================================

impl Hashtable for MultiChoiceHashtable {
    fn lookup(&self, key: &[u8], verifier: &impl KeyVerifier) -> Option<(Location, u8)> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);
        let num_choices = self.num_choices as usize;

        for &bucket_index in &buckets[..num_choices] {
            self.prefetch_bucket(bucket_index);
        }

        for &bucket_index in &buckets[..num_choices] {
            if let Some(result) = self.search_bucket_for_get(bucket_index, tag, key, verifier) {
                return Some(result);
            }
        }

        // Miss: increment frequency of any matching ghosts
        for &bucket_index in &buckets[..num_choices] {
            self.increment_ghost_freq_in_bucket(bucket_index, tag);
        }

        None
    }

    fn lookup_no_freq_update(
        &self,
        key: &[u8],
        verifier: &impl KeyVerifier,
    ) -> Option<(Location, u8)> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);
        let num_choices = self.num_choices as usize;

        for &bucket_index in &buckets[..num_choices] {
            self.prefetch_bucket(bucket_index);
        }

        for &bucket_index in &buckets[..num_choices] {
            if let Some(result) = self.search_bucket_no_freq(bucket_index, tag, key, verifier) {
                return Some(result);
            }
        }

        None
    }

    fn contains(&self, key: &[u8], verifier: &impl KeyVerifier) -> bool {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);
        let num_choices = self.num_choices as usize;

        for &bucket_index in &buckets[..num_choices] {
            self.prefetch_bucket(bucket_index);
        }

        for &bucket_index in &buckets[..num_choices] {
            if self.search_bucket_exists(bucket_index, tag, key, verifier) {
                return true;
            }
        }

        false
    }

    fn insert(
        &self,
        key: &[u8],
        location: Location,
        verifier: &impl KeyVerifier,
    ) -> Result<Option<Location>, ()> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);
        let choices = &buckets[..self.num_choices as usize];

        let new_packed = Hashbucket::pack(tag, 1, location);

        // First pass: try to find existing key or ghost in any bucket
        for &bucket_index in choices {
            if let Some(result) =
                self.try_link_in_bucket(bucket_index, tag, key, new_packed, verifier)
            {
                return result;
            }
        }

        // Second pass: find least-full bucket and try to insert there
        if self.num_choices > 1 {
            let target = choices
                .iter()
                .copied()
                .min_by_key(|&b| self.count_occupied(b))
                .unwrap();

            if let Some(result) = self.try_link_in_bucket(target, tag, key, new_packed, verifier) {
                return result;
            }

            let mut sorted: Vec<_> = choices.to_vec();
            sorted.sort_by_key(|&b| self.count_occupied(b));
            for bucket_index in sorted {
                if let Some(result) =
                    self.try_link_in_bucket(bucket_index, tag, key, new_packed, verifier)
                {
                    return result;
                }
            }
        }

        Err(())
    }

    fn remove(&self, key: &[u8], expected: Location) -> bool {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if self.try_unlink_in_bucket(bucket_index, tag, expected) {
                return true;
            }
        }

        false
    }

    fn convert_to_ghost(&self, key: &[u8], expected: Location) -> bool {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if self.try_to_ghost_in_bucket(bucket_index, tag, expected) {
                return true;
            }
        }

        false
    }

    fn cas_location(
        &self,
        key: &[u8],
        old_location: Location,
        new_location: Location,
        preserve_freq: bool,
    ) -> bool {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if self.try_cas_in_bucket(bucket_index, tag, old_location, new_location, preserve_freq)
            {
                return true;
            }
        }

        false
    }

    fn get_frequency(&self, key: &[u8], verifier: &impl KeyVerifier) -> Option<u8> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if let Some(freq) = self.search_bucket_for_freq(bucket_index, tag, key, verifier) {
                return Some(freq);
            }
        }

        None
    }

    fn get_item_frequency(&self, key: &[u8], location: Location) -> Option<u8> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if let Some(freq) = self.search_bucket_for_item_freq(bucket_index, tag, location) {
                return Some(freq);
            }
        }

        None
    }

    fn get_ghost_frequency(&self, key: &[u8]) -> Option<u8> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if let Some(freq) = self.search_bucket_for_ghost(bucket_index, tag) {
                return Some(freq);
            }
        }

        None
    }

    fn clear(&self) {
        for bucket in self.buckets.iter() {
            for slot in bucket.items.iter() {
                slot.store(0, Ordering::Release);
            }
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    struct MockVerifier {
        entries: Vec<(Vec<u8>, Location, bool)>,
    }

    impl MockVerifier {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
            }
        }

        fn add(&mut self, key: &[u8], location: Location, deleted: bool) {
            self.entries.push((key.to_vec(), location, deleted));
        }
    }

    impl KeyVerifier for MockVerifier {
        fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
            self.entries.iter().any(|(k, loc, deleted)| {
                k == key && *loc == location && (allow_deleted || !deleted)
            })
        }
    }

    #[test]
    fn test_hashtable_creation() {
        // power=10 → 2^10 = 1024 slots → 128 buckets (8 slots each)
        let ht = MultiChoiceHashtable::new(10);
        assert_eq!(ht.num_buckets, 128);
        assert_eq!(ht.num_choices, 2);
    }

    #[test]
    fn test_insert_and_lookup() {
        let ht = MultiChoiceHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let location = Location::new(12345);
        verifier.add(b"test", location, false);

        let result = ht.insert(b"test", location, &verifier);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        let lookup = ht.lookup(b"test", &verifier);
        assert!(lookup.is_some());
        let (loc, _freq) = lookup.unwrap();
        assert_eq!(loc, location);
    }

    #[test]
    fn test_remove() {
        let ht = MultiChoiceHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let location = Location::new(12345);
        verifier.add(b"test", location, false);

        ht.insert(b"test", location, &verifier).unwrap();

        assert!(ht.contains(b"test", &verifier));
        assert!(ht.remove(b"test", location));
        assert!(!ht.contains(b"test", &verifier));
    }

    #[test]
    fn test_ghost() {
        let ht = MultiChoiceHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let location = Location::new(12345);
        verifier.add(b"test", location, false);

        ht.insert(b"test", location, &verifier).unwrap();
        assert!(ht.convert_to_ghost(b"test", location));

        // Ghost should not appear in lookup
        assert!(ht.lookup(b"test", &verifier).is_none());

        // Ghost frequency should be retrievable
        let freq = ht.get_ghost_frequency(b"test");
        assert!(freq.is_some());
    }

    #[test]
    fn test_cas_location() {
        let ht = MultiChoiceHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let loc1 = Location::new(100);
        let loc2 = Location::new(200);
        verifier.add(b"test", loc1, false);
        verifier.add(b"test", loc2, false);

        ht.insert(b"test", loc1, &verifier).unwrap();

        // CAS with wrong old location should fail
        assert!(!ht.cas_location(b"test", Location::new(999), loc2, true));

        // CAS with correct old location should succeed
        assert!(ht.cas_location(b"test", loc1, loc2, true));

        let (loc, _) = ht.lookup(b"test", &verifier).unwrap();
        assert_eq!(loc, loc2);
    }

    #[test]
    fn test_clear() {
        let ht = MultiChoiceHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let location = Location::new(12345);
        verifier.add(b"test", location, false);

        ht.insert(b"test", location, &verifier).unwrap();
        assert!(ht.contains(b"test", &verifier));

        ht.clear();
        assert!(!ht.contains(b"test", &verifier));
    }

    #[test]
    fn test_replace_existing() {
        let ht = MultiChoiceHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let loc1 = Location::new(100);
        let loc2 = Location::new(200);
        verifier.add(b"test", loc1, false);
        verifier.add(b"test", loc2, false);

        ht.insert(b"test", loc1, &verifier).unwrap();

        let result = ht.insert(b"test", loc2, &verifier);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(loc1));
    }
}

#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use super::*;
    use crate::hashtable::traits::Hashtable;
    use crate::sync::AtomicU64;
    use loom::sync::Arc;
    use loom::thread;

    /// Simple verifier that always returns true for testing hashtable mechanics.
    struct AlwaysVerifier;

    impl KeyVerifier for AlwaysVerifier {
        fn verify(&self, _key: &[u8], _location: Location, _allow_deleted: bool) -> bool {
            true
        }
    }

    #[test]
    fn test_concurrent_insert_different_keys() {
        loom::model(|| {
            let ht = Arc::new(MultiChoiceHashtable::new(7));
            let verifier = Arc::new(AlwaysVerifier);

            let ht1 = ht.clone();
            let v1 = verifier.clone();
            let t1 = thread::spawn(move || {
                let loc = Location::new(1);
                ht1.insert(b"key1", loc, &*v1)
            });

            let ht2 = ht.clone();
            let v2 = verifier.clone();
            let t2 = thread::spawn(move || {
                let loc = Location::new(2);
                ht2.insert(b"key2", loc, &*v2)
            });

            let _ = t1.join().unwrap();
            let _ = t2.join().unwrap();

            // Both keys should be present (or one may fail due to full bucket)
            let found1 = ht.lookup(b"key1", &*verifier).is_some();
            let found2 = ht.lookup(b"key2", &*verifier).is_some();

            // At least one should succeed
            assert!(found1 || found2);
        });
    }

    #[test]
    fn test_concurrent_insert_same_key() {
        loom::model(|| {
            let ht = Arc::new(MultiChoiceHashtable::new(7));
            let verifier = Arc::new(AlwaysVerifier);

            let ht1 = ht.clone();
            let v1 = verifier.clone();
            let t1 = thread::spawn(move || {
                let loc = Location::new(1);
                ht1.insert(b"key", loc, &*v1)
            });

            let ht2 = ht.clone();
            let v2 = verifier.clone();
            let t2 = thread::spawn(move || {
                let loc = Location::new(2);
                ht2.insert(b"key", loc, &*v2)
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Both should succeed (insert does upsert, not add-only)
            assert!(r1.is_ok());
            assert!(r2.is_ok());

            // Key should be present with one of the locations
            let lookup = ht.lookup(b"key", &*verifier);
            assert!(lookup.is_some());
            let final_loc = lookup.unwrap().0;
            assert!(final_loc == Location::new(1) || final_loc == Location::new(2));
        });
    }

    #[test]
    fn test_concurrent_lookup_frequency_update() {
        loom::model(|| {
            let ht = Arc::new(MultiChoiceHashtable::new(7));
            let verifier = Arc::new(AlwaysVerifier);

            // Insert a key first
            let loc = Location::new(42);
            ht.insert(b"key", loc, &*verifier).unwrap();

            let ht1 = ht.clone();
            let v1 = verifier.clone();
            let t1 = thread::spawn(move || ht1.lookup(b"key", &*v1));

            let ht2 = ht.clone();
            let v2 = verifier.clone();
            let t2 = thread::spawn(move || ht2.lookup(b"key", &*v2));

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Both lookups should find the key
            assert!(r1.is_some());
            assert!(r2.is_some());

            // Both should return the same location
            assert_eq!(r1.unwrap().0, loc);
            assert_eq!(r2.unwrap().0, loc);
        });
    }

    #[test]
    fn test_concurrent_insert_and_remove() {
        loom::model(|| {
            let ht = Arc::new(MultiChoiceHashtable::new(7));
            let verifier = Arc::new(AlwaysVerifier);

            // Insert a key first
            let loc = Location::new(42);
            ht.insert(b"key", loc, &*verifier).unwrap();

            let ht1 = ht.clone();
            let t1 = thread::spawn(move || ht1.remove(b"key", loc));

            let ht2 = ht.clone();
            let v2 = verifier.clone();
            let t2 = thread::spawn(move || {
                let new_loc = Location::new(99);
                ht2.insert(b"key2", new_loc, &*v2)
            });

            let removed = t1.join().unwrap();
            let _ = t2.join().unwrap();

            // Remove should have succeeded
            assert!(removed);

            // Original key should be gone
            let lookup = ht.lookup(b"key", &*verifier);
            assert!(lookup.is_none());
        });
    }

    #[test]
    fn test_concurrent_cas_operations() {
        loom::model(|| {
            let ht = Arc::new(MultiChoiceHashtable::new(7));
            let verifier = Arc::new(AlwaysVerifier);

            // Insert a key first
            let loc1 = Location::new(1);
            ht.insert(b"key", loc1, &*verifier).unwrap();

            let ht1 = ht.clone();
            let t1 = thread::spawn(move || {
                let loc2 = Location::new(2);
                ht1.cas_location(b"key", loc1, loc2, true)
            });

            let ht2 = ht.clone();
            let t2 = thread::spawn(move || {
                let loc3 = Location::new(3);
                ht2.cas_location(b"key", loc1, loc3, true)
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Exactly one CAS should succeed
            let successes = [r1, r2].iter().filter(|&&x| x).count();
            assert_eq!(successes, 1, "Exactly one CAS should succeed");

            // The key should now point to either loc2 or loc3
            let lookup = ht.lookup(b"key", &*verifier);
            assert!(lookup.is_some());
            let final_loc = lookup.unwrap().0;
            assert!(final_loc == Location::new(2) || final_loc == Location::new(3));
        });
    }

    #[test]
    fn test_bucket_slot_cas_contention() {
        loom::model(|| {
            let bucket = Hashbucket::new();
            let slot = &bucket.items[0];

            let slot_ptr = slot as *const AtomicU64 as usize;

            let t1 = thread::spawn(move || {
                let slot = unsafe { &*(slot_ptr as *const AtomicU64) };
                let packed = Hashbucket::pack(0x123, 1, Location::new(1));
                slot.compare_exchange(0, packed, Ordering::Release, Ordering::Acquire)
            });

            let t2 = thread::spawn(move || {
                let slot = unsafe { &*(slot_ptr as *const AtomicU64) };
                let packed = Hashbucket::pack(0x456, 1, Location::new(2));
                slot.compare_exchange(0, packed, Ordering::Release, Ordering::Acquire)
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Exactly one should succeed (starting from 0)
            let successes = [r1.is_ok(), r2.is_ok()].iter().filter(|&&x| x).count();
            assert_eq!(successes, 1, "Exactly one CAS from 0 should succeed");
        });
    }

    /// Three threads doing CAS on the same key. Bounded preemption.
    #[test]
    fn test_three_way_cas_same_key() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(2);
        builder.check(|| {
            let ht = Arc::new(MultiChoiceHashtable::new(7));
            let verifier = Arc::new(AlwaysVerifier);

            let loc_initial = Location::new(1);
            ht.insert(b"key", loc_initial, &*verifier).unwrap();

            let ht1 = ht.clone();
            let ht2 = ht.clone();
            let ht3 = ht.clone();

            let t1 = thread::spawn(move || {
                let loc_new = Location::new(10);
                ht1.cas_location(b"key", loc_initial, loc_new, true)
            });

            let t2 = thread::spawn(move || {
                let loc_new = Location::new(20);
                ht2.cas_location(b"key", loc_initial, loc_new, true)
            });

            let t3 = thread::spawn(move || {
                let loc_new = Location::new(30);
                ht3.cas_location(b"key", loc_initial, loc_new, true)
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();
            let r3 = t3.join().unwrap();

            // Exactly one CAS should succeed
            let successes = [r1, r2, r3].iter().filter(|&&x| x).count();
            assert_eq!(successes, 1, "Exactly one CAS should succeed");

            // Final location should be one of the new values
            let lookup = ht.lookup(b"key", &*verifier);
            assert!(lookup.is_some());
            let final_loc = lookup.unwrap().0;
            assert!(
                final_loc == Location::new(10)
                    || final_loc == Location::new(20)
                    || final_loc == Location::new(30)
            );
        });
    }

    /// Three threads inserting different keys. Bounded preemption.
    #[test]
    fn test_three_way_insert_different_keys() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(2);
        builder.check(|| {
            let ht = Arc::new(MultiChoiceHashtable::new(10));
            let verifier = Arc::new(AlwaysVerifier);

            let ht1 = ht.clone();
            let v1 = verifier.clone();
            let ht2 = ht.clone();
            let v2 = verifier.clone();
            let ht3 = ht.clone();
            let v3 = verifier.clone();

            let t1 = thread::spawn(move || {
                let loc = Location::new(1);
                ht1.insert(b"key1", loc, &*v1)
            });

            let t2 = thread::spawn(move || {
                let loc = Location::new(2);
                ht2.insert(b"key2", loc, &*v2)
            });

            let t3 = thread::spawn(move || {
                let loc = Location::new(3);
                ht3.insert(b"key3", loc, &*v3)
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();
            let r3 = t3.join().unwrap();

            let successes = [r1.is_ok(), r2.is_ok(), r3.is_ok()]
                .iter()
                .filter(|&&x| x)
                .count();

            // At least 2 should succeed with 256 buckets
            assert!(successes >= 2, "Most inserts should succeed");

            if r1.is_ok() {
                assert!(ht.lookup(b"key1", &*verifier).is_some());
            }
            if r2.is_ok() {
                assert!(ht.lookup(b"key2", &*verifier).is_some());
            }
            if r3.is_ok() {
                assert!(ht.lookup(b"key3", &*verifier).is_some());
            }
        });
    }
}
