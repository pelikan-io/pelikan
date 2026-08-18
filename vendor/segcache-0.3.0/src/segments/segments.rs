//! The `Segments` collection — owns all segment headers and mmap-backed data,
//! manages the free queue, and implements eviction execution (merge, S3-FIFO,
//! random, etc.).

use crate::eviction::*;
use crate::segments::segment::SEG_MAGIC;
use crate::segments::*;
use crate::*;
use core::hash::{BuildHasher, Hasher};
use core::num::NonZeroU32;
use memmap2::MmapOptions;

/// `Segments` contain all items within the cache. This struct is a collection
/// of individual `Segment`s which are represented by a `SegmentHeader` and a
/// subslice of bytes from a contiguous anonymous mmap allocation.
pub(crate) struct Segments {
    /// Segment metadata headers (one per segment, cache-line aligned).
    headers: Box<[SegmentHeader]>,
    /// Anonymous mmap-backed heap for segment data.
    data: memmap2::MmapMut,
    /// Segment size in bytes.
    segment_size: i32,
    /// Number of free segments.
    free: u32,
    /// Total number of segments.
    cap: u32,
    /// Head of the free segment queue.
    free_q: Option<NonZeroU32>,
    /// Eviction configuration and state.
    evict: Box<Eviction>,
    /// Max segments in the admission pool (S3-FIFO only, 0 for other policies).
    admission_cap: u32,
    /// Current number of segments in the admission pool.
    admission_count: u32,
}

impl Segments {
    /// Allocate and initialize segments by consuming the builder. The backing
    /// heap is an anonymous mmap region instead of a boxed slice so that large
    /// caches do not fragment the process heap.
    pub(super) fn from_builder(builder: SegmentsBuilder) -> Result<Self, SegmentsError> {
        let segment_size = builder.segment_size;
        let segments = builder.heap_size / (segment_size as usize);

        debug!(
            "heap size: {} seg size: {} segments: {}",
            builder.heap_size, segment_size, segments
        );

        assert!(
            segments < (1 << 24),
            "heap size requires too many segments, reduce heap size or increase segment size"
        );

        let evict_policy = builder.evict_policy;

        debug!("eviction policy: {evict_policy:?}");

        // Build headers array.
        let mut headers = Vec::with_capacity(0);
        headers.reserve_exact(segments);
        for idx in 0..segments {
            // SAFETY: idx + 1 is always >= 1 and constrained to < 2^24.
            let header = SegmentHeader::new(unsafe { NonZeroU32::new_unchecked(idx as u32 + 1) });
            headers.push(header);
        }
        let headers = headers.into_boxed_slice();

        // Allocate the data heap via anonymous mmap.
        let heap_size = segments * segment_size as usize;
        let mut data = MmapOptions::new().populate().len(heap_size).map_anon()?;

        // Initialize each segment and link the free queue.
        for idx in 0..segments {
            let begin = segment_size as usize * idx;
            let end = begin + segment_size as usize;

            let mut segment = Segment::from_raw_parts(&headers[idx], &mut data[begin..end]);
            segment.init();

            let id = idx as u32 + 1; // segments are 1-indexed
            headers[idx].set_prev_seg(NonZeroU32::new(id - 1));
            if id < segments as u32 {
                headers[idx].set_next_seg(NonZeroU32::new(id + 1));
            }
        }

        #[cfg(feature = "metrics")]
        {
            SEGMENT_CURRENT.set(segments as _);
            SEGMENT_FREE.set(segments as _);
        }

        let admission_cap = if let Policy::S3Fifo { admission_ratio } = evict_policy {
            (segments as f64 * admission_ratio).round() as u32
        } else {
            0
        };

        Ok(Self {
            headers,
            segment_size,
            cap: segments as u32,
            free: segments as u32,
            free_q: NonZeroU32::new(1),
            data,
            evict: Box::new(Eviction::new(segments, evict_policy)),
            admission_cap,
            admission_count: 0,
        })
    }

    // ── Pool helpers ─────────────────────────────────────────────────

    /// Check if the given pool has room for another segment.
    pub(crate) fn pool_has_room(&self, pool: SegmentPool) -> bool {
        match pool {
            SegmentPool::Admission => self.admission_count < self.admission_cap,
            SegmentPool::Main => true,
        }
    }

    /// Track a segment transitioning to the given pool.
    pub(crate) fn incr_pool(&mut self, pool: SegmentPool) {
        if pool == SegmentPool::Admission {
            self.admission_count += 1;
        }
    }

    // ── Accessors ────────────────────────────────────────────────────

    /// Return the configured eviction policy.
    #[inline]
    pub fn evict_policy(&self) -> Policy {
        self.evict.policy()
    }

    /// Return the size of each segment in bytes.
    #[inline]
    pub fn segment_size(&self) -> i32 {
        self.segment_size
    }

    /// Create a `SegmentsVerifier` for key verification in the hashtable.
    pub(crate) fn verifier(&self) -> SegmentsVerifier<'_> {
        SegmentsVerifier::new(
            &self.data[..],
            self.segment_size as usize,
            self.cap as usize,
        )
    }

    /// Returns the number of free segments.
    #[cfg(test)]
    pub fn free(&self) -> usize {
        self.free as usize
    }

    /// Returns the generation counter for a segment. Bumped each time the
    /// segment is returned to the free queue, so CAS tokens built from it
    /// are invalidated when the segment is recycled.
    #[inline]
    pub(crate) fn generation(&self, seg_id: NonZeroU32) -> u16 {
        self.headers[seg_id.get() as usize - 1].generation()
    }

    // ── Item access ──────────────────────────────────────────────────

    /// Retrieve a `RawItem` from a specific segment id at the given offset.
    /// This can take `&self` because we only need a shared reference to the
    /// header and we construct the `RawItem` directly from a data pointer.
    pub(crate) fn get_item_at(&self, seg_id: Option<NonZeroU32>, offset: usize) -> Option<RawItem> {
        let seg_id = seg_id.map(|v| v.get())?;
        trace!("getting item from: seg: {seg_id} offset: {offset}");
        assert!(seg_id <= self.cap);

        let byte_offset = self.segment_size() as usize * (seg_id as usize - 1) + offset;
        Some(RawItem::from_ptr(unsafe {
            (self.data.as_ptr() as *mut u8).add(byte_offset)
        }))
    }

    // ── Segment views ────────────────────────────────────────────────

    /// Returns a `Segment` view for the segment with the specified id. The
    /// header is borrowed as a shared reference (all fields are atomic) while
    /// the data slice is borrowed mutably.
    pub(crate) fn get_mut(&mut self, id: NonZeroU32) -> Result<Segment<'_>, SegmentsError> {
        let idx = id.get() as usize - 1;
        if idx < self.headers.len() {
            let header = &self.headers[idx];

            let seg_start = self.segment_size as usize * idx;
            let seg_end = self.segment_size as usize * (idx + 1);

            let seg_data = &mut self.data[seg_start..seg_end];

            let segment = Segment::from_raw_parts(header, seg_data);
            segment.check_magic();
            Ok(segment)
        } else {
            Err(SegmentsError::BadSegmentId)
        }
    }

    /// Gets a `Segment` view for two segments after ensuring the data borrows
    /// are disjoint. Because headers are shared refs (all fields are atomic),
    /// they can alias freely — we only need to split the data slice.
    pub(crate) fn get_mut_pair(
        &mut self,
        a: NonZeroU32,
        b: NonZeroU32,
    ) -> Result<(Segment<'_>, Segment<'_>), SegmentsError> {
        if a == b {
            return Err(SegmentsError::BadSegmentId);
        }

        let a_idx = a.get() as usize - 1;
        let b_idx = b.get() as usize - 1;
        if a_idx >= self.headers.len() || b_idx >= self.headers.len() {
            return Err(SegmentsError::BadSegmentId);
        }

        // Headers are shared refs — aliasing is fine.
        let header_a = &self.headers[a_idx];
        let header_b = &self.headers[b_idx];

        // Split data into non-overlapping slices.
        let seg_size = self.segment_size() as usize;

        // SAFETY: a_idx != b_idx is guaranteed above, so the data ranges are
        // disjoint. We split the mmap slice at the boundary between the two
        // lower-indexed and higher-indexed segments.
        {
            let data: &mut [u8] = &mut self.data;
            let split = (std::cmp::min(a_idx, b_idx) + 1) * seg_size;
            let (first, second) = data.split_at_mut(split);

            let (data_a, data_b) = if a_idx < b_idx {
                let start_a = seg_size * a_idx;
                let end_a = seg_size * (a_idx + 1);

                let start_b = (seg_size * b_idx) - first.len();
                let end_b = (seg_size * (b_idx + 1)) - first.len();

                (&mut first[start_a..end_a], &mut second[start_b..end_b])
            } else {
                let start_a = (seg_size * a_idx) - first.len();
                let end_a = (seg_size * (a_idx + 1)) - first.len();

                let start_b = seg_size * b_idx;
                let end_b = seg_size * (b_idx + 1);

                (&mut second[start_a..end_a], &mut first[start_b..end_b])
            };

            let segment_a = Segment::from_raw_parts(header_a, data_a);
            let segment_b = Segment::from_raw_parts(header_b, data_b);

            segment_a.check_magic();
            segment_b.check_magic();
            Ok((segment_a, segment_b))
        }
    }

    // ── Chain helpers ────────────────────────────────────────────────

    /// Unlink a segment from its chain by patching the prev/next pointers of
    /// its neighbours.
    ///
    /// *NOTE*: this must not be used on segments in the free queue.
    fn unlink(&mut self, id: NonZeroU32) {
        let id_idx = id.get() as usize - 1;

        if let Some(next) = self.headers[id_idx].next_seg() {
            let prev = self.headers[id_idx].prev_seg();
            self.headers[next.get() as usize - 1].set_prev_seg(prev);
        }

        if let Some(prev) = self.headers[id_idx].prev_seg() {
            let next = self.headers[id_idx].next_seg();
            self.headers[prev.get() as usize - 1].set_next_seg(next);
        }
    }

    /// Push a segment onto the front of a chain.
    fn push_front(&mut self, this: NonZeroU32, head: Option<NonZeroU32>) {
        let this_idx = this.get() as usize - 1;
        self.headers[this_idx].set_next_seg(head);
        self.headers[this_idx].set_prev_seg(None);

        if let Some(head_id) = head {
            let head_idx = head_id.get() as usize - 1;
            debug_assert!(self.headers[head_idx].prev_seg().is_none());
            self.headers[head_idx].set_prev_seg(Some(this));
        }
    }

    // ── Free queue ───────────────────────────────────────────────────

    /// Return a segment to the free queue after it has been cleared.
    pub(crate) fn push_free(&mut self, id: NonZeroU32) {
        #[cfg(feature = "metrics")]
        {
            SEGMENT_RETURN.increment();
            SEGMENT_FREE.increment();
        }

        let id_idx = id.get() as usize - 1;

        // Unlink from current chain.
        self.unlink(id);

        // Relink as the free queue head.
        self.push_front(id, self.free_q);
        self.free_q = Some(id);

        assert!(!self.headers[id_idx].evictable());
        self.headers[id_idx].set_accessible(false);

        // Decrement pool counter before resetting to default.
        if self.headers[id_idx].pool() == SegmentPool::Admission {
            self.admission_count = self.admission_count.saturating_sub(1);
        }
        self.headers[id_idx].set_pool(SegmentPool::Main);

        self.headers[id_idx].reset();

        self.free += 1;
    }

    /// Try to take a segment from the free queue. Returns the segment id which
    /// must then be linked into a segment chain.
    pub(crate) fn pop_free(&mut self) -> Option<NonZeroU32> {
        assert!(self.free <= self.cap);

        if self.free == 0 {
            None
        } else {
            #[cfg(feature = "metrics")]
            {
                SEGMENT_REQUEST.increment();
                SEGMENT_REQUEST_SUCCESS.increment();
                SEGMENT_FREE.decrement();
            }

            self.free -= 1;
            let id = self.free_q;
            assert!(id.is_some());

            let id_idx = id.unwrap().get() as usize - 1;

            if let Some(next) = self.headers[id_idx].next_seg() {
                self.free_q = Some(next);
                self.headers[next.get() as usize - 1].set_prev_seg(None);
            } else {
                self.free_q = None;
            }

            #[cfg(not(feature = "integrity"))]
            assert_eq!(self.headers[id_idx].write_offset(), 0);

            #[cfg(feature = "integrity")]
            assert_eq!(
                self.headers[id_idx].write_offset() as usize,
                std::mem::size_of_val(&SEG_MAGIC),
                "segment: ({}) in free queue has write_offset: ({})",
                id.unwrap(),
                self.headers[id_idx].write_offset()
            );

            self.headers[id_idx].mark_created();
            self.headers[id_idx].mark_merged();

            id
        }
    }

    // ── Eviction ─────────────────────────────────────────────────────

    /// Tries to clear a segment by id.
    fn clear_segment(
        &mut self,
        id: NonZeroU32,
        hashtable: &MultiChoiceHashtable,
        expire: bool,
    ) -> Result<(), ()> {
        let mut segment = self.get_mut(id).unwrap();
        if segment.next_seg().is_none() && !expire {
            Err(())
        } else {
            assert!(segment.evictable(), "segment was not evictable");
            segment.set_evictable(false);
            segment.set_accessible(false);
            segment.clear(hashtable, expire);
            Ok(())
        }
    }

    /// Perform eviction based on the configured eviction policy. A success
    /// indicates that a segment was put onto the free queue and `pop_free()`
    /// should return some segment id.
    pub fn evict(
        &mut self,
        ttl_buckets: &mut TtlBuckets,
        hashtable: &MultiChoiceHashtable,
    ) -> Result<(), SegmentsError> {
        #[cfg(feature = "metrics")]
        let now = Instant::now();

        match self.evict.policy() {
            Policy::Merge { .. } => {
                #[cfg(feature = "metrics")]
                SEGMENT_EVICT.increment();

                let mut seg_idx = self.evict.random();

                seg_idx %= self.cap;
                let ttl = self.headers[seg_idx as usize].ttl();
                let offset = ttl_buckets.get_bucket_index(ttl);
                let buckets = ttl_buckets.buckets.len();

                // Since merging starts in the middle of a segment chain, we
                // may need to loop back around to the first TTL bucket.
                for i in 0..=buckets {
                    let bucket_id = (offset + i) % buckets;
                    let ttl_bucket = &mut ttl_buckets.buckets[bucket_id];
                    if let Some(first_seg) = ttl_bucket.head() {
                        let start = ttl_bucket.next_to_merge().unwrap_or(first_seg);
                        match self.merge_evict(start, hashtable) {
                            Ok(next_to_merge) => {
                                debug!("merged ttl_bucket: {bucket_id} seg: {start}");
                                ttl_bucket.set_next_to_merge(next_to_merge);

                                #[cfg(feature = "metrics")]
                                EVICT_TIME.add(now.elapsed().as_nanos() as _);

                                return Ok(());
                            }
                            Err(_) => {
                                #[cfg(feature = "metrics")]
                                SEGMENT_EVICT_EX.increment();

                                ttl_bucket.set_next_to_merge(None);
                                continue;
                            }
                        }
                    }
                }

                #[cfg(feature = "metrics")]
                {
                    SEGMENT_EVICT_EX.increment();
                    EVICT_TIME.add(now.elapsed().as_nanos() as _);
                }

                Err(SegmentsError::NoEvictableSegments)
            }
            Policy::S3Fifo { .. } => {
                #[cfg(feature = "metrics")]
                SEGMENT_EVICT.increment();

                let result = self.s3fifo_evict(ttl_buckets, hashtable);

                #[cfg(feature = "metrics")]
                EVICT_TIME.add(now.elapsed().as_nanos() as _);

                result
            }
            Policy::None => {
                #[cfg(feature = "metrics")]
                EVICT_TIME.add(now.elapsed().as_nanos() as _);

                Err(SegmentsError::NoEvictableSegments)
            }
            _ => {
                #[cfg(feature = "metrics")]
                SEGMENT_EVICT.increment();

                if let Some(id) = self.least_valuable_seg(ttl_buckets) {
                    let result = self
                        .clear_segment(id, hashtable, false)
                        .map_err(|_| SegmentsError::EvictFailure);

                    if result.is_err() {
                        #[cfg(feature = "metrics")]
                        EVICT_TIME.add(now.elapsed().as_nanos() as _);

                        return result;
                    }

                    let id_idx = id.get() as usize - 1;
                    if self.headers[id_idx].prev_seg().is_none() {
                        let ttl_bucket = ttl_buckets.get_mut_bucket(self.headers[id_idx].ttl());
                        ttl_bucket.set_head(self.headers[id_idx].next_seg());
                    }
                    self.push_free(id);

                    #[cfg(feature = "metrics")]
                    EVICT_TIME.add(now.elapsed().as_nanos() as _);

                    Ok(())
                } else {
                    #[cfg(feature = "metrics")]
                    {
                        SEGMENT_EVICT_EX.increment();
                        EVICT_TIME.add(now.elapsed().as_nanos() as _);
                    }

                    Err(SegmentsError::NoEvictableSegments)
                }
            }
        }
    }

    /// Returns the least valuable segment based on the configured eviction
    /// policy.
    pub(crate) fn least_valuable_seg(
        &mut self,
        ttl_buckets: &mut TtlBuckets,
    ) -> Option<NonZeroU32> {
        match self.evict.policy() {
            Policy::None => None,
            Policy::Random => {
                let mut start: u32 = self.evict.random();

                start %= self.cap;

                for i in 0..self.cap {
                    let idx = (start + i) % self.cap;
                    if self.headers[idx as usize].can_evict() {
                        // SAFETY: we are always adding 1 to the index.
                        return Some(unsafe { NonZeroU32::new_unchecked(idx + 1) });
                    }
                }

                None
            }
            Policy::RandomFifo => {
                // Pick a random accessible segment and look up the head of the
                // corresponding TtlBucket. This is equivalent to a weighted
                // random over buckets by segment count.
                let mut start: u32 = self.evict.random();

                start %= self.cap;

                for i in 0..self.cap {
                    let idx = (start + i) % self.cap;
                    if self.headers[idx as usize].accessible() {
                        let ttl = self.headers[idx as usize].ttl();
                        let ttl_bucket = ttl_buckets.get_mut_bucket(ttl);
                        return ttl_bucket.head();
                    }
                }

                None
            }
            _ => {
                if self.evict.should_rerank() {
                    self.evict.rerank(&self.headers);
                }
                while let Some(id) = self.evict.least_valuable_seg() {
                    if let Ok(seg) = self.get_mut(id) {
                        if seg.can_evict() {
                            return Some(id);
                        }
                    }
                }
                None
            }
        }
    }

    // ── Remove ───────────────────────────────────────────────────────

    /// Remove a single item from a segment based on the segment id and offset.
    /// May trigger merge compaction if the merge eviction policy is active and
    /// the segment occupancy drops below the compact ratio.
    pub(crate) fn remove_at(
        &mut self,
        seg_id: NonZeroU32,
        offset: usize,
        ttl_buckets: &mut TtlBuckets,
        hashtable: &MultiChoiceHashtable,
    ) -> Result<(), SegmentsError> {
        // Remove the item.
        {
            let mut segment = self.get_mut(seg_id)?;
            segment.remove_item_at(offset);

            // If the segment is now empty and evictable, free it immediately.
            if segment.live_items() == 0 && segment.can_evict() {
                segment.clear(hashtable, false);

                segment.set_evictable(false);
                if segment.prev_seg().is_none() {
                    let ttl_bucket = ttl_buckets.get_mut_bucket(segment.ttl());
                    ttl_bucket.set_head(segment.next_seg());
                }
                self.push_free(seg_id);
                return Ok(());
            }
        }

        // For merge eviction, check if the segment is below the compact ratio
        // low watermark. If so, perform a no-evict merge (compaction only).
        if let Policy::Merge { .. } = self.evict.policy() {
            let target_ratio = self.evict.compact_ratio();

            let id_idx = seg_id.get() as usize - 1;

            let ratio = self.headers[id_idx].live_bytes() as f64 / self.segment_size() as f64;

            if ratio > target_ratio {
                return Ok(());
            }

            if let Some(next_id) = self.headers[id_idx].next_seg() {
                let next_idx = next_id.get() as usize - 1;

                if !self.headers[next_idx].can_evict() {
                    return Ok(());
                }

                let next_ratio =
                    self.headers[next_idx].live_bytes() as f64 / self.segment_size() as f64;

                if next_ratio <= target_ratio {
                    let _ = self.merge_compact(seg_id, hashtable);
                    let ttl_bucket = ttl_buckets.get_mut_bucket(self.headers[id_idx].ttl());
                    ttl_bucket.set_next_to_merge(None);
                }
            }
        }

        Ok(())
    }

    // ── Merge eviction ───────────────────────────────────────────────

    /// Count how many evictable segments follow `start` in the chain (up to
    /// `max_merge`).
    fn merge_evict_chain_len(&mut self, start: NonZeroU32) -> usize {
        let mut len = 0;
        let mut id = start;
        let max = self.evict.max_merge();

        while len < max {
            if let Ok(seg) = self.get_mut(id) {
                if seg.can_evict() {
                    len += 1;
                    match seg.next_seg() {
                        Some(i) => {
                            id = i;
                        }
                        None => {
                            break;
                        }
                    }
                } else {
                    break;
                }
            } else {
                warn!("invalid segment id: {id}");
                break;
            }
        }

        len
    }

    /// Count how many evictable segments follow `start` whose combined live
    /// bytes fit within a single segment.
    fn merge_compact_chain_len(&mut self, start: NonZeroU32) -> usize {
        let mut len = 0;
        let mut id = start;
        let max = self.evict.max_merge();
        let mut occupied = 0;
        let seg_size = self.segment_size();

        while len < max {
            if let Ok(seg) = self.get_mut(id) {
                if seg.can_evict() {
                    occupied += seg.live_bytes();
                    if occupied > seg_size {
                        break;
                    }
                    len += 1;
                    match seg.next_seg() {
                        Some(i) => {
                            id = i;
                        }
                        None => {
                            break;
                        }
                    }
                } else {
                    break;
                }
            } else {
                warn!("invalid segment id: {id}");
                break;
            }
        }

        len
    }

    /// Merge a chain of segments starting at `start`, pruning low-frequency
    /// items and copying survivors into the first segment. Returns the next
    /// segment id to merge from (if any).
    fn merge_evict(
        &mut self,
        start: NonZeroU32,
        hashtable: &MultiChoiceHashtable,
    ) -> Result<Option<NonZeroU32>, SegmentsError> {
        #[cfg(feature = "metrics")]
        SEGMENT_MERGE.increment();

        let dst_id = start;
        let chain_len = self.merge_evict_chain_len(start);

        if chain_len < 3 {
            return Err(SegmentsError::NoEvictableSegments);
        }

        let mut next_id = self.get_mut(start).map(|s| s.next_seg())?;

        // Merge state.
        let mut cutoff = 1.0;
        let mut merged = 0;

        // Fixed merge parameters.
        let max_merge = self.evict.max_merge();
        let n_merge = self.evict.n_merge();
        let stop_ratio = self.evict.stop_ratio();
        let stop_bytes = (stop_ratio * self.segment_size() as f64) as i32;

        // Dynamically set target ratio based on chain length.
        let target_ratio = if chain_len < n_merge {
            1.0 / chain_len as f64
        } else {
            self.evict.target_ratio()
        };

        // Prune and compact the destination segment.
        {
            let mut dst = self.get_mut(start)?;
            let dst_old_size = dst.live_bytes();

            trace!("prune merge with cutoff: {cutoff}");
            cutoff = dst.prune(hashtable, cutoff, target_ratio);
            trace!("cutoff is now: {cutoff}");

            dst.compact(hashtable)?;

            let dst_new_size = dst.live_bytes();
            trace!("dst {dst_id}: {dst_old_size} bytes -> {dst_new_size} bytes");

            dst.mark_merged();
            merged += 1;
        }

        // Walk the chain, pruning source segments and copying survivors into
        // the destination.
        while let Some(src_id) = next_id {
            if merged > max_merge {
                trace!("stop merge: merged max segments");
                break;
            }

            if !self.get_mut(src_id).map(|s| s.can_evict()).unwrap_or(false) {
                trace!("stop merge: can't evict source segment");
                return Ok(None);
            }

            let (mut dst, mut src) = self.get_mut_pair(dst_id, src_id)?;

            let dst_start_size = dst.live_bytes();
            let src_start_size = src.live_bytes();

            if dst_start_size >= stop_bytes {
                trace!("stop merge: target segment is full");
                break;
            }

            trace!("pruning source segment");
            cutoff = src.prune(hashtable, cutoff, target_ratio);

            trace!(
                "src {}: {} bytes -> {} bytes",
                src_id,
                src_start_size,
                src.live_bytes()
            );

            trace!("copying source into target");
            let _ = src.copy_into(&mut dst, hashtable);
            trace!("copy dropped {} bytes", src.live_bytes());

            trace!(
                "dst {}: {} bytes -> {} bytes",
                dst_id,
                dst_start_size,
                dst.live_bytes()
            );

            next_id = src.next_seg();
            src.clear(hashtable, false);
            self.push_free(src_id);
            merged += 1;
        }

        Ok(next_id)
    }

    /// Merge-compact a chain of segments without pruning. Combines segments
    /// whose total live bytes fit within one segment.
    fn merge_compact(
        &mut self,
        start: NonZeroU32,
        hashtable: &MultiChoiceHashtable,
    ) -> Result<Option<NonZeroU32>, SegmentsError> {
        #[cfg(feature = "metrics")]
        SEGMENT_MERGE.increment();

        let dst_id = start;

        let chain_len = self.merge_compact_chain_len(start);

        if chain_len < 2 {
            return Err(SegmentsError::NoEvictableSegments);
        }

        let mut next_id = self.get_mut(start).map(|s| s.next_seg())?;

        if next_id.is_none() {
            return Err(SegmentsError::NoEvictableSegments);
        }

        // Merge state.
        let mut merged = 0;

        // Fixed merge parameters.
        let seg_size = self.segment_size();
        let max_merge = self.evict.max_merge();
        let stop_ratio = self.evict.stop_ratio();
        let stop_bytes = (stop_ratio * self.segment_size() as f64) as i32;

        // Compact the destination segment.
        {
            let mut dst = self.get_mut(start)?;
            let dst_old_size = dst.live_bytes();

            dst.compact(hashtable)?;

            let dst_new_size = dst.live_bytes();
            trace!("dst {dst_id}: {dst_old_size} bytes -> {dst_new_size} bytes");

            dst.mark_merged();
            merged += 1;
        }

        // Copy sources into the destination.
        while let Some(src_id) = next_id {
            if merged > max_merge {
                trace!("stop merge: merged max segments");
                break;
            }

            if !self.get_mut(src_id).map(|s| s.can_evict()).unwrap_or(false) {
                trace!("stop merge: can't evict source segment");
                return Ok(None);
            }

            let (mut dst, mut src) = self.get_mut_pair(dst_id, src_id)?;

            let dst_start_size = dst.live_bytes();
            let src_start_size = src.live_bytes();

            if dst_start_size >= stop_bytes {
                trace!("stop merge: target segment is full");
                break;
            }

            if dst_start_size + src_start_size > seg_size {
                break;
            }

            trace!(
                "src {}: {} bytes -> {} bytes",
                src_id,
                src_start_size,
                src.live_bytes()
            );

            trace!("copying source into target");
            let _ = src.copy_into(&mut dst, hashtable);
            trace!("copy dropped {} bytes", src.live_bytes());

            trace!(
                "dst {}: {} bytes -> {} bytes",
                dst_id,
                dst_start_size,
                dst.live_bytes()
            );

            next_id = src.next_seg();
            src.clear(hashtable, false);
            self.push_free(src_id);
            merged += 1;
        }

        Ok(next_id)
    }

    // ── S3-FIFO eviction ─────────────────────────────────────────────

    /// Find the oldest evictable segment in the given pool across all TTL
    /// buckets.
    fn find_oldest_seg_in_pool(
        &self,
        ttl_buckets: &TtlBuckets,
        pool: SegmentPool,
    ) -> Option<NonZeroU32> {
        let mut best: Option<(NonZeroU32, Instant)> = None;

        for bucket in &ttl_buckets.buckets[..] {
            let mut id_opt = bucket.head();
            while let Some(id) = id_opt {
                let hdr = &self.headers[id.get() as usize - 1];
                if hdr.pool() == pool && hdr.can_evict() {
                    let age = std::cmp::max(hdr.create_at(), hdr.merge_at());
                    if best.is_none() || age < best.unwrap().1 {
                        best = Some((id, age));
                    }
                }
                id_opt = hdr.next_seg();
            }
        }

        best.map(|(id, _)| id)
    }

    /// S3-FIFO eviction entry point. Tries admission pool first (the
    /// filtering step), then main pool (CLOCK second-chance).
    fn s3fifo_evict(
        &mut self,
        ttl_buckets: &mut TtlBuckets,
        hashtable: &MultiChoiceHashtable,
    ) -> Result<(), SegmentsError> {
        // Try evicting an admission-pool segment first (promoting freq > 0).
        if let Some(seg_id) = self.find_oldest_seg_in_pool(ttl_buckets, SegmentPool::Admission) {
            return self.s3fifo_evict_admission(seg_id, ttl_buckets, hashtable);
        }

        // No admission-pool segments evictable; try main pool.
        if let Some(seg_id) = self.find_oldest_seg_in_pool(ttl_buckets, SegmentPool::Main) {
            return self.s3fifo_evict_main(seg_id, ttl_buckets, hashtable);
        }

        #[cfg(feature = "metrics")]
        SEGMENT_EVICT_EX.increment();

        Err(SegmentsError::NoEvictableSegments)
    }

    /// Evict an admission-pool segment. Items with freq > 0 are promoted
    /// (copied to a main-pool segment). Items with freq == 0 are dropped
    /// and their key hashes are added to the ghost queue.
    fn s3fifo_evict_admission(
        &mut self,
        seg_id: NonZeroU32,
        ttl_buckets: &mut TtlBuckets,
        hashtable: &MultiChoiceHashtable,
    ) -> Result<(), SegmentsError> {
        // First pass: copy items with freq > 0 into a main-pool segment.
        let target_id = self.pop_free();

        if let Some(tid) = target_id {
            self.headers[tid.get() as usize - 1].set_pool(SegmentPool::Main);

            let src_ttl = self.headers[seg_id.get() as usize - 1].ttl();
            self.headers[tid.get() as usize - 1].set_ttl(src_ttl);
            self.headers[tid.get() as usize - 1].set_accessible(true);
            self.headers[tid.get() as usize - 1].set_evictable(true);

            // Link target into the TTL bucket.
            let ttl_bucket = ttl_buckets.get_mut_bucket(src_ttl);
            let old_head = ttl_bucket.head();
            ttl_bucket.set_head(Some(tid));
            self.push_front(tid, old_head);

            self.s3fifo_promote_from(seg_id, tid, hashtable);
        }
        // If no free segment, we just drop everything (all items evicted).

        // Add hashes of remaining (freq == 0) items to ghost queue.
        self.s3fifo_ghost_remaining(seg_id, hashtable);

        // Clear and free the source segment.
        self.clear_segment(seg_id, hashtable, false)
            .map_err(|_| SegmentsError::EvictFailure)?;

        let id_idx = seg_id.get() as usize - 1;
        if self.headers[id_idx].prev_seg().is_none() {
            let ttl_bucket = ttl_buckets.get_mut_bucket(self.headers[id_idx].ttl());
            ttl_bucket.set_head(self.headers[id_idx].next_seg());
        }
        self.push_free(seg_id);

        Ok(())
    }

    /// Copy items with freq > 0 from src to dst (promotion).
    fn s3fifo_promote_from(
        &mut self,
        src_id: NonZeroU32,
        dst_id: NonZeroU32,
        hashtable: &MultiChoiceHashtable,
    ) {
        let seg_size = self.segment_size() as usize;
        let (src, dst) = match self.get_mut_pair(src_id, dst_id) {
            Ok(pair) => pair,
            Err(_) => return,
        };

        let max_offset = src.max_item_offset();
        let mut offset = if cfg!(feature = "integrity") {
            std::mem::size_of_val(&SEG_MAGIC)
        } else {
            0
        };

        while offset <= max_offset {
            let item = match src.get_item_at(offset) {
                Some(i) => i,
                None => break,
            };
            if item.klen() == 0 && src.live_items() == 0 {
                break;
            }
            item.check_magic();

            let item_size = item.size();
            let old_loc = pack_location(src.id(), offset as u64);
            if item.is_deleted() {
                offset += item_size;
                continue;
            }
            let freq = hashtable
                .get_item_frequency(item.key(), old_loc)
                .unwrap_or(0);
            if freq == 0 {
                offset += item_size;
                continue;
            }

            if freq > 0 {
                let write_offset = dst.write_offset() as usize;
                let new_loc = pack_location(dst.id(), write_offset as u64);
                if write_offset + item_size < seg_size
                    && hashtable.cas_location(item.key(), old_loc, new_loc, true)
                {
                    unsafe {
                        let s = src.data_ptr().add(offset);
                        let d = dst.data_ptr().add(write_offset);
                        std::ptr::copy_nonoverlapping(s, d, item_size);
                    }
                    src.remove_item_at(offset);
                    dst.incr_live_items();
                    dst.incr_live_bytes(item_size as i32);
                    dst.set_write_offset(write_offset as i32 + item_size as i32);

                    #[cfg(feature = "metrics")]
                    ITEM_COMPACTED.increment();
                }
                // If no room in target, item stays in source and will be evicted.
            }

            offset += item_size;
        }
    }

    /// Add hashes of remaining live items in a segment to the ghost queue.
    fn s3fifo_ghost_remaining(&mut self, seg_id: NonZeroU32, hashtable: &MultiChoiceHashtable) {
        // Collect hashes first to avoid borrow conflict with self.evict.ghost.
        let mut hashes = Vec::new();
        {
            let segment = match self.get_mut(seg_id) {
                Ok(s) => s,
                Err(_) => return,
            };

            let max_offset = segment.max_item_offset();
            let mut offset = if cfg!(feature = "integrity") {
                std::mem::size_of_val(&SEG_MAGIC)
            } else {
                0
            };

            while offset <= max_offset {
                let item = match segment.get_item_at(offset) {
                    Some(i) => i,
                    None => break,
                };
                if item.klen() == 0 {
                    break;
                }

                let item_size = item.size();
                if !item.is_deleted() {
                    let loc = pack_location(segment.id(), offset as u64);
                    let deleted = hashtable.get_item_frequency(item.key(), loc).is_none();
                    if !deleted {
                        let mut hasher = hashtable.hash_builder().build_hasher();
                        hasher.write(item.key());
                        hashes.push(hasher.finish());
                    }
                }

                offset += item_size;
            }
        }

        for hash in hashes {
            self.evict.ghost.insert(hash);
        }
    }

    /// Evict a main-pool segment using CLOCK-style second chance. Items with
    /// freq > 0 are copied to a fresh main segment. Items with freq == 0 are
    /// dropped.
    fn s3fifo_evict_main(
        &mut self,
        seg_id: NonZeroU32,
        ttl_buckets: &mut TtlBuckets,
        hashtable: &MultiChoiceHashtable,
    ) -> Result<(), SegmentsError> {
        // Try to get a target segment for second-chance items.
        let target_id = self.pop_free();

        if let Some(tid) = target_id {
            self.headers[tid.get() as usize - 1].set_pool(SegmentPool::Main);

            let src_ttl = self.headers[seg_id.get() as usize - 1].ttl();
            self.headers[tid.get() as usize - 1].set_ttl(src_ttl);
            self.headers[tid.get() as usize - 1].set_accessible(true);
            self.headers[tid.get() as usize - 1].set_evictable(true);

            let ttl_bucket = ttl_buckets.get_mut_bucket(src_ttl);
            let old_head = ttl_bucket.head();
            ttl_bucket.set_head(Some(tid));
            self.push_front(tid, old_head);

            // Copy freq > 0 items (same promote logic, but no ghost).
            self.s3fifo_promote_from(seg_id, tid, hashtable);
        }

        // Clear and free the source.
        self.clear_segment(seg_id, hashtable, false)
            .map_err(|_| SegmentsError::EvictFailure)?;

        let id_idx = seg_id.get() as usize - 1;
        if self.headers[id_idx].prev_seg().is_none() {
            let ttl_bucket = ttl_buckets.get_mut_bucket(self.headers[id_idx].ttl());
            ttl_bucket.set_head(self.headers[id_idx].next_seg());
        }
        self.push_free(seg_id);

        Ok(())
    }

    // ── Ghost queue ──────────────────────────────────────────────────

    /// Check if a key hash is in the ghost queue (S3-FIFO).
    pub(crate) fn ghost_contains(&self, hash: u64) -> bool {
        self.evict.ghost.contains(hash)
    }

    /// Remove a hash from the ghost queue (on ghost hit).
    pub(crate) fn ghost_remove(&mut self, hash: u64) {
        self.evict.ghost.remove(hash);
    }

    // ── Debug / test helpers ─────────────────────────────────────────

    /// Count the total number of live items across all segments.
    #[cfg(any(test, feature = "debug"))]
    pub(crate) fn items(&mut self) -> usize {
        let mut total = 0;
        for id in 1..=self.cap {
            // SAFETY: id starts at 1.
            let segment = self
                .get_mut(unsafe { NonZeroU32::new_unchecked(id) })
                .unwrap();
            segment.check_magic();
            let count = segment.live_items();
            debug!("{count} items in segment {id} segment: {segment:?}");
            total += segment.live_items() as usize;
        }
        total
    }

    /// Print all segment headers to stdout.
    #[cfg(test)]
    pub(crate) fn print_headers(&self) {
        for id in 0..self.cap {
            println!("segment header: {:?}", self.headers[id as usize]);
        }
    }

    /// Verify that every segment's counted live items match its header.
    #[cfg(feature = "debug")]
    pub(crate) fn check_integrity(&self, hashtable: &MultiChoiceHashtable) -> bool {
        let mut integrity = true;
        for id in 0..self.cap {
            let idx = id as usize;
            let seg_start = self.segment_size as usize * idx;
            let seg_end = seg_start + self.segment_size as usize;
            let header = &self.headers[idx];
            // SAFETY: we only read the data here; the borrow is scoped.
            let data = unsafe {
                std::slice::from_raw_parts_mut(self.data.as_ptr() as *mut u8, self.data.len())
            };
            let segment = Segment::from_raw_parts(header, &mut data[seg_start..seg_end]);
            if !segment.check_integrity(hashtable) {
                integrity = false;
            }
        }
        integrity
    }
}
