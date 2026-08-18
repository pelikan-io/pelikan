//! A single TTL bucket containing a segment chain.
//!
//! Items with similar TTLs are stored in segments linked together in a
//! doubly-linked chain. The head segment is always the oldest, enabling
//! O(1) expiration by checking only the head.
//!
//! ```text
//! ┌──────────────┬──────────────┬─────────────┬──────────────┐
//! │   HEAD SEG   │   TAIL SEG   │     TTL     │     NSEG     │
//! │              │              │             │              │
//! │    32 bit    │    32 bit    │    32 bit   │    32 bit    │
//! ├──────────────┼──────────────┴─────────────┴──────────────┤
//! │  NEXT MERGE  │                  PADDING                  │
//! │              │                                           │
//! │    32 bit    │                  96 bit                   │
//! ├──────────────┴───────────────────────────────────────────┤
//! │                         PADDING                          │
//! │                                                          │
//! │                         128 bit                          │
//! ├──────────────────────────────────────────────────────────┤
//! │                         PADDING                          │
//! │                                                          │
//! │                         128 bit                          │
//! └──────────────────────────────────────────────────────────┘
//! ```

use crate::*;
use core::num::NonZeroU32;

/// A TTL bucket holding a doubly-linked segment chain.
///
/// Padded to exactly 64 bytes (one cache line).
pub struct TtlBucket {
    head: Option<NonZeroU32>,
    tail: Option<NonZeroU32>,
    ttl: i32,
    nseg: i32,
    next_to_merge: Option<NonZeroU32>,
    _pad: [u8; 44],
}

impl TtlBucket {
    /// Create an empty bucket for the given TTL.
    pub(super) fn new(ttl: i32) -> Self {
        Self {
            head: None,
            tail: None,
            ttl,
            nseg: 0,
            next_to_merge: None,
            _pad: [0; 44],
        }
    }

    /// Head of the segment chain (oldest segment).
    pub fn head(&self) -> Option<NonZeroU32> {
        self.head
    }

    /// Set the head segment.
    pub fn set_head(&mut self, id: Option<NonZeroU32>) {
        self.head = id;
    }

    /// Next segment to merge (for merge eviction policy).
    pub fn next_to_merge(&self) -> Option<NonZeroU32> {
        self.next_to_merge
    }

    /// Set the next merge target.
    pub fn set_next_to_merge(&mut self, next: Option<NonZeroU32>) {
        self.next_to_merge = next;
    }

    /// Expire segments whose TTL has elapsed.
    ///
    /// Walks the chain from head, clearing and freeing segments whose
    /// `create_at + ttl <= now`. Returns the number of segments expired.
    pub(super) fn expire(
        &mut self,
        hashtable: &MultiChoiceHashtable,
        segments: &mut Segments,
    ) -> usize {
        if self.head.is_none() {
            return 0;
        }

        let mut expired = 0;
        let now = Instant::now();

        loop {
            let seg_id = match self.head {
                Some(id) => id,
                None => return expired,
            };

            let mut segment = segments.get_mut(seg_id).unwrap();
            if segment.create_at() + segment.ttl() <= now {
                self.head = segment.next_seg();
                if self.head.is_none() {
                    self.tail = None;
                }
                segment.clear(hashtable, true);
                segments.push_free(seg_id);

                #[cfg(feature = "metrics")]
                SEGMENT_EXPIRE.increment();

                expired += 1;
            } else {
                return expired;
            }
        }
    }

    /// Clear all segments in this bucket. Returns the count cleared.
    pub(super) fn clear(
        &mut self,
        hashtable: &MultiChoiceHashtable,
        segments: &mut Segments,
    ) -> usize {
        if self.head.is_none() {
            return 0;
        }

        let mut cleared = 0;

        loop {
            let seg_id = match self.head {
                Some(id) => id,
                None => return cleared,
            };

            let mut segment = segments.get_mut(seg_id).unwrap();
            self.head = segment.next_seg();
            if self.head.is_none() {
                self.tail = None;
            }
            segment.clear(hashtable, true);
            segments.push_free(seg_id);

            #[cfg(feature = "metrics")]
            SEGMENT_CLEAR.increment();

            cleared += 1;
        }
    }

    /// Allocate a new segment and link it as the tail of this bucket.
    fn try_expand(&mut self, segments: &mut Segments) -> Result<(), TtlBucketsError> {
        let id = segments.pop_free().ok_or(TtlBucketsError::NoFreeSegments)?;

        // Link the new segment after the current tail.
        if let Some(tail_id) = self.tail {
            let tail = segments.get_mut(tail_id).unwrap();
            tail.set_next_seg(Some(id));
        }

        let segment = segments.get_mut(id).unwrap();
        segment.set_prev_seg(self.tail);
        segment.set_next_seg(None);
        segment.set_ttl(Duration::from_secs(self.ttl as u32));

        if self.head.is_none() {
            debug_assert!(self.tail.is_none());
            self.head = Some(id);
        }
        self.tail = Some(id);
        self.nseg += 1;

        debug_assert!(
            !segment.evictable(),
            "fresh segment should not be evictable"
        );
        segment.set_evictable(true);
        segment.set_accessible(true);
        Ok(())
    }

    /// Reserve space for an item in this bucket's tail segment.
    ///
    /// Expands the bucket with a new segment if the current tail is full
    /// or inaccessible. Returns a `ReservedItem` pointing to the allocated
    /// space, or an error if the item is oversized or no segments are free.
    pub(crate) fn reserve(
        &mut self,
        size: usize,
        segments: &mut Segments,
    ) -> Result<ReservedItem, TtlBucketsError> {
        let seg_size = segments.segment_size() as usize;

        if size > seg_size {
            return Err(TtlBucketsError::ItemOversized { size });
        }

        loop {
            if let Some(id) = self.tail {
                if let Ok(segment) = segments.get_mut(id) {
                    if !segment.accessible() {
                        continue;
                    }
                    let offset = segment.write_offset() as usize;
                    if offset + size <= seg_size {
                        let item = segment.alloc_item(size as i32);
                        return Ok(ReservedItem::new(item, segment.id(), offset));
                    }
                }
            }
            self.try_expand(segments)?;
        }
    }
}
