//! Builder for configuring segment storage.

use crate::eviction::*;
use crate::segments::*;

/// Configuration builder for [`Segments`].
///
/// Validation is deferred to [`build()`](SegmentsBuilder::build) so that
/// setters never panic.
pub(crate) struct SegmentsBuilder {
    pub(super) heap_size: usize,
    pub(super) segment_size: i32,
    pub(super) evict_policy: Policy,
}

impl Default for SegmentsBuilder {
    fn default() -> Self {
        Self {
            segment_size: 1024 * 1024,
            heap_size: 64 * 1024 * 1024,
            evict_policy: Policy::Random,
        }
    }
}

impl SegmentsBuilder {
    /// Set the segment size in bytes.
    pub fn segment_size(mut self, bytes: i32) -> Self {
        self.segment_size = bytes;
        self
    }

    /// Set the total heap size in bytes. The number of segments is
    /// `heap_size / segment_size`.
    pub fn heap_size(mut self, bytes: usize) -> Self {
        self.heap_size = bytes;
        self
    }

    /// Set the eviction [`Policy`].
    pub fn eviction_policy(mut self, policy: Policy) -> Self {
        self.evict_policy = policy;
        self
    }

    /// Validate configuration and build the [`Segments`].
    ///
    /// Returns an error if:
    /// - `segment_size` is not larger than the per-item header overhead
    /// - `heap_size` is zero or not a multiple of `segment_size`
    pub fn build(self) -> Result<Segments, SegmentsError> {
        let min_size = crate::ITEM_HDR_SIZE as i32 + 1;

        if self.segment_size < min_size {
            return Err(SegmentsError::SegmentTooSmall);
        }

        let seg_size = self.segment_size as usize;
        if self.heap_size == 0 || !self.heap_size.is_multiple_of(seg_size) {
            return Err(SegmentsError::InvalidHeapSize {
                heap_size: self.heap_size,
                segment_size: seg_size,
            });
        }

        Segments::from_builder(self)
    }
}
