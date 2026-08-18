//! Lock-free N-choice hashtable with SIMD-accelerated bucket scanning.
//!
//! The hashtable maps keys to opaque [`Location`] values using 12-bit tags,
//! 8-bit frequency counters, and N-choice hashing. Ghost entries preserve
//! frequency counters after eviction for second-chance admission.
//!
//! The hashtable is fully decoupled from storage via the [`KeyVerifier`] trait.
//! Storage backends implement this trait to verify tag matches against actual keys.

pub(crate) mod bucket;
pub(crate) mod location;
pub(crate) mod table;
pub(crate) mod traits;

pub use location::Location;
pub(crate) use table::MultiChoiceHashtable;
pub(crate) use traits::{Hashtable, KeyVerifier};

use core::num::NonZeroU32;
use keyvalue::RawItem;

/// Pack a segment id and offset into a Location.
///
/// Layout (44 bits total):
/// - bits 43..20: segment id (24 bits)
/// - bits 19..0: offset / 8 (20 bits, 8-byte aligned)
#[inline]
pub(crate) fn pack_location(seg_id: NonZeroU32, offset: u64) -> Location {
    Location::new(((seg_id.get() as u64) << 20) | (offset >> 3))
}

/// Unpack a Location into (segment_id, byte_offset).
///
/// Returns (0, _) for invalid locations — callers must check.
#[inline]
pub(crate) fn unpack_location(loc: Location) -> (u32, usize) {
    let raw = loc.as_raw();
    let seg_id = (raw >> 20) as u32;
    let offset = ((raw & 0xFFFFF) << 3) as usize;
    (seg_id, offset)
}

/// Adapter that implements [`KeyVerifier`] for the existing Segments data buffer.
///
/// This is temporary — it will be removed when Segments is replaced in Phase 2.
/// It only needs read access to the segment data for key comparison.
pub(crate) struct SegmentsVerifier<'a> {
    data: &'a [u8],
    segment_size: usize,
    num_segments: usize,
}

impl<'a> SegmentsVerifier<'a> {
    /// Create a new verifier from the segments data buffer.
    #[inline]
    pub(crate) fn new(data: &'a [u8], segment_size: usize, num_segments: usize) -> Self {
        Self {
            data,
            segment_size,
            num_segments,
        }
    }
}

impl KeyVerifier for SegmentsVerifier<'_> {
    fn verify(&self, key: &[u8], location: Location, _allow_deleted: bool) -> bool {
        let (seg_id, offset) = unpack_location(location);

        if seg_id == 0 || seg_id as usize > self.num_segments {
            return false;
        }

        let byte_offset = self.segment_size * (seg_id as usize - 1) + offset;

        if byte_offset + keyvalue::ITEM_HDR_SIZE > self.data.len() {
            return false;
        }

        // SAFETY: We verified the offset is within the data buffer.
        // The data buffer is the segment heap and items are written with valid headers.
        let item = RawItem::from_ptr(unsafe { (self.data.as_ptr() as *mut u8).add(byte_offset) });
        item.key() == key
    }

    #[inline]
    fn prefetch(&self, location: Location) {
        let (seg_id, offset) = unpack_location(location);
        if seg_id == 0 || seg_id as usize > self.num_segments {
            return;
        }
        let byte_offset = self.segment_size * (seg_id as usize - 1) + offset;
        if byte_offset >= self.data.len() {
            return;
        }
        let ptr = unsafe { self.data.as_ptr().add(byte_offset) as *const i8 };

        #[cfg(all(target_arch = "x86_64", target_feature = "sse"))]
        unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(ptr);
        }

        #[cfg(target_arch = "aarch64")]
        unsafe {
            std::arch::asm!(
                "prfm pldl1keep, [{ptr}]",
                ptr = in(reg) ptr,
                options(nostack, preserves_flags)
            );
        }

        #[cfg(not(any(
            all(target_arch = "x86_64", target_feature = "sse"),
            target_arch = "aarch64"
        )))]
        let _ = ptr;
    }
}

// SAFETY: SegmentsVerifier only holds a shared reference to a byte slice.
unsafe impl Send for SegmentsVerifier<'_> {}
unsafe impl Sync for SegmentsVerifier<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_unpack_roundtrip() {
        let seg_id = NonZeroU32::new(42).unwrap();
        let offset = 1024u64; // must be 8-byte aligned

        let loc = pack_location(seg_id, offset);
        let (unpacked_seg, unpacked_offset) = unpack_location(loc);

        assert_eq!(unpacked_seg, 42);
        assert_eq!(unpacked_offset, 1024);
    }

    #[test]
    fn test_pack_max_seg_id() {
        // 24-bit max segment id
        let seg_id = NonZeroU32::new((1 << 24) - 1).unwrap();
        let offset = 0u64;

        let loc = pack_location(seg_id, offset);
        let (unpacked_seg, _) = unpack_location(loc);
        assert_eq!(unpacked_seg, (1 << 24) - 1);
    }

    #[test]
    fn test_pack_max_offset() {
        let seg_id = NonZeroU32::new(1).unwrap();
        // 20-bit offset field × 8 = max ~8MB offset
        let offset = ((1u64 << 20) - 1) << 3;

        let loc = pack_location(seg_id, offset);
        let (_, unpacked_offset) = unpack_location(loc);
        assert_eq!(unpacked_offset, offset as usize);
    }
}
