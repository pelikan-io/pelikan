//! Cache-line aligned hashtable bucket.
//!
//! Each bucket contains 8 item slots packed as `[12-bit tag][8-bit freq][44-bit location]`.
//! All slots are symmetric (no dedicated metadata slot), enabling SIMD scanning.

use crate::hashtable::location::Location;
use crate::sync::AtomicU64;

/// A single hashtable bucket (64 bytes, cache-line aligned).
///
/// Contains 8 item slots, each packed as:
/// `[12 bits tag][8 bits freq][44 bits location]`
///
/// Empty slots have value 0. No separate metadata — SIMD scans all 8 slots.
#[repr(C, align(64))]
pub struct Hashbucket {
    /// Item slots (8 items per bucket, no metadata).
    pub(crate) items: [AtomicU64; 8],
}

const _: () = assert!(std::mem::size_of::<Hashbucket>() == 64);
const _: () = assert!(std::mem::align_of::<Hashbucket>() == 64);

impl Hashbucket {
    /// Number of item slots per bucket.
    pub const NUM_ITEM_SLOTS: usize = 8;

    /// Create a new empty bucket.
    pub fn new() -> Self {
        Self {
            items: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// Pack an entry into a u64.
    ///
    /// Layout: `[12 bits tag][8 bits freq][44 bits location]`
    #[inline]
    pub fn pack(tag: u16, freq: u8, location: Location) -> u64 {
        let tag_64 = (tag as u64 & 0xFFF) << 52;
        let freq_64 = (freq as u64 & 0xFF) << 44;
        let loc_64 = location.as_raw() & Location::MAX_RAW;
        tag_64 | freq_64 | loc_64
    }

    /// Extract tag (12 bits).
    #[inline(always)]
    pub fn tag(packed: u64) -> u16 {
        (packed >> 52) as u16
    }

    /// Extract frequency (8 bits).
    #[inline(always)]
    pub fn freq(packed: u64) -> u8 {
        ((packed >> 44) & 0xFF) as u8
    }

    /// Extract location (44 bits).
    #[inline(always)]
    pub fn location(packed: u64) -> Location {
        Location::from_raw(packed)
    }

    /// Check if entry is a ghost.
    #[inline(always)]
    pub fn is_ghost(packed: u64) -> bool {
        packed != 0 && Self::location(packed).is_ghost()
    }

    /// Pack a ghost entry (tag + frequency only).
    #[inline]
    pub fn pack_ghost(tag: u16, freq: u8) -> u64 {
        Self::pack(tag, freq, Location::GHOST)
    }

    /// Convert a live entry to ghost.
    #[inline]
    pub fn to_ghost(packed: u64) -> u64 {
        Self::pack_ghost(Self::tag(packed), Self::freq(packed))
    }

    /// Update frequency in a packed value.
    #[inline]
    pub fn with_freq(packed: u64, freq: u8) -> u64 {
        let freq_mask = 0xFF_u64 << 44;
        (packed & !freq_mask) | ((freq as u64) << 44)
    }

    /// Try to update frequency using ASFC algorithm.
    ///
    /// Returns `Some(new_packed)` if frequency should increment.
    #[inline]
    pub fn try_update_freq(packed: u64, freq: u8) -> Option<u64> {
        if freq >= 127 {
            return None;
        }

        // ASFC: probabilistic increment
        let should_increment = if freq <= 16 {
            true
        } else {
            #[cfg(not(feature = "loom"))]
            let rand = {
                use rand::RngExt;
                rand::rng().random::<u64>()
            };
            #[cfg(feature = "loom")]
            let rand = 0u64;

            rand.is_multiple_of(freq as u64)
        };

        if should_increment {
            Some(Self::with_freq(packed, freq + 1))
        } else {
            None
        }
    }
}

impl Default for Hashbucket {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_size_and_alignment() {
        assert_eq!(std::mem::size_of::<Hashbucket>(), 64);
        assert_eq!(std::mem::align_of::<Hashbucket>(), 64);
    }

    #[test]
    fn test_pack_basic() {
        let tag = 0xABC;
        let freq = 42;
        let location = Location::new(0x123_4567_89AB);

        let packed = Hashbucket::pack(tag, freq, location);

        assert_eq!(Hashbucket::tag(packed), tag);
        assert_eq!(Hashbucket::freq(packed), freq);
        assert_eq!(Hashbucket::location(packed), location);
    }

    #[test]
    fn test_pack_max_values() {
        let tag = 0xFFF;
        let freq = 0xFF;
        let location = Location::new(Location::MAX_RAW - 1);

        let packed = Hashbucket::pack(tag, freq, location);

        assert_eq!(Hashbucket::tag(packed), tag);
        assert_eq!(Hashbucket::freq(packed), freq);
        assert_eq!(Hashbucket::location(packed), location);
        assert!(!Hashbucket::is_ghost(packed));
    }

    #[test]
    fn test_ghost_entries() {
        let tag = 0x123;
        let freq = 50;

        let ghost = Hashbucket::pack_ghost(tag, freq);

        assert!(Hashbucket::is_ghost(ghost));
        assert_eq!(Hashbucket::tag(ghost), tag);
        assert_eq!(Hashbucket::freq(ghost), freq);
        assert!(Hashbucket::location(ghost).is_ghost());
    }

    #[test]
    fn test_to_ghost() {
        let packed = Hashbucket::pack(0x456, 75, Location::new(1000));
        let ghost = Hashbucket::to_ghost(packed);

        assert!(Hashbucket::is_ghost(ghost));
        assert_eq!(Hashbucket::tag(ghost), 0x456);
        assert_eq!(Hashbucket::freq(ghost), 75);
    }

    #[test]
    fn test_with_freq() {
        let packed = Hashbucket::pack(0xABC, 10, Location::new(500));
        let updated = Hashbucket::with_freq(packed, 99);

        assert_eq!(Hashbucket::tag(updated), 0xABC);
        assert_eq!(Hashbucket::freq(updated), 99);
        assert_eq!(Hashbucket::location(updated), Location::new(500));
    }

    #[test]
    fn test_empty_slot() {
        let packed = 0u64;
        assert!(!Hashbucket::is_ghost(packed));
        assert_eq!(Hashbucket::tag(packed), 0);
        assert_eq!(Hashbucket::freq(packed), 0);
    }
}
