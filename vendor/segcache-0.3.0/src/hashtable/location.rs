//! Opaque location type for cache storage.
//!
//! `Location` is a 44-bit packed value that identifies where an item is stored.
//! The hashtable treats this as an opaque identifier — storage backends define
//! their own interpretation of the bits.

use std::fmt;

/// Opaque 44-bit location value.
///
/// The hashtable stores this alongside a 12-bit tag and 8-bit frequency,
/// fitting in a single 64-bit atomic. The meaning of the 44 bits is defined
/// by the storage backend:
///
/// ```text
/// Hashtable entry layout:
/// +--------+--------+---------------------------+
/// | 63..52 | 51..44 |          43..0            |
/// |  tag   |  freq  |         location          |
/// | 12 bits| 8 bits |         44 bits           |
/// +--------+--------+---------------------------+
/// ```
///
/// For segcache, the location encodes:
/// - bits 43..20: segment id (24 bits)
/// - bits 19..0: offset / 8 (20 bits, 8-byte aligned)
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Location(u64);

impl Location {
    /// Maximum raw value (44 bits set).
    pub const MAX_RAW: u64 = 0xFFF_FFFF_FFFF;

    /// Sentinel value indicating a ghost entry (recently evicted).
    /// All 44 location bits set to 1.
    pub const GHOST: Self = Self(Self::MAX_RAW);

    /// Create a location from a raw 44-bit value.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if `raw > MAX_RAW`.
    #[inline]
    pub fn new(raw: u64) -> Self {
        debug_assert!(raw <= Self::MAX_RAW, "location exceeds 44 bits");
        Self(raw)
    }

    /// Get the raw 44-bit value.
    #[inline(always)]
    pub fn as_raw(&self) -> u64 {
        self.0
    }

    /// Construct from raw value, masking to 44 bits.
    #[inline(always)]
    pub fn from_raw(raw: u64) -> Self {
        Self(raw & Self::MAX_RAW)
    }

    /// Check if this is the ghost sentinel.
    #[inline(always)]
    pub fn is_ghost(&self) -> bool {
        *self == Self::GHOST
    }
}

impl fmt::Debug for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_ghost() {
            write!(f, "Location::GHOST")
        } else {
            write!(f, "Location(0x{:011X})", self.0)
        }
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_ghost() {
            write!(f, "GHOST")
        } else {
            write!(f, "0x{:011X}", self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_and_as_raw() {
        let loc = Location::new(0x123_4567_89AB);
        assert_eq!(loc.as_raw(), 0x123_4567_89AB);
        assert!(!loc.is_ghost());
    }

    #[test]
    fn test_ghost_sentinel() {
        assert!(Location::GHOST.is_ghost());
        assert_eq!(Location::GHOST.as_raw(), Location::MAX_RAW);
    }

    #[test]
    fn test_from_raw_masks() {
        let loc = Location::from_raw(0xFFFF_FFFF_FFFF_FFFF);
        assert_eq!(loc.as_raw(), Location::MAX_RAW);
        assert!(loc.is_ghost());
    }

    #[test]
    fn test_equality() {
        let a = Location::new(12345);
        let b = Location::new(12345);
        let c = Location::new(12346);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
