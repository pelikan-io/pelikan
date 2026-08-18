//! CAS (Compare-And-Swap) token for memcached CAS operations.
//!
//! The CAS token uniquely identifies a specific version of an item in the
//! cache. It combines the item's location with the segment's generation
//! counter to prevent ABA problems when segments are reused.
//!
//! # Layout
//!
//! ```text
//! +---------------------------+------------------+
//! |          43..0            |      59..44      |
//! |         location          |    generation    |
//! |          44 bits          |     16 bits      |
//! +---------------------------+------------------+
//! ```
//!
//! The token is 60 bits total, fitting within a u64.

use crate::hashtable::Location;
use std::fmt;

/// A CAS token combining location and generation for ABA-safe versioning.
///
/// The token uniquely identifies a specific version of an item:
/// - **Location (44 bits)**: Identifies where the item is stored (segment + offset)
/// - **Generation (16 bits)**: Segment generation counter, incremented on reuse
///
/// When an item is updated, it gets a new location and/or generation, causing
/// CAS operations with the old token to fail. This prevents the ABA problem
/// where an item is deleted and a new item is written to the same location.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CasToken(u64);

impl CasToken {
    /// Mask for the 44-bit location portion.
    pub(crate) const LOCATION_MASK: u64 = 0xFFF_FFFF_FFFF;

    /// Shift for the 16-bit generation portion.
    const GENERATION_SHIFT: u32 = 44;

    /// Create a new CAS token from location and generation.
    #[inline]
    pub fn new(location: Location, generation: u16) -> Self {
        let raw = location.as_raw() | ((generation as u64) << Self::GENERATION_SHIFT);
        Self(raw)
    }

    /// Get the raw 60-bit value.
    #[inline]
    pub fn as_raw(&self) -> u64 {
        self.0
    }

    /// Construct from a raw 60-bit value.
    #[inline]
    #[allow(dead_code)]
    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Extract the location portion.
    #[inline]
    pub fn location(&self) -> Location {
        Location::from_raw(self.0 & Self::LOCATION_MASK)
    }

    /// Extract the generation portion.
    #[inline]
    pub fn generation(&self) -> u16 {
        (self.0 >> Self::GENERATION_SHIFT) as u16
    }
}

impl fmt::Debug for CasToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CasToken(loc={:?}, gen={})",
            self.location(),
            self.generation()
        )
    }
}

impl fmt::Display for CasToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_new_and_extract() {
        let loc = Location::new(0x123_4567_89AB);
        let generation = 0x1234;
        let token = CasToken::new(loc, generation);

        assert_eq!(token.location(), loc);
        assert_eq!(token.generation(), generation);
    }

    #[test]
    fn test_from_raw_roundtrip() {
        let loc = Location::new(0xABC_DEF0_1234);
        let generation = 0xFFFF;
        let token = CasToken::new(loc, generation);

        let raw = token.as_raw();
        let restored = CasToken::from_raw(raw);

        assert_eq!(restored.location(), loc);
        assert_eq!(restored.generation(), generation);
    }

    #[test]
    fn test_zero_generation() {
        let loc = Location::new(0x100);
        let token = CasToken::new(loc, 0);

        assert_eq!(token.generation(), 0);
        assert_eq!(token.location(), loc);
    }

    #[test]
    fn test_max_generation() {
        let loc = Location::new(0x100);
        let token = CasToken::new(loc, u16::MAX);

        assert_eq!(token.generation(), u16::MAX);
        assert_eq!(token.location(), loc);
    }

    #[test]
    fn test_max_location() {
        let loc = Location::new(Location::MAX_RAW);
        let generation = 0x5678;
        let token = CasToken::new(loc, generation);

        assert_eq!(token.location().as_raw(), Location::MAX_RAW);
        assert_eq!(token.generation(), generation);
    }

    #[test]
    fn test_equality() {
        let token1 = CasToken::new(Location::new(100), 5);
        let token2 = CasToken::new(Location::new(100), 5);
        let token3 = CasToken::new(Location::new(100), 6);
        let token4 = CasToken::new(Location::new(101), 5);

        assert_eq!(token1, token2);
        assert_ne!(token1, token3); // Different generation
        assert_ne!(token1, token4); // Different location
    }
}
