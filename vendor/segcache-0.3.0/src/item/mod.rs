//! Items are the base unit of data stored within the cache.

mod reserved;

use crate::SegcacheError;
use keyvalue::{RawItem, Value};

pub(crate) use reserved::ReservedItem;

/// The base unit of data returned by a cache lookup.
pub struct Item {
    cas: u64,
    raw: RawItem,
}

impl Item {
    pub(crate) fn new(raw: RawItem, cas: u64) -> Self {
        Item { cas, raw }
    }

    #[allow(dead_code)]
    pub(crate) fn check_magic(&self) {
        self.raw.check_magic()
    }

    /// Borrow the item key
    pub fn key(&self) -> &[u8] {
        self.raw.key()
    }

    /// Borrow the item value
    pub fn value(&self) -> Value<'_> {
        self.raw.value()
    }

    /// CAS value for the item, matching the memcache protocol's 64-bit
    /// "cas unique". Combines the item's location with its segment's
    /// generation counter; any update, relocation, or segment recycle
    /// invalidates outstanding values.
    pub fn cas(&self) -> u64 {
        self.cas
    }

    /// Borrow the optional data
    pub fn optional(&self) -> Option<&[u8]> {
        self.raw.optional()
    }

    /// Returns true if the item has been soft-deleted.
    pub fn is_deleted(&self) -> bool {
        self.raw.is_deleted()
    }

    /// Perform a wrapping addition on a numeric value.
    pub fn wrapping_add(&mut self, rhs: u64) -> Result<(), SegcacheError> {
        self.raw
            .wrapping_add(rhs)
            .map_err(|_| SegcacheError::NotNumeric)
    }

    /// Perform a saturating subtraction on a numeric value.
    pub fn saturating_sub(&mut self, rhs: u64) -> Result<(), SegcacheError> {
        self.raw
            .saturating_sub(rhs)
            .map_err(|_| SegcacheError::NotNumeric)
    }
}

impl std::fmt::Debug for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("Item")
            .field("cas", &self.cas())
            .field("raw", &self.raw)
            .finish()
    }
}
