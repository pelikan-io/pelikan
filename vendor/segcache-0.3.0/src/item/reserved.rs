//! A reserved item is an item which has been allocated but not yet linked
//! in the hashtable.

use crate::RawItem;
use crate::Value;
use core::num::NonZeroU32;

/// An item that has been allocated in a segment but is not yet defined or
/// linked in the hashtable.
#[derive(Debug)]
pub(crate) struct ReservedItem {
    item: RawItem,
    seg: NonZeroU32,
    offset: usize,
}

impl ReservedItem {
    /// Create a `ReservedItem` from its parts
    pub fn new(item: RawItem, seg: NonZeroU32, offset: usize) -> Self {
        Self { item, seg, offset }
    }

    /// Store the key, value, and optional data into the item
    pub fn define(&mut self, key: &[u8], value: Value, optional: &[u8]) {
        self.item.define(key, value, optional)
    }

    /// Get the `RawItem` that backs the `ReservedItem`
    pub fn item(&self) -> RawItem {
        self.item
    }

    /// Get the segment offset
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Get the segment id
    pub fn seg(&self) -> NonZeroU32 {
        self.seg
    }
}
