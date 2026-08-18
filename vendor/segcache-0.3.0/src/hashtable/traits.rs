//! Hashtable trait and key verification for cache operations.

use crate::hashtable::location::Location;

/// Trait for verifying that a key exists at a location.
///
/// The hashtable calls this during lookup/insert to confirm that a tag match
/// corresponds to an actual key match (avoiding false positives from hash
/// collisions in the 12-bit tag).
///
/// # Thread Safety
///
/// Implementations must be thread-safe (`Send + Sync`) as verification may be
/// called concurrently from multiple threads.
pub trait KeyVerifier: Send + Sync {
    /// Verify that `key` exists at `location`.
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool;

    /// Prefetch memory at the given location.
    ///
    /// Called by the hashtable after a tag match but before full verification.
    /// This allows overlapping memory prefetch with the Acquire barrier overhead.
    #[inline]
    fn prefetch(&self, _location: Location) {}
}

/// Core trait for hashtable operations.
///
/// A hashtable maps keys to `Location` values, tracking the physical
/// location of items in storage. It also maintains frequency counters
/// for each item, supporting eviction algorithms like S3-FIFO.
///
/// # Ghost Entries
///
/// When an item is evicted, its hashtable entry can be converted to a "ghost"
/// entry. Ghosts preserve the frequency counter but mark the location as invalid
/// (`Location::GHOST`). When re-inserting a previously evicted key, the ghost's
/// frequency can be preserved, giving "second chance" semantics.
#[allow(dead_code)]
pub trait Hashtable: Send + Sync {
    /// Look up a key and return its location and frequency.
    ///
    /// This also increments the frequency counter (probabilistically for
    /// values > 16 using the ASFC algorithm).
    fn lookup(&self, key: &[u8], verifier: &impl KeyVerifier) -> Option<(Location, u8)>;

    /// Look up a key without updating frequency.
    fn lookup_no_freq_update(
        &self,
        key: &[u8],
        verifier: &impl KeyVerifier,
    ) -> Option<(Location, u8)>;

    /// Check if a key exists without updating frequency.
    fn contains(&self, key: &[u8], verifier: &impl KeyVerifier) -> bool;

    /// Insert or update a key's location.
    ///
    /// If the key already exists (live or ghost), updates the location and
    /// preserves the frequency. For ghosts, this "resurrects" the entry.
    ///
    /// # Returns
    /// - `Ok(Some(old_location))` if an existing entry was replaced
    /// - `Ok(None)` if this was a new entry or ghost resurrection
    /// - `Err(())` if the hashtable is full
    fn insert(
        &self,
        key: &[u8],
        location: Location,
        verifier: &impl KeyVerifier,
    ) -> Result<Option<Location>, ()>;

    /// Remove a key from the hashtable.
    ///
    /// The entry must match the expected location (for ABA safety).
    fn remove(&self, key: &[u8], expected: Location) -> bool;

    /// Convert an entry to a ghost (preserves frequency).
    fn convert_to_ghost(&self, key: &[u8], expected: Location) -> bool;

    /// Update an item's location atomically.
    ///
    /// Used during compaction and tier migration. The entry must match
    /// the expected old location for the update to succeed.
    fn cas_location(
        &self,
        key: &[u8],
        old_location: Location,
        new_location: Location,
        preserve_freq: bool,
    ) -> bool;

    /// Get the frequency of an item by key.
    fn get_frequency(&self, key: &[u8], verifier: &impl KeyVerifier) -> Option<u8>;

    /// Get the frequency of an item at a specific location.
    fn get_item_frequency(&self, key: &[u8], location: Location) -> Option<u8>;

    /// Get the frequency of a ghost entry.
    fn get_ghost_frequency(&self, key: &[u8]) -> Option<u8>;

    /// Clear all entries from the hashtable.
    fn clear(&self);
}
