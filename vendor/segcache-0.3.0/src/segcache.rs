// Copyright 2021 Twitter, Inc.
// Copyright 2023 Pelikan Cache contributors
// Licensed under the MIT and Apache-2.0 licenses

//! Core datastructure

use crate::Value;
use crate::*;
use core::num::NonZeroU32;
use std::cmp::min;

const RESERVE_RETRIES: usize = 3;

/// A pre-allocated key-value store with eager expiration. It uses a
/// segment-structured design that stores data in fixed-size segments, grouping
/// objects with nearby expiration time into the same segment, and lifting most
/// per-object metadata into the shared segment header.
pub struct Segcache {
    pub(crate) hashtable: MultiChoiceHashtable,
    pub(crate) segments: Segments,
    pub(crate) ttl_buckets: TtlBuckets,
    pub(crate) time: Instant,
}

impl Segcache {
    /// Returns a new `Builder` which is used to configure and construct a
    /// `Segcache` instance.
    ///
    /// ```
    /// use segcache::{Policy, Segcache};
    ///
    /// const MB: usize = 1024 * 1024;
    ///
    /// // create a heap using 1MB segments
    /// let cache = Segcache::builder()
    ///     .heap_size(64 * MB)
    ///     .segment_size(1 * MB as i32)
    ///     .hash_power(16)
    ///     .eviction(Policy::Random).build().expect("failed to create cache");
    /// ```
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Create a SegmentsVerifier for the current segments state.
    #[inline]
    fn verifier(&self) -> SegmentsVerifier<'_> {
        self.segments.verifier()
    }

    /// Gets a count of items in the `Segcache` instance. This is an expensive
    /// operation and is only enabled for tests and builds with the `debug`
    /// feature enabled.
    ///
    /// ```
    /// use segcache::{Policy, Segcache};
    ///
    /// let mut cache = Segcache::builder().build().expect("failed to create cache");
    /// assert_eq!(cache.items(), 0);
    /// ```
    #[cfg(any(test, feature = "debug"))]
    pub fn items(&mut self) -> usize {
        trace!("getting segment item counts");
        self.segments.items()
    }

    /// Get the item in the `Segcache` with the provided key
    ///
    /// ```
    /// use segcache::{Policy, Segcache};
    /// use std::time::Duration;
    ///
    /// let mut cache = Segcache::builder().build().expect("failed to create cache");
    /// assert!(cache.get(b"coffee").is_none());
    ///
    /// cache.insert(b"coffee", b"strong", None, Duration::ZERO);
    /// let item = cache.get(b"coffee").expect("didn't get item back");
    /// assert_eq!(item.value(), b"strong");
    /// ```
    pub fn get(&mut self, key: &[u8]) -> Option<Item> {
        let verifier = self.verifier();
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;
        let (seg_id, offset) = unpack_location(location);
        let seg_id = NonZeroU32::new(seg_id)?;
        let raw = self.segments.get_item_at(Some(seg_id), offset)?;
        raw.check_magic();

        let cas = CasToken::new(location, self.segments.generation(seg_id)).as_raw();
        Some(Item::new(raw, cas))
    }

    /// Get the item in the `Segcache` with the provided key without
    /// increasing the item frequency - useful for combined operations that
    /// check for presence - eg replace is a get + set
    /// ```
    /// use segcache::{Policy, Segcache};
    ///
    /// let mut cache = Segcache::builder().build().expect("failed to create cache");
    /// assert!(cache.get_no_freq_incr(b"coffee").is_none());
    /// ```
    pub fn get_no_freq_incr(&mut self, key: &[u8]) -> Option<Item> {
        let verifier = self.verifier();
        let (location, _freq) = self.hashtable.lookup_no_freq_update(key, &verifier)?;
        let (seg_id, offset) = unpack_location(location);
        let seg_id = NonZeroU32::new(seg_id)?;
        let raw = self.segments.get_item_at(Some(seg_id), offset)?;
        raw.check_magic();

        let cas = CasToken::new(location, self.segments.generation(seg_id)).as_raw();
        Some(Item::new(raw, cas))
    }

    /// Insert a new item into the cache. May return an error indicating that
    /// the insert was not successful.
    /// ```
    /// use segcache::{Policy, Segcache};
    /// use std::time::Duration;
    ///
    /// let mut cache = Segcache::builder().build().expect("failed to create cache");
    /// assert!(cache.get(b"drink").is_none());
    ///
    /// cache.insert(b"drink", b"coffee", None, Duration::ZERO);
    /// let item = cache.get(b"drink").expect("didn't get item back");
    /// assert_eq!(item.value(), b"coffee");
    ///
    /// cache.insert(b"drink", b"whisky", None, Duration::ZERO);
    /// let item = cache.get(b"drink").expect("didn't get item back");
    /// assert_eq!(item.value(), b"whisky");
    /// ```
    pub fn insert<'a, T: Into<Value<'a>>>(
        &mut self,
        key: &'a [u8],
        value: T,
        optional: Option<&[u8]>,
        ttl: std::time::Duration,
    ) -> Result<(), SegcacheError> {
        let value: Value = value.into();

        // default optional data is empty
        let optional = optional.unwrap_or(&[]);

        // calculate size for item
        let size = (((ITEM_HDR_SIZE + key.len() + size_of(&value) + optional.len()) >> 3) + 1) << 3;

        let ttl = Duration::from_secs(min(u32::MAX as u64, ttl.as_secs()) as u32);

        // For S3-FIFO: determine target pool based on ghost queue
        let target_pool = if matches!(self.segments.evict_policy(), Policy::S3Fifo { .. }) {
            let hash = {
                let mut hasher = self.hashtable.hash_builder().build_hasher();
                hasher.write(key);
                hasher.finish()
            };
            if self.segments.ghost_contains(hash) {
                self.segments.ghost_remove(hash);
                SegmentPool::Main
            } else {
                SegmentPool::Admission
            }
        } else {
            SegmentPool::Main
        };

        // For S3-FIFO: ensure the target pool has room by evicting from it
        // if it's at capacity. This enforces the small/main ratio computed
        // at construction time.
        if matches!(self.segments.evict_policy(), Policy::S3Fifo { .. })
            && !self.segments.pool_has_room(target_pool)
        {
            let _ = self.segments.evict(&mut self.ttl_buckets, &self.hashtable);
        }

        // try to get a `ReservedItem`
        let mut retries = RESERVE_RETRIES;
        let reserved;
        loop {
            match self
                .ttl_buckets
                .get_mut_bucket(ttl)
                .reserve(size, &mut self.segments)
            {
                Ok(mut reserved_item) => {
                    reserved_item.define(key, value, optional);
                    // Set the segment pool for S3-FIFO (only transitions
                    // Main→Admission need a counter update; fresh segments
                    // default to Main)
                    if let Ok(seg) = self.segments.get_mut(reserved_item.seg()) {
                        if target_pool == SegmentPool::Admission
                            && seg.pool() != SegmentPool::Admission
                        {
                            seg.set_pool(target_pool);
                            self.segments.incr_pool(SegmentPool::Admission);
                        }
                    }
                    reserved = reserved_item;
                    break;
                }
                Err(TtlBucketsError::ItemOversized { size }) => {
                    return Err(SegcacheError::ItemOversized { size });
                }
                Err(TtlBucketsError::NoFreeSegments) => {
                    if self
                        .segments
                        .evict(&mut self.ttl_buckets, &self.hashtable)
                        .is_err()
                    {
                        retries -= 1;
                    } else {
                        // we successfully evicted a segment, return to start of
                        // loop to reserve the item
                        continue;
                    }
                }
            }
            if retries == 0 {
                // segment acquire failed, increment the stats and return with
                // an error

                #[cfg(feature = "metrics")]
                {
                    SEGMENT_REQUEST.increment();
                    SEGMENT_REQUEST_FAILURE.increment();
                }

                return Err(SegcacheError::NoFreeSegments);
            }
            retries -= 1;
        }

        let location = pack_location(reserved.seg(), reserved.offset() as u64);
        let verifier = self.verifier();

        match self
            .hashtable
            .insert(reserved.item().key(), location, &verifier)
        {
            Ok(Some(old_location)) => {
                // Replaced existing key — remove old item from segment
                let (old_seg_id, old_offset) = unpack_location(old_location);
                if let Some(old_seg_id) = NonZeroU32::new(old_seg_id) {
                    #[cfg(feature = "metrics")]
                    ITEM_REPLACE.increment();

                    let _ = self.segments.remove_at(
                        old_seg_id,
                        old_offset,
                        &mut self.ttl_buckets,
                        &self.hashtable,
                    );
                }
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(()) => {
                // Hashtable full — roll back the segment allocation
                let _ = self.segments.remove_at(
                    reserved.seg(),
                    reserved.offset(),
                    &mut self.ttl_buckets,
                    &self.hashtable,
                );
                Err(SegcacheError::HashTableInsertEx)
            }
        }
    }

    /// Performs a CAS operation, inserting the item only if the CAS value
    /// matches the current value for that item.
    ///
    /// ```
    /// use segcache::{Policy, Segcache, SegcacheError};
    /// use std::time::Duration;
    ///
    /// let mut cache = Segcache::builder().build().expect("failed to create cache");
    ///
    /// // If the item is not in the cache, CAS will fail as 'NotFound'
    /// assert_eq!(
    ///     cache.cas(b"drink", b"coffee", None, Duration::ZERO, 0),
    ///     Err(SegcacheError::NotFound)
    /// );
    ///
    /// // If a stale CAS value is provided, CAS will fail as 'Exists'
    /// cache.insert(b"drink", b"coffee", None, Duration::ZERO);
    /// assert_eq!(
    ///     cache.cas(b"drink", b"coffee", None, Duration::ZERO, 0),
    ///     Err(SegcacheError::Exists)
    /// );
    ///
    /// // Getting the CAS value and then performing the operation ensures
    /// // success in absence of a race with another client
    /// let current = cache.get(b"drink").expect("not found");
    /// assert!(cache.cas(b"drink", b"whisky", None, Duration::ZERO, current.cas()).is_ok());
    /// let item = cache.get(b"drink").expect("not found");
    /// assert_eq!(item.value(), b"whisky"); // item is updated
    /// ```
    pub fn cas<'a, T: Into<Value<'a>>>(
        &mut self,
        key: &'a [u8],
        value: T,
        optional: Option<&[u8]>,
        ttl: std::time::Duration,
        cas: u64,
    ) -> Result<(), SegcacheError> {
        // Look up the current item to check its CAS token
        let verifier = self.verifier();
        let (location, _freq) = self
            .hashtable
            .lookup_no_freq_update(key, &verifier)
            .ok_or(SegcacheError::NotFound)?;

        let (seg_id, _offset) = unpack_location(location);
        let seg_id = NonZeroU32::new(seg_id).ok_or(SegcacheError::NotFound)?;
        let current_cas = CasToken::new(location, self.segments.generation(seg_id)).as_raw();
        if current_cas != cas {
            return Err(SegcacheError::Exists);
        }

        self.insert(key, value, optional, ttl)
    }

    /// Remove the item with the given key, returns a bool indicating if it was
    /// removed.
    /// ```
    /// use segcache::{Policy, Segcache, SegcacheError};
    /// use std::time::Duration;
    ///
    /// let mut cache = Segcache::builder().build().expect("failed to create cache");
    ///
    /// // If the item is not in the cache, delete will return false
    /// assert_eq!(cache.delete(b"coffee"), false);
    ///
    /// // And will return true on success
    /// cache.insert(b"coffee", b"strong", None, Duration::ZERO);
    /// assert!(cache.get(b"coffee").is_some());
    /// assert_eq!(cache.delete(b"coffee"), true);
    /// assert!(cache.get(b"coffee").is_none());
    /// ```
    // TODO(bmartin): a result would be better here
    pub fn delete(&mut self, key: &[u8]) -> bool {
        // Look up the item to get its location
        let verifier = self.verifier();
        let (location, _freq) = match self.hashtable.lookup_no_freq_update(key, &verifier) {
            Some(result) => result,
            None => return false,
        };

        // Remove from hashtable
        if !self.hashtable.remove(key, location) {
            return false;
        }

        #[cfg(feature = "metrics")]
        {
            HASH_REMOVE.increment();
            ITEM_DELETE.increment();
        }

        // Remove from segment
        let (seg_id, offset) = unpack_location(location);
        if let Some(seg_id) = NonZeroU32::new(seg_id) {
            if let Some(mut item) = self.segments.get_item_at(Some(seg_id), offset) {
                item.set_deleted(true);
            }
            let _ = self
                .segments
                .remove_at(seg_id, offset, &mut self.ttl_buckets, &self.hashtable);
        }

        true
    }

    /// Loops through the TTL Buckets to handle eager expiration, returns the
    /// number of segments expired
    /// ```
    /// use segcache::{Policy, Segcache, SegcacheError};
    /// use std::time::Duration;
    ///
    /// let mut cache = Segcache::builder().build().expect("failed to create cache");
    ///
    /// // Insert an item with a short ttl
    /// cache.insert(b"coffee", b"strong", None, Duration::from_secs(5));
    ///
    /// // The item is still in the cache
    /// assert!(cache.get(b"coffee").is_some());
    ///
    /// // Delay and then trigger expiration
    /// std::thread::sleep(Duration::from_secs(6));
    /// cache.expire();
    ///
    /// // And the expired item is not in the cache
    /// assert!(cache.get(b"coffee").is_none());
    /// ```
    pub fn expire(&mut self) -> usize {
        self.time = Instant::now();
        self.ttl_buckets.expire(&self.hashtable, &mut self.segments)
    }

    pub fn clear(&mut self) -> usize {
        self.time = Instant::now();
        self.ttl_buckets.clear(&self.hashtable, &mut self.segments)
    }

    /// Checks the integrity of all segments
    /// *NOTE*: this operation is relatively expensive
    #[cfg(feature = "debug")]
    pub fn check_integrity(&mut self) -> Result<(), SegcacheError> {
        if self.segments.check_integrity(&self.hashtable) {
            Ok(())
        } else {
            Err(SegcacheError::DataCorrupted)
        }
    }

    /// Perform a wrapping addition on the value stored at the supplied key.
    /// Returns an error if the key is invalid, the item is not found, or the
    /// stored value is not a numeric type.
    pub fn wrapping_add(&mut self, key: &[u8], rhs: u64) -> Result<Item, SegcacheError> {
        let verifier = self.verifier();
        let (location, _freq) = self
            .hashtable
            .lookup(key, &verifier)
            .ok_or(SegcacheError::NotFound)?;
        let (seg_id, offset) = unpack_location(location);
        let seg_id = NonZeroU32::new(seg_id).ok_or(SegcacheError::NotFound)?;
        let raw = self
            .segments
            .get_item_at(Some(seg_id), offset)
            .ok_or(SegcacheError::NotFound)?;
        let cas = CasToken::new(location, self.segments.generation(seg_id)).as_raw();
        let mut item = Item::new(raw, cas);
        item.wrapping_add(rhs)?;
        Ok(item)
    }

    /// Perform a saturating subtraction on the value stored at the supplied
    /// key. Returns an error if the key is invalid, the item is not found, or
    /// the stored value is not a numeric type.
    pub fn saturating_sub(&mut self, key: &[u8], rhs: u64) -> Result<Item, SegcacheError> {
        let verifier = self.verifier();
        let (location, _freq) = self
            .hashtable
            .lookup(key, &verifier)
            .ok_or(SegcacheError::NotFound)?;
        let (seg_id, offset) = unpack_location(location);
        let seg_id = NonZeroU32::new(seg_id).ok_or(SegcacheError::NotFound)?;
        let raw = self
            .segments
            .get_item_at(Some(seg_id), offset)
            .ok_or(SegcacheError::NotFound)?;
        let cas = CasToken::new(location, self.segments.generation(seg_id)).as_raw();
        let mut item = Item::new(raw, cas);
        item.saturating_sub(rhs)?;
        Ok(item)
    }
}
