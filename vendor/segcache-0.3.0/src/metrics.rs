// Copyright 2022 Twitter, Inc.
// Copyright 2023 Pelikan Cache contributors
// Licensed under the MIT and Apache-2.0 licenses

// All metrics for the Seg crate

use metriken::*;

// segment related
#[metric(
    name = "segment_request",
    description = "number of segment allocation attempts",
    metadata = { engine = "segcache" }
)]
pub static SEGMENT_REQUEST: Counter = Counter::new();

#[metric(
    name = "segment_request_failure",
    description = "number of segment allocation attempts which failed",
    metadata = { engine = "segcache" }
)]
pub static SEGMENT_REQUEST_FAILURE: Counter = Counter::new();

#[metric(
    name = "segment_request_success",
    description = "number of segment allocation attempts which were successful",
    metadata = { engine = "segcache" }
)]
pub static SEGMENT_REQUEST_SUCCESS: Counter = Counter::new();

#[metric(
    name = "segment_evict",
    description = "number of segments evicted",
    metadata = { engine = "segcache" }
)]
pub static SEGMENT_EVICT: Counter = Counter::new();

#[metric(
    name = "segment_evict_ex",
    description = "number of exceptions while evicting segments",
    metadata = { engine = "segcache" }
)]
pub static SEGMENT_EVICT_EX: Counter = Counter::new();

#[metric(
    name = "segment_return",
    description = "total number of segments returned to the free pool",
    metadata = { engine = "segcache" }
)]
pub static SEGMENT_RETURN: Counter = Counter::new();

#[metric(
    name = "segment_merge",
    description = "total number of segments merged",
    metadata = { engine = "segcache" }
)]
pub static SEGMENT_MERGE: Counter = Counter::new();

#[metric(
    name = "segment_clear",
    description = "total number of segments cleared",
    metadata = { engine = "segcache" }
)]
pub static SEGMENT_CLEAR: Counter = Counter::new();

#[metric(
    name = "segment_expire",
    description = "total number of segments expired",
    metadata = { engine = "segcache" }
)]
pub static SEGMENT_EXPIRE: Counter = Counter::new();

#[metric(
    name = "clear_time",
    description = "amount of time, in nanoseconds, spent clearing segments",
    metadata = { engine = "segcache" }
)]
pub static CLEAR_TIME: Counter = Counter::new();

#[metric(
    name = "expire_time",
    description = "amount of time, in nanoseconds, spent expiring segments",
    metadata = { engine = "segcache" }
)]
pub static EXPIRE_TIME: Counter = Counter::new();

#[metric(
    name = "evict_time",
    description = "amount of time, in nanoseconds, spent evicting segments",
    metadata = { engine = "segcache" }
)]
pub static EVICT_TIME: Counter = Counter::new();

#[metric(
    name = "segment_free",
    description = "current number of free segments",
    metadata = { engine = "segcache" }
)]
pub static SEGMENT_FREE: Gauge = Gauge::new();

#[metric(
    name = "segment_current",
    description = "current total number of segments",
    metadata = { engine = "segcache" }
)]
pub static SEGMENT_CURRENT: Gauge = Gauge::new();

// hash table related
#[metric(
    name = "hash_tag_collision",
    description = "number of partial hash collisions",
    metadata = { engine = "segcache" }
)]
pub static HASH_TAG_COLLISION: Counter = Counter::new();

#[metric(
    name = "hash_insert",
    description = "number of inserts into the hash table",
    metadata = { engine = "segcache" }
)]
pub static HASH_INSERT: Counter = Counter::new();

#[metric(
    name = "hash_insert_ex",
    description = "number of hash table inserts which failed, likely due to capacity",
    metadata = { engine = "segcache" }
)]
pub static HASH_INSERT_EX: Counter = Counter::new();

#[metric(
    name = "hash_remove",
    description = "number of hash table entries which have been removed",
    metadata = { engine = "segcache" }
)]
pub static HASH_REMOVE: Counter = Counter::new();

#[metric(
    name = "hash_lookup",
    description = "total number of lookups against the hash table",
    metadata = { engine = "segcache" }
)]
pub static HASH_LOOKUP: Counter = Counter::new();

// item related
#[metric(
    name = "item_allocate",
    description = "number of times items have been allocated",
    metadata = { engine = "segcache" }
)]
pub static ITEM_ALLOCATE: Counter = Counter::new();

#[metric(
    name = "item_replace",
    description = "number of times items have been replaced",
    metadata = { engine = "segcache" }
)]
pub static ITEM_REPLACE: Counter = Counter::new();

#[metric(
    name = "item_delete",
    description = "number of items removed from the hash table",
    metadata = { engine = "segcache" }
)]
pub static ITEM_DELETE: Counter = Counter::new();

#[metric(
    name = "item_expire",
    description = "number of items removed due to expiration",
    metadata = { engine = "segcache" }
)]
pub static ITEM_EXPIRE: Counter = Counter::new();

#[metric(
    name = "item_evict",
    description = "number of items removed due to eviction",
    metadata = { engine = "segcache" }
)]
pub static ITEM_EVICT: Counter = Counter::new();

#[metric(
    name = "item_compacted",
    description = "number of items which have been compacted",
    metadata = { engine = "segcache" }
)]
pub static ITEM_COMPACTED: Counter = Counter::new();

#[metric(
    name = "item_relink",
    description = "number of times items have been relinked to different locations",
    metadata = { engine = "segcache" }
)]
pub static ITEM_RELINK: Counter = Counter::new();

#[metric(
    name = "item_current",
    description = "current number of live items",
    metadata = { engine = "segcache" }
)]
pub static ITEM_CURRENT: Gauge = Gauge::new();

#[metric(
    name = "item_current_bytes",
    description = "current number of live bytes for storing items",
    metadata = { engine = "segcache" }
)]
pub static ITEM_CURRENT_BYTES: Gauge = Gauge::new();

#[metric(
    name = "item_dead",
    description = "current number of dead items",
    metadata = { engine = "segcache" }
)]
pub static ITEM_DEAD: Gauge = Gauge::new();

#[metric(
    name = "item_dead_bytes",
    description = "current number of dead bytes for storing items",
    metadata = { engine = "segcache" }
)]
pub static ITEM_DEAD_BYTES: Gauge = Gauge::new();
