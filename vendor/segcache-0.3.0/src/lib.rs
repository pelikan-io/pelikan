// Copyright 2021 Twitter, Inc.
// Copyright 2023 Pelikan Cache contributors
// Licensed under the MIT and Apache-2.0 licenses

//! This crate is a Rust implementation of the Segcache storage layer.
//!
//! It is a high-throughput and memory-efficient key-value store with eager
//! expiration. Segcache uses a segment-structured design that stores data in
//! fixed-size segments, grouping objects with nearby expiration time into the
//! same segment, and lifting most per-object metadata into the shared segment
//! header. This reduces object metadata by 88% compared to Memcached.
//!
//! A blog post about the overall design can be found here:
//! <https://pelikan.io/2021/segcache.html>
//!
//! Goals:
//! * high-throughput item storage
//! * eager expiration of items
//! * low metadata overhead

// macro includes
#[macro_use]
extern crate log;

// external crate includes
use clocksource::coarse::{Duration, Instant};

// includes from core/std
use core::hash::{BuildHasher, Hasher};

// submodules
mod builder;
mod cas;
mod error;
mod eviction;
mod hashtable;
mod item;
mod rand;
mod segcache;
mod segments;
mod sync;
mod ttl_buckets;

#[cfg(feature = "metrics")]
mod metrics;

// tests
#[cfg(test)]
mod tests;

// publicly exported items from submodules
pub use crate::segcache::Segcache;
pub use builder::Builder;
pub use error::SegcacheError;
pub use eviction::Policy;
pub use hashtable::Location;
pub use item::Item;
pub use keyvalue::Value;

// items from submodules which are imported for convenience to the crate level
pub(crate) use crate::rand::*;
pub(crate) use cas::CasToken;
pub(crate) use hashtable::{
    pack_location, unpack_location, Hashtable, MultiChoiceHashtable, SegmentsVerifier,
};
pub(crate) use item::*;
pub(crate) use keyvalue::{size_of, RawItem, ITEM_HDR_SIZE};
pub(crate) use segments::*;
pub(crate) use ttl_buckets::*;

#[cfg(feature = "metrics")]
pub(crate) use metrics::*;
