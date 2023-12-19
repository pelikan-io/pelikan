// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! This crate is a Rust implementation of the Segcache storage layer.
//!
//! It is a high-throughput and memory-efficient key-value store with eager
//! expiration. Segcache uses a segment-structured design that stores data in
//! fixed-size segments, grouping objects with nearby expiration time into the
//! same segment, and lifting most per-object metadata into the shared segment
//! header. This reduces object metadata by 88% compared to Memcached.
//!
//! A blog post about the overall design can be found here:
//! <https://twitter.github.io/pelikan/2021/segcache.html>
//!
//! Goals:
//! * high-throughput item storage
//! * eager expiration of items
//! * low metadata overhead
//!
//! Non-goals:
//! * not designed for concurrent access
//!

// macro includes
#[macro_use]
extern crate log;

// external crate includes
use clocksource::Seconds;

// includes from core/std
use core::hash::{BuildHasher, Hasher};
use std::convert::TryInto;

// NOTE: this represents the versioning of the internal data layout and must be
// incremented when breaking changes are made to the datastructures
const VERSION: u64 = 0;

// submodules
mod builder;
mod error;
mod eviction;
mod hashtable;
mod item;
mod rand;
mod seg;
mod segments;
mod ttl_buckets;
mod value;

#[cfg(feature = "metrics")]
mod metrics;

// tests
#[cfg(test)]
mod tests;

// publicly exported items from submodules
pub use crate::seg::Segcache;
pub use builder::Builder;
pub use error::SegcacheError;
pub use eviction::Policy;
pub use item::Item;
pub use value::Value;

// type aliases
pub(crate) type Duration = clocksource::Duration<Seconds<u32>>;
pub(crate) type Instant = clocksource::Instant<Seconds<u32>>;

// items from submodules which are imported for convenience to the crate level
pub(crate) use crate::rand::*;
pub(crate) use hashtable::*;
pub(crate) use item::*;
pub(crate) use segments::*;
pub(crate) use ttl_buckets::*;

#[cfg(feature = "metrics")]
pub(crate) use metrics::*;
