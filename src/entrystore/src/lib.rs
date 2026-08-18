// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! A collection of storage datastructures suitable for use within Pelikan. A
//! typical storage module will implement one or more storage protocol traits in
//! addition to the base `EntryStore` trait. For example [`Seg`] implements both
//! [`EntryStore`] and [`protocol::memcache::MemcacheStorage`].

mod noop;
mod segcache;

pub use self::noop::*;
pub use self::segcache::*;

/// A trait defining the basic requirements of a type which may be used for
/// storage.
pub trait EntryStore {
    /// Remove all existing values from the entry store.
    fn clear(&self);
}
