// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Segcache is an implementation of a cache backend that implements a subset of
//! the Memcache ASCII protocol and is backed with segment based storage. By
//! grouping items with a similar TTL, it is able to provide efficient eager
//! expiration.
//!
//! More details about the benefits of this design can be found in this
//! [blog post](https://twitter.github.io/pelikan/2021/segcache.html).
//!
//! Running this binary is the primary way of using Segcache.

#[macro_use]
extern crate logger;

use config::SegcacheConfig;
use pelikan_segcache::Segcache;
use server::PERCENTILES;

common::pelikan_main! {
    about: "One of the unified cache backends implemented in Rust. It \
        uses segment-based storage to cache key/val pairs. It speaks the \
        memcached ASCII protocol and supports some ASCII memcached \
        commands.",
    config: SegcacheConfig,
    percentiles: PERCENTILES,
    print_config,
    launch: |config| match Segcache::new(config) {
        Ok(segcache) => segcache.wait(),
        Err(e) => {
            eprintln!("error launching segcache: {e}");
            std::process::exit(1);
        }
    },
}
