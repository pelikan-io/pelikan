// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Rds is an implementation of a cache backend that implements a subset of
//! Redis commands and is currently backed with segment based storage. By
//! grouping items with a similar TTL, it is able to provide efficient eager
//! expiration.
//!
//! More details about the benefits of this design can be found in this
//! [blog post](https://twitter.github.io/pelikan/2021/segcache.html).
//!
//! Running this binary is the primary way of using Rds.

#[macro_use]
extern crate logger;

use config::RdsConfig;
use pelikan_rds::Rds;
use server::PERCENTILES;

common::pelikan_main! {
    about: "One of the unified cache backends implemented in Rust. It \
        uses segment-based storage to cache key/val pairs. It speaks the \
        redis ASCII protocol (RESP) and supports some ASCII redis \
        commands.",
    config: RdsConfig,
    percentiles: PERCENTILES,
    print_config,
    launch: |config| match Rds::new(config) {
        Ok(rds) => rds.wait(),
        Err(e) => {
            eprintln!("error launching rds: {e}");
            std::process::exit(1);
        }
    },
}
