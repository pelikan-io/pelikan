// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! A collection of protocol implementations which implement a set of common
//! traits so that the a server implementation can easily switch between
//! protocol implementations.

// TODO(bmartin): this crate should probably be split into one crate per
// protocol to help separate the metrics namespaces.

#[macro_use]
extern crate logger;

pub use protocol_common::*;

mod ping;

pub use ping::*;

#[cfg(feature = "stats")]
use stats::*;

#[cfg(feature = "stats")]
mod stats {
    use metriken::*;

    #[cfg(feature = "server")]
    #[metric(name = "ping", description = "the number of ping requests")]
    pub static PING: Counter = Counter::new();

    #[cfg(feature = "client")]
    #[metric(name = "pong", description = "the number of pong responses")]
    pub static PONG: Counter = Counter::new();
}

common::metrics::test_no_duplicates!();
