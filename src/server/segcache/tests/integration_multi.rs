// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! This test module runs the integration test suite against a multi-threaded
//! instance of Segcache.

#[macro_use]
extern crate logger;

mod common;

use crate::common::*;

use config::{SegcacheConfig, WorkerConfig};
use pelikan_segcache::Segcache;

use std::time::Duration;

fn main() {
    debug!("launching multi-worker server");
    let mut config = SegcacheConfig::default();
    // eight workers rather than two: with only a couple of workers a single
    // connection is almost always served by the same one, so nothing about
    // sharing the storage is exercised. eight also makes `flush_all_tests`
    // meaningful — a worker that becomes free while others are still busy is
    // exactly the interleaving that test needs to observe.
    config.worker_mut().set_threads(8);
    let server = Segcache::new(config).expect("failed to launch segcache");

    // wait for server to startup. duration is chosen to be longer than we'd
    // expect startup to take in a slow ci environment.
    std::thread::sleep(Duration::from_secs(10));

    tests();

    admin_tests();

    flush_all_tests();

    // shutdown server and join
    info!("shutdown...");
    server.shutdown();

    info!("passed!");
}
