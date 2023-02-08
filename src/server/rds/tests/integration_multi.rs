// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! This test module runs the integration test suite against a multi-threaded
//! instance of Rds.

#[macro_use]
extern crate logger;

mod common;

use crate::common::*;

use config::{RdsConfig, WorkerConfig};
use pelikan_rds::Rds;

use std::time::Duration;

fn main() {
    debug!("launching multi-worker server");
    let mut config = RdsConfig::default();
    config.worker_mut().set_threads(2);
    let server = Rds::new(config).expect("failed to launch rds");

    // wait for server to startup. duration is chosen to be longer than we'd
    // expect startup to take in a slow ci environment.
    std::thread::sleep(Duration::from_secs(10));

    tests();

    admin_tests();

    // shutdown server and join
    info!("shutdown...");
    server.shutdown();

    info!("passed!");
}
