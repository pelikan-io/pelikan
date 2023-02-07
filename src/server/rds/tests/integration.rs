// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! This test module runs the integration test suite against a single-threaded
//! instance of Rds.

mod common;

#[macro_use]
extern crate logger;

use crate::common::*;

use pelikan_rds::Rds;

use config::RdsConfig;
use std::time::Duration;

fn main() {
    debug!("launching server");
    let server = Rds::new(RdsConfig::default()).expect("failed to launch rds");

    // wait for server to startup. duration is chosen to be longer than we'd
    // expect startup to take in a slow ci environment.
    std::thread::sleep(Duration::from_secs(10));

    tests();

    admin_tests();

    // shutdown server and join
    info!("shutdown...");
    let _ = server.shutdown();

    info!("passed!");
}
