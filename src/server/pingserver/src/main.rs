#[macro_use]
extern crate logger;

use config::PingserverConfig;
use pelikan_pingserver::Pingserver;
use server::PERCENTILES;

common::pelikan_main! {
    about: "A minimal ping/pong server built with Pelikan libraries. \
        Useful for testing and benchmarking the framework with \
        near-zero application overhead.",
    config: PingserverConfig,
    percentiles: PERCENTILES,
    launch: |config: PingserverConfig| {
        Pingserver::new(config)
            .expect("failed to initialize process")
            .wait()
    },
}
