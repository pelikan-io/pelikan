// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

#[macro_use]
extern crate logger;

use config::PingproxyConfig;
use pelikan_pingproxy::Pingproxy;
use proxy::PERCENTILES;

common::pelikan_main! {
    about: "A Pelikan proxy server which speaks the ASCII `ping` protocol. It \
        accepts connections on the listening port, routing requests to the \
        backend servers and responses back to clients.",
    config: PingproxyConfig,
    percentiles: PERCENTILES,
    launch: |config| Pingproxy::new(config).wait(),
}
