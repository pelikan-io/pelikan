// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Rds is an implementation of a cache backend that implements a subset of
//! the Redis ASCII protocol and is currently backed with segment based storage. By
//! grouping items with a similar TTL, it is able to provide efficient eager
//! expiration.
//!
//! More details about the benefits of this design can be found in this
//! [blog post](https://twitter.github.io/pelikan/2021/segcache.html).
//!
//! Running this binary is the primary way of using Rds.

#[macro_use]
extern crate logger;

use backtrace::Backtrace;
use clap::{Arg, Command};
use config::SegcacheConfig;
use metriken::*;
use pelikan_rds::Rds;
use server::PERCENTILES;

/// The entry point into the running Segcache instance. This function parses the
/// command line options, loads the configuration, and launches the core
/// threads.
fn main() {
    // custom panic hook to terminate whole process after unwinding
    std::panic::set_hook(Box::new(|s| {
        eprintln!("{}", s);
        eprintln!("{:?}", Backtrace::new());
        std::process::exit(101);
    }));

    // parse command line options
    let matches = Command::new(env!("CARGO_BIN_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .long_about(
            "One of the unified cache backends implemented in Rust. It \
            uses segment-based storage to cache key/val pairs. It speaks the \
            redis ASCII protocol (RESP) and supports some ASCII redis \
            commands.",
        )
        .arg(
            Arg::new("stats")
                .short('s')
                .long("stats")
                .help("List all metrics in stats")
                .num_args(0)
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("CONFIG")
                .help("Server configuration file")
                .index(1),
        )
        .arg(
            Arg::new("print-config")
                .help("List all options in config")
                .long("config")
                .short('c')
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

    // output stats descriptions and exit if the `stats` option was provided
    if matches.get_flag("stats") {
        println!("{:<31} {:<15} DESCRIPTION", "NAME", "TYPE");

        let mut metrics = Vec::new();

        for metric in &metriken::metrics() {
            let any = match metric.as_any() {
                Some(any) => any,
                None => {
                    continue;
                }
            };

            if any.downcast_ref::<Counter>().is_some() {
                metrics.push(format!("{:<31} counter", metric.name()));
            } else if any.downcast_ref::<Gauge>().is_some() {
                metrics.push(format!("{:<31} gauge", metric.name()));
            } else if any.downcast_ref::<Heatmap>().is_some() {
                for (label, _) in PERCENTILES {
                    let name = format!("{}_{}", metric.name(), label);
                    metrics.push(format!("{:<31} percentile", name));
                }
            } else {
                continue;
            }
        }

        metrics.sort();
        for metric in metrics {
            println!("{}", metric);
        }
        std::process::exit(0);
    }

    // load config from file
    let config = if let Some(file) = matches.get_one::<String>("CONFIG") {
        debug!("loading config: {}", file);
        match SegcacheConfig::load(file) {
            Ok(c) => c,
            Err(error) => {
                eprintln!("error loading config file: {file}\n{error}");
                std::process::exit(1);
            }
        }
    } else {
        Default::default()
    };

    if matches.get_flag("print-config") {
        config.print();
        std::process::exit(0);
    }

    // launch rds
    match Rds::new(config) {
        Ok(rds) => rds.wait(),
        Err(e) => {
            eprintln!("error launching rds: {}", e);
            std::process::exit(1);
        }
    }
}
