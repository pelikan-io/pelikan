// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Pingserver is an implementation of a basic backend service which responds to
//! each `PING` with a `PONG`. This allows for testing of the core components
//! without the overheads associated with managing any state.
//!
//! Running this binary is the primary way of using Pingserver.

#[macro_use]
extern crate logger;

use backtrace::Backtrace;
use clap::{Arg, Command};
use config::PingserverConfig;
use metriken::*;
use pelikan_pingserver_rs::Pingserver;
use server::PERCENTILES;

/// The entry point into the running Pingserver instance. This function parses
/// parses the command line options, loads the configuration, and launches the
/// core threads.
fn main() {
    // custom panic hook to terminate whole process after unwinding
    std::panic::set_hook(Box::new(|s| {
        eprintln!("{s}");
        eprintln!("{:?}", Backtrace::new());
        std::process::exit(101);
    }));

    // parse command line options
    let matches = Command::new(env!("CARGO_BIN_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .long_about(
            "A rust implementation of, arguably, the most over-engineered ping \
            server.\n\n\
            The purpose is to demonstrate how to create an otherwise minimal \
            service with the libraries and modules provied by Pelikan, which \
            meets stringent requirements on latencies, observability, \
            configurability, and other valuable traits in a typical production \
            environment.",
        )
        .arg(
            Arg::new("stats")
                .short('s')
                .long("stats")
                .help("List all metrics in stats")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("CONFIG")
                .help("Server configuration file")
                .action(clap::ArgAction::Set)
                .index(1),
        )
        .get_matches();

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
            // } else if any.downcast_ref::<Heatmap>().is_some() {
            //     for (label, _) in PERCENTILES {
            //         let name = format!("{}_{}", metric.name(), label);
            //         metrics.push(format!("{name:<31} percentile"));
            //     }
            } else {
                continue;
            }
        }

        metrics.sort();
        for metric in metrics {
            println!("{metric}");
        }
        std::process::exit(0);
    }

    // load config from file
    let config = if let Some(file) = matches.get_one::<String>("CONFIG") {
        debug!("loading config: {}", file);
        match PingserverConfig::load(file) {
            Ok(c) => c,
            Err(error) => {
                eprintln!("error loading config file: {file}\n{error}");
                std::process::exit(1);
            }
        }
    } else {
        Default::default()
    };

    // launch
    match Pingserver::new(config) {
        Ok(s) => s.wait(),
        Err(e) => {
            eprintln!("error launching pingserver: {e}");
            std::process::exit(1);
        }
    }
}
