// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

#[macro_use]
extern crate logger;

use backtrace::Backtrace;
use clap::{Arg, Command};
use config::PingproxyConfig;
use metriken::*;
use pingproxy::Pingproxy;

use proxy::PERCENTILES;

fn main() {
    // custom panic hook to terminate whole process after unwinding
    std::panic::set_hook(Box::new(|s| {
        error!("{}", s);
        println!("{:?}", Backtrace::new());
        std::process::exit(101);
    }));

    // parse command line options
    let matches = Command::new(env!("CARGO_BIN_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .long_about(
            "A Pelikan proxy server which speaks the ASCII `ping` protocol. It \
            accepts connections on the listening port, routing requests to the \
            backend servers and responses back to clients.",
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
        match PingproxyConfig::load(file) {
            Ok(c) => c,
            Err(e) => {
                println!("{e}");
                std::process::exit(1);
            }
        }
    } else {
        Default::default()
    };

    // launch proxy
    Pingproxy::new(config).wait()
}
