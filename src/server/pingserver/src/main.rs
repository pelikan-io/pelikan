#[macro_use]
extern crate logger;

use backtrace::Backtrace;
use clap::{Arg, Command};
use config::PingserverConfig;
use entrystore::Noop;
use logger::configure_logging;
use metriken::*;
use protocol_ping::{PingProtocol, Request, Response};
use server::{ProcessBuilder, PERCENTILES};

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
            "A minimal ping/pong server built with Pelikan libraries. \
            Useful for testing and benchmarking the framework with \
            near-zero application overhead.",
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
            } else if any.downcast_ref::<AtomicHistogram>().is_some()
                || any.downcast_ref::<RwLockHistogram>().is_some()
            {
                for (label, _) in PERCENTILES {
                    let name = format!("{}_{}", metric.name(), label);
                    metrics.push(format!("{name:<31} percentile"));
                }
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
        debug!("loading config: {file}");
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

    // initialize logging
    let log = configure_logging(&config);

    // initialize metrics
    common::metrics::init();

    // initialize storage
    let storage = Noop::new();

    // initialize parser
    let protocol = PingProtocol::default();

    // initialize process
    let process_builder = ProcessBuilder::<PingProtocol, Request, Response, Noop>::new(
        &config, log, protocol, storage,
    )
    .expect("failed to initialize process");

    // spawn threads and wait
    let process = process_builder.spawn();
    process.wait();
}
