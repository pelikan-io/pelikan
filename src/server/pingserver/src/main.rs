#[macro_use]
extern crate logger;

use config::{Config, Engine};

use entrystore::Noop;
use logger::{configure_logging, Drain};
use protocol_ping::{Request, RequestParser, Response};
use server::{PERCENTILES, ProcessBuilder};

use backtrace::Backtrace;
use clap::{Arg, Command};
use metriken::*;

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

type Parser = RequestParser;
type Storage = Noop;

mod config;
mod tokio;

static RUNNING: AtomicBool = AtomicBool::new(true);

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
            Arg::new("CONFIG")
                .help("Server configuration file")
                .action(clap::ArgAction::Set)
                .index(1),
        )
        .arg(
            Arg::new("stats")
                .short('s')
                .long("stats")
                .help("List all metrics in stats")
                .action(clap::ArgAction::SetTrue),
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
        debug!("loading config: {}", file);
        match Config::load(file) {
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

    // launch the server
    match config.general.engine {
        Engine::Mio => {
            // initialize storage
            let storage = Storage::new();

            // initialize parser
            let parser = Parser::new();

            // initialize process
            let process_builder = ProcessBuilder::<Parser, Request, Response, Storage>::new(
                &config, log, parser, storage,
            )
            .expect("failed to initialize process");

            // spawn threads
            let process = process_builder.spawn();
            process.wait();
        }
        Engine::Tokio => tokio::spawn(config, log),
    }
}
