#[macro_use]
extern crate logger;

use config::{Config, Engine};

use entrystore::Noop;
use logger::{configure_logging, Drain};
use protocol_ping::{Request, RequestParser, Response};
use server::ProcessBuilder;

use backtrace::Backtrace;
use clap::{Arg, Command};

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
        .arg(
            Arg::new("CONFIG")
                .help("Server configuration file")
                .action(clap::ArgAction::Set)
                .index(1),
        )
        .get_matches();

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
