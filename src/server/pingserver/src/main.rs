#[macro_use]
extern crate logger;

use config::{Config, Engine};

use entrystore::Noop;
use logger::{configure_logging, Drain};
use protocol_ping::{Request, RequestParser, Response};
use server::{Process, ProcessBuilder};

use backtrace::Backtrace;
use clap::{Arg, Command};
use ::tokio::runtime::Runtime;

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

    // launch the server
    match Pingserver::new(config) {
        Ok(s) => s.wait(),
        Err(e) => {
            eprintln!("error launching pingserver: {e}");
            std::process::exit(1);
        }
    }
}

enum Pingserver {
    Mio { process: Process },
    Tokio { control: Runtime, data: Runtime },
}

impl Pingserver {
    pub fn new(config: Config) -> Result<Self, std::io::Error> {
        // initialize logging
        let log = configure_logging(&config);

        // initialize metrics
        common::metrics::init();

        match config.general.engine {
            Engine::Mio => {
                // initialize storage
                let storage = Storage::new();

                // initialize parser
                let parser = Parser::new();

                // initialize process
                let process_builder = ProcessBuilder::<Parser, Request, Response, Storage>::new(
                    &config, log, parser, storage,
                )?;

                // spawn threads
                let process = process_builder.spawn();

                Ok(Pingserver::Mio { process })
            }
            Engine::Tokio => tokio::spawn(config, log),
        }
    }

    /// Triggers a shutdown of the process and blocks until the process has
    /// fully terminated. This is more likely to be used for running integration
    /// tests or other automated testing.
    pub fn shutdown(self) {
        match self {
            Pingserver::Mio { process } => process.shutdown(),
            Pingserver::Tokio { control, data } => {
                data.shutdown_timeout(std::time::Duration::from_millis(100));
                control.shutdown_timeout(std::time::Duration::from_millis(100));
            }
        }
    }

    /// Wait for all threads to complete. Blocks until the process has fully
    /// terminated. Under normal conditions, this will block indefinitely.
    pub fn wait(self) {
        match self {
            Pingserver::Mio { process } => process.wait(),
            Pingserver::Tokio { control, data } => {
                while RUNNING.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(250));
                }
                data.shutdown_timeout(std::time::Duration::from_millis(100));
                control.shutdown_timeout(std::time::Duration::from_millis(100));
            }
        }
    }
}

