// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Logging backend for Pelikan, built on the `tracing` ecosystem.
//!
//! Sets up a non-blocking async writer via `tracing-appender` and bridges
//! existing `log` crate callsites through `tracing-log`. The `klog!` macro
//! provides callsite-sampled command logging.

use config::{DebugConfig, KlogConfig, LogRotationInterval};
use logroller::{Compression, LogRollerBuilder, Rotation, RotationAge, RotationSize};
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::prelude::*;

/// Re-export log macros so existing `#[macro_use] extern crate logger` +
/// `error!()` etc. keep working.
pub use log::{debug, error, info, trace, warn};

/// Log a fatal error and terminate the process.
#[macro_export]
macro_rules! fatal {
    () => {{
        $crate::error!("fatal error");
        std::process::exit(1);
    }};
    ($fmt:expr) => {{
        $crate::error!($fmt);
        std::process::exit(1);
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        $crate::error!($fmt, $($arg)*);
        std::process::exit(1);
    }};
}

/// The sample rate for klog, set during `configure_logging`.
static KLOG_SAMPLE: AtomicUsize = AtomicUsize::new(100);

/// Log a command execution at the configured sample rate.
/// Only every Nth call actually emits a log event, avoiding format overhead
/// on non-sampled invocations.
#[macro_export]
macro_rules! klog {
    ($($arg:tt)*) => {{
        static COUNTER: ::std::sync::atomic::AtomicUsize =
            ::std::sync::atomic::AtomicUsize::new(0);
        let sample = $crate::klog_sample();
        if sample > 0
            && COUNTER.fetch_add(1, ::std::sync::atomic::Ordering::Relaxed) % sample == 0
        {
            $crate::error!(target: "klog", $($arg)*);
        }
    }}
}

/// Returns the current klog sample rate.
#[inline]
pub fn klog_sample() -> usize {
    KLOG_SAMPLE.load(Ordering::Relaxed)
}

pub trait Klog {
    type Response;

    fn klog(&self, response: &Self::Response);
}

/// Handle returned by `configure_logging`. Holds the worker guards for the
/// non-blocking appenders. Logs are flushed when this is dropped.
pub struct LogDrain {
    _guards: Vec<WorkerGuard>,
}

/// Initialize the tracing subscriber with non-blocking file/stdout output
/// and the tracing-log bridge for `log` crate compatibility.
///
/// Returns a `LogDrain` that must be kept alive for the process lifetime.
pub fn configure_logging<T: DebugConfig + KlogConfig>(config: &T) -> LogDrain {
    let debug_config = config.debug();
    let klog_config = config.klog();

    // Store the klog sample rate globally for the klog! macro
    KLOG_SAMPLE.store(klog_config.sample(), Ordering::Relaxed);

    // Map log::Level to tracing::Level
    let level = match debug_config.log_level() {
        log::Level::Error => tracing::Level::ERROR,
        log::Level::Warn => tracing::Level::WARN,
        log::Level::Info => tracing::Level::INFO,
        log::Level::Debug => tracing::Level::DEBUG,
        log::Level::Trace => tracing::Level::TRACE,
    };

    // Set up the debug log writer (file with rotation, or stdout)
    let (debug_writer, debug_guard) = if let Some(file) = debug_config.log_file() {
        let path = std::path::Path::new(&file);
        let dir = path.parent().unwrap_or(std::path::Path::new("."));
        let filename = path
            .file_name()
            .map(std::path::Path::new)
            .unwrap_or(std::path::Path::new("pelikan.log"));
        match debug_config.log_rotation_interval() {
            LogRotationInterval::None => {
                // No rotation — user manages rotation externally (e.g. logrotate)
                let file_appender = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&file)
                    .expect("failed to open debug log file");
                tracing_appender::non_blocking(file_appender)
            }
            interval => {
                let rotation_age = match interval {
                    LogRotationInterval::Minutely => RotationAge::Minutely,
                    LogRotationInterval::Hourly => RotationAge::Hourly,
                    LogRotationInterval::Daily => RotationAge::Daily,
                    LogRotationInterval::None => unreachable!(),
                };
                let file_appender = LogRollerBuilder::new(dir, filename)
                    .rotation(Rotation::AgeBased(rotation_age))
                    .max_keep_files(debug_config.log_max_keep_files())
                    .compression(Compression::Gzip)
                    .graceful_shutdown(true)
                    .build()
                    .expect("failed to create debug log appender");
                tracing_appender::non_blocking(file_appender)
            }
        }
    } else {
        tracing_appender::non_blocking(std::io::stdout())
    };

    let mut guards = vec![debug_guard];

    // Set up the klog writer with size-based rotation if configured
    let klog_writer_and_guard = klog_config.file().map(|file| {
        let path = std::path::Path::new(&file);
        let dir = path.parent().unwrap_or(std::path::Path::new("."));
        let filename = path
            .file_name()
            .map(std::path::Path::new)
            .unwrap_or(std::path::Path::new("klog"));
        let max_bytes = klog_config.max_size();
        let file_appender = LogRollerBuilder::new(dir, filename)
            .rotation(Rotation::SizeBased(RotationSize::Bytes(max_bytes)))
            .max_keep_files(klog_config.max_keep_files())
            .compression(Compression::Gzip)
            .graceful_shutdown(true)
            .build()
            .expect("failed to create klog appender");
        tracing_appender::non_blocking(file_appender)
    });

    if let Some((klog_writer, klog_guard)) = klog_writer_and_guard {
        guards.push(klog_guard);

        // Two-layer setup: debug log for everything, klog file for target="klog"
        let debug_layer = tracing_subscriber::fmt::layer()
            .with_writer(debug_writer.with_max_level(level))
            .with_target(true)
            .with_ansi(false);

        let klog_layer = tracing_subscriber::fmt::layer()
            .with_writer(
                klog_writer.with_filter(|meta: &tracing::Metadata<'_>| meta.target() == "klog"),
            )
            .with_target(false)
            .with_level(false)
            .with_ansi(false);

        tracing_subscriber::registry()
            .with(debug_layer)
            .with(klog_layer)
            .try_init()
            .ok();
    } else {
        // Single layer: debug log only
        let debug_layer = tracing_subscriber::fmt::layer()
            .with_writer(debug_writer.with_max_level(level))
            .with_target(true)
            .with_ansi(false);

        tracing_subscriber::registry()
            .with(debug_layer)
            .try_init()
            .ok();
    }

    // Bridge log crate events to tracing (ignore if already initialized)
    let _ = tracing_log::LogTracer::init();

    LogDrain { _guards: guards }
}
