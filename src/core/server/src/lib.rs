// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! This crate defines a Pelikan cache server which is a single process with
//! multiple threads.
//!
//! # Thread Model
//! The Pelikan cache servers are comprised of multiple threads which allow us
//! to separate the control and data planes. There is one thread model: an
//! `admin` thread for the control plane, and a `listener` thread plus N
//! `worker` threads for the data plane. The workers share the underlying
//! cache datastructure, which is internally synchronized, through an `Arc`
//! and execute requests in place. The worker count is a scaling knob — not a
//! mode switch — so a single-worker configuration is simply N = 1 of the
//! same model.
//!
//! ```text
//! ┌──────────┐                 ┌──────────┐
//! │  admin   │                 │ listener │
//! │          │                 │          │
//! │  :9999   │                 │  :12321  │
//! └──────────┘                 └──────────┘
//!                                    │
//!                            ┌───────┴───────┐
//!                            │               │
//!                            ▼               ▼
//! ┌ ─ ─ ─ ─ ─          ┌──────────┐    ┌──────────┐        ┌ ─ ─ ─ ─ ─
//!            │─        │          │    │          │                   │─
//! │  client   ◀┼──────▶│  worker  │    │  worker  │◀──────▶│  client    │─
//!            │   │     │          │    │          │                   │   │
//! └ ─ ─ ─ ─ ─  │       └──────────┘    └──────────┘        └ ─ ─ ─ ─ ─  │
//!   └ ─ ─ ─ ─ ─  │           │               │               └ ─ ─ ─ ─ ─  │
//!     └ ─ ─ ─ ─ ─            └───────┬───────┘                 └ ─ ─ ─ ─ ─
//!                                    ▼
//!                             ┌────────────┐
//!                             │   cache    │
//!                             │ Arc-shared │
//!                             └────────────┘
//! ```
//!
//! ## Control Plane
//! The control plane is handled by a single `admin` thread. This thread is
//! responsible for handling administrative commands and metrics exposition.
//!
//! ## Data Plane
//! The data plane is handled by a `listener` thread and one or more `worker`
//! threads.
//!
//! ### Listener
//! At a minimum we have one `listener` thread which owns the listening socket,
//! accepts new connections, and handles TLS negotiation if it is enabled by the
//! configuration. Fully negotiated sessions are then handed off to one or more
//! worker threads.
//!
//! ### Worker
//! Worker threads handle ongoing communications for an established session.
//! Each worker parses requests, executes them directly against the shared
//! cache datastructure — a plain value shared via `Arc`, not a thread — and
//! composes responses. There is no periodic expiration pass: the storage
//! engine treats expired items as missing on access and reclaims expired
//! segments under write pressure.

#[macro_use]
extern crate logger;

use admin::AdminBuilder;
use common::signal::Signal;
use common::ssl::tls_acceptor;
use config::*;
use core::time::Duration;
use crossbeam_channel::{bounded, Sender};
use entrystore::EntryStore;
use logger::{Klog, LogDrain};
use metriken::*;
use pelikan_net::event::{Event, Source};
use pelikan_net::*;
use protocol_common::{Compose, Execute};
use queues::{Queues, Waker};
use session::{Buf, ServerSession, Session};
use slab::Slab;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

mod listener;
mod process;
mod workers;

use listener::ListenerBuilder;
use workers::WorkersBuilder;

pub use process::{Process, ProcessBuilder};

// TODO(bmartin): this *should* be plenty safe, the queue should rarely ever be
// full, and a single wakeup should drain at least one message and make room for
// the response. A stat to prove that this is sufficient would be good.
const QUEUE_RETRIES: usize = 3;

const QUEUE_CAPACITY: usize = 64 * 1024;

// determines the max number of calls to accept when the listener is ready
const ACCEPT_BATCH: usize = 8;

const LISTENER_TOKEN: Token = Token(usize::MAX - 1);
const WAKER_TOKEN: Token = Token(usize::MAX);

const THREAD_PREFIX: &str = "pelikan";

pub static PERCENTILES: &[(&str, f64)] = &[
    ("p25", 25.0),
    ("p50", 50.0),
    ("p75", 75.0),
    ("p90", 90.0),
    ("p99", 99.0),
    ("p999", 99.9),
    ("p9999", 99.99),
];

// stats
#[metric(name = "process_req")]
pub static PROCESS_REQ: Counter = Counter::new();

fn map_err(e: std::io::Error) -> Result<()> {
    match e.kind() {
        ErrorKind::WouldBlock => Ok(()),
        _ => Err(e),
    }
}

common::metrics::test_no_duplicates!();
