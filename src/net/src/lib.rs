// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

mod connector;
mod listener;
mod stream;
mod tcp;

#[cfg(any(feature = "boringssl", feature = "openssl"))]
mod tls_tcp;

pub use connector::*;
pub use listener::*;
pub use stream::*;
pub use tcp::*;

#[cfg(any(feature = "boringssl", feature = "openssl"))]
pub use tls_tcp::*;

pub mod event {
    pub use mio::event::*;
}

pub use mio::*;

#[cfg(feature = "metrics")]
mod metrics;

#[cfg(feature = "metrics")]
pub use metrics::*;

#[cfg(feature = "metrics")]
macro_rules! metrics {
    { $( $tt:tt )* } => { $( $tt )* }
}

#[cfg(not(feature = "metrics"))]
macro_rules! metrics {
    { $( $tt:tt)* } => {}
}

pub(crate) use metrics;

use core::fmt::Debug;
use core::ops::Deref;
use std::io::{Error, ErrorKind, Read, Write};
use std::net::{SocketAddr, ToSocketAddrs};

type Result<T> = std::io::Result<T>;
