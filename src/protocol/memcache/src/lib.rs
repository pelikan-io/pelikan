// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

#[macro_use]
extern crate logger;

pub mod binary;
pub mod text;

pub use binary::BinaryProtocol;
pub use text::TextProtocol;

mod request;
mod response;
mod storage;
mod util;

pub(crate) use util::*;

pub use request::*;
pub use response::*;
pub use storage::*;

pub use protocol_common::{Compose, Parse, ParseOk, Protocol};

pub use common::expiry::TimeType;
use logger::Klog;

const CRLF: &[u8] = b"\r\n";

pub enum MemcacheError {
    Error(Error),
    ClientError(ClientError),
    ServerError(ServerError),
}

#[cfg(feature = "metrics")]
pub mod metrics;

#[cfg(feature = "metrics")]
pub use metrics::*;
