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
use metriken::{metric, AtomicHistogram, Counter};

const CRLF: &[u8] = b"\r\n";

pub enum MemcacheError {
    Error(Error),
    ClientError(ClientError),
    ServerError(ServerError),
}

/*
 * GET
 */

#[metric(name = "get")]
pub static GET: Counter = Counter::new();

#[metric(name = "get_ex")]
pub static GET_EX: Counter = Counter::new();

#[metric(name = "get_key")]
pub static GET_KEY: Counter = Counter::new();

#[metric(name = "get_key_hit")]
pub static GET_KEY_HIT: Counter = Counter::new();

#[metric(name = "get_key_miss")]
pub static GET_KEY_MISS: Counter = Counter::new();

#[metric(
    name = "get_cardinality",
    description = "distribution of key cardinality for get requests"
)]
pub static GET_CARDINALITY: AtomicHistogram = AtomicHistogram::new(7, 20);

/*
 * GETS
 */

#[metric(name = "gets")]
pub static GETS: Counter = Counter::new();

#[metric(name = "gets_ex")]
pub static GETS_EX: Counter = Counter::new();

#[metric(name = "gets_key")]
pub static GETS_KEY: Counter = Counter::new();

#[metric(name = "gets_key_hit")]
pub static GETS_KEY_HIT: Counter = Counter::new();

#[metric(name = "gets_key_miss")]
pub static GETS_KEY_MISS: Counter = Counter::new();

/*
 * SET
 */

#[metric(name = "set")]
pub static SET: Counter = Counter::new();

#[metric(name = "set_ex")]
pub static SET_EX: Counter = Counter::new();

#[metric(name = "set_stored")]
pub static SET_STORED: Counter = Counter::new();

#[metric(name = "set_not_stored")]
pub static SET_NOT_STORED: Counter = Counter::new();

/*
 * ADD
 */

#[metric(name = "add")]
pub static ADD: Counter = Counter::new();

#[metric(name = "add_ex")]
pub static ADD_EX: Counter = Counter::new();

#[metric(name = "add_stored")]
pub static ADD_STORED: Counter = Counter::new();

#[metric(name = "add_not_stored")]
pub static ADD_NOT_STORED: Counter = Counter::new();

/*
 * REPLACE
 */

#[metric(name = "replace")]
pub static REPLACE: Counter = Counter::new();

#[metric(name = "replace_ex")]
pub static REPLACE_EX: Counter = Counter::new();

#[metric(name = "replace_stored")]
pub static REPLACE_STORED: Counter = Counter::new();

#[metric(name = "replace_not_stored")]
pub static REPLACE_NOT_STORED: Counter = Counter::new();

/*
 * APPEND
 */

#[metric(name = "append")]
pub static APPEND: Counter = Counter::new();

#[metric(name = "append_ex")]
pub static APPEND_EX: Counter = Counter::new();

#[metric(name = "append_stored")]
pub static APPEND_STORED: Counter = Counter::new();

#[metric(name = "append_not_stored")]
pub static APPEND_NOT_STORED: Counter = Counter::new();

/*
 * PREPEND
 */

#[metric(name = "prepend")]
pub static PREPEND: Counter = Counter::new();

#[metric(name = "prepend_ex")]
pub static PREPEND_EX: Counter = Counter::new();

#[metric(name = "prepend_stored")]
pub static PREPEND_STORED: Counter = Counter::new();

#[metric(name = "prepend_not_stored")]
pub static PREPEND_NOT_STORED: Counter = Counter::new();

/*
 * DELETE
 */

#[metric(name = "delete")]
pub static DELETE: Counter = Counter::new();

#[metric(name = "delete_ex")]
pub static DELETE_EX: Counter = Counter::new();

#[metric(name = "delete_deleted")]
pub static DELETE_DELETED: Counter = Counter::new();

#[metric(name = "delete_not_found")]
pub static DELETE_NOT_FOUND: Counter = Counter::new();

/*
 * INCR
 */

#[metric(name = "incr")]
pub static INCR: Counter = Counter::new();

#[metric(name = "incr_ex")]
pub static INCR_EX: Counter = Counter::new();

#[metric(name = "incr_stored")]
pub static INCR_STORED: Counter = Counter::new();

#[metric(name = "incr_not_found")]
pub static INCR_NOT_FOUND: Counter = Counter::new();

/*
 * DECR
 */

#[metric(name = "decr")]
pub static DECR: Counter = Counter::new();

#[metric(name = "decr_ex")]
pub static DECR_EX: Counter = Counter::new();

#[metric(name = "decr_stored")]
pub static DECR_STORED: Counter = Counter::new();

#[metric(name = "decr_not_found")]
pub static DECR_NOT_FOUND: Counter = Counter::new();

/*
 * CAS
 */

#[metric(name = "cas")]
pub static CAS: Counter = Counter::new();

#[metric(name = "cas_ex")]
pub static CAS_EX: Counter = Counter::new();

#[metric(name = "cas_exists")]
pub static CAS_EXISTS: Counter = Counter::new();

#[metric(name = "cas_not_found")]
pub static CAS_NOT_FOUND: Counter = Counter::new();

#[metric(name = "cas_stored")]
pub static CAS_STORED: Counter = Counter::new();

/*
 * FLUSH_ALL
 */

#[metric(name = "flush_all")]
pub static FLUSH_ALL: Counter = Counter::new();

#[metric(name = "flush_all_ex")]
pub static FLUSH_ALL_EX: Counter = Counter::new();

/*
 * QUIT
 */

#[metric(name = "quit")]
pub static QUIT: Counter = Counter::new();

common::metrics::test_no_duplicates!();
