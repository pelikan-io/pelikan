// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

mod message;
mod request;
mod response;
mod util;

pub(crate) use util::*;

pub use request::*;
pub use response::*;

use rustcommon_metrics::*;

counter!(HGET);
counter!(HGET_EX);
counter!(HGET_HIT);
counter!(HGET_MISS);

common::metrics::test_no_duplicates!();
