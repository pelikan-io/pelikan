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

use metriken::*;

use std::sync::Arc;

type ArcByteSlice = Arc<[u8]>;

common::metrics::test_no_duplicates!();
