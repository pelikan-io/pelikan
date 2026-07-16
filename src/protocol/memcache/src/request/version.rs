// Copyright 2026 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

#[derive(Debug, PartialEq, Eq)]
pub struct Version {
    pub(crate) opaque: Option<u32>,
}

impl Version {}

impl Klog for Version {
    type Response = Response;

    fn klog(&self, _response: &Self::Response) {}
}
