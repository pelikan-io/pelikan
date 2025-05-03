// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

#[derive(Debug, PartialEq, Eq)]
pub struct Quit {}

impl Quit {}

impl Klog for Quit {
    type Response = Response;

    fn klog(&self, _response: &Self::Response) {}
}
