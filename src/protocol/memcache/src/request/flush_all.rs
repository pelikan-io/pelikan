// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

#[derive(Debug, PartialEq, Eq)]
pub struct FlushAll {
    pub(crate) delay: u32,
    pub(crate) noreply: bool,
}

impl FlushAll {
    pub fn delay(&self) -> u32 {
        self.delay
    }

    pub fn noreply(&self) -> bool {
        self.noreply
    }
}

impl Klog for FlushAll {
    type Response = Response;

    fn klog(&self, _response: &Self::Response) {}
}
