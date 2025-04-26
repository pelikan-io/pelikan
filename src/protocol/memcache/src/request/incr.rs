// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

#[derive(Debug, PartialEq, Eq)]
pub struct Incr {
    pub(crate) key: Box<[u8]>,
    pub(crate) value: u64,
    pub(crate) noreply: bool,
}

impl Incr {
    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn value(&self) -> u64 {
        self.value
    }

    pub fn noreply(&self) -> bool {
        self.noreply
    }
}

impl Klog for Incr {
    type Response = Response;

    fn klog(&self, response: &Self::Response) {
        let (code, len) = match response {
            Response::Numeric(ref res) => {
                INCR_STORED.increment();
                (STORED, res.len())
            }
            Response::NotFound(ref res) => {
                INCR_NOT_FOUND.increment();
                (NOT_STORED, res.len())
            }
            _ => {
                return;
            }
        };
        klog!("\"incr {}\" {} {}", string_key(self.key()), code, len);
    }
}
