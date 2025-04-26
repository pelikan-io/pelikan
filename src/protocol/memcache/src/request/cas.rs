// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

#[derive(Debug, PartialEq, Eq)]
pub struct Cas {
    pub(crate) key: Box<[u8]>,
    pub(crate) value: Box<[u8]>,
    pub(crate) flags: u32,
    pub(crate) ttl: Ttl,
    pub(crate) cas: u64,
    pub(crate) noreply: bool,
}

impl Cas {
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn ttl(&self) -> Ttl {
        self.ttl
    }

    pub fn flags(&self) -> u32 {
        self.flags
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn noreply(&self) -> bool {
        self.noreply
    }
}

impl Klog for Cas {
    type Response = Response;

    fn klog(&self, response: &Self::Response) {
        let (code, len) = match response {
            Response::Stored(ref res) => {
                CAS_STORED.increment();
                (STORED, res.len())
            }
            Response::Exists(ref res) => {
                CAS_EXISTS.increment();
                (EXISTS, res.len())
            }
            Response::NotFound(ref res) => {
                CAS_NOT_FOUND.increment();
                (NOT_FOUND, res.len())
            }
            _ => {
                return;
            }
        };
        klog!(
            "\"cas {} {} {} {} {}\" {} {}",
            string_key(self.key()),
            self.flags(),
            self.ttl.get().unwrap_or(0),
            self.value().len(),
            self.cas(),
            code,
            len
        );
    }
}
