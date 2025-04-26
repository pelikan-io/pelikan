// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

#[derive(Debug, PartialEq, Eq)]
pub struct Delete {
    pub(crate) key: Box<[u8]>,
    pub(crate) noreply: bool,
    pub(crate) opaque: Option<u32>,
}

impl Delete {
    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn noreply(&self) -> bool {
        self.noreply
    }
}

impl Klog for Delete {
    type Response = Response;

    fn klog(&self, response: &Self::Response) {
        let (code, len) = match response {
            Response::Deleted(ref res) => {
                DELETE_DELETED.increment();
                (DELETED, res.len())
            }
            Response::NotFound(ref res) => {
                DELETE_NOT_FOUND.increment();
                (NOT_FOUND, res.len())
            }
            _ => {
                return;
            }
        };
        klog!("\"delete {}\" {} {}", string_key(self.key()), code, len);
    }
}
