// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

#[derive(Debug, PartialEq, Eq)]
pub struct Get {
    pub(crate) key: bool,
    pub(crate) cas: bool,
    pub(crate) opaque: Option<u32>,
    pub(crate) keys: Box<[Box<[u8]>]>,
}

impl Get {
    pub fn cas(&self) -> bool {
        self.cas
    }

    pub fn keys(&self) -> &[Box<[u8]>] {
        self.keys.as_ref()
    }
}

impl Klog for Get {
    type Response = Response;

    fn klog(&self, response: &Self::Response) {
        if let Response::Values(ref res) = response {
            let verb = if self.cas { "gets" } else { "get" };

            for value in res.values() {
                if value.len().is_none() {
                    klog!(
                        "\"{verb} {}\" {} 0",
                        String::from_utf8_lossy(value.key()),
                        MISS
                    );
                } else {
                    klog!(
                        "\"{verb} {}\" {} {}",
                        String::from_utf8_lossy(value.key()),
                        HIT,
                        value.len().unwrap(),
                    );
                }
            }
        }
    }
}
