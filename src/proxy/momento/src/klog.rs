// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use core::fmt::Display;

#[allow(dead_code)]
pub enum Status {
    Miss = 0,
    Hit = 4,
    Stored = 5,
    Exists = 6,
    Deleted = 7,
    NotFound = 8,
    NotStored = 9,
}

pub(crate) fn klog_1(
    command: &dyn Display,
    key: &dyn AsRef<[u8]>,
    status: Status,
    response_len: usize,
) {
    klog!(
        "\"{} {}\" {} {}",
        command,
        EscapedStr::new(key),
        status as u8,
        response_len
    );
}

pub(crate) fn klog_2(
    command: &dyn Display,
    key: &dyn AsRef<[u8]>,
    field: &dyn AsRef<[u8]>,
    status: Status,
    response_len: usize,
) {
    klog!(
        "\"{} {} {}\" {} {}",
        command,
        EscapedStr::new(key),
        EscapedStr::new(field),
        status as u8,
        response_len
    );
}

pub(crate) fn klog_7(
    command: &dyn Display,
    key: &dyn AsRef<[u8]>,
    field: &dyn AsRef<[u8]>,
    ttl: i32,
    value_len: usize,
    status: Status,
    response_len: usize,
) {
    klog!(
        "\"{} {} {} {} {}\" {} {}",
        command,
        EscapedStr::new(key),
        EscapedStr::new(field),
        ttl,
        value_len,
        status as u8,
        response_len
    );
}

pub fn klog_set(
    key: &dyn AsRef<[u8]>,
    flags: u32,
    ttl: i32,
    value_len: usize,
    result_code: usize,
    response_len: usize,
) {
    klog!(
        "\"set {} {} {} {}\" {} {}",
        EscapedStr::new(key),
        flags,
        ttl,
        value_len,
        result_code,
        response_len
    );
}

struct EscapedStr<'a> {
    inner: &'a [u8],
}

impl<'a> EscapedStr<'a> {
    fn new(input: &'a dyn AsRef<[u8]>) -> EscapedStr<'a> {
        Self {
            inner: input.as_ref(),
        }
    }
}

impl<'a> std::fmt::Display for EscapedStr<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        for inbyte in self.inner.iter() {
            for outbyte in std::ascii::escape_default(*inbyte) {
                write!(f, "{}", unsafe { char::from_u32_unchecked(outbyte as u32) })?;
            }
        }
        Ok(())
    }
}
