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

pub(crate) fn klog_1(command: &dyn Display, key: &str, status: Status, response_len: usize) {
    klog!("\"{} {}\" {} {}", command, key, status as u8, response_len);
}

pub(crate) fn klog_2(
    command: &dyn Display,
    key: &str,
    field: &str,
    status: Status,
    response_len: usize,
) {
    klog!(
        "\"{} {} {}\" {} {}",
        command,
        key,
        field,
        status as u8,
        response_len
    );
}

pub(crate) fn klog_7(
    command: &dyn Display,
    key: &str,
    field: &str,
    ttl: i32,
    value_len: usize,
    status: Status,
    response_len: usize,
) {
    klog!(
        "\"{} {} {} {} {}\" {} {}",
        command,
        key,
        field,
        ttl,
        value_len,
        status as u8,
        response_len
    );
}

pub fn klog_set(
    key: &str,
    flags: u32,
    ttl: i32,
    value_len: usize,
    result_code: usize,
    response_len: usize,
) {
    klog!(
        "\"set {} {} {} {}\" {} {}",
        key,
        flags,
        ttl,
        value_len,
        result_code,
        response_len
    );
}
