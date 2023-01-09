// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use core::fmt::Display;

pub enum Status {
    Miss = 0,
    Hit = 4,
    Stored = 5,
    Exists = 6,
    Deleted = 7,
    NotFound = 8,
    NotStored = 9,
}

// response codes for klog
const MISS: u8 = 0;
const HIT: u8 = 4;
const STORED: u8 = 5;
const EXISTS: u8 = 6;
const DELETED: u8 = 7;
const NOT_FOUND: u8 = 8;
const NOT_STORED: u8 = 9;

pub(crate) fn klog_1(command: &dyn Display, key: &str, status: Status, response_len: usize) {
    klog!("\"{} {}\" {} {}", command, key, status as u8, response_len);
}

pub(crate) fn klog_2(command: &dyn Display, key: &str, field: &str, status: Status, response_len: usize) {
    klog!("\"{} {} {}\" {} {}", command, key, field, status as u8, response_len);
}

pub(crate) fn klog_hget(key: &str, field: &str, response_len: usize) {
    if response_len == 0 {
        klog!("\"hget {} {}\" 0 {}", key, field, response_len);
    } else {
        klog!("\"hget {} {}\" 4 {}", key, field, response_len);
    }
}

pub(crate) fn klog_hgetall(key: &str, response_len: usize) {
    if response_len == 0 {
        klog!("\"hgetall {}\" 0 {}", key, response_len);
    } else {
        klog!("\"hgetall {}\" 4 {}", key, response_len);
    }
}

pub(crate) fn klog_hlen(key: &str, response_len: usize) {
    if response_len == 0 {
        klog!("\"hlen {}\" 0 {}", key, response_len);
    } else {
        klog!("\"hlen {}\" 4 {}", key, response_len);
    }
}

pub(crate) fn klog_hmget(key: &str, field: &str, response_len: usize) {
    if response_len == 0 {
        klog!("\"hmget {} {}\" 0 {}", key, field, response_len);
    } else {
        klog!("\"hmget {} {}\" 4 {}", key, field, response_len);
    }
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
