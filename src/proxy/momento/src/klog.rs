// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

pub(crate) fn klog_get(key: &str, response_len: usize) {
    if response_len == 0 {
        klog!("\"get {}\" 0 {}", key, response_len);
    } else {
        klog!("\"get {}\" 4 {}", key, response_len);
    }
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
