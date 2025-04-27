//! Fuzz test target for the Memcache binary protocol parser.

#![no_main]
use libfuzzer_sys::fuzz_target;

use protocol_memcache::*;

const MAX_KEY_LEN: usize = u16::MAX as usize;
const MAX_BATCH_SIZE: usize = 1;
const MAX_VALUE_SIZE: usize = u32::MAX as usize;

fuzz_target!(|data: &[u8]| {
    let protocol = binary::BinaryProtocol::default();

    if let Ok(request) = protocol.parse_request(data) {
        match request.into_inner() {
            Request::Get(get) => {
                if get.keys().is_empty() {
                    panic!("no keys");
                }
                if get.keys().len() > MAX_BATCH_SIZE {
                    panic!("batch size exceeds max");
                }
                for key in get.keys().iter() {
                    validate_key(key);
                }
            }
            Request::Set(set) => {
                validate_key(set.key());
                validate_value(set.value());
            }
            Request::Add(add) => {
                validate_key(add.key());
                validate_value(add.value());
            }
            Request::Replace(replace) => {
                validate_key(replace.key());
                validate_value(replace.value());
            }
            Request::Append(append) => {
                validate_key(append.key());
                validate_value(append.value());
            }
            Request::Prepend(prepend) => {
                validate_key(prepend.key());
                validate_value(prepend.value());
            }
            Request::Cas(cas) => {
                validate_key(cas.key());
                validate_value(cas.value());
            }
            Request::Delete(delete) => {
                validate_key(delete.key());
            }
            Request::Incr(incr) => {
                validate_key(incr.key());
            }
            Request::Decr(decr) => {
                validate_key(decr.key());
            }
            Request::FlushAll(_) => {}
            Request::Quit(_) => {}
        }
    }
});

fn validate_key(key: &[u8]) {
    if key.is_empty() {
        panic!("key is zero-length");
    }
    if key.len() > MAX_KEY_LEN {
        panic!("key is too long");
    }
    if key.windows(1).any(|w| w == b" ") {
        panic!("key contains SPACE: {:?}", key);
    }
    if key.windows(2).any(|w| w == b"\r\n") {
        panic!("key contains CRLF: {:?}", key);
    }
}

fn validate_value(value: &[u8]) {
    if value.len() > MAX_VALUE_SIZE {
        panic!("key is too long");
    }
}
