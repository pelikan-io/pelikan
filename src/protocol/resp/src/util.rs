// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

pub use nom::bytes::streaming::*;
pub use nom::character::streaming::*;
pub use nom::{AsChar, Err, IResult, Needed};
pub use std::io::{Error, ErrorKind, Write};

use crate::message::*;
use std::sync::Arc;

pub fn take_bulk_string(array: &mut Vec<Message>) -> Result<Option<Arc<[u8]>>, Error> {
    if array.is_empty() {
        return Ok(None);
    }

    if let Message::BulkString(s) = array.remove(0) {
        if s.inner.is_none() {
            return Err(Error::new(ErrorKind::Other, "bulk string is null"));
        }

        let s = s.inner.unwrap();

        Ok(Some(s))
    } else {
        Err(Error::new(
            ErrorKind::Other,
            "next array element is not a bulk string",
        ))
    }
}

pub fn take_bulk_string_as_utf8(array: &mut Vec<Message>) -> Result<Option<String>, Error> {
    let s = take_bulk_string(array)?;

    if s.is_none() {
        return Ok(None);
    }

    std::str::from_utf8(&s.unwrap())
        .map_err(|_| Error::new(ErrorKind::Other, "bulk string not valid utf8"))
        .map(|s| Some(s.to_owned()))
}

pub fn take_bulk_string_as_u64(array: &mut Vec<Message>) -> Result<Option<u64>, Error> {
    let s = take_bulk_string(array)?;

    if s.is_none() {
        return Ok(None);
    }

    std::str::from_utf8(&s.unwrap())
        .map_err(|_| Error::new(ErrorKind::Other, "bulk string not valid utf8"))?
        .parse::<u64>()
        .map_err(|_| Error::new(ErrorKind::Other, "bulk string is not a u64"))
        .map(Some)
}

pub fn take_bulk_string_as_i64(array: &mut Vec<Message>) -> Result<Option<i64>, Error> {
    if array.is_empty() {
        return Ok(None);
    }

    match array.remove(0) {
        Message::BulkString(value) => {
            let text = value
                .inner
                .ok_or_else(|| Error::new(ErrorKind::Other, "bulk string is null"))?;
            std::str::from_utf8(&text)
                .map_err(|_| Error::new(ErrorKind::Other, "bulk string not valid utf8"))?
                .parse::<i64>()
                .map_err(|_| Error::new(ErrorKind::Other, "bulk string is not a i64"))
                .map(Some)
        }
        _ => Err(Error::new(
            ErrorKind::Other,
            "next array element is not a bulk string",
        )),
    }
}
