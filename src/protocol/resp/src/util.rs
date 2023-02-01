// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

pub use nom::bytes::streaming::*;
pub use nom::character::streaming::*;
pub use nom::{AsChar, Err, IResult, InputTakeAtPosition, Needed};
pub use protocol_common::Compose;
pub use std::io::{Error, ErrorKind, Write};

use crate::message::*;
use std::sync::Arc;

// consumes one or more literal spaces
pub fn space1(input: &[u8]) -> IResult<&[u8], &[u8]> {
    input.split_at_position1(
        |item| {
            let c = item.as_char();
            c != ' '
        },
        nom::error::ErrorKind::Space,
    )
}

// parses a string that is binary safe if wrapped in quotes, otherwise it must
// not contain a space, carriage return, or newline
pub fn string(input: &[u8]) -> IResult<&[u8], &[u8]> {
    match input.first() {
        Some(b'\"') => {
            let (input, _) = take(1usize)(input)?;
            let (input, key) = take_till(|b| b == b'\"')(input)?;
            let (input, _) = take(1usize)(input)?;
            Ok((input, key))
        }
        Some(_) => take_till(|b| (b == b' ' || b == b'\r' || b == b'\n'))(input),
        None => Err(Err::Incomplete(Needed::new(1))),
    }
}

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

    match dbg!(array.remove(0)) {
        Message::BulkString(value) => {
            let text = value
                .inner
                .ok_or_else(|| Error::new(ErrorKind::Other, "bulk string is null"))?;
            std::str::from_utf8(&*text)
                .map_err(|_| Error::new(ErrorKind::Other, "bulk string not valid utf8"))?
                .parse::<i64>()
                .map_err(|_| Error::new(ErrorKind::Other, "bulk string is not a i64"))
                .map(Some)
        }
        _ => {
            return Err(Error::new(
                ErrorKind::Other,
                "next array element is not a bulk string",
            ))
        }
    }
}
