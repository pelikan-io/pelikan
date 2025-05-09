// Copyright 2025 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Error;
use std::sync::Arc;

use super::*;

#[metric(name = "zcount")]
pub static ZCOUNT: Counter = Counter::new();

#[metric(name = "zcount_ex")]
pub static ZCOUNT_EX: Counter = Counter::new();

#[metric(name = "zcount_hit")]
pub static ZCOUNT_HIT: Counter = Counter::new();

#[metric(name = "zcount_miss")]
pub static ZCOUNT_MISS: Counter = Counter::new();

#[derive(Debug, PartialEq)]
pub struct SortedSetCount {
    key: Arc<[u8]>,
    min_score: f64,
    min_score_exclusive: bool,
    max_score: f64,
    max_score_exclusive: bool,
}

impl TryFrom<Message> for SortedSetCount {
    type Error = Error;

    fn try_from(other: Message) -> Result<Self, Error> {
        let array = match other {
            Message::Array(array) => array,
            _ => return Err(Error::new(ErrorKind::Other, "malformed command")),
        };

        if array.inner.is_none() {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }

        let mut array = array.inner.unwrap();

        if array.len() != 4 {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }

        let _command = take_bulk_string(&mut array)?;
        let key = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;
        let min_score_string = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;
        let max_score_string = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed content"))?;

        let (min_score, min_score_exclusive) = parse_score_boundary_as_float(&min_score_string)?;
        let (max_score, max_score_exclusive) = parse_score_boundary_as_float(&max_score_string)?;

        Ok(Self {
            key,
            min_score,
            min_score_exclusive,
            max_score,
            max_score_exclusive,
        })
    }
}

impl SortedSetCount {
    pub fn new(
        key: &[u8],
        min_score: f64,
        min_score_exclusive: bool,
        max_score: f64,
        max_score_exclusive: bool,
    ) -> Self {
        Self {
            key: key.into(),
            min_score,
            min_score_exclusive,
            max_score,
            max_score_exclusive,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn min_score(&self) -> f64 {
        self.min_score
    }

    pub fn max_score(&self) -> f64 {
        self.max_score
    }

    pub fn min_score_exclusive(&self) -> bool {
        self.min_score_exclusive
    }

    pub fn max_score_exclusive(&self) -> bool {
        self.max_score_exclusive
    }
}

impl From<&SortedSetCount> for Message {
    fn from(value: &SortedSetCount) -> Message {
        let min_score_string = match (value.min_score, value.min_score_exclusive) {
            (f64::INFINITY, false) => "+inf".to_string(),
            (f64::NEG_INFINITY, false) => "-inf".to_string(),
            (score, false) => format!("{}", score),
            (score, true) => format!("({}", score),
        };
        let max_score_string = match (value.max_score, value.max_score_exclusive) {
            (f64::INFINITY, false) => "+inf".to_string(),
            (f64::NEG_INFINITY, false) => "-inf".to_string(),
            (score, false) => format!("{}", score),
            (score, true) => format!("({}", score),
        };
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"ZCOUNT")),
                Message::BulkString(BulkString::new(value.key())),
                Message::BulkString(BulkString::new(min_score_string.as_bytes())),
                Message::BulkString(BulkString::new(max_score_string.as_bytes())),
            ]),
        })
    }
}

impl Compose for SortedSetCount {
    fn compose(&self, buf: &mut dyn BufMut) -> usize {
        Message::from(self).compose(buf)
    }
}

// Returns a tuple of (value, is_exclusive)
fn parse_score_boundary_as_float(value: &[u8]) -> Result<(f64, bool), Error> {
    // First check if the value is +inf or -inf
    if value == b"+inf" {
        return Ok((f64::INFINITY, false));
    }
    if value == b"-inf" {
        return Ok((f64::NEG_INFINITY, false));
    }

    // Otherwise, split apart '(' and the value if present
    let (exclusive_symbol, number) = if value[0] == b'(' {
        (true, &value[1..])
    } else {
        (false, value)
    };

    let score = std::str::from_utf8(number)
        .map_err(|_| Error::new(ErrorKind::Other, "ZRANGE score is not valid utf8"))?
        .parse::<f64>()
        .map_err(|_| Error::new(ErrorKind::Other, "ZRANGE score is not a float"))?;

    if exclusive_symbol {
        Ok((score, true))
    } else {
        Ok((score, false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser() {
        let parser = RequestParser::new();
        assert_eq!(
            parser
                .parse(b"ZCOUNT z -inf +inf\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetCount(SortedSetCount::new(
                b"z",
                f64::NEG_INFINITY,
                false,
                f64::INFINITY,
                false
            ))
        );

        assert_eq!(
            parser.parse(b"ZCOUNT z (1 3\r\n").unwrap().into_inner(),
            Request::SortedSetCount(SortedSetCount::new(b"z", 1.0, true, 3.0, false))
        );

        assert_eq!(
            parser
                .parse(b"*4\r\n$6\r\nZCOUNT\r\n$1\r\nz\r\n$1\r\n1\r\n$1\r\n3\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetCount(SortedSetCount::new(b"z", 1.0, false, 3.0, false))
        );

        assert_eq!(
            parser
                .parse(b"*4\r\n$6\r\nZCOUNT\r\n$1\r\nz\r\n$1\r\n1\r\n$2\r\n(3\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetCount(SortedSetCount::new(b"z", 1.0, false, 3.0, true))
        );

        assert_eq!(
            parser
                .parse(b"*4\r\n$6\r\nZCOUNT\r\n$1\r\nz\r\n$4\r\n-inf\r\n$4\r\n+inf\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetCount(SortedSetCount::new(
                b"z",
                f64::NEG_INFINITY,
                false,
                f64::INFINITY,
                false
            ))
        );
    }
}
