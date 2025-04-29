// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Error;
use std::sync::Arc;

use super::*;

#[metric(name = "zrange")]
pub static ZRANGE: Counter = Counter::new();

#[metric(name = "zrange_ex")]
pub static ZRANGE_EX: Counter = Counter::new();

#[metric(name = "zrange_hit")]
pub static ZRANGE_HIT: Counter = Counter::new();

#[metric(name = "zrange_miss")]
pub static ZRANGE_MISS: Counter = Counter::new();

#[derive(Debug, PartialEq, Eq)]
pub enum MomentoSortedSetFetchArgs {
    ByRank(i64, i64), // inclusive start, exclusive stop
    ByScore(StartStopValue, StartStopValue, Option<u64>, Option<i64>), // inclusive min score, inclusive max score, offset, count
}

#[derive(Debug, PartialEq, Eq)]
pub struct SortedSetRange {
    key: Arc<[u8]>,
    reversed: bool,
    with_scores: bool,
    args: MomentoSortedSetFetchArgs,
}

#[derive(Debug, PartialEq, Eq)]
pub enum StartStopValue {
    Inclusive(i64),
    Exclusive(i64),
    PositiveInfinity,
    NegativeInfinity,
}

impl StartStopValue {
    fn parse_value(value: Option<Arc<[u8]>>) -> Result<Self, Error> {
        if let Some(some_value) = value {
            // Make two copies of the value so that we can try calling both take_bulk_string_as_i64 and take_bulk_string
            let mut value_for_int_conversion =
                &mut vec![Message::BulkString(BulkString::new(&some_value.clone()))];
            let mut value_for_string_conversion =
                &mut vec![Message::BulkString(BulkString::new(&some_value.clone()))];

            if let Ok(Some(integer_start)) = take_bulk_string_as_i64(&mut value_for_int_conversion)
            {
                // Extracted the value as an integer that has no "(" or has a "+" or "-" to denote signage
                return Ok(StartStopValue::Inclusive(integer_start));
            } else {
                // Otherwise, the value may contain a "(" in front of a number
                let string_start =
                    take_bulk_string(&mut value_for_string_conversion)?.ok_or_else(|| {
                        Error::new(
                            ErrorKind::Other,
                            "malformed command, unable to extract range boundary",
                        )
                    })?;
                if string_start.is_empty() {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "malformed command, range boundary is empty",
                    ));
                }

                let positive_infinity = "+".as_bytes();
                let negative_infinity = "-".as_bytes();
                let exclusive_symbol = "(".as_bytes();
                let inclusive_symbol = "[".as_bytes(); // Used only with BYLEX which Momento does not yet support

                if string_start[0] == positive_infinity[0] {
                    return Ok(StartStopValue::PositiveInfinity);
                } else if string_start[0] == negative_infinity[0] {
                    return Ok(StartStopValue::NegativeInfinity);
                } else if string_start[0] == exclusive_symbol[0] {
                    // Extract the value without the "(", and try to convert it to an integer
                    let without_symbol = Message::BulkString(BulkString::new(&string_start[1..]));
                    let integer_start = take_bulk_string_as_i64(&mut vec![without_symbol])?
          .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command, unable to extract range boundary with exclusive symbol"))?;
                    return Ok(StartStopValue::Exclusive(integer_start));
                } else if string_start[0] == inclusive_symbol[0] {
                    // "[" is used only with BYLEX which Momento does not yet support
                    return Err(Error::new(
                ErrorKind::Other,
                "malformed command, BYLEX and associated [ range boundary is not yet supported",
            ));
                } else {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "malformed command, invalid range boundary",
                    ));
                }
            }
        } else {
            return Err(Error::new(
                ErrorKind::Other,
                "malformed command, range boundary value is none",
            ));
        }
    }
}

#[derive(Debug, PartialEq, Eq, Default)]
struct SortedSetRangeOptionalArguments {
    start: Option<StartStopValue>,
    stop: Option<StartStopValue>,
    reversed: Option<bool>,
    with_scores: Option<bool>,
    by_score: Option<bool>,
    offset: Option<u64>,
    count: Option<i64>,
}

impl TryFrom<Message> for SortedSetRange {
    type Error = Error;

    fn try_from(other: Message) -> Result<Self, Error> {
        let array = match other {
            Message::Array(array) => array,
            _ => {
                return Err(Error::new(
                    ErrorKind::Other,
                    "malformed command, empty array",
                ))
            }
        };

        if array.inner.is_none() {
            return Err(Error::new(
                ErrorKind::Other,
                "malformed command, inner array is none",
            ));
        }

        let mut array = array.inner.unwrap();
        let _command = take_bulk_string(&mut array)?;

        let key = take_bulk_string(&mut array)?.ok_or_else(|| {
            Error::new(
                ErrorKind::Other,
                "malformed command, invalid sorted set name",
            )
        })?;

        let mut optional_args = SortedSetRangeOptionalArguments::default();

        let start_value = take_bulk_string(&mut array)?;
        optional_args.start = Some(StartStopValue::parse_value(start_value)?);
        let stop_value = take_bulk_string(&mut array)?;
        optional_args.stop = Some(StartStopValue::parse_value(stop_value)?);

        // Parse the remaining optional arguments
        while let Some(arg) = take_bulk_string(&mut array)? {
            if arg.is_empty() {
                return Err(Error::new(
                    ErrorKind::Other,
                    "malformed command, empty string",
                ));
            }

            match &*arg {
                b"BYSCORE" => {
                    optional_args.by_score = Some(true);
                }
                b"REV" => optional_args.reversed = Some(true),
                b"LIMIT" => {
                    let offset = take_bulk_string_as_u64(&mut array)?.ok_or_else(|| {
                        Error::new(
                            ErrorKind::Other,
                            "malformed command, unable to extract offset",
                        )
                    })?;
                    optional_args.offset = Some(offset);
                    let count = take_bulk_string_as_i64(&mut array)?.ok_or_else(|| {
                        Error::new(
                            ErrorKind::Other,
                            "malformed command, unable to extract count",
                        )
                    })?;
                    optional_args.count = Some(count);
                }
                b"WITHSCORES" => optional_args.with_scores = Some(true),
                b"BYLEX" => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "BYLEX is not yet supported by Momento",
                    ))
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "malformed command, invalid optional argument",
                    ))
                }
            }
        }

        // Validate the combination of arguments before returning them
        match optional_args {
            // If there are no additional arguments, we know it's a basic fetch by rank request
            // and there should be no decorators on the start and stop values.
            SortedSetRangeOptionalArguments {
                by_score: None,
                offset: None,
                count: None,
                ..
            } => {
                let start =
                    if let Some(StartStopValue::Inclusive(inclusive_start)) = optional_args.start {
                        inclusive_start
                    } else {
                        return Err(Error::new(
                            ErrorKind::Other,
                            "malformed command, invalid start value",
                        ));
                    };

                let stop =
                    if let Some(StartStopValue::Inclusive(inclusive_stop)) = optional_args.stop {
                        inclusive_stop
                    } else {
                        return Err(Error::new(
                            ErrorKind::Other,
                            "malformed command, invalid stop value",
                        ));
                    };

                return Ok(Self {
                    key,
                    reversed: optional_args.reversed.unwrap_or(false),
                    with_scores: optional_args.with_scores.unwrap_or(false),
                    args: MomentoSortedSetFetchArgs::ByRank(start, stop),
                });
            }
            // If LIMIT offset count is present but BYSCORE is not, it is an invalid request.
            SortedSetRangeOptionalArguments {
                offset: Some(_),
                count: Some(_),
                by_score: None,
                ..
            } => {
                return Err(Error::new(
                    ErrorKind::Other,
                    "malformed command, BYSCORE must be present with LIMIT offset count",
                ))
            }
            // BYSCORE is present
            SortedSetRangeOptionalArguments {
                by_score: Some(_), ..
            } => {
                let start = if let Some(start_value) = optional_args.start {
                    start_value
                } else {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "malformed command, nonexistent start value",
                    ));
                };
                let stop = if let Some(stop_value) = optional_args.stop {
                    stop_value
                } else {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "malformed command, nonexistent stop value",
                    ));
                };

                return Ok(Self {
                    key,
                    reversed: optional_args.reversed.unwrap_or(false),
                    with_scores: optional_args.with_scores.unwrap_or(false),
                    args: MomentoSortedSetFetchArgs::ByScore(
                        start,
                        stop,
                        optional_args.offset,
                        optional_args.count,
                    ),
                });
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::Other,
                    "malformed command, invalid optional arguments",
                ))
            }
        }
    }
}

impl SortedSetRange {
    pub fn new(
        key: &[u8],
        reversed: bool,
        with_scores: bool,
        args: MomentoSortedSetFetchArgs,
    ) -> Self {
        Self {
            key: key.into(),
            reversed,
            with_scores,
            args,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn reversed(&self) -> bool {
        self.reversed
    }

    pub fn with_scores(&self) -> bool {
        self.with_scores
    }

    pub fn args(&self) -> &MomentoSortedSetFetchArgs {
        &self.args
    }
}

impl From<&SortedSetRange> for Message {
    fn from(value: &SortedSetRange) -> Message {
        let args = value.args();

        let reversed = if value.reversed() {
            Message::BulkString(BulkString::new(b"REV"))
        } else {
            Message::BulkString(BulkString::new(b""))
        };

        let with_scores = if value.with_scores() {
            Message::BulkString(BulkString::new(b"WITHSCORES"))
        } else {
            Message::BulkString(BulkString::new(b""))
        };

        match args {
            MomentoSortedSetFetchArgs::ByRank(start, stop) => Message::Array(Array {
                inner: Some(vec![
                    Message::BulkString(BulkString::new(b"ZRANGE")),
                    Message::BulkString(BulkString::new(value.key())),
                    Message::BulkString(BulkString::new(start.to_string().as_bytes())),
                    Message::BulkString(BulkString::new(stop.to_string().as_bytes())),
                    reversed,
                    with_scores,
                    Message::BulkString(BulkString::new(b"")),
                    Message::BulkString(BulkString::new(b"")),
                    Message::BulkString(BulkString::new(b"")),
                ]),
            }),
            MomentoSortedSetFetchArgs::ByScore(start, stop, offset, count) => {
                let start = match start {
                    StartStopValue::Inclusive(inclusive_start) => {
                        Message::BulkString(BulkString::new(inclusive_start.to_string().as_bytes()))
                    }
                    StartStopValue::Exclusive(exclusive_start) => {
                        Message::BulkString(BulkString::new(exclusive_start.to_string().as_bytes()))
                    }
                    StartStopValue::PositiveInfinity => {
                        Message::BulkString(BulkString::new(b"+inf"))
                    }
                    StartStopValue::NegativeInfinity => {
                        Message::BulkString(BulkString::new(b"-inf"))
                    }
                };

                let stop = match stop {
                    StartStopValue::Inclusive(inclusive_stop) => {
                        Message::BulkString(BulkString::new(inclusive_stop.to_string().as_bytes()))
                    }
                    StartStopValue::Exclusive(exclusive_stop) => {
                        Message::BulkString(BulkString::new(exclusive_stop.to_string().as_bytes()))
                    }
                    StartStopValue::PositiveInfinity => {
                        Message::BulkString(BulkString::new(b"+inf"))
                    }
                    StartStopValue::NegativeInfinity => {
                        Message::BulkString(BulkString::new(b"-inf"))
                    }
                };

                let offset = if let Some(offset) = offset {
                    Message::BulkString(BulkString::new(offset.to_string().as_bytes()))
                } else {
                    Message::BulkString(BulkString::new(b""))
                };

                let count = if let Some(count) = count {
                    Message::BulkString(BulkString::new(count.to_string().as_bytes()))
                } else {
                    Message::BulkString(BulkString::new(b""))
                };

                Message::Array(Array {
                    inner: Some(vec![
                        Message::BulkString(BulkString::new(b"ZRANGE")),
                        Message::BulkString(BulkString::new(value.key())),
                        start,
                        stop,
                        reversed,
                        with_scores,
                        Message::BulkString(BulkString::new(b"BYSCORE")),
                        offset,
                        count,
                    ]),
                })
            }
        }
    }
}

impl Compose for SortedSetRange {
    fn compose(&self, buf: &mut dyn BufMut) -> usize {
        Message::from(self).compose(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser() {
        let parser = RequestParser::new();
        assert_eq!(
            parser.parse(b"ZRANGE z 0 10\r\n").unwrap().into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                false,
                false,
                MomentoSortedSetFetchArgs::ByRank(0, 10)
            ))
        );

        assert_eq!(
            parser.parse(b"ZRANGE z 0 10 REV\r\n").unwrap().into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                true,
                false,
                MomentoSortedSetFetchArgs::ByRank(0, 10)
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 WITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                false,
                true,
                MomentoSortedSetFetchArgs::ByRank(0, 10)
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 REV WITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                true,
                true,
                MomentoSortedSetFetchArgs::ByRank(0, 10)
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 BYSCORE\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                false,
                false,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Inclusive(0),
                    StartStopValue::Inclusive(10),
                    None,
                    None
                )
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z (0 10 BYSCORE\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                false,
                false,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Exclusive(0),
                    StartStopValue::Inclusive(10),
                    None,
                    None
                )
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 (10 BYSCORE\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                false,
                false,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Inclusive(0),
                    StartStopValue::Exclusive(10),
                    None,
                    None
                )
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z (0 (10 BYSCORE\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                false,
                false,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Exclusive(0),
                    StartStopValue::Exclusive(10),
                    None,
                    None
                )
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 BYSCORE WITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                false,
                true,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Inclusive(0),
                    StartStopValue::Inclusive(10),
                    None,
                    None
                )
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 BYSCORE REV\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                true,
                false,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Inclusive(0),
                    StartStopValue::Inclusive(10),
                    None,
                    None
                )
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 BYSCORE REV WITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                true,
                true,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Inclusive(0),
                    StartStopValue::Inclusive(10),
                    None,
                    None
                )
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 BYSCORE LIMIT 1 5\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                false,
                false,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Inclusive(0),
                    StartStopValue::Inclusive(10),
                    Some(1),
                    Some(5)
                )
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 BYSCORE REV LIMIT 1 5\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                true,
                false,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Inclusive(0),
                    StartStopValue::Inclusive(10),
                    Some(1),
                    Some(5)
                )
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 BYSCORE WITHSCORES LIMIT 1 5\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                false,
                true,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Inclusive(0),
                    StartStopValue::Inclusive(10),
                    Some(1),
                    Some(5)
                )
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 BYSCORE LIMIT 1 5 REV WITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                true,
                true,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Inclusive(0),
                    StartStopValue::Inclusive(10),
                    Some(1),
                    Some(5)
                )
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 (10 BYSCORE LIMIT 1 5\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                false,
                false,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Inclusive(0),
                    StartStopValue::Exclusive(10),
                    Some(1),
                    Some(5)
                )
            ))
        );

        // RESP protocol format tests
        assert_eq!(
            parser
                .parse(b"*4\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                false,
                false,
                MomentoSortedSetFetchArgs::ByRank(0, 10)
            ))
        );

        assert_eq!(
            parser
                .parse(b"*5\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$3\r\nREV\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                true,
                false,
                MomentoSortedSetFetchArgs::ByRank(0, 10)
            ))
        );

        assert_eq!(
            parser
                .parse(b"*5\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$10\r\nWITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z", false, true, MomentoSortedSetFetchArgs::ByRank(0, 10)
            ))
        );

        assert_eq!(
            parser
                .parse(b"*6\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$3\r\nREV\r\n$10\r\nWITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z", true, true, MomentoSortedSetFetchArgs::ByRank(0, 10)
            ))
        );

        assert_eq!(
            parser
                .parse(b"*5\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$7\r\nBYSCORE\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                false,
                false,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Inclusive(0),
                    StartStopValue::Inclusive(10),
                    None,
                    None
                )
            ))
        );

        assert_eq!(
            parser
                .parse(
                    b"*5\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$2\r\n(0\r\n$2\r\n10\r\n$7\r\nBYSCORE\r\n"
                )
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                false,
                false,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Exclusive(0),
                    StartStopValue::Inclusive(10),
                    None,
                    None
                )
            ))
        );

        assert_eq!(
            parser
                .parse(
                    b"*5\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$3\r\n(10\r\n$7\r\nBYSCORE\r\n"
                )
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                false,
                false,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Inclusive(0),
                    StartStopValue::Exclusive(10),
                    None,
                    None
                )
            ))
        );

        assert_eq!(
            parser
                .parse(
                    b"*5\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$2\r\n(0\r\n$3\r\n(10\r\n$7\r\nBYSCORE\r\n"
                )
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                false,
                false,
                MomentoSortedSetFetchArgs::ByScore(
                    StartStopValue::Exclusive(0),
                    StartStopValue::Exclusive(10),
                    None,
                    None
                )
            ))
        );

        assert_eq!(
            parser
                .parse(b"*6\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$7\r\nBYSCORE\r\n$10\r\nWITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z", false, true, MomentoSortedSetFetchArgs::ByScore(StartStopValue::Inclusive(0), StartStopValue::Inclusive(10), None, None)
            ))
        );

        assert_eq!(
            parser
                .parse(b"*6\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$7\r\nBYSCORE\r\n$3\r\nREV\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z", true, false, MomentoSortedSetFetchArgs::ByScore(StartStopValue::Inclusive(0), StartStopValue::Inclusive(10), None, None)
            ))
        );

        assert_eq!(
            parser
                .parse(b"*7\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$7\r\nBYSCORE\r\n$3\r\nREV\r\n$10\r\nWITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z", true, true, MomentoSortedSetFetchArgs::ByScore(StartStopValue::Inclusive(0), StartStopValue::Inclusive(10), None, None)
            ))
        );

        assert_eq!(
            parser
                .parse(b"*8\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$7\r\nBYSCORE\r\n$5\r\nLIMIT\r\n$1\r\n1\r\n$1\r\n5\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z", false, false, MomentoSortedSetFetchArgs::ByScore(StartStopValue::Inclusive(0), StartStopValue::Inclusive(10), Some(1), Some(5))
            ))
        );

        assert_eq!(
            parser
                .parse(b"*9\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$7\r\nBYSCORE\r\n$5\r\nLIMIT\r\n$1\r\n1\r\n$1\r\n5\r\n$3\r\nREV\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z", true, false, MomentoSortedSetFetchArgs::ByScore(StartStopValue::Inclusive(0), StartStopValue::Inclusive(10), Some(1), Some(5))
            ))
        );

        assert_eq!(
            parser
                .parse(b"*9\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$7\r\nBYSCORE\r\n$5\r\nLIMIT\r\n$1\r\n1\r\n$1\r\n5\r\n$10\r\nWITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z", false, true, MomentoSortedSetFetchArgs::ByScore(StartStopValue::Inclusive(0), StartStopValue::Inclusive(10), Some(1), Some(5))
            ))
        );

        assert_eq!(
            parser
                .parse(b"*10\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$7\r\nBYSCORE\r\n$5\r\nLIMIT\r\n$1\r\n1\r\n$1\r\n5\r\n$3\r\nREV\r\n$10\r\nWITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z", true, true, MomentoSortedSetFetchArgs::ByScore(StartStopValue::Inclusive(0), StartStopValue::Inclusive(10), Some(1), Some(5))
            ))
        );

        assert_eq!(
            parser
                .parse(b"*8\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$3\r\n(10\r\n$7\r\nBYSCORE\r\n$5\r\nLIMIT\r\n$1\r\n1\r\n$1\r\n5\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z", false, false, MomentoSortedSetFetchArgs::ByScore(StartStopValue::Inclusive(0), StartStopValue::Exclusive(10), Some(1), Some(5))
            ))
        );
    }
}
