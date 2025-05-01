// Copyright 2025 Pelikan Foundation LLC.
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
pub enum RangeType {
    ByIndex, // Default when [BYSCORE | BYLEX] not provided
    ByScore,
    ByLex,
}

// Represents the optional arguments to the `ZRANGE` command:
// [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES].
// Note: [LIMIT offset count] can be used only in conjunction with [BYSCORE | BYLEX].
#[derive(Debug, PartialEq, Eq, Default)]
pub struct SortedSetRangeOptionalArguments {
    pub reversed: Option<bool>,
    pub with_scores: Option<bool>,
    pub offset: Option<u64>,
    pub count: Option<i64>,
}

// Represents the arguments to the `ZRANGE` command.
#[derive(Debug, PartialEq, Eq)]
pub struct SortedSetRange {
    key: Arc<[u8]>,
    start: Arc<[u8]>,
    stop: Arc<[u8]>,
    range_type: RangeType,
    optional_args: SortedSetRangeOptionalArguments,
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
        let start = take_bulk_string(&mut array)?.ok_or_else(|| {
            Error::new(
                ErrorKind::Other,
                "malformed command, unable to extract start value",
            )
        })?;
        let stop = take_bulk_string(&mut array)?.ok_or_else(|| {
            Error::new(
                ErrorKind::Other,
                "malformed command, unable to extract stop value",
            )
        })?;

        // Parse for any remaining optional arguments
        let mut range_type = RangeType::ByIndex;
        let mut optional_args = SortedSetRangeOptionalArguments::default();
        while let Some(arg) = take_bulk_string(&mut array)? {
            if arg.is_empty() {
                return Err(Error::new(
                    ErrorKind::Other,
                    "malformed command, empty string for optional argument",
                ));
            }

            match &*arg {
                b"BYSCORE" => {
                    if range_type == RangeType::ByScore {
                        return Err(Error::new(
                            ErrorKind::Other,
                            "malformed command, BYSCORE already provided",
                        ));
                    } else if range_type == RangeType::ByLex {
                        return Err(Error::new(
                            ErrorKind::Other,
                            "malformed command, BYSCORE and BYLEX cannot be provided together",
                        ));
                    } else {
                        range_type = RangeType::ByScore;
                    }
                }
                b"BYLEX" => {
                    if range_type == RangeType::ByScore {
                        return Err(Error::new(
                            ErrorKind::Other,
                            "malformed command, BYSCORE and BYLEX cannot be provided together",
                        ));
                    } else if range_type == RangeType::ByLex {
                        return Err(Error::new(
                            ErrorKind::Other,
                            "malformed command, BYLEX already provided",
                        ));
                    } else {
                        range_type = RangeType::ByLex;
                    }
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
                _ => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "malformed command, invalid optional argument",
                    ))
                }
            }
        }

        // Validate the combination of optional arguments:

        // LIMIT can only be used with BYSCORE or BYLEX
        if optional_args.offset.is_some()
            && optional_args.count.is_some()
            && range_type == RangeType::ByIndex
        {
            return Err(Error::new(
                ErrorKind::Other,
                "malformed command, LIMIT can only be used with BYSCORE or BYLEX",
            ));
        }

        // WITHSCORES cannot be used with BYLEX
        if optional_args.with_scores.is_some() && range_type == RangeType::ByLex {
            return Err(Error::new(
                ErrorKind::Other,
                "malformed command, WITHSCORES cannot be used with BYLEX",
            ));
        }

        Ok(SortedSetRange {
            key,
            start,
            stop,
            range_type,
            optional_args,
        })
    }
}

impl SortedSetRange {
    pub fn new(
        key: &[u8],
        start: &[u8],
        stop: &[u8],
        range_type: RangeType,
        optional_args: SortedSetRangeOptionalArguments,
    ) -> Self {
        Self {
            key: key.into(),
            start: start.into(),
            stop: stop.into(),
            range_type,
            optional_args,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn start(&self) -> &[u8] {
        &self.start
    }

    pub fn stop(&self) -> &[u8] {
        &self.stop
    }

    pub fn range_type(&self) -> &RangeType {
        &self.range_type
    }

    pub fn optional_args(&self) -> &SortedSetRangeOptionalArguments {
        &self.optional_args
    }
}

impl From<&SortedSetRange> for Message {
    fn from(value: &SortedSetRange) -> Message {
        let key = value.key();
        let start = value.start();
        let stop = value.stop();
        let range_type = value.range_type();
        let optional_args = value.optional_args();

        let range_arg = match *range_type {
            RangeType::ByScore => "BYSCORE",
            RangeType::ByLex => "BYLEX",
            RangeType::ByIndex => "",
        };

        let reversed = if optional_args.reversed.is_some() {
            Message::BulkString(BulkString::new(b"REV"))
        } else {
            Message::BulkString(BulkString::new(b""))
        };

        let with_scores = if optional_args.with_scores.is_some() {
            Message::BulkString(BulkString::new(b"WITHSCORES"))
        } else {
            Message::BulkString(BulkString::new(b""))
        };

        let limit = if optional_args.offset.is_some() && optional_args.count.is_some() {
            Message::BulkString(BulkString::new(b"LIMIT"))
        } else {
            Message::BulkString(BulkString::new(b""))
        };

        let offset = if let Some(offset) = optional_args.offset {
            Message::BulkString(BulkString::new(offset.to_string().as_bytes()))
        } else {
            Message::BulkString(BulkString::new(b""))
        };

        let count = if let Some(count) = optional_args.count {
            Message::BulkString(BulkString::new(count.to_string().as_bytes()))
        } else {
            Message::BulkString(BulkString::new(b""))
        };

        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"ZRANGE")),
                Message::BulkString(BulkString::new(key)),
                Message::BulkString(BulkString::new(start)),
                Message::BulkString(BulkString::new(stop)),
                Message::BulkString(BulkString::new(range_arg.as_bytes())),
                reversed,
                with_scores,
                limit,
                offset,
                count,
            ]),
        })
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
                b"0",
                b"10",
                RangeType::ByIndex,
                SortedSetRangeOptionalArguments::default()
            ))
        );

        assert_eq!(
            parser.parse(b"ZRANGE z 0 10 REV\r\n").unwrap().into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByIndex,
                SortedSetRangeOptionalArguments {
                    reversed: Some(true),
                    ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 WITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByIndex,
                SortedSetRangeOptionalArguments {
                    with_scores: Some(true),
                    ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 REV WITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByIndex,
                SortedSetRangeOptionalArguments {
                    reversed: Some(true),
                    with_scores: Some(true),
                    ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 BYSCORE\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments::default()
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z (0 10 BYSCORE\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"(0",
                b"10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments::default()
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 +inf BYSCORE\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"+inf",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments::default()
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z -inf (10 BYSCORE\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"-inf",
                b"(10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments::default()
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z -inf +inf BYSCORE WITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"-inf",
                b"+inf",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments {
                    with_scores: Some(true),
                    ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z -inf +inf BYSCORE REV\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"-inf",
                b"+inf",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments {
                    reversed: Some(true),
                    ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z -inf +inf BYSCORE REV WITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"-inf",
                b"+inf",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments {
                    reversed: Some(true),
                    with_scores: Some(true),
                    ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 BYSCORE LIMIT 1 5\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments {
                    offset: Some(1),
                    count: Some(5),
                    ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 BYSCORE REV LIMIT 1 5\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments {
                    reversed: Some(true),
                    offset: Some(1),
                    count: Some(5),
                    ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z (5 10 BYSCORE WITHSCORES LIMIT 1 5\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"(5",
                b"10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments {
                    with_scores: Some(true),
                    offset: Some(1),
                    count: Some(5),
                    ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 10 BYSCORE LIMIT 1 5 REV WITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments {
                    offset: Some(1),
                    count: Some(5),
                    reversed: Some(true),
                    with_scores: Some(true),
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z 0 (10 BYSCORE LIMIT 1 5\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"(10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments {
                    offset: Some(1),
                    count: Some(5),
                    ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z - + BYLEX\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"-",
                b"+",
                RangeType::ByLex,
                SortedSetRangeOptionalArguments::default()
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZRANGE z - + BYLEX LIMIT 1 5\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"-",
                b"+",
                RangeType::ByLex,
                SortedSetRangeOptionalArguments {
                    offset: Some(1),
                    count: Some(5),
                    ..Default::default()
                }
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
                b"0",
                b"10",
                RangeType::ByIndex,
                SortedSetRangeOptionalArguments::default()
            ))
        );

        assert_eq!(
            parser
                .parse(b"*5\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$3\r\nREV\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByIndex,
                SortedSetRangeOptionalArguments {
                    reversed: Some(true),
                    ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"*5\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$10\r\nWITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByIndex,
                SortedSetRangeOptionalArguments {
                  with_scores: Some(true),
                  ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"*6\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$3\r\nREV\r\n$10\r\nWITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByIndex,
                SortedSetRangeOptionalArguments {
                  reversed: Some(true),
                  with_scores: Some(true),
                  ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"*5\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$7\r\nBYSCORE\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments::default()
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
                b"(0",
                b"10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments::default()
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
                b"0",
                b"(10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments::default()
            ))
        );

        assert_eq!(
            parser
                .parse(
                    b"*5\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$4\r\n-inf\r\n$3\r\n(10\r\n$7\r\nBYSCORE\r\n"
                )
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"-inf",
                b"(10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments::default()
            ))
        );

        assert_eq!(
            parser
                .parse(b"*6\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$4\r\n+inf\r\n$7\r\nBYSCORE\r\n$10\r\nWITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"+inf",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments {
                  with_scores: Some(true),
                  ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"*6\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$7\r\nBYSCORE\r\n$3\r\nREV\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments {
                  reversed: Some(true),
                  ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"*7\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$4\r\n-inf\r\n$4\r\n+inf\r\n$7\r\nBYSCORE\r\n$3\r\nREV\r\n$10\r\nWITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"-inf",
                b"+inf",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments {
                  reversed: Some(true),
                  with_scores: Some(true),
                  ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"*8\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$7\r\nBYSCORE\r\n$5\r\nLIMIT\r\n$1\r\n1\r\n$1\r\n5\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments {
                  offset: Some(1),
                  count: Some(5),
                  ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"*9\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$7\r\nBYSCORE\r\n$5\r\nLIMIT\r\n$1\r\n1\r\n$1\r\n5\r\n$3\r\nREV\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments {
                  offset: Some(1),
                  count: Some(5),
                  reversed: Some(true),
                  ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"*9\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$7\r\nBYSCORE\r\n$5\r\nLIMIT\r\n$1\r\n1\r\n$1\r\n5\r\n$10\r\nWITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments {
                  offset: Some(1),
                  count: Some(5),
                  with_scores: Some(true),
                  ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"*10\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n$7\r\nBYSCORE\r\n$5\r\nLIMIT\r\n$1\r\n1\r\n$1\r\n5\r\n$3\r\nREV\r\n$10\r\nWITHSCORES\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"0",
                b"10",
                RangeType::ByScore,
                SortedSetRangeOptionalArguments {
                  offset: Some(1),
                  count: Some(5),
                  reversed: Some(true),
                  with_scores: Some(true),
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"*5\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n-\r\n$1\r\n+\r\n$5\r\nBYLEX\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"-",
                b"+",
                RangeType::ByLex,
                SortedSetRangeOptionalArguments::default()
            ))
        );

        assert_eq!(
            parser
                .parse(b"*8\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n-\r\n$1\r\n+\r\n$5\r\nBYLEX\r\n$5\r\nLIMIT\r\n$1\r\n1\r\n$1\r\n5\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetRange(SortedSetRange::new(
                b"z",
                b"-",
                b"+",
                RangeType::ByLex,
                SortedSetRangeOptionalArguments {
                  offset: Some(1),
                  count: Some(5),
                  ..Default::default()
                }
            ))
        );
    }
}
