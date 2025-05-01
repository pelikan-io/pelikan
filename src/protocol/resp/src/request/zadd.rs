// Copyright 2025 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Error;
use std::sync::Arc;

use super::*;

#[metric(name = "zadd")]
pub static ZADD: Counter = Counter::new();

#[metric(name = "zadd_ex")]
pub static ZADD_EX: Counter = Counter::new();

#[derive(Debug, PartialEq, Eq)]
pub struct SortedSetAdd {
    key: Arc<[u8]>,
    members: Box<[ScoreMemberPair]>,
    optional_args: SortedSetAddOptionalArguments,
}

#[derive(Debug, PartialEq, Eq, Default)]
pub struct SortedSetAddOptionalArguments {
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
    pub ch: bool,
    pub incr: bool,
}

impl TryFrom<Message> for SortedSetAdd {
    type Error = Error;

    fn try_from(value: Message) -> std::io::Result<Self> {
        let array = match value {
            Message::Array(array) => array,
            _ => return Err(Error::new(ErrorKind::Other, "malformed command")),
        };

        let mut array = array.inner.unwrap();
        if array.len() < 3 {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }

        let _command = take_bulk_string(&mut array)?;

        let key = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

        // There may be some optional arguments that come before the score-value pairs:
        // [NX | XX] [GT | LT] [CH] [INCR]
        let mut optional_args = SortedSetAddOptionalArguments::default();
        let mut members = Vec::with_capacity(array.len());

        while let Some(arg) = take_bulk_string(&mut array)? {
            if arg.is_empty() {
                return Err(Error::new(
                    ErrorKind::Other,
                    "malformed command, empty string",
                ));
            }

            match &*arg {
                b"NX" => optional_args.nx = true,
                b"XX" => optional_args.xx = true,
                b"GT" => optional_args.gt = true,
                b"LT" => optional_args.lt = true,
                b"CH" => optional_args.ch = true,
                b"INCR" => optional_args.incr = true,
                _ => {
                    // Otherwise assume it's a score or member
                    members.push(arg);
                }
            }
        }

        // If INCR is set, then ZADD should behave like ZINCRBY (as per the docs), which accepts only a single score-member pair
        if optional_args.incr && members.len() != 2 {
            return Err(Error::new(
                ErrorKind::Other,
                "INCR option accepts only a single score-member pair",
            ));
        }

        // Verify the score-member pairs and convert them to ScoreMemberPair objects
        if members.len() % 2 != 0 {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }
        let mut verified_score_member_pairs = Vec::with_capacity(members.len() / 2);
        for i in (0..members.len()).step_by(2) {
            verified_score_member_pairs.push((members[i].clone(), members[i + 1].clone()));
        }

        Ok(Self {
            key,
            members: verified_score_member_pairs.into_boxed_slice(),
            optional_args,
        })
    }
}

impl SortedSetAdd {
    pub fn new(
        key: &[u8],
        members: &[(&[u8], &[u8])],
        optional_args: SortedSetAddOptionalArguments,
    ) -> Self {
        let mut data = Vec::with_capacity(members.len());
        for (score, member) in members.iter() {
            data.push(((*score).into(), (*member).into()));
        }
        Self {
            key: key.into(),
            members: data.into(),
            optional_args,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn members(&self) -> &[ScoreMemberPair] {
        &self.members
    }

    pub fn optional_args(&self) -> &SortedSetAddOptionalArguments {
        &self.optional_args
    }
}

impl From<&SortedSetAdd> for Message {
    fn from(value: &SortedSetAdd) -> Self {
        // x2 for the score-member pairs, 6 for the optional arguments, 2 for the command and sorted set name
        let mut vals = Vec::with_capacity(value.members().len() * 2 + 6 + 2);

        vals.push(Message::BulkString(BulkString::new(b"ZADD")));
        vals.push(Message::BulkString(BulkString::new(value.key())));

        // Add any optional arguments set to true
        if value.optional_args().nx {
            vals.push(Message::BulkString(BulkString::new(b"NX")));
        }
        if value.optional_args().xx {
            vals.push(Message::BulkString(BulkString::new(b"XX")));
        }
        if value.optional_args().gt {
            vals.push(Message::BulkString(BulkString::new(b"GT")));
        }
        if value.optional_args().lt {
            vals.push(Message::BulkString(BulkString::new(b"LT")));
        }
        if value.optional_args().ch {
            vals.push(Message::BulkString(BulkString::new(b"CH")));
        }
        if value.optional_args().incr {
            vals.push(Message::BulkString(BulkString::new(b"INCR")));
        }

        // Then add the score-member pairs
        for (score, member) in value.members() {
            vals.push(Message::BulkString(BulkString::new(score)));
            vals.push(Message::BulkString(BulkString::new(member)));
        }

        Message::Array(Array { inner: Some(vals) })
    }
}

impl Compose for SortedSetAdd {
    fn compose(&self, dst: &mut dyn BufMut) -> usize {
        Message::from(self).compose(dst)
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
                .parse(b"zadd z 1 a 2 b 3 c\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetAdd(SortedSetAdd::new(
                b"z",
                &[
                    ("1".as_bytes(), "a".as_bytes()),
                    ("2".as_bytes(), "b".as_bytes()),
                    ("3".as_bytes(), "c".as_bytes())
                ],
                SortedSetAddOptionalArguments::default()
            ))
        );

        assert_eq!(
            parser
                .parse(b"zadd z 1.23 a 2.34 b 3.45 c\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetAdd(SortedSetAdd::new(
                b"z",
                &[
                    ("1.23".as_bytes(), "a".as_bytes()),
                    ("2.34".as_bytes(), "b".as_bytes()),
                    ("3.45".as_bytes(), "c".as_bytes())
                ],
                SortedSetAddOptionalArguments::default()
            ))
        );

        assert_eq!(
            parser
                .parse(b"zadd z -inf abc +inf xyz\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetAdd(SortedSetAdd::new(
                b"z",
                &[
                    ("-inf".as_bytes(), "abc".as_bytes()),
                    ("+inf".as_bytes(), "xyz".as_bytes())
                ],
                SortedSetAddOptionalArguments::default()
            ))
        );

        assert_eq!(
            parser
                .parse(b"zadd z INCR 123 abc\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetAdd(SortedSetAdd::new(
                b"z",
                &[("123".as_bytes(), "abc".as_bytes())],
                SortedSetAddOptionalArguments {
                    incr: true,
                    ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"zadd z XX LT CH 123 abc 321 xyz\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetAdd(SortedSetAdd::new(
                b"z",
                &[
                    ("123".as_bytes(), "abc".as_bytes()),
                    ("321".as_bytes(), "xyz".as_bytes())
                ],
                SortedSetAddOptionalArguments {
                    xx: true,
                    lt: true,
                    ch: true,
                    ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"*5\r\n$4\r\nZADD\r\n$1\r\nz\r\n$4\r\nINCR\r\n$3\r\n1.2\r\n$1\r\na\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetAdd(SortedSetAdd::new(
                b"z",
                &[("1.2".as_bytes(), "a".as_bytes())],
                SortedSetAddOptionalArguments {
                    incr: true,
                    ..Default::default()
                }
            ))
        );

        assert_eq!(
            parser
                .parse(b"*6\r\n$4\r\nZADD\r\n$1\r\nz\r\n$4\r\n-inf\r\n$3\r\nabc\r\n$4\r\n+inf\r\n$3\r\nxyz\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetAdd(SortedSetAdd::new(
                b"z",
                &[("-inf".as_bytes(), "abc".as_bytes()), ("+inf".as_bytes(), "xyz".as_bytes())],
                SortedSetAddOptionalArguments::default()
            ))
        );

        assert_eq!(
          parser
              .parse(b"*8\r\n$4\r\nZADD\r\n$1\r\nz\r\n$4\r\n1.23\r\n$1\r\na\r\n$4\r\n23.4\r\n$1\r\nb\r\n$3\r\n345\r\n$1\r\nc\r\n")
              .unwrap()
              .into_inner(),
          Request::SortedSetAdd(SortedSetAdd::new(
              b"z",
              &[("1.23".as_bytes(), "a".as_bytes()), ("23.4".as_bytes(), "b".as_bytes()), ("345".as_bytes(), "c".as_bytes())],
              SortedSetAddOptionalArguments::default()
          ))
        );

        assert_eq!(
          parser
              .parse(b"*9\r\n$4\r\nZADD\r\n$1\r\nz\r\n$2\r\nNX\r\n$2\r\nGT\r\n$2\r\nCH\r\n$3\r\n123\r\n$1\r\na\r\n$3\r\n321\r\n$1\r\nb\r\n")
              .unwrap()
              .into_inner(),
          Request::SortedSetAdd(SortedSetAdd::new(
              b"z",
              &[("123".as_bytes(), "a".as_bytes()), ("321".as_bytes(), "b".as_bytes())],
              SortedSetAddOptionalArguments {
                  nx: true,
                  gt: true,
                  ch: true,
                  ..Default::default()
              }
          ))
        );
    }
}
