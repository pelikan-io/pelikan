// Copyright 2025 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Error;
use std::sync::Arc;

use super::*;

#[metric(name = "zunionstore")]
pub static ZUNIONSTORE: Counter = Counter::new();

#[metric(name = "zunionstore_ex")]
pub static ZUNIONSTORE_EX: Counter = Counter::new();

#[derive(Debug, PartialEq, Eq)]
pub enum AggregateFunction {
    Sum, // default
    Min,
    Max,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SortedSetUnionStore {
    destination_key: Arc<[u8]>,
    num_keys: u64,
    source_keys: Vec<Arc<[u8]>>,
    weights: Option<Vec<Arc<[u8]>>>, // cannot be Vec<f64> because of Eq constraint
    aggregate_function: Option<AggregateFunction>,
}

impl TryFrom<Message> for SortedSetUnionStore {
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

        let destination_key = take_bulk_string(&mut array)?.ok_or_else(|| {
            Error::new(
                ErrorKind::Other,
                "malformed command, invalid sorted set name",
            )
        })?;

        let num_keys = take_bulk_string_as_u64(&mut array)?.ok_or_else(|| {
            Error::new(
                ErrorKind::Other,
                "malformed command, invalid number of keys",
            )
        })?;
        if num_keys == 0 {
            return Err(Error::new(
                ErrorKind::Other,
                "malformed command, number of keys cannot be 0",
            ));
        }

        // Collect num_keys source keys
        let mut source_keys = Vec::new();
        for _ in 0..num_keys {
            let key = take_bulk_string(&mut array)?.ok_or_else(|| {
                Error::new(ErrorKind::Other, "malformed command, invalid source key")
            })?;
            source_keys.push(key);
        }

        // Collect all remaining arguments as strings
        let mut remaining_args = vec![];
        while let Some(arg) = take_bulk_string(&mut array)? {
            if arg.is_empty() {
                return Err(Error::new(
                    ErrorKind::Other,
                    "malformed command, empty string for optional argument",
                ));
            }
            remaining_args.push(arg);
        }

        // If the AGGREGATE header is present somewhere in the remaining arguments,
        // then the first argument after the AGGREGATE header should be the aggregate function.
        let aggregate_function =
            if let Some(index) = remaining_args.iter().position(|arg| &**arg == b"AGGREGATE") {
                if index + 1 >= remaining_args.len() {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "malformed command, no aggregate function provided after AGGREGATE",
                    ));
                }
                // Make sure there's either a WEIGHTS header or the end of the array after the AGGREGATE args.
                if index + 2 < remaining_args.len() && &*remaining_args[index + 2] != b"WEIGHTS" {
                    return Err(Error::new(
                    ErrorKind::Other,
                    "malformed command, expected WEIGHTS header or end of array after AGGREGATE",
                ));
                }

                let aggregate_string = remaining_args[index + 1].clone();
                match &*aggregate_string {
                    b"SUM" => Some(AggregateFunction::Sum),
                    b"MIN" => Some(AggregateFunction::Min),
                    b"MAX" => Some(AggregateFunction::Max),
                    _ => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            "malformed command, invalid aggregate function",
                        ))
                    }
                }
            } else {
                None
            };

        // If the WEIGHTS header is present somewhere in the remaining arguments,
        // then the arguments after the WEIGHTS header but before the AGGREGATE header or end of the array should be the weights.
        let weights =
            if let Some(index) = remaining_args.iter().position(|arg| &**arg == b"WEIGHTS") {
                let mut weight_args = vec![];
                for weight_arg in remaining_args[index + 1..].iter() {
                    if &**weight_arg == b"AGGREGATE" {
                        break;
                    }
                    weight_args.push(weight_arg.clone());
                }
                println!("\n===weight_args length: {:?}", weight_args.len());
                println!("\n===weight_args: {:?}", weight_args);
                if weight_args.len() != source_keys.len() {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "malformed command, number of weights must match number of source sets",
                    ));
                }
                Some(weight_args)
            } else {
                None
            };

        Ok(SortedSetUnionStore {
            destination_key,
            num_keys,
            source_keys,
            weights,
            aggregate_function,
        })
    }
}

impl SortedSetUnionStore {
    pub fn new(
        destination_key: &[u8],
        num_keys: u64,
        source_keys: &[&[u8]],
        weights: Option<&[&[u8]]>,
        aggregate_function: Option<AggregateFunction>,
    ) -> Self {
        Self {
            destination_key: destination_key.into(),
            num_keys,
            source_keys: source_keys.iter().map(|s| (*s).into()).collect(),
            weights: weights.map(|w| w.iter().map(|w| (*w).into()).collect()),
            aggregate_function,
        }
    }

    pub fn destination_key(&self) -> &[u8] {
        &self.destination_key
    }

    pub fn num_keys(&self) -> u64 {
        self.num_keys
    }

    pub fn source_keys(&self) -> &[Arc<[u8]>] {
        &self.source_keys
    }

    pub fn weights(&self) -> &Option<Vec<Arc<[u8]>>> {
        &self.weights
    }

    pub fn aggregate_function(&self) -> &Option<AggregateFunction> {
        &self.aggregate_function
    }
}

impl From<&SortedSetUnionStore> for Message {
    fn from(value: &SortedSetUnionStore) -> Message {
        let source_keys_args = if value.num_keys() > 0 {
            Message::Array(Array {
                inner: Some(
                    value
                        .source_keys()
                        .iter()
                        .map(|m| Message::BulkString(BulkString::new(m)))
                        .collect(),
                ),
            })
        } else {
            Message::BulkString(BulkString::new(b""))
        };

        let weights_header = if value.weights().is_some() {
            Message::BulkString(BulkString::new(b"WEIGHTS"))
        } else {
            Message::BulkString(BulkString::new(b""))
        };
        let weights_args = if let Some(weights) = value.weights() {
            Message::Array(Array {
                inner: Some(
                    weights
                        .iter()
                        .map(|w| Message::BulkString(BulkString::new(w)))
                        .collect(),
                ),
            })
        } else {
            Message::BulkString(BulkString::new(b""))
        };

        let aggregate_header = if value.aggregate_function().is_some() {
            Message::BulkString(BulkString::new(b"AGGREGATE"))
        } else {
            Message::BulkString(BulkString::new(b""))
        };
        let aggregate_arg = if let Some(aggregate_function) = value.aggregate_function() {
            match aggregate_function {
                AggregateFunction::Sum => Message::BulkString(BulkString::new(b"SUM")),
                AggregateFunction::Min => Message::BulkString(BulkString::new(b"MIN")),
                AggregateFunction::Max => Message::BulkString(BulkString::new(b"MAX")),
            }
        } else {
            Message::BulkString(BulkString::new(b""))
        };

        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"ZUNIONSTORE")),
                Message::BulkString(BulkString::new(value.destination_key())),
                Message::BulkString(BulkString::new(value.num_keys().to_string().as_bytes())),
                source_keys_args,
                weights_header,
                weights_args,
                aggregate_header,
                aggregate_arg,
            ]),
        })
    }
}

impl Compose for SortedSetUnionStore {
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
            parser
                .parse(b"ZUNIONSTORE dest 1 zset1\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetUnionStore(SortedSetUnionStore::new(
                b"dest",
                1,
                &[b"zset1"],
                None,
                None
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZUNIONSTORE dest 2 zset1 zset2\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetUnionStore(SortedSetUnionStore::new(
                b"dest",
                2,
                &[b"zset1", b"zset2"],
                None,
                None
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZUNIONSTORE dest 2 zset1 zset2 WEIGHTS 1 2\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetUnionStore(SortedSetUnionStore::new(
                b"dest",
                2,
                &[b"zset1", b"zset2"],
                Some(&[b"1", b"2"]),
                None
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZUNIONSTORE dest 2 zset1 zset2 AGGREGATE MAX\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetUnionStore(SortedSetUnionStore::new(
                b"dest",
                2,
                &[b"zset1", b"zset2"],
                None,
                Some(AggregateFunction::Max)
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZUNIONSTORE dest 2 zset1 zset2 AGGREGATE SUM WEIGHTS 1 2\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetUnionStore(SortedSetUnionStore::new(
                b"dest",
                2,
                &[b"zset1", b"zset2"],
                Some(&[b"1", b"2"]),
                Some(AggregateFunction::Sum)
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZUNIONSTORE dest 2 zset1 zset2 WEIGHTS 1 2 AGGREGATE MIN\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetUnionStore(SortedSetUnionStore::new(
                b"dest",
                2,
                &[b"zset1", b"zset2"],
                Some(&[b"1", b"2"]),
                Some(AggregateFunction::Min)
            ))
        );

        assert_eq!(
            parser
                .parse(b"ZUNIONSTORE dest 3 zset1 zset2 zset3 WEIGHTS 1 2 3\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetUnionStore(SortedSetUnionStore::new(
                b"dest",
                3,
                &[b"zset1", b"zset2", b"zset3"],
                Some(&[b"1", b"2", b"3"]),
                None
            ))
        );

        // RESP protocol format tests
        assert_eq!(
            parser
                .parse(b"*4\r\n$11\r\nZUNIONSTORE\r\n$4\r\ndest\r\n$1\r\n1\r\n$5\r\nzset1\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetUnionStore(SortedSetUnionStore::new(
                b"dest",
                1,
                &[b"zset1"],
                None,
                None
            ))
        );

        assert_eq!(
            parser
                .parse(b"*5\r\n$11\r\nZUNIONSTORE\r\n$4\r\ndest\r\n$1\r\n2\r\n$5\r\nzset1\r\n$5\r\nzset2\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetUnionStore(SortedSetUnionStore::new(
                b"dest",
                2,
                &[b"zset1", b"zset2"],
                None,
                None
            )),
        );

        println!("\n===test failing");
        assert_eq!(
            parser
                .parse(b"*8\r\n$11\r\nZUNIONSTORE\r\n$4\r\ndest\r\n$1\r\n2\r\n$5\r\nzset1\r\n$5\r\nzset2\r\n$7\r\nWEIGHTS\r\n$1\r\n4\r\n$1\r\n5\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetUnionStore(SortedSetUnionStore::new(
                b"dest",
                2,
                &[b"zset1", b"zset2"],
                Some(&[b"4", b"5"]),
                None
            )),
        );

        assert_eq!(
            parser
                .parse(b"*7\r\n$11\r\nZUNIONSTORE\r\n$4\r\ndest\r\n$1\r\n2\r\n$5\r\nzset1\r\n$5\r\nzset2\r\n$9\r\nAGGREGATE\r\n$3\r\nMAX\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetUnionStore(SortedSetUnionStore::new(
                b"dest",
                2,
                &[b"zset1", b"zset2"],
                None,
                Some(AggregateFunction::Max)
            )),
        );

        assert_eq!(
            parser
                .parse(b"*10\r\n$11\r\nZUNIONSTORE\r\n$4\r\ndest\r\n$1\r\n2\r\n$5\r\nzset1\r\n$5\r\nzset2\r\n$9\r\nAGGREGATE\r\n$3\r\nSUM\r\n$7\r\nWEIGHTS\r\n$1\r\n1\r\n$1\r\n2\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetUnionStore(SortedSetUnionStore::new(
                b"dest",
                2,
                &[b"zset1", b"zset2"],
                Some(&[b"1", b"2"]),
                Some(AggregateFunction::Sum)
            ))
        );

        assert_eq!(
            parser
                .parse(
                    b"*10\r\n$11\r\nZUNIONSTORE\r\n$4\r\ndest\r\n$1\r\n2\r\n$5\r\nzset1\r\n$5\r\nzset2\r\n$7\r\nWEIGHTS\r\n$1\r\n1\r\n$1\r\n2\r\n$9\r\nAGGREGATE\r\n$3\r\nMIN\r\n"
                )
                .unwrap()
                .into_inner(),
            Request::SortedSetUnionStore(SortedSetUnionStore::new(
                b"dest",
                2,
                &[b"zset1", b"zset2"],
                Some(&[b"1", b"2"]),
                Some(AggregateFunction::Min)
            ))
        );

        assert_eq!(
            parser
                .parse(
                    b"*10\r\n$11\r\nZUNIONSTORE\r\n$4\r\ndest\r\n$1\r\n3\r\n$5\r\nzset1\r\n$5\r\nzset2\r\n$5\r\nzset3\r\n$7\r\nWEIGHTS\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"
                )
                .unwrap()
                .into_inner(),
            Request::SortedSetUnionStore(SortedSetUnionStore::new(
                b"dest",
                3,
                &[b"zset1", b"zset2", b"zset3"],
                Some(&[b"1", b"2", b"3"]),
                None
            ))
        );
    }
}
