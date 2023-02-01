// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use logger::klog;
use std::fmt::{Display, Formatter};
use std::io::{Error, ErrorKind};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum SetMode {
    Add,
    Replace,
    Set,
}
impl Display for SetMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            SetMode::Add => "add",
            SetMode::Replace => "replace",
            SetMode::Set => "set",
        };
        write!(f, "{}", string)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Set {
    key: Arc<[u8]>,
    value: Arc<[u8]>,
    expire_time: Option<ExpireTime>,
    mode: SetMode,
    get_old: bool,
}

impl Set {
    pub fn new(
        key: &[u8],
        value: &[u8],
        expire_time: Option<ExpireTime>,
        mode: SetMode,
        get_old: bool,
    ) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            expire_time,
            mode,
            get_old,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn expire_time(&self) -> Option<ExpireTime> {
        self.expire_time
    }

    pub fn mode(&self) -> SetMode {
        self.mode
    }

    pub fn get_old(&self) -> bool {
        self.get_old
    }
}

impl TryFrom<Message> for Set {
    type Error = Error;

    fn try_from(other: Message) -> Result<Self, Error> {
        if let Message::Array(array) = other {
            if array.inner.is_none() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let mut array = array.inner.unwrap();

            if array.len() < 3 {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let _command = take_bulk_string(&mut array)?;

            let key = take_bulk_string(&mut array)?
                .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

            if key.is_empty() {
                return Err(Error::new(ErrorKind::Other, "malformed command"));
            }

            let value = take_bulk_string(&mut array)?
                .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

            let mut expire_time = None;
            let mut mode = SetMode::Set;
            let mut get_old = false;

            while let Some(token) = take_bulk_string_as_utf8(&mut array)? {
                match token.as_str() {
                    "EX" => {
                        if expire_time.is_some() {
                            return Err(Error::new(ErrorKind::Other, "malformed command"));
                        }

                        let s = take_bulk_string_as_u64(&mut array)?
                            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

                        expire_time = Some(ExpireTime::Seconds(s));
                    }
                    "PX" => {
                        if expire_time.is_some() {
                            return Err(Error::new(ErrorKind::Other, "malformed command"));
                        }

                        let ms = take_bulk_string_as_u64(&mut array)?
                            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

                        expire_time = Some(ExpireTime::Milliseconds(ms));
                    }
                    "EXAT" => {
                        if expire_time.is_some() {
                            return Err(Error::new(ErrorKind::Other, "malformed command"));
                        }

                        let s = take_bulk_string_as_u64(&mut array)?
                            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

                        expire_time = Some(ExpireTime::UnixSeconds(s));
                    }
                    "PXAT" => {
                        if expire_time.is_some() {
                            return Err(Error::new(ErrorKind::Other, "malformed command"));
                        }

                        let ms = take_bulk_string_as_u64(&mut array)?
                            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

                        expire_time = Some(ExpireTime::UnixMilliseconds(ms));
                    }
                    "KEEPTTL" => {
                        if expire_time.is_some() {
                            return Err(Error::new(ErrorKind::Other, "malformed command"));
                        }
                        expire_time = Some(ExpireTime::KeepTtl);
                    }
                    "NX" => {
                        if mode != SetMode::Set {
                            return Err(Error::new(ErrorKind::Other, "malformed command"));
                        }

                        mode = SetMode::Add;
                    }
                    "XX" => {
                        if mode != SetMode::Set {
                            return Err(Error::new(ErrorKind::Other, "malformed command"));
                        }

                        mode = SetMode::Replace;
                    }
                    "GET" => {
                        if get_old {
                            return Err(Error::new(ErrorKind::Other, "malformed command"));
                        }

                        get_old = true;
                    }
                    _ => {
                        return Err(Error::new(ErrorKind::Other, "malformed command"));
                    }
                }
            }

            Ok(Self {
                key,
                value,
                expire_time,
                mode,
                get_old,
            })
        } else {
            Err(Error::new(ErrorKind::Other, "malformed command"))
        }
    }
}

impl From<&Set> for Message {
    fn from(other: &Set) -> Message {
        let mut v = vec![
            Message::bulk_string(b"SET"),
            Message::BulkString(BulkString::from(other.key.clone())),
            Message::BulkString(BulkString::from(other.value.clone())),
        ];

        match other.expire_time {
            Some(ExpireTime::Seconds(s)) => {
                v.push(Message::bulk_string(b"EX"));
                v.push(Message::bulk_string(format!("{}", s).as_bytes()));
            }
            Some(ExpireTime::Milliseconds(ms)) => {
                v.push(Message::bulk_string(b"PX"));
                v.push(Message::bulk_string(format!("{}", ms).as_bytes()));
            }
            Some(ExpireTime::UnixSeconds(s)) => {
                v.push(Message::bulk_string(b"EXAT"));
                v.push(Message::bulk_string(format!("{}", s).as_bytes()));
            }
            Some(ExpireTime::UnixMilliseconds(ms)) => {
                v.push(Message::bulk_string(b"PXAT"));
                v.push(Message::bulk_string(format!("{}", ms).as_bytes()));
            }
            Some(ExpireTime::KeepTtl) => {
                v.push(Message::bulk_string(b"KEEPTTL"));
            }
            None => {}
        }

        match other.mode {
            SetMode::Add => {
                v.push(Message::bulk_string(b"NX"));
            }
            SetMode::Replace => {
                v.push(Message::bulk_string(b"XX"));
            }
            SetMode::Set => {}
        }

        if other.get_old {
            v.push(Message::bulk_string(b"GET"));
        }

        Message::Array(Array { inner: Some(v) })
    }
}

impl Compose for Set {
    fn compose(&self, buf: &mut dyn BufMut) -> usize {
        let message = Message::from(self);
        message.compose(buf)
    }
}

impl Klog for Set {
    type Response = Response;

    fn klog(&self, response: &Self::Response) {
        let code = match response {
            Message::SimpleString(_) => STORED,
            _ => NOT_STORED,
        };

        klog!(
            "\"set {} {} {} {}\" {}",
            string_key(self.key()),
            self.mode(),
            self.expire_time().unwrap_or(ExpireTime::default()),
            self.value().len(),
            code
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser() {
        let parser = RequestParser::new();
        if let Request::Set(request) = parser.parse(b"set 0 1\r\n").unwrap().into_inner() {
            assert_eq!(request.key(), b"0");
            assert_eq!(request.value(), b"1");
        } else {
            panic!("invalid parse result");
        }

        if let Request::Set(request) = parser
            .parse(b"SET key value NX EX 1000\r\n")
            .unwrap()
            .into_inner()
        {
            assert_eq!(request.key(), b"key");
            assert_eq!(request.value(), b"value");
            assert_eq!(request.expire_time(), Some(ExpireTime::Seconds(1000)))
        } else {
            panic!("invalid parse result");
        }

        if let Request::Set(request) = parser
            .parse(b"SET key value EX 1000 NX\r\n")
            .unwrap()
            .into_inner()
        {
            assert_eq!(request.key(), b"key");
            assert_eq!(request.value(), b"value");
            assert_eq!(request.expire_time(), Some(ExpireTime::Seconds(1000)));
        } else {
            panic!("invalid parse result");
        }

        if let Request::Set(request) = parser
            .parse(b"*3\r\n$3\r\nset\r\n$1\r\n0\r\n$1\r\n1\r\n")
            .unwrap()
            .into_inner()
        {
            assert_eq!(request.key(), b"0");
            assert_eq!(request.value(), b"1");
        } else {
            panic!("invalid parse result");
        }
    }
}
