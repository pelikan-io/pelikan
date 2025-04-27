// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::*;
use clocksource::coarse::UnixInstant;
use std::borrow::Cow;
use std::fmt::Display;
use std::fmt::Formatter;
use std::num::NonZeroI32;

mod add;
mod append;
mod cas;
mod decr;
mod delete;
mod flush_all;
mod get;
mod incr;
mod prepend;
mod quit;
mod replace;
mod set;

pub use add::Add;
pub use append::Append;
pub use cas::Cas;
pub use decr::Decr;
pub use delete::Delete;
pub use flush_all::FlushAll;
pub use get::Get;
pub use incr::Incr;
pub use prepend::Prepend;
pub use quit::Quit;
pub use replace::Replace;
pub use set::Set;

pub const DEFAULT_MAX_BATCH_SIZE: usize = 1024;
pub const DEFAULT_MAX_KEY_LEN: usize = 250;
pub const DEFAULT_MAX_VALUE_SIZE: usize = 512 * 1024 * 1024; // 512MB max value size

// response codes for klog
const MISS: u8 = 0;
const HIT: u8 = 4;
const STORED: u8 = 5;
const EXISTS: u8 = 6;
const DELETED: u8 = 7;
const NOT_FOUND: u8 = 8;
const NOT_STORED: u8 = 9;

fn string_key(key: &[u8]) -> Cow<'_, str> {
    String::from_utf8_lossy(key)
}

#[derive(Debug, PartialEq, Eq)]
pub enum Request {
    Add(Add),
    Append(Append),
    Cas(Cas),
    Decr(Decr),
    Delete(Delete),
    FlushAll(FlushAll),
    Incr(Incr),
    Get(Get),
    Prepend(Prepend),
    Quit(Quit),
    Replace(Replace),
    Set(Set),
}

impl Request {
    pub fn add(key: Box<[u8]>, value: Box<[u8]>, flags: u32, ttl: Ttl, noreply: bool) -> Self {
        Self::Add(Add {
            key,
            value,
            flags,
            ttl,
            noreply,
        })
    }

    pub fn cas(
        key: Box<[u8]>,
        value: Box<[u8]>,
        flags: u32,
        ttl: Ttl,
        cas: u64,
        noreply: bool,
    ) -> Self {
        Self::Cas(Cas {
            key,
            value,
            flags,
            ttl,
            cas,
            noreply,
        })
    }

    pub fn decr(key: Box<[u8]>, value: u64, noreply: bool) -> Self {
        Self::Decr(Decr {
            key,
            value,
            noreply,
        })
    }

    pub fn delete(key: Box<[u8]>, noreply: bool) -> Self {
        Self::Delete(Delete {
            key,
            noreply,
            opaque: None,
        })
    }

    pub fn get(keys: Box<[Box<[u8]>]>) -> Self {
        Self::Get(Get {
            key: true,
            cas: false,
            opaque: None,
            keys,
        })
    }

    pub fn gets(keys: Box<[Box<[u8]>]>) -> Self {
        Self::Get(Get {
            key: true,
            cas: true,
            opaque: None,
            keys,
        })
    }

    pub fn incr(key: Box<[u8]>, value: u64, noreply: bool) -> Self {
        Self::Incr(Incr {
            key,
            value,
            noreply,
        })
    }

    pub fn replace(key: Box<[u8]>, value: Box<[u8]>, flags: u32, ttl: Ttl, noreply: bool) -> Self {
        Self::Replace(Replace {
            key,
            value,
            flags,
            ttl,
            noreply,
        })
    }

    pub fn set(key: Box<[u8]>, value: Box<[u8]>, flags: u32, ttl: Ttl, noreply: bool) -> Self {
        Self::Set(Set {
            key,
            value,
            flags,
            ttl,
            noreply,
        })
    }
}

impl Display for Request {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Request::Add(_) => write!(f, "add"),
            Request::Append(_) => write!(f, "append"),
            Request::Cas(_) => write!(f, "cas"),
            Request::Decr(_) => write!(f, "decr"),
            Request::Delete(_) => write!(f, "delete"),
            Request::FlushAll(_) => write!(f, "flush_all"),
            Request::Incr(_) => write!(f, "incr"),
            Request::Get(r) => {
                if r.cas {
                    write!(f, "gets")
                } else {
                    write!(f, "get")
                }
            }
            Request::Prepend(_) => write!(f, "prepend"),
            Request::Quit(_) => write!(f, "quit"),
            Request::Replace(_) => write!(f, "replace"),
            Request::Set(_) => write!(f, "set"),
        }
    }
}

impl Klog for Request {
    type Response = Response;

    fn klog(&self, response: &Self::Response) {
        match self {
            Self::Add(r) => r.klog(response),
            Self::Append(r) => r.klog(response),
            Self::Cas(r) => r.klog(response),
            Self::Decr(r) => r.klog(response),
            Self::Delete(r) => r.klog(response),
            Self::FlushAll(r) => r.klog(response),
            Self::Incr(r) => r.klog(response),
            Self::Get(r) => r.klog(response),
            Self::Prepend(r) => r.klog(response),
            Self::Quit(r) => r.klog(response),
            Self::Replace(r) => r.klog(response),
            Self::Set(r) => r.klog(response),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Add,
    Append,
    Cas,
    Decr,
    Delete,
    FlushAll,
    Incr,
    Get,
    Gets,
    Prepend,
    Quit,
    Replace,
    Set,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct Ttl {
    inner: Option<NonZeroI32>,
}

impl Ttl {
    /// Converts an expiration time from the Memcache ASCII format into a valid
    /// TTL. Negative values are always treated as immediate expiration. An
    /// expiration time of zero is always treated as no expiration. Positive
    /// value handling depends on the `TimeType`.
    ///
    /// For `TimeType::Unix` the expiration time is interpreted as a UNIX epoch
    /// time between 1970-01-01 T 00:00:00Z and 2106-02-06 T 06:28:15Z. If the
    /// provided expiration time is a previous or the current UNIX time, it is
    /// treated as immediate expiration. Times in the future are converted to a
    /// duration in seconds which is handled using the same logic as
    /// `TimeType::Delta`.
    ///
    /// For `TimeType::Delta` the expiration time is interpreted as a number of
    /// whole seconds and must be in the range of a signed 32bit integer. Values
    /// which exceed `i32::MAX` will be clamped, resulting in a max TTL of
    /// approximately 68 years.
    ///
    /// For `TimeType::Memcache` the expiration time is treated as
    /// `TimeType::Delta` if it is a duration of less than 30 days in seconds.
    /// If the provided expiration time is larger than that, it is treated as
    /// a UNIX epoch time following the `TimeType::Unix` rules.
    pub fn new(exptime: i64, time_type: TimeType) -> Self {
        // all negative values mean to expire immediately, early return
        if exptime < 0 {
            return Self {
                inner: NonZeroI32::new(-1),
            };
        }

        // all zero values are treated as no expiration
        if exptime == 0 {
            return Self { inner: None };
        }

        // normalize all expiration times into delta
        let exptime = if time_type == TimeType::Unix
            || (time_type == TimeType::Memcache && exptime > 60 * 60 * 24 * 30)
        {
            // treat it as a unix timestamp

            // clamp to a valid u32
            let exptime = if exptime > u32::MAX as i64 {
                u32::MAX
            } else {
                exptime as u32
            };

            // calculate the ttl in seconds
            let now = UnixInstant::now()
                .duration_since(UnixInstant::EPOCH)
                .as_secs();

            // would immediately expire, early return
            if now >= exptime {
                return Self {
                    inner: NonZeroI32::new(-1),
                };
            }

            (exptime - now) as i64
        } else {
            exptime
        };

        // clamp long TTLs
        if exptime > i32::MAX as i64 {
            Self {
                inner: NonZeroI32::new(i32::MAX),
            }
        } else {
            Self {
                inner: NonZeroI32::new(exptime as i32),
            }
        }
    }

    /// Return the TTL in seconds. A `None` variant should be treated as no
    /// expiration. Some storage implementations may treat it as the maximum
    /// TTL. Positive values will always be one second or greater. Negative
    /// values must be treated as immediate expiration.
    pub fn get(&self) -> Option<i32> {
        self.inner.map(|v| v.get())
    }

    pub fn none() -> Self {
        Self { inner: None }
    }
}
