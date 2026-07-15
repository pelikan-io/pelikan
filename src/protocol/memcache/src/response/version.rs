// Copyright 2026 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

#[derive(Debug, PartialEq, Eq)]
pub struct Version {
    pub(crate) inner: String,
}

impl Compose for Version {
    fn compose(&self, session: &mut dyn BufMut) -> usize {
        let header = b"VERSION ";
        let version = self.inner.as_bytes();
        session.put_slice(header);
        session.put_slice(version);
        session.put_slice(b"\r\n");
        header.len() + version.len() + 2
    }
}

pub(crate) fn parse(input: &[u8]) -> IResult<&[u8], Version> {
    let (input, _) = space0(input)?;
    let (input, version) = take_till(|b| b == b' ' || b == b'\r')(input)?;
    let (input, _) = space0(input)?;
    let (input, _) = crlf(input)?;

    Ok((
        input,
        Version {
            inner: String::from_utf8_lossy(version).to_string(),
        },
    ))
}
