// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Common, performance-oriented mechanisms of parsing byte strings into various types

/// maximum length of a string that could be stored as a u64 in Redis
/// comment from redis/util.h
/// > Bytes needed for long -> str + '\0'
const REDIS_LONG_STR_SIZE: usize = 22;

/// optionally parse a bytestring into a signed integer
/// implementation inspired by Redis
pub fn parse_signed_redis(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() || bytes.len() >= REDIS_LONG_STR_SIZE {
        return None;
    }

    // Special case: first and only digit is 0.
    if bytes.len() == 1 && bytes[0] == b'0' {
        return Some(0);
    }
    ///parses the remainder of the byte string as a number, returning None if at any point
    /// it is determined it isn't a canonical integer
    #[inline]
    fn parse_rest(start: i64, rest: &[u8], sign: i64) -> Option<i64> {
        let mut number: i64 = start;
        for byte in rest {
            let multiplied = number.checked_mul(10)?;
            let digit = convert_digit(*byte)?;
            let signed_digit = (digit as i64) * sign;
            let added = multiplied.checked_add(signed_digit)?;
            number = added;
        }
        Some(number)
    }
    #[inline]
    fn convert_digit(u: u8) -> Option<u32> {
        (u as char).to_digit(10)
    }

    match &bytes[0] {
        b'-' if bytes.len() >= 2 && bytes[1] != b'0' => {
            let first_digit = convert_digit(bytes[1])?;
            parse_rest(-(first_digit as i64), &bytes[2..], -1)
        }
        other if (b'1'..=b'9').contains(other) => {
            let digit = convert_digit(*other)?;
            parse_rest(digit as i64, &bytes[1..], 1)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::parsing::parse_signed_redis;

    #[test]
    fn it_should_parse_obvious_numbers() {
        for x in 0..=10_000 {
            assert_eq!(parse_signed_redis(x.to_string().as_bytes()), Some(x as i64));
            assert_eq!(
                parse_signed_redis((-x).to_string().as_bytes()),
                Some(-x as i64)
            );
        }
        assert_eq!(parse_signed_redis(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_signed_redis(b"-9223372036854775808"), Some(i64::MIN));
    }

    #[test]
    fn it_should_not_parse_non_canonical_signed_ints() {
        //leading zeroes
        assert_eq!(parse_signed_redis(b"042"), None);
        assert_eq!(parse_signed_redis(b"007"), None);
        assert_eq!(parse_signed_redis(b"000"), None);

        //negative numbers with leading zeroes
        assert_eq!(parse_signed_redis(b"-042"), None);
        assert_eq!(parse_signed_redis(b"-007"), None);
        assert_eq!(parse_signed_redis(b"-0007"), None);

        //negative zero
        assert_eq!(parse_signed_redis(b"-0"), None);
        assert_eq!(parse_signed_redis(b"-00"), None);
        assert_eq!(parse_signed_redis(b"-0000"), None);

        //won't parse overflowed values
        assert_eq!(parse_signed_redis(b"9223372036854775808"), None);
        assert_eq!(parse_signed_redis(b"-9223372036854775809"), None);

        //text strings
        assert_eq!(parse_signed_redis(b"foobar"), None);
        assert_eq!(parse_signed_redis(b"42foobar"), None);
        assert_eq!(parse_signed_redis(b"foobar42"), None);
        assert_eq!(parse_signed_redis(b"0f"), None);
        assert_eq!(parse_signed_redis(b"8f"), None);

        //symbols
        assert_eq!(parse_signed_redis(b"0&"), None);
        assert_eq!(parse_signed_redis(b"8&"), None);
        assert_eq!(parse_signed_redis(b"&$@!@#@0"), None);
        assert_eq!(parse_signed_redis(b"42-42"), None);
    }
}
