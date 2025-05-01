// Copyright 2025 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::time::Duration;

use momento::cache::{SortedSetFetchByScoreRequest, SortedSetFetchResponse, SortedSetOrder};
use momento::CacheClient;
use protocol_resp::RangeType;
use protocol_resp::{SortedSetRange, ZRANGE, ZRANGE_EX, ZRANGE_HIT, ZRANGE_MISS};
use tokio::time;

use crate::error::ProxyResult;
use crate::klog::{klog_1, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn zrange(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SortedSetRange,
) -> ProxyResult {
    update_method_metrics(&ZRANGE, &ZRANGE_EX, async move {
        if *req.range_type() == RangeType::ByLex {
            klog_1(&"zrange", &req.key(), Status::ServerError, 0);
            return Err(ProxyError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Momento proxy does not support BYLEX for ZRANGE",
            )));
        }

        let response = match *req.range_type() {
            RangeType::ByIndex => {
                // ByIndex accepts only integers as inclusive start and inclusive stop values
                let start = parse_as_integer(req.start())?;
                let stop = parse_as_integer(req.stop())?;

                let order = match req.optional_args().reversed {
                    Some(true) => SortedSetOrder::Descending,
                    _ => SortedSetOrder::Ascending,
                };

                match time::timeout(
                    Duration::from_millis(200),
                    client.sorted_set_fetch_by_rank(
                        cache_name,
                        req.key(),
                        order,
                        Some(start),
                        Some(stop),
                    ),
                )
                .await
                {
                    Ok(Ok(r)) => r,
                    Ok(Err(e)) => {
                        klog_1(&"zrange", &req.key(), Status::ServerError, 0);
                        return Err(ProxyError::from(e));
                    }
                    Err(e) => {
                        klog_1(&"zrange", &req.key(), Status::Timeout, 0);
                        return Err(ProxyError::from(e));
                    }
                }
            }
            RangeType::ByScore => {
                // ByScore can accept start/stop values: `(` for exclusive boundary, `+inf`, `-inf`.
                let (start, exclusive_start) = parse_as_float(req.start())?;
                let (stop, exclusive_stop) = parse_as_float(req.stop())?;

                let order = match req.optional_args().reversed {
                    Some(true) => SortedSetOrder::Descending,
                    _ => SortedSetOrder::Ascending,
                };

                // Momento accepts only inclusive min and max scores, so we add one if the boundary is exclusive
                let fetch_request = SortedSetFetchByScoreRequest::new(cache_name, req.key())
                    .order(order)
                    .min_score(start + if exclusive_start { 1.0 } else { 0.0 })
                    .max_score(stop + if exclusive_stop { 1.0 } else { 0.0 })
                    .offset(req.optional_args().offset.map(|o| o as u32))
                    .count(req.optional_args().count.map(|c| c as i32));

                match time::timeout(
                    Duration::from_millis(200),
                    client.send_request(fetch_request),
                )
                .await
                {
                    Ok(Ok(r)) => r,
                    Ok(Err(e)) => {
                        klog_1(&"zrange", &req.key(), Status::ServerError, 0);
                        return Err(ProxyError::from(e));
                    }
                    Err(e) => {
                        klog_1(&"zrange", &req.key(), Status::Timeout, 0);
                        return Err(ProxyError::from(e));
                    }
                }
            }
            _ => {
                klog_1(&"zrange", &req.key(), Status::ServerError, 0);
                return Err(ProxyError::from(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "malformed command",
                )));
            }
        };

        let include_scores = match req.optional_args().with_scores {
            Some(true) => true,
            _ => false,
        };
        match response {
            SortedSetFetchResponse::Hit { value } => {
                ZRANGE_HIT.increment();

                if include_scores {
                    // Return elements and scores
                    response_buf
                        .extend_from_slice(format!("*{}\r\n", value.elements.len() * 2).as_bytes());
                } else {
                    // Return elements only
                    response_buf
                        .extend_from_slice(format!("*{}\r\n", value.elements.len()).as_bytes());
                }

                for (element, score) in value.elements {
                    response_buf.extend_from_slice(format!("${}\r\n", element.len()).as_bytes());
                    response_buf.extend_from_slice(&element);
                    response_buf.extend_from_slice(b"\r\n");

                    if include_scores {
                        response_buf.extend_from_slice(
                            format!("${}\r\n", score.to_string().len()).as_bytes(),
                        );
                        response_buf.extend_from_slice(score.to_string().as_bytes());
                        response_buf.extend_from_slice(b"\r\n");
                    }
                }
                klog_1(&"zrange", &req.key(), Status::Hit, response_buf.len());
            }
            SortedSetFetchResponse::Miss => {
                ZRANGE_MISS.increment();
                // return empty list on miss
                response_buf.extend_from_slice(b"*0\r\n");
                klog_1(&"zrange", &req.key(), Status::Miss, response_buf.len());
            }
        }

        Ok(())
    })
    .await
}

fn parse_as_integer(value: &[u8]) -> Result<i32, ProxyError> {
    let index = std::str::from_utf8(value)
        .map_err(|_| {
            ProxyError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "ZRANGE index is not valid utf8",
            ))
        })?
        .parse::<i32>()
        .map_err(|_| {
            ProxyError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "ZRANGE index is not an integer",
            ))
        })?;
    Ok(index)
}

// Returns a tuple of (value, is_exclusive)
fn parse_as_float(value: &[u8]) -> Result<(f64, bool), ProxyError> {
    // First check if the value is +inf or -inf
    if value == b"+inf" {
        return Ok((f64::INFINITY, false));
    }
    if value == b"-inf" {
        return Ok((f64::NEG_INFINITY, false));
    }

    // Otherwise, split apart '(' and the value if present
    let (inclusive_symbol, number) = if value[0] == b'(' {
        (true, &value[1..])
    } else {
        (false, value)
    };

    let score = std::str::from_utf8(number)
        .map_err(|_| {
            ProxyError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "ZRANGE score is not valid utf8",
            ))
        })?
        .parse::<f64>()
        .map_err(|_| {
            ProxyError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "ZRANGE score is not a float",
            ))
        })?;

    if inclusive_symbol {
        Ok((score, true))
    } else {
        Ok((score, false))
    }
}
