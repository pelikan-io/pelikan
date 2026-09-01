use bytes::BytesMut;
pub use clocksource::precise::Instant as RequestStart;
use protocol_common::{Compose, Protocol};
use session::REQUEST_LATENCY;
use std::collections::VecDeque;
use std::io::{self, ErrorKind};
use std::marker::PhantomData;

/// The result of parsing one request from a Ringline connection buffer.
#[derive(Debug, PartialEq, Eq)]
pub enum Parsed<Request> {
    Complete { request: Request, consumed: usize },
    NeedMore,
}

impl<Request> Parsed<Request> {
    /// Returns the parsed request.
    ///
    /// # Panics
    ///
    /// Panics when the parser needs more bytes before a request is available.
    pub fn request(&self) -> &Request {
        match self {
            Self::Complete { request, .. } => request,
            Self::NeedMore => panic!("a partial parse has no request"),
        }
    }

    /// Returns the number of bytes that form the parsed request.
    pub fn consumed(&self) -> usize {
        match self {
            Self::Complete { consumed, .. } => *consumed,
            Self::NeedMore => 0,
        }
    }
}

/// Adapts Pelikan protocols to Ringline's connection buffer and send lifecycle.
pub struct RinglineSession<P, Request, Response> {
    protocol: P,
    compose_buffer: BytesMut,
    pending: VecDeque<RequestStart>,
    outstanding: VecDeque<(Option<RequestStart>, usize)>,
    _request: PhantomData<Request>,
    _response: PhantomData<Response>,
}

impl<P, Request, Response> RinglineSession<P, Request, Response>
where
    P: Protocol<Request, Response>,
    Response: Compose,
{
    /// Creates an adapter for the given Pelikan protocol implementation.
    pub fn new(protocol: P) -> Self {
        Self {
            protocol,
            compose_buffer: BytesMut::new(),
            pending: VecDeque::new(),
            outstanding: VecDeque::new(),
            _request: PhantomData,
            _response: PhantomData,
        }
    }

    /// Parses at most one request from `data`.
    ///
    /// This captures the request timestamp immediately before parsing. A
    /// Ringline read callback should instead call [`Self::parse_at`] with a
    /// timestamp captured at its read boundary.
    pub fn parse(&mut self, data: &[u8]) -> io::Result<Parsed<Request>> {
        self.parse_at(data, RequestStart::now())
    }

    /// Parses at most one request from `data` using its read-boundary timestamp.
    ///
    /// The timestamp is queued only after a complete request is parsed, so a
    /// partial frame does not create an unmatched response timestamp.
    pub fn parse_at(
        &mut self,
        data: &[u8],
        request_started: RequestStart,
    ) -> io::Result<Parsed<Request>> {
        match self.protocol.parse_request(data) {
            Ok(parsed) => {
                self.pending.push_back(request_started);
                Ok(Parsed::Complete {
                    consumed: parsed.consumed(),
                    request: parsed.into_inner(),
                })
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => Ok(Parsed::NeedMore),
            Err(error) => Err(error),
        }
    }

    /// Composes a response into reusable storage for Ringline to send.
    pub fn compose(&mut self, response: &Response) -> &[u8] {
        self.compose_buffer.clear();

        let timestamp = self.pending.pop_front();
        let bytes = response.compose(&mut self.compose_buffer);

        if bytes == 0 {
            if let Some(timestamp) = timestamp {
                let latency = RequestStart::now() - timestamp;
                let _ = REQUEST_LATENCY.increment(latency.as_nanos());
            }
        } else {
            self.outstanding.push_back((timestamp, bytes));
        }

        &self.compose_buffer
    }

    /// Reports response bytes Ringline has completed sending.
    pub fn response_completed(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }

        let now = RequestStart::now();
        let mut bytes = bytes;

        while bytes > 0 {
            let Some((timestamp, remaining)) = self.outstanding.pop_front() else {
                break;
            };

            if remaining > bytes {
                self.outstanding.push_front((timestamp, remaining - bytes));
                break;
            }

            bytes -= remaining;
            if let Some(timestamp) = timestamp {
                let latency = now - timestamp;
                let _ = REQUEST_LATENCY.increment(latency.as_nanos());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Parsed, RequestStart, RinglineSession};
    use protocol_common::{BufMut, Compose, ParseOk, Protocol};
    use std::io::{self, ErrorKind};
    use std::time::Duration;

    #[derive(Default)]
    struct LineProtocol;

    struct LineResponse(Vec<u8>);

    impl Compose for LineResponse {
        fn compose(&self, dst: &mut dyn BufMut) -> usize {
            dst.put_slice(&self.0);
            self.0.len()
        }
    }

    impl Protocol<Vec<u8>, LineResponse> for LineProtocol {
        fn parse_request(&self, buffer: &[u8]) -> io::Result<ParseOk<Vec<u8>>> {
            if buffer.starts_with(b"!") {
                return Err(io::Error::new(ErrorKind::InvalidData, "invalid line"));
            }

            let Some(end) = buffer.iter().position(|byte| *byte == b'\n') else {
                return Err(io::Error::from(ErrorKind::WouldBlock));
            };

            Ok(ParseOk::new(buffer[..end].to_vec(), end + 1))
        }

        fn compose_request(&self, request: &Vec<u8>, buffer: &mut dyn BufMut) -> io::Result<usize> {
            buffer.put_slice(request);
            Ok(request.len())
        }

        fn parse_response(
            &self,
            _request: &Vec<u8>,
            _buffer: &[u8],
        ) -> io::Result<ParseOk<LineResponse>> {
            Err(io::Error::new(ErrorKind::Unsupported, "not used by server"))
        }

        fn compose_response(
            &self,
            _request: &Vec<u8>,
            response: &LineResponse,
            buffer: &mut dyn BufMut,
        ) -> io::Result<usize> {
            Ok(response.compose(buffer))
        }
    }

    struct SlowLineProtocol;

    impl Protocol<Vec<u8>, LineResponse> for SlowLineProtocol {
        fn parse_request(&self, buffer: &[u8]) -> io::Result<ParseOk<Vec<u8>>> {
            std::thread::sleep(Duration::from_millis(10));
            LineProtocol.parse_request(buffer)
        }

        fn compose_request(&self, request: &Vec<u8>, buffer: &mut dyn BufMut) -> io::Result<usize> {
            LineProtocol.compose_request(request, buffer)
        }

        fn parse_response(
            &self,
            request: &Vec<u8>,
            buffer: &[u8],
        ) -> io::Result<ParseOk<LineResponse>> {
            LineProtocol.parse_response(request, buffer)
        }

        fn compose_response(
            &self,
            request: &Vec<u8>,
            response: &LineResponse,
            buffer: &mut dyn BufMut,
        ) -> io::Result<usize> {
            LineProtocol.compose_response(request, response, buffer)
        }
    }

    #[test]
    fn parses_one_frame_and_reports_consumed_bytes() {
        let mut session = RinglineSession::new(LineProtocol);

        let parsed = session.parse(b"one\ntwo\n").unwrap();

        assert_eq!(parsed.request(), b"one");
        assert_eq!(parsed.consumed(), 4);
    }

    #[test]
    fn incomplete_frame_needs_more_data() {
        let mut session = RinglineSession::new(LineProtocol);

        assert!(matches!(session.parse(b"one"), Ok(Parsed::NeedMore)));
    }

    #[test]
    fn supplied_read_timestamp_survives_parsing_before_becoming_pending() {
        let mut session = RinglineSession::new(SlowLineProtocol);
        let read_started = RequestStart::now();

        let parsed = session.parse_at(b"one\n", read_started).unwrap();
        let parse_finished = RequestStart::now();

        assert_eq!(parsed.request(), b"one");
        assert!(
            parse_finished.duration_since(read_started).as_nanos()
                >= Duration::from_millis(10).as_nanos() as u64
        );
        assert_eq!(session.pending.front(), Some(&read_started));
    }

    #[test]
    fn pipelined_data_leaves_the_next_frame_for_the_caller() {
        let mut session = RinglineSession::new(LineProtocol);

        let parsed = session.parse(b"one\ntwo\n").unwrap();

        assert_eq!(&b"one\ntwo\n"[parsed.consumed()..], b"two\n");
    }

    #[test]
    fn invalid_frame_preserves_the_parser_error() {
        let mut session = RinglineSession::new(LineProtocol);

        let error = session.parse(b"!invalid\n").unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "invalid line");
    }

    #[test]
    fn composes_response_into_reused_buffer() {
        let mut session = RinglineSession::new(LineProtocol);

        assert_eq!(session.compose(&LineResponse(b"ok".to_vec())), b"ok");
        assert_eq!(session.compose(&LineResponse(b"next".to_vec())), b"next");
    }
}
