use metriken::*;

#[metric(
    name = "tcp_accept",
    description = "number of TCP streams passively opened with accept"
)]
pub static TCP_ACCEPT: Counter = Counter::new();

#[metric(
    name = "tcp_connect",
    description = "number of TCP streams actively opened with connect"
)]
pub static TCP_CONNECT: Counter = Counter::new();

#[metric(name = "tcp_close", description = "number of TCP streams closed")]
pub static TCP_CLOSE: Counter = Counter::new();

#[metric(
    name = "tcp_conn_curr",
    description = "current number of open TCP streams"
)]
pub static TCP_CONN_CURR: Gauge = Gauge::new();

#[metric(
    name = "tcp_recv_byte",
    description = "number of bytes received on TCP streams"
)]
pub static TCP_RECV_BYTE: Counter = Counter::new();

#[metric(
    name = "tcp_send_byte",
    description = "number of bytes sent on TCP streams"
)]
pub static TCP_SEND_BYTE: Counter = Counter::new();

#[metric(name = "stream_accept", description = "number of calls to accept")]
pub static STREAM_ACCEPT: Counter = Counter::new();

#[metric(
    name = "stream_accept_ex",
    description = "number of times calling accept resulted in an exception"
)]
pub static STREAM_ACCEPT_EX: Counter = Counter::new();

#[metric(name = "stream_close", description = "number of streams closed")]
pub static STREAM_CLOSE: Counter = Counter::new();

#[metric(
    name = "stream_handshake",
    description = "number of times stream handshaking was attempted"
)]
pub static STREAM_HANDSHAKE: Counter = Counter::new();

#[metric(
    name = "stream_handshake_ex",
    description = "number of exceptions while handshaking"
)]
pub static STREAM_HANDSHAKE_EX: Counter = Counter::new();

#[metric(
    name = "stream_shutdown",
    description = "number of streams gracefully shutdown"
)]
pub static STREAM_SHUTDOWN: Counter = Counter::new();

#[metric(
    name = "stream_shutdown_ex",
    description = "number of exceptions while attempting to gracefully shutdown a stream"
)]
pub static STREAM_SHUTDOWN_EX: Counter = Counter::new();
