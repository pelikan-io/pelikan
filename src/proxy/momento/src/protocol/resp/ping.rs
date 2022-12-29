// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::Error;
use net::TCP_SEND_BYTE;
use session::{SESSION_SEND, SESSION_SEND_BYTE, SESSION_SEND_EX};
use tokio::io::AsyncWriteExt;

const PONG_RSP: &[u8; 7] = b"+PONG\r\n";

pub async fn ping(socket: &mut tokio::net::TcpStream) -> Result<(), Error> {
    let mut response_buf = Vec::new();
    response_buf.extend_from_slice(PONG_RSP);
    SESSION_SEND.increment();
    SESSION_SEND_BYTE.add(response_buf.len() as _);
    TCP_SEND_BYTE.add(response_buf.len() as _);
    if let Err(e) = socket.write_all(&response_buf).await {
        SESSION_SEND_EX.increment();
        return Err(e);
    }
    Ok(())
}
