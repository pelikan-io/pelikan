use crate::klog::{klog_1, Status};
use crate::{Error, *};
use bytes::BytesMut;
use pelikan_net::*;
use protocol_memcache::*;

pub async fn delete(
    client: &mut CacheClient,
    cache_name: &str,
    socket: &mut tokio::net::TcpStream,
    request: Delete,
) -> Result<(), Error> {
    DELETE.increment();

    let key = request.key().to_owned();

    // check if the key is invalid before sending the requests to the backend
    if std::str::from_utf8(&key).is_err() {
        DELETE_EX.increment();

        // invalid key
        let _ = socket.write_all(b"ERROR\r\n").await;
        return Err(Error::from(ErrorKind::InvalidInput));
    }

    let mut response_buf = BytesMut::new();

    BACKEND_REQUEST.increment();

    let protocol = protocol_memcache::binary::BinaryProtocol::default();

    match timeout(
        Duration::from_millis(200),
        client.delete(cache_name, key.clone()),
    )
    .await
    {
        Ok(Ok(_result)) => {
            // it appears we can't tell deleted from not found in the momento
            // protocol, so we treat all non-error responses as if the key has
            // been deleted

            DELETE_DELETED.increment();

            if request.noreply() {
                klog_1(&"delete", &key, Status::Deleted, 0);
            } else {
                let response = Response::deleted(false);
                let _ = protocol.compose_response(
                    &Request::Delete(request),
                    &response,
                    &mut response_buf,
                );

                klog_1(&"delete", &key, Status::Deleted, response_buf.len());
                SESSION_SEND.increment();
                SESSION_SEND_BYTE.add(response_buf.len() as _);
                TCP_SEND_BYTE.add(response_buf.len() as _);
                if let Err(e) = socket.write_all(&response_buf).await {
                    SESSION_SEND_EX.increment();
                    // hangup if we can't send a response back
                    return Err(e);
                }
            }
        }
        Ok(Err(e)) => {
            BACKEND_EX.increment();

            DELETE_EX.increment();
            SESSION_SEND.increment();

            klog_1(&"delete", &key, Status::ServerError, 0);

            let message = format!("SERVER_ERROR {e}\r\n");

            SESSION_SEND_BYTE.add(message.len() as _);
            TCP_SEND_BYTE.add(message.len() as _);

            if let Err(e) = socket.write_all(message.as_bytes()).await {
                SESSION_SEND_EX.increment();
                // hangup if we can't send a response back
                return Err(e);
            }
        }
        Err(_) => {
            // timeout
            BACKEND_EX.increment();
            BACKEND_EX_TIMEOUT.increment();

            DELETE_EX.increment();
            SESSION_SEND.increment();

            klog_1(&"delete", &key, Status::Timeout, 0);

            let message = "SERVER_ERROR backend timeout\r\n";

            SESSION_SEND_BYTE.add(message.len() as _);
            TCP_SEND_BYTE.add(message.len() as _);

            if let Err(e) = socket.write_all(message.as_bytes()).await {
                SESSION_SEND_EX.increment();
                // hangup if we can't send a response back
                return Err(e);
            }
        }
    }

    Ok(())
}
