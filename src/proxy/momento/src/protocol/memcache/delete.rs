use crate::klog::{klog_1, Status};
use crate::{Error, *};
use pelikan_net::*;
use protocol_memcache::*;

pub async fn delete(
    client: &mut CacheClient,
    cache_name: &str,
    socket: &mut tokio::net::TcpStream,
    request: &protocol_memcache::Delete,
) -> Result<(), Error> {
    let key = request.key();

    // check if the key is invalid before sending the requests to the backend
    if std::str::from_utf8(key).is_err() {
        GET_EX.increment();

        // invalid key
        let _ = socket.write_all(b"ERROR\r\n").await;
        return Err(Error::from(ErrorKind::InvalidInput));
    }

    BACKEND_REQUEST.increment();

    match timeout(Duration::from_millis(200), client.delete(cache_name, key)).await {
        Ok(Ok(_result)) => {
            // it appears we can't tell deleted from not found in the momento
            // protocol, so we treat all non-error responses as if the key has
            // been deleted

            DELETE_DELETED.increment();

            if request.noreply() {
                klog_1(&"delete", &key, Status::Deleted, 0);
            } else {
                klog_1(&"delete", &key, Status::Deleted, 8);
                SESSION_SEND.increment();
                SESSION_SEND_BYTE.add(8);
                TCP_SEND_BYTE.add(8);
                if let Err(e) = socket.write_all(b"DELETED\r\n").await {
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
