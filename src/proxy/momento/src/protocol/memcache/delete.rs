// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::{klog_1, Status};
use crate::{Error, *};
use protocol_memcache::*;

pub async fn delete(
    client: &mut CacheClient,
    cache_name: &str,
    request: &Delete,
) -> Result<Response, Error> {
    DELETE.increment();

    let key = request.key().to_owned();

    // check if the key is invalid before sending the requests to the backend
    if std::str::from_utf8(&key).is_err() {
        DELETE_EX.increment();

        // invalid key
        return Ok(Response::client_error("invalid key"));
    }

    BACKEND_REQUEST.increment();

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
                Ok(Response::deleted(true))
            } else {
                // TODO(brian): this logs the wrong response len
                klog_1(&"delete", &key, Status::Deleted, 0);
                Ok(Response::deleted(false))
            }
        }
        Ok(Err(e)) => {
            BACKEND_EX.increment();

            DELETE_EX.increment();
            SESSION_SEND.increment();

            klog_1(&"delete", &key, Status::ServerError, 0);

            Ok(Response::server_error(format!("{e}")))
        }
        Err(_) => {
            // timeout
            BACKEND_EX.increment();
            BACKEND_EX_TIMEOUT.increment();

            DELETE_EX.increment();
            SESSION_SEND.increment();

            klog_1(&"delete", &key, Status::Timeout, 0);

            Ok(Response::server_error("backend timeout"))
        }
    }
}
