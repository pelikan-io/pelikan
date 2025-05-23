// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::*;
use ::config::momento_proxy::Protocol;
use momento::CacheClientBuilder;
use pelikan_net::{TCP_ACCEPT, TCP_CLOSE, TCP_CONN_CURR};

pub(crate) async fn listener(
    listener: TcpListener,
    client_builder: CacheClientBuilder<ReadyToBuild>,
    cache_name: String,
    protocol: Protocol,
    flags: bool,
) {
    let client = client_builder.clone().build().unwrap_or_else(|e| {
        // Note: this will not happen since we validated the client build in the main thread already
        eprintln!("could not create cache client: {}", e);
        std::process::exit(1);
    });
    // this acts as our listener thread and spawns tasks for each client
    loop {
        // accept a new client
        if let Ok((socket, _)) = listener.accept().await {
            TCP_ACCEPT.increment();

            let client = client.clone();
            let cache_name = cache_name.clone();

            // spawn a task for managing requests for the client
            tokio::spawn(async move {
                TCP_CONN_CURR.increment();
                match protocol {
                    Protocol::Memcache => {
                        crate::frontend::handle_memcache_client(socket, client, cache_name, flags)
                            .await;
                    }
                    Protocol::Resp => {
                        crate::frontend::handle_resp_client(socket, client, cache_name).await;
                    }
                }

                TCP_CONN_CURR.decrement();
                TCP_CLOSE.increment();
            });
        }
    }
}
