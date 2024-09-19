use crate::Config;

use session::REQUEST_LATENCY;

use bytes::BytesMut;
use chrono::Utc;
use http::{HeaderMap, HeaderValue, Version};
use tokio::net::TcpListener;

use std::sync::Arc;
use std::time::Instant;

pub async fn run(config: Arc<Config>) {
    let listener = TcpListener::bind(config.listen()).await.unwrap();

    loop {
        if let Ok((stream, _)) = listener.accept().await {
            let _ = stream.set_nodelay(true).is_err();

            tokio::task::spawn(async move {
                match ::h2::server::handshake(stream).await {
                    Ok(mut conn) => {
                        loop {
                            match conn.accept().await {
                                Some(Ok((request, mut sender))) => {
                                    let start = Instant::now();

                                    tokio::spawn(async move {
                                        let (_parts, mut body) = request.into_parts();

                                        let mut content = BytesMut::new();

                                        // receive all request body content
                                        while let Some(data) = body.data().await {
                                            if data.is_err() {
                                                // TODO(bmartin): increment error stats
                                                return;
                                            }

                                            let data = data.unwrap();

                                            content.extend_from_slice(&data);
                                            let _ =
                                                body.flow_control().release_capacity(data.len());
                                        }

                                        // we don't need the trailers, but read them here
                                        if body.trailers().await.is_err() {
                                            // TODO(bmartin): increment error stats
                                            return;
                                        }

                                        let mut date =
                                            HeaderValue::from_str(&Utc::now().to_rfc2822())
                                                .unwrap();
                                        date.set_sensitive(true);

                                        // build our response
                                        let response = http::response::Builder::new()
                                            .status(200)
                                            .version(Version::HTTP_2)
                                            .header("content-type", "application/grpc")
                                            .header("date", date)
                                            .body(())
                                            .unwrap();

                                        let content = BytesMut::zeroed(5);

                                        let mut trailers = HeaderMap::new();
                                        trailers.append("grpc-status", 0.into());

                                        // send the response
                                        if let Ok(mut stream) =
                                            sender.send_response(response, false)
                                        {
                                            if stream.send_data(content.into(), false).is_ok()
                                                && stream.send_trailers(trailers).is_ok()
                                            {
                                                let stop = Instant::now();
                                                let latency = stop.duration_since(start).as_nanos();

                                                let _ = REQUEST_LATENCY.increment(latency as _);
                                            }
                                        }

                                        // TODO(bmartin): increment error stats
                                    });
                                }
                                Some(Err(e)) => {
                                    eprintln!("error: {e}");
                                    break;
                                }
                                None => {
                                    continue;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("error during handshake: {e}");
                    }
                }
            });
        }
    }
}
