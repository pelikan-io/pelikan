use crate::Config;
use bytes::Buf;
use bytes::BytesMut;
use chrono::Utc;
use common::ssl::TlsConfig as TlsConfigTrait;
use config::TlsConfig;
use http::HeaderMap;
use http::HeaderValue;
use http::Version;
use quinn::crypto::rustls::QuicServerConfig;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use session::REQUEST_LATENCY;
use std::sync::Arc;
use std::time::Instant;

pub async fn run(config: Arc<Config>) {
    let cert_file = config
        .tls()
        .certificate()
        .expect("no certificate configured");
    let cert_content = std::fs::read(cert_file).expect("failed to read cert");
    let cert = CertificateDer::from(cert_content);

    let key_file = config
        .tls()
        .private_key()
        .expect("no private key configured");
    let key_content = std::fs::read(key_file).expect("failed to read private key");
    let key = PrivateKeyDer::try_from(key_content).expect("failed to load private key");

    let mut tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .expect("error configuring tls");

    tls_config.max_early_data_size = u32::MAX;
    tls_config.alpn_protocols = vec!["h3".into()];

    let quic_config = QuicServerConfig::try_from(tls_config).expect("failed to configure quic");

    let server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_config));
    let endpoint = quinn::Endpoint::server(server_config, config.listen())
        .expect("failed to start quic endpoint");

    loop {
        if let Some(incoming_conn) = endpoint.accept().await {
            tokio::spawn(async move {
                if let Ok(conn) = incoming_conn.await {
                    if let Ok(mut conn) =
                        h3::server::Connection::new(h3_quinn::Connection::new(conn)).await
                    {
                        loop {
                            match conn.accept().await {
                                Ok(Some((request, mut stream))) => {
                                    let start = Instant::now();

                                    tokio::spawn(async move {
                                        let (_parts, _body) = request.into_parts();

                                        let mut content = BytesMut::new();

                                        while let Ok(data) = stream.recv_data().await {
                                            if let Some(mut data) = data {
                                                while data.has_remaining() {
                                                    let chunk: &[u8] = data.chunk();
                                                    content.extend_from_slice(chunk);
                                                    data.advance(chunk.len());
                                                }
                                            } else {
                                                break;
                                            }
                                        }

                                        if let Ok(_trailers) = stream.recv_trailers().await {
                                            let date =
                                                HeaderValue::from_str(&Utc::now().to_rfc2822())
                                                    .unwrap();

                                            let response = http::response::Builder::new()
                                                .status(200)
                                                .version(Version::HTTP_3)
                                                .header("content-type", "application/grpc")
                                                .header("date", date)
                                                .body(())
                                                .unwrap();

                                            let content = BytesMut::zeroed(5);

                                            let mut trailers = HeaderMap::new();
                                            trailers.append("grpc-status", 0.into());

                                            if stream.send_response(response).await.is_err() {
                                                return;
                                            }

                                            if stream.send_data(content).await.is_err() {
                                                return;
                                            }

                                            if stream.send_trailers(trailers).await.is_err() {
                                                return;
                                            }

                                            let stop = Instant::now();
                                            let latency = stop.duration_since(start).as_nanos();

                                            let _ = REQUEST_LATENCY.increment(latency as _);
                                        }
                                    });
                                }
                                Ok(None) => {
                                    // break if no Request is accepted
                                    break;
                                }
                                Err(err) => {
                                    match err.get_error_level() {
                                        // break on connection errors
                                        h3::error::ErrorLevel::ConnectionError => break,
                                        // continue on stream errors
                                        h3::error::ErrorLevel::StreamError => continue,
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }
    }
}
