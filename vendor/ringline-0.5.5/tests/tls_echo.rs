//! End-to-end TLS echo tests.
//!
//! Tests the core TLS machinery: server-side TLS accept (handshake + data
//! exchange), and outbound `connect_tls` from one ringline worker to another.
//! Uses self-signed certificates generated at test time via `rcgen`.

use std::future::Future;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use ringline::{
    AsyncEventHandler, ConfigBuilder, ConnCtx, ParseResult, RinglineBuilder, TlsConfig, TlsInfo,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};

// ── Helpers ─────────────────────────────────────────────────────────────

static TEST_SERIALIZE: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn test_config_builder() -> ConfigBuilder {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(64, 4096)
        .max_connections(64)
        .send_pool(64, 16384)
}

fn free_port() -> u16 {
    // Tests run on many threads; the naive bind(:0)-drop-rebind pattern
    // races (the kernel can hand the same port to two tests before either
    // rebinds), which shows up as AddrInUse launch failures or clients
    // connecting to another test's server. A process-global claimed set
    // makes each handed-out port unique within the test binary.
    use std::sync::Mutex;
    static CLAIMED: Mutex<Option<std::collections::HashSet<u16>>> = Mutex::new(None);
    loop {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let mut guard = CLAIMED.lock().unwrap();
        if guard.get_or_insert_with(Default::default).insert(port) {
            return port;
        }
    }
}

fn wait_for_server(addr: &str) {
    for _ in 0..200 {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("server did not start on {addr}");
}

fn generate_self_signed() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    let cert_der = CertificateDer::from(cert.cert);
    (vec![cert_der], key.into())
}

fn server_tls_config(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Arc<rustls::ServerConfig> {
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    Arc::new(config)
}

fn client_tls_config(certs: &[CertificateDer<'static>]) -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in certs {
        roots.add(cert.clone()).unwrap();
    }
    rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
        .into()
}

// ── TLS Echo Handler ────────────────────────────────────────────────────

struct TlsEchoHandler;

impl AsyncEventHandler for TlsEchoHandler {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            loop {
                let n = conn
                    .with_data(|data| {
                        let _ = conn.send_nowait(data);
                        ParseResult::Consumed(data.len())
                    })
                    .await;
                if n == 0 {
                    break;
                }
            }
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        TlsEchoHandler
    }
}

// ── Test 1: External rustls client → ringline TLS server ────────────────

#[test]
fn tls_echo_with_external_client() {
    let _guard = TEST_SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());

    let (certs, key) = generate_self_signed();
    let server_config = server_tls_config(certs.clone(), key);

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let config = test_config_builder()
        .tls(TlsConfig::new(server_config))
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<TlsEchoHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Connect with a rustls client.
    let client_config = client_tls_config(&certs);
    let server_name: ServerName<'_> = "localhost".try_into().unwrap();
    let mut tls_conn = rustls::ClientConnection::new(client_config, server_name).unwrap();
    let mut tcp = TcpStream::connect(&addr).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    tcp.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

    let mut stream = rustls::Stream::new(&mut tls_conn, &mut tcp);

    // Send and receive data over TLS.
    let msg = b"Hello, TLS ringline!";
    stream.write_all(msg).unwrap();
    stream.flush().unwrap();

    let mut buf = vec![0u8; msg.len()];
    let mut total = 0;
    while total < msg.len() {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("TLS read error: {e}"),
        }
    }
    assert_eq!(&buf[..total], msg, "echoed data mismatch");

    // Send a larger message to exercise multi-chunk TLS.
    let large_msg = vec![0xABu8; 8192];
    stream.write_all(&large_msg).unwrap();
    stream.flush().unwrap();

    let mut large_buf = vec![0u8; large_msg.len()];
    let mut total = 0;
    while total < large_msg.len() {
        match stream.read(&mut large_buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // rustls::Stream may return WouldBlock if the TLS record
                // isn't fully available yet; retry after a short delay.
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }
            Err(e) => panic!("TLS read error (large): {e}"),
        }
    }
    assert_eq!(&large_buf[..total], &large_msg[..], "large echo mismatch");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Test 1b: Large multi-chunk payloads (BufRead fill_buf/consume loop) ──

/// Echo payloads larger than rustls's internal plaintext buffer (~16 KiB) so
/// the recv-side `fill_buf`/`consume` drain loop iterates across multiple
/// chunks and across multiple TLS records. This is the regression guard for
/// the zero-scratch drain: any off-by-one in the chunk advance would corrupt
/// the round-trip. A non-constant byte pattern makes mis-ordering detectable.
///
/// Gated to the io_uring backend: this guards the `fill_buf`/`consume` recv
/// drain (shared by both backends, but the change this regression-tests is in
/// the io_uring path). The mio backend has a pre-existing large-payload TLS
/// busy-spin (reproduces on `main` without this change), tracked separately;
/// running this test there hangs for reasons unrelated to the drain.
/// A handler that responds to the first received byte with one large
/// `send()` — larger than rustls's 64 KiB ciphertext buffer cap
/// (`DEFAULT_BUFFER_LIMIT`). Exercises the interleaved encrypt/drain loop:
/// a single oversized `writer().write_all()` used to fail with WriteZero
/// after the first 64 KiB was already encrypted and queued.
struct TlsBigSendHandler;

const BIG_SEND_SIZE: usize = 150 * 1024;

fn big_send_payload() -> Vec<u8> {
    (0..BIG_SEND_SIZE)
        .map(|i| (i as u32).wrapping_mul(2246822519) as u8)
        .collect()
}

impl AsyncEventHandler for TlsBigSendHandler {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let n = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if n == 0 {
                return;
            }
            let payload = big_send_payload();
            conn.send(&payload)
                .expect("large TLS send submit failed")
                .await
                .expect("large TLS send failed");
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        TlsBigSendHandler
    }
}

#[test]
fn tls_single_send_larger_than_rustls_buffer() {
    let _guard = TEST_SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());

    let (certs, key) = generate_self_signed();
    let server_config = server_tls_config(certs.clone(), key);

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let config = test_config_builder()
        .tls(TlsConfig::new(server_config))
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<TlsBigSendHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let client_config = client_tls_config(&certs);
    let server_name: ServerName<'_> = "localhost".try_into().unwrap();
    let mut tls_conn = rustls::ClientConnection::new(client_config, server_name).unwrap();
    let mut tcp = TcpStream::connect(&addr).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    tcp.set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut stream = rustls::Stream::new(&mut tls_conn, &mut tcp);

    stream.write_all(b"go").unwrap();
    stream.flush().unwrap();

    let expected = big_send_payload();
    let mut buf = vec![0u8; BIG_SEND_SIZE];
    let mut total = 0;
    while total < BIG_SEND_SIZE {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5));
                continue;
            }
            Err(e) => panic!("TLS read error: {e}"),
        }
    }
    assert_eq!(total, BIG_SEND_SIZE, "short read");
    assert_eq!(buf, expected, "byte-exact mismatch — chunk reorder or loss");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn tls_echo_large_multichunk() {
    let _guard = TEST_SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());

    let (certs, key) = generate_self_signed();
    let server_config = server_tls_config(certs.clone(), key);

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let config = test_config_builder()
        .tls(TlsConfig::new(server_config))
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<TlsEchoHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let client_config = client_tls_config(&certs);
    let server_name: ServerName<'_> = "localhost".try_into().unwrap();
    let mut tls_conn = rustls::ClientConnection::new(client_config, server_name).unwrap();
    let mut tcp = TcpStream::connect(&addr).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    tcp.set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    let mut stream = rustls::Stream::new(&mut tls_conn, &mut tcp);

    // Two sizes, both well past rustls's ~16 KiB plaintext buffer so the
    // drain loop must iterate over several chunks and several records.
    for &size in &[64 * 1024usize, 100 * 1024usize, 200 * 1024usize] {
        // Distinctive, position-dependent pattern so any chunk reorder or
        // off-by-one in the advance is caught by the byte-exact comparison.
        let msg: Vec<u8> = (0..size)
            .map(|i| (i as u32).wrapping_mul(2654435761) as u8)
            .collect();

        stream.write_all(&msg).unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0u8; size];
        let mut total = 0;
        while total < size {
            match stream.read(&mut buf[total..]) {
                Ok(0) => break,
                Ok(n) => total += n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(e) => panic!("TLS read error ({size} bytes): {e}"),
            }
        }
        assert_eq!(total, size, "short read for {size}-byte payload");
        assert_eq!(buf, msg, "byte-exact mismatch for {size}-byte payload");
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Test 2: Outbound connect_tls from ringline worker ───────────────────

static TLS_SERVER_ADDR: OnceLock<SocketAddr> = OnceLock::new();
static TLS_CONNECT_RESULT: OnceLock<String> = OnceLock::new();

struct TlsClientHandler;

impl AsyncEventHandler for TlsClientHandler {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async {}
    }

    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let server_addr = *TLS_SERVER_ADDR.get().expect("server addr not set");
        Some(Box::pin(async move {
            let conn = match ringline::connect_tls(server_addr, "localhost") {
                Ok(fut) => match fut.await {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        TLS_CONNECT_RESULT.set(format!("CONNECT_ERR:{e}")).ok();
                        ringline::request_shutdown().ok();
                        return;
                    }
                },
                Err(e) => {
                    TLS_CONNECT_RESULT.set(format!("SUBMIT_ERR:{e}")).ok();
                    ringline::request_shutdown().ok();
                    return;
                }
            };

            // Send data over TLS and read back.
            let msg = b"ringline-to-ringline TLS echo";
            if let Err(e) = conn.send(msg) {
                TLS_CONNECT_RESULT.set(format!("SEND_ERR:{e}")).ok();
                ringline::request_shutdown().ok();
                return;
            }

            let mut received = Vec::new();
            let n = conn
                .with_data(|data| {
                    received.extend_from_slice(data);
                    ParseResult::Consumed(data.len())
                })
                .await;

            if n == 0 {
                TLS_CONNECT_RESULT.set("EOF".to_string()).ok();
            } else if received == msg {
                TLS_CONNECT_RESULT.set("OK".to_string()).ok();
            } else {
                TLS_CONNECT_RESULT
                    .set(format!("MISMATCH:{}", String::from_utf8_lossy(&received)))
                    .ok();
            }
            ringline::request_shutdown().ok();
        }))
    }

    fn create_for_worker(_id: usize) -> Self {
        TlsClientHandler
    }
}

#[test]
fn tls_outbound_connect_and_echo() {
    let _guard = TEST_SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());

    let (certs, key) = generate_self_signed();
    let server_config = server_tls_config(certs.clone(), key);

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    // Start TLS server.
    let srv_config = test_config_builder()
        .tls(TlsConfig::new(server_config))
        .build()
        .expect("valid config");

    let (s_shutdown, s_handles) = RinglineBuilder::new(srv_config)
        .bind(addr.parse().unwrap())
        .launch::<TlsEchoHandler>()
        .expect("server launch failed");

    wait_for_server(&addr);
    TLS_SERVER_ADDR.set(addr.parse().unwrap()).ok();

    // Start client-only ringline with TLS client config.
    let client_tls = client_tls_config(&certs);
    let cli_config = test_config_builder()
        .tls_client(ringline::TlsClientConfig::new(client_tls))
        .build()
        .expect("valid config");

    let (_c_shutdown, c_handles) = RinglineBuilder::new(cli_config)
        .launch::<TlsClientHandler>()
        .expect("client launch failed");

    for h in c_handles {
        h.join().unwrap().unwrap();
    }

    let result = TLS_CONNECT_RESULT
        .get()
        .expect("on_start did not set result");
    assert_eq!(result, "OK", "expected OK, got: {result}");

    s_shutdown.shutdown();
    for h in s_handles {
        h.join().unwrap().unwrap();
    }
}

// ── Test 3: TlsInfo accessors ────────────────────────────────────────────

/// Snapshot of TlsInfo fields recorded by the handler on the first recv.
struct TlsInfoSnapshot {
    is_some: bool,
    protocol_version_some: bool,
    cipher_suite_some: bool,
    alpn_protocol: Option<Vec<u8>>,
    sni_hostname: Option<String>,
}

static TLS_INFO_SNAPSHOT: Mutex<Option<TlsInfoSnapshot>> = Mutex::new(None);

struct TlsInfoHandler;

impl AsyncEventHandler for TlsInfoHandler {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            let mut recorded = false;
            loop {
                let n = conn
                    .with_data(|data| {
                        // Record TlsInfo on the first data arrival (handshake is
                        // already complete — data is decrypted plaintext).
                        if !recorded {
                            recorded = true;
                            let info: Option<TlsInfo> = conn.tls_info();
                            let snapshot = TlsInfoSnapshot {
                                is_some: info.is_some(),
                                protocol_version_some: info
                                    .as_ref()
                                    .and_then(|i| i.protocol_version())
                                    .is_some(),
                                cipher_suite_some: info
                                    .as_ref()
                                    .and_then(|i| i.cipher_suite())
                                    .is_some(),
                                alpn_protocol: info
                                    .as_ref()
                                    .and_then(|i| i.alpn_protocol())
                                    .map(|b| b.to_vec()),
                                sni_hostname: info
                                    .as_ref()
                                    .and_then(|i| i.sni_hostname())
                                    .map(|s| s.to_string()),
                            };
                            *TLS_INFO_SNAPSHOT.lock().unwrap() = Some(snapshot);
                        }
                        let _ = conn.send_nowait(data);
                        ParseResult::Consumed(data.len())
                    })
                    .await;
                if n == 0 {
                    break;
                }
            }
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        TlsInfoHandler
    }
}

#[test]
fn tls_info_accessors() {
    let _guard = TEST_SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());

    // Reset shared state from any prior test run.
    *TLS_INFO_SNAPSHOT.lock().unwrap() = None;

    let (certs, key) = generate_self_signed();
    let server_config = server_tls_config(certs.clone(), key);

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let config = test_config_builder()
        .tls(TlsConfig::new(server_config))
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<TlsInfoHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Connect with a rustls client using SNI "localhost".
    let client_config = client_tls_config(&certs);
    let server_name: ServerName<'_> = "localhost".try_into().unwrap();
    let mut tls_conn = rustls::ClientConnection::new(client_config, server_name).unwrap();
    let mut tcp = TcpStream::connect(&addr).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    tcp.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

    let mut stream = rustls::Stream::new(&mut tls_conn, &mut tcp);

    // One round-trip to trigger on_accept and populate TLS_INFO_SNAPSHOT.
    let msg = b"tls-info-probe";
    stream.write_all(msg).unwrap();
    stream.flush().unwrap();

    let mut buf = vec![0u8; msg.len()];
    let mut total = 0;
    while total < msg.len() {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("TLS read error: {e}"),
        }
    }
    assert_eq!(&buf[..total], msg, "echoed data mismatch");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }

    // Assert all four TlsInfo accessors.
    let guard = TLS_INFO_SNAPSHOT.lock().unwrap();
    let snap = guard
        .as_ref()
        .expect("TlsInfo was never recorded — handler did not run");

    assert!(
        snap.is_some,
        "conn.tls_info() returned None after handshake"
    );
    assert!(
        snap.protocol_version_some,
        "protocol_version() was None after completed TLS handshake"
    );
    assert!(
        snap.cipher_suite_some,
        "cipher_suite() was None after completed TLS handshake"
    );
    // No ALPN protocols are configured in server_tls_config, so the handshake
    // does not negotiate one.
    assert_eq!(
        snap.alpn_protocol, None,
        "expected alpn_protocol() == None (no ALPN configured)"
    );
    // rustls ServerConnection::server_name() returns the SNI hostname from the
    // client's ClientHello.  The client above used "localhost" as ServerName.
    assert_eq!(
        snap.sni_hostname.as_deref(),
        Some("localhost"),
        "sni_hostname() should reflect the client's SNI value"
    );
}

// ── Test 4: Segmented recv over TLS (copy-per-chunk owned segments) ──────

// Segmented recv (`ConnCtx::segments()`) is io_uring-only; the reader is backed
// by the provided-buffer ring. On a TLS connection the decrypted plaintext can
// never be zero-copy (rustls owns its plaintext buffer), so each drained chunk
// is delivered as an *owned* `RecvSegment`. This test drives that path
// end-to-end: a segmented reader on the server reassembles a multi-chunk value
// pushed over TLS and echoes it back, and a clean TLS close surfaces `Ok(None)`
// (EOF) to the parked reader rather than hanging.
#[cfg(has_io_uring)]
static TLS_SEG_SAW_EOF: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[cfg(has_io_uring)]
struct TlsSegmentedHandler;

#[cfg(has_io_uring)]
impl AsyncEventHandler for TlsSegmentedHandler {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        async move {
            // Opt this TLS connection into segmented delivery: decrypted
            // plaintext arrives as owned segments in the hold, not the
            // accumulator.
            let mut reader = conn.segments();
            loop {
                match reader.next().await {
                    Ok(Some(seg)) => {
                        // Echo each decrypted plaintext segment straight back;
                        // the client reassembles and byte-compares.
                        let _ = conn.send_nowait(&seg);
                    }
                    Ok(None) => {
                        // Clean TLS close surfaced as EOF (not a hang).
                        TLS_SEG_SAW_EOF.store(true, std::sync::atomic::Ordering::SeqCst);
                        break;
                    }
                    Err(_) => break,
                }
            }
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        TlsSegmentedHandler
    }
}

#[cfg(has_io_uring)]
#[test]
fn tls_segmented_recv_reassembles_and_eofs() {
    let _guard = TEST_SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());

    TLS_SEG_SAW_EOF.store(false, std::sync::atomic::Ordering::SeqCst);

    let (certs, key) = generate_self_signed();
    let server_config = server_tls_config(certs.clone(), key);

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let config = test_config_builder()
        .tls(TlsConfig::new(server_config))
        .build()
        .expect("valid config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr.parse().unwrap())
        .launch::<TlsSegmentedHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let client_config = client_tls_config(&certs);
    let server_name: ServerName<'_> = "localhost".try_into().unwrap();
    let mut tls_conn = rustls::ClientConnection::new(client_config, server_name).unwrap();
    let mut tcp = TcpStream::connect(&addr).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    tcp.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

    // A multi-chunk value: larger than rustls's internal plaintext buffer so the
    // server's drain loop produces several owned segments across several TLS
    // records. A non-constant pattern makes any mis-ordering detectable.
    const SIZE: usize = 100 * 1024;
    let msg: Vec<u8> = (0..SIZE)
        .map(|i| (i as u32).wrapping_mul(2654435761) as u8)
        .collect();

    {
        let mut stream = rustls::Stream::new(&mut tls_conn, &mut tcp);
        stream.write_all(&msg).unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0u8; SIZE];
        let mut total = 0;
        while total < SIZE {
            match stream.read(&mut buf[total..]) {
                Ok(0) => break,
                Ok(n) => total += n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(e) => panic!("TLS read error: {e}"),
            }
        }
        assert_eq!(total, SIZE, "short read reassembling segmented echo");
        assert_eq!(buf, msg, "segmented TLS echo byte-exact mismatch");
    }

    // Clean TLS close: send close_notify then FIN. The server's parked segmented
    // reader must wake and surface Ok(None) (EOF), not hang.
    tls_conn.send_close_notify();
    tls_conn.write_tls(&mut tcp).unwrap();
    let _ = tcp.shutdown(std::net::Shutdown::Both);

    // Wait (bounded) for the handler to observe EOF; a hang would leave this
    // false until the timeout.
    let mut saw_eof = false;
    for _ in 0..300 {
        if TLS_SEG_SAW_EOF.load(std::sync::atomic::Ordering::SeqCst) {
            saw_eof = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        saw_eof,
        "segmented TLS reader did not observe EOF (Ok(None)) after clean close"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}
