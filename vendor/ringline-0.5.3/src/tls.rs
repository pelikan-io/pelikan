#[allow(unused_imports)]
use std::io::{self, Read as _, Write as _};
use std::sync::Arc;

use rustls::pki_types::ServerName;
use rustls::{ClientConnection, ServerConnection};

#[allow(unused_imports)]
use crate::accumulator::AccumulatorTable;
#[cfg(has_io_uring)]
use crate::backend::Ring;
#[allow(unused_imports)]
use crate::buffer::send_copy::SendCopyPool;

/// Information about a negotiated TLS session.
pub struct TlsInfo {
    pub(crate) protocol_version: Option<rustls::ProtocolVersion>,
    pub(crate) cipher_suite: Option<rustls::SupportedCipherSuite>,
    pub(crate) alpn_protocol: Option<Vec<u8>>,
    pub(crate) sni_hostname: Option<String>,
}

impl TlsInfo {
    /// The negotiated TLS protocol version, if the handshake has completed.
    pub fn protocol_version(&self) -> Option<rustls::ProtocolVersion> {
        self.protocol_version
    }

    /// The negotiated cipher suite, if the handshake has completed.
    pub fn cipher_suite(&self) -> Option<rustls::SupportedCipherSuite> {
        self.cipher_suite
    }

    /// The ALPN protocol negotiated for this session, if any.
    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        self.alpn_protocol.as_deref()
    }

    /// The SNI hostname the peer requested, if any.
    pub fn sni_hostname(&self) -> Option<&str> {
        self.sni_hostname.as_deref()
    }
}

/// TLS connection kind — server (inbound) or client (outbound).
pub enum TlsConnKind {
    Server(ServerConnection),
    Client(ClientConnection),
}

impl TlsConnKind {
    pub fn read_tls(&mut self, rd: &mut dyn io::Read) -> io::Result<usize> {
        match self {
            TlsConnKind::Server(c) => c.read_tls(rd),
            TlsConnKind::Client(c) => c.read_tls(rd),
        }
    }

    pub fn write_tls(&mut self, wr: &mut dyn io::Write) -> io::Result<usize> {
        match self {
            TlsConnKind::Server(c) => c.write_tls(wr),
            TlsConnKind::Client(c) => c.write_tls(wr),
        }
    }

    pub fn process_new_packets(&mut self) -> Result<rustls::IoState, rustls::Error> {
        match self {
            TlsConnKind::Server(c) => c.process_new_packets(),
            TlsConnKind::Client(c) => c.process_new_packets(),
        }
    }

    pub fn reader(&mut self) -> rustls::Reader<'_> {
        match self {
            TlsConnKind::Server(c) => c.reader(),
            TlsConnKind::Client(c) => c.reader(),
        }
    }

    pub fn writer(&mut self) -> rustls::Writer<'_> {
        match self {
            TlsConnKind::Server(c) => c.writer(),
            TlsConnKind::Client(c) => c.writer(),
        }
    }

    pub fn wants_write(&self) -> bool {
        match self {
            TlsConnKind::Server(c) => c.wants_write(),
            TlsConnKind::Client(c) => c.wants_write(),
        }
    }

    pub fn is_handshaking(&self) -> bool {
        match self {
            TlsConnKind::Server(c) => c.is_handshaking(),
            TlsConnKind::Client(c) => c.is_handshaking(),
        }
    }

    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        match self {
            TlsConnKind::Server(c) => c.alpn_protocol(),
            TlsConnKind::Client(c) => c.alpn_protocol(),
        }
    }

    pub fn negotiated_cipher_suite(&self) -> Option<rustls::SupportedCipherSuite> {
        match self {
            TlsConnKind::Server(c) => c.negotiated_cipher_suite(),
            TlsConnKind::Client(c) => c.negotiated_cipher_suite(),
        }
    }

    pub fn protocol_version(&self) -> Option<rustls::ProtocolVersion> {
        match self {
            TlsConnKind::Server(c) => c.protocol_version(),
            TlsConnKind::Client(c) => c.protocol_version(),
        }
    }

    pub fn sni_hostname(&self) -> Option<&str> {
        match self {
            TlsConnKind::Server(c) => c.server_name(),
            TlsConnKind::Client(_) => None,
        }
    }

    pub fn send_close_notify(&mut self) {
        match self {
            TlsConnKind::Server(c) => c.send_close_notify(),
            TlsConnKind::Client(c) => c.send_close_notify(),
        }
    }
}

/// Per-connection TLS state.
pub struct TlsConn {
    pub conn: TlsConnKind,
    pub handshake_complete: bool,
    /// True once the peer's close_notify alert has been processed. A TCP
    /// FIN arriving while this is false is a truncation (possibly an
    /// attacker-injected FIN) and must not look like a clean EOF.
    pub peer_sent_close_notify: bool,
    /// True when `send_close_notify` has been called. Used by the
    /// close_notify timeout mechanism to detect stalled shutdowns.
    pub close_notify_sent: bool,
}

/// Table of TLS connections, indexed by connection slot.
/// Stored as a separate EventLoop field for borrow splitting.
pub struct TlsTable {
    conns: Vec<Option<TlsConn>>,
    server_config: Option<Arc<rustls::ServerConfig>>,
    client_config: Option<Arc<rustls::ClientConfig>>,
    /// Single shared ciphertext scratch buffer (one per worker thread).
    /// Only used synchronously — we process one connection at a time.
    /// io_uring builds write ciphertext directly into pool slots via
    /// `PoolWriter` and don't need it.
    #[cfg(not(has_io_uring))]
    write_buf: Vec<u8>,
}

impl TlsTable {
    /// Create a table with capacity for `max_connections`.
    pub fn new(
        max_connections: u32,
        server_config: Option<Arc<rustls::ServerConfig>>,
        client_config: Option<Arc<rustls::ClientConfig>>,
    ) -> Self {
        let mut conns = Vec::with_capacity(max_connections as usize);
        conns.resize_with(max_connections as usize, || None);
        TlsTable {
            conns,
            server_config,
            client_config,
            #[cfg(not(has_io_uring))]
            write_buf: Vec::new(),
        }
    }

    /// Whether a server config is present (for TLS accept on inbound connections).
    pub fn has_server_config(&self) -> bool {
        self.server_config.is_some()
    }

    /// Whether a client config is present (for TLS connect on outbound connections).
    pub fn has_client_config(&self) -> bool {
        self.client_config.is_some()
    }

    /// Create a new TLS server connection at the given index.
    pub fn create(&mut self, conn_index: u32) -> Result<(), rustls::Error> {
        let server_config = self
            .server_config
            .as_ref()
            .expect("create() called without server_config");
        let conn = ServerConnection::new(server_config.clone())?;
        self.conns[conn_index as usize] = Some(TlsConn {
            conn: TlsConnKind::Server(conn),
            handshake_complete: false,
            peer_sent_close_notify: false,
            close_notify_sent: false,
        });
        Ok(())
    }

    /// Create a new TLS client connection at the given index.
    pub fn create_client(
        &mut self,
        conn_index: u32,
        server_name: ServerName<'static>,
    ) -> Result<(), rustls::Error> {
        let client_config = self
            .client_config
            .as_ref()
            .expect("create_client() called without client_config");
        let conn = ClientConnection::new(client_config.clone(), server_name)?;
        self.conns[conn_index as usize] = Some(TlsConn {
            conn: TlsConnKind::Client(conn),
            handshake_complete: false,
            peer_sent_close_notify: false,
            close_notify_sent: false,
        });
        Ok(())
    }

    /// Get a mutable reference to the TLS connection at the given index.
    pub fn get_mut(&mut self, conn_index: u32) -> Option<&mut TlsConn> {
        self.conns[conn_index as usize].as_mut()
    }

    /// Check if a connection has TLS state.
    pub fn has(&self, conn_index: u32) -> bool {
        self.conns[conn_index as usize].is_some()
    }

    /// Remove TLS state for a connection.
    pub fn remove(&mut self, conn_index: u32) {
        self.conns[conn_index as usize] = None;
    }

    /// Get TLS session information for a connection.
    pub fn get_info(&self, conn_index: u32) -> Option<TlsInfo> {
        let tls_conn = self.conns[conn_index as usize].as_ref()?;
        Some(TlsInfo {
            protocol_version: tls_conn.conn.protocol_version(),
            cipher_suite: tls_conn.conn.negotiated_cipher_suite(),
            alpn_protocol: tls_conn.conn.alpn_protocol().map(|s| s.to_vec()),
            sni_hostname: tls_conn.conn.sni_hostname().map(|s| s.to_string()),
        })
    }

    /// Send a TLS close_notify alert and flush the resulting ciphertext.
    /// All SQEs are submitted with IOSQE_IO_LINK so the caller's subsequent
    /// Close SQE is chained and only runs after the close_notify is sent.
    #[cfg(has_io_uring)]
    pub fn send_close_notify(
        &mut self,
        conn_index: u32,
        ring: &mut Ring,
        send_copy_pool: &mut SendCopyPool,
    ) {
        if let Some(tls_conn) = self.get_mut(conn_index) {
            tls_conn.conn.send_close_notify();
            tls_conn.close_notify_sent = true;
            flush_close_notify_linked(tls_conn, ring, send_copy_pool, conn_index);
        }
    }
}

/// Result of feeding ciphertext into a TLS connection.
pub enum TlsRecvResult {
    /// Data processed successfully.
    Ok,
    /// TLS handshake just completed — caller should fire on_accept.
    HandshakeJustCompleted,
    /// TLS error occurred.
    #[allow(dead_code)] // variant matched; inner value reserved for future error reporting
    Error(rustls::Error),
    /// Peer sent close_notify or connection is cleanly closed.
    Closed,
}

/// Where decrypted TLS plaintext chunks are delivered by [`drain_tls_plaintext`].
///
/// TLS recv is *copy-per-chunk*: rustls owns its decrypted-plaintext buffer, so
/// the bytes must be copied out either way — there is no zero-copy TLS recv (see
/// `docs/segmented-recv-design.md`, "## TLS"). The sink chooses the destination
/// of that copy based on the connection's recv domain.
pub(crate) enum PlaintextSink<'a> {
    /// Default path: append each chunk into the connection's contiguous recv
    /// accumulator (one copy). Bounded by the accumulator's `max_size`
    /// (`Config::recv_accumulator_max`); `append` returning `false` is the
    /// flood-kill signal.
    Accumulator(&'a mut AccumulatorTable),
    /// Segmented recv domain (io_uring only): each drained plaintext chunk is
    /// copied into an owned [`Bytes`] and pushed as a `HeldRecvBuf::Owned`
    /// segment to the connection's hold. TLS segments are *always* owned — the
    /// decrypt copy is the release, so they never pin the provided ring. The
    /// same outstanding bound as the accumulator path is enforced: `outstanding`
    /// tracks total held owned bytes and `max` mirrors `recv_accumulator_max`;
    /// exceeding it is the flood-kill signal.
    #[cfg(has_io_uring)]
    Segments {
        hold: &'a mut std::collections::VecDeque<crate::backend::HeldRecvBuf>,
        outstanding: usize,
        max: usize,
    },
}

/// Drain all currently-decrypted plaintext from a TLS connection into `sink`,
/// with no intermediate scratch buffer.
///
/// rustls's `Reader` implements `BufRead`: `fill_buf()` exposes the decrypted
/// plaintext in rustls's own buffer, and `consume()` advances past what we copied.
/// The chunk `fill_buf` returns is *not* one ≤16 KiB record — it is as much
/// contiguous plaintext as rustls has buffered, so segment sizes are arbitrary.
///
/// Returns `false` if the sink hit its outstanding bound (accumulator
/// `max_size`, or the held-plaintext `max` for the segmented domain) — the
/// plaintext was NOT consumed from rustls, and the caller must treat the
/// connection as broken (silently consuming would put a permanent gap in the
/// byte stream; an unbounded plaintext flood must kill the connection).
#[must_use]
fn drain_tls_plaintext(
    tls_conn: &mut TlsConn,
    sink: &mut PlaintextSink<'_>,
    conn_index: u32,
) -> bool {
    use std::io::BufRead;
    let mut reader = tls_conn.conn.reader();
    loop {
        let chunk = match reader.fill_buf() {
            Ok([]) => break,
            Ok(b) => b,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(_) => break,
        };
        let n = chunk.len();
        match sink {
            PlaintextSink::Accumulator(accumulators) => {
                if !accumulators.append(conn_index, chunk) {
                    return false;
                }
            }
            #[cfg(has_io_uring)]
            PlaintextSink::Segments {
                hold,
                outstanding,
                max,
            } => {
                // Bound total outstanding held plaintext exactly as the
                // accumulator path bounds its buffer: an over-limit chunk is
                // NOT consumed from rustls, and the caller kills the connection.
                if outstanding.saturating_add(n) > *max {
                    return false;
                }
                hold.push_back(crate::backend::HeldRecvBuf::Owned(
                    bytes::Bytes::copy_from_slice(chunk),
                ));
                *outstanding += n;
            }
        }
        reader.consume(n);
    }
    true
}

/// Feed received ciphertext into the TLS connection, decrypt plaintext into
/// the accumulator, and flush any TLS output (handshake responses, alerts).
/// Any TLS output produced (handshake responses, alerts) is appended to
/// `out_sends`; the caller must route those through the per-connection send
/// queue whatever the return value (dropping them leaks their pool slots).
///
/// `sink` selects where decrypted plaintext lands: the recv accumulator (the
/// default `with_data`/`with_bytes` path) or, for a connection in the segmented
/// recv domain, owned segments pushed to its hold (see [`PlaintextSink`]).
#[cfg(has_io_uring)]
pub fn feed_tls_recv(
    tls_table: &mut TlsTable,
    mut sink: PlaintextSink<'_>,
    send_copy_pool: &mut SendCopyPool,
    conn_index: u32,
    ciphertext: &[u8],
    out_sends: &mut Vec<crate::handler::BuiltSend>,
) -> TlsRecvResult {
    let tls_conn = match tls_table.conns[conn_index as usize].as_mut() {
        Some(tc) => tc,
        None => return TlsRecvResult::Closed,
    };

    let was_handshaking = !tls_conn.handshake_complete;

    // Feed ciphertext into rustls. Loop until rustls has consumed the
    // entire ciphertext slice, OR a single `read_tls` call returned
    // 0 (meaning rustls's internal buffer is full and refuses more
    // bytes until we drain plaintext). rustls's `read_tls` reads in
    // chunks bounded by its internal buffer size (4 KiB at the time
    // of writing); a single ciphertext slice that crosses that
    // boundary — common for any TLS record carrying ≥ ~4 KiB of
    // plaintext, since rustls hasn't decrypted enough yet to free
    // buffer space — would otherwise leave the tail unfed,
    // permanently desynchronising the application from the wire.
    let mut cursor = io::Cursor::new(ciphertext);
    while cursor.position() < ciphertext.len() as u64 {
        match tls_conn.conn.read_tls(&mut cursor) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                return TlsRecvResult::Error(rustls::Error::General(e.to_string()));
            }
        }
        // Drive the state machine after each chunk so rustls can
        // free buffer space (by decrypting+queueing plaintext) and
        // accept the next chunk on the following iteration.
        let state = match tls_conn.conn.process_new_packets() {
            Ok(state) => state,
            Err(e) => {
                if tls_conn.conn.wants_write() {
                    let _ = take_tls_output_sends(tls_conn, send_copy_pool, conn_index, out_sends);
                }
                return TlsRecvResult::Error(e);
            }
        };

        // Drain plaintext after each call so rustls's internal
        // buffer has room for the next `read_tls`.
        if state.plaintext_bytes_to_read() > 0
            && !drain_tls_plaintext(tls_conn, &mut sink, conn_index)
        {
            return TlsRecvResult::Error(rustls::Error::General(
                "recv accumulator limit exceeded".into(),
            ));
        }
    }

    // Final state read for the wants_write / handshake / closed
    // checks below.
    let state = match tls_conn.conn.process_new_packets() {
        Ok(state) => state,
        Err(e) => {
            if tls_conn.conn.wants_write() {
                let _ = take_tls_output_sends(tls_conn, send_copy_pool, conn_index, out_sends);
            }
            return TlsRecvResult::Error(e);
        }
    };

    // Drain any remaining plaintext that the final state machine
    // tick produced (e.g. from a record whose ciphertext was
    // entirely buffered earlier in the loop).
    if state.plaintext_bytes_to_read() > 0 && !drain_tls_plaintext(tls_conn, &mut sink, conn_index)
    {
        return TlsRecvResult::Error(rustls::Error::General(
            "recv accumulator limit exceeded".into(),
        ));
    }

    // Collect any TLS output (handshake messages, alerts, etc.).
    if tls_conn.conn.wants_write()
        && !take_tls_output_sends(tls_conn, send_copy_pool, conn_index, out_sends)
    {
        return TlsRecvResult::Error(rustls::Error::General(
            "send pool exhausted during TLS output flush".into(),
        ));
    }

    // Check if handshake just completed.
    if was_handshaking && !tls_conn.conn.is_handshaking() {
        tls_conn.handshake_complete = true;
        return TlsRecvResult::HandshakeJustCompleted;
    }

    // Check for clean close.
    if state.peer_has_closed() {
        tls_conn.peer_sent_close_notify = true;
        return TlsRecvResult::Closed;
    }

    TlsRecvResult::Ok
}

/// Collect pending TLS output as queueable sends. Public entry point takes
/// `&mut TlsTable`. Returns `false` if pool exhaustion prevented draining
/// all output; sends already appended must still be queued by the caller.
#[cfg(has_io_uring)]
pub fn flush_tls_output(
    tls_table: &mut TlsTable,
    send_copy_pool: &mut SendCopyPool,
    conn_index: u32,
    out_sends: &mut Vec<crate::handler::BuiltSend>,
) -> bool {
    if let Some(tls_conn) = tls_table.get_mut(conn_index) {
        take_tls_output_sends(tls_conn, send_copy_pool, conn_index, out_sends)
    } else {
        true
    }
}

/// `io::Write` adapter that lands `write_tls` output directly in
/// `SendCopyPool` slots, eliminating the ciphertext bounce through a scratch
/// `Vec` (rustls buffer → scratch → pool becomes rustls buffer → pool).
///
/// Slots are filled front-to-back and sealed when full; the partially
/// filled tail slot is sealed by [`into_filled`](Self::into_filled). On pool
/// exhaustion `write` reports whatever it copied (rustls keeps the rest
/// buffered) and errors on the next call — callers treat exhaustion as a
/// broken connection either way, and [`release_all`](Self::release_all)
/// returns every slot on the error path.
#[cfg(has_io_uring)]
struct PoolWriter<'a> {
    pool: &'a mut SendCopyPool,
    /// Slot currently being filled: (slot, base, capacity, used).
    current: Option<(u16, *mut u8, usize, usize)>,
    /// Sealed slots in fill order: (slot, len).
    filled: Vec<(u16, u32)>,
    exhausted: bool,
}

#[cfg(has_io_uring)]
impl<'a> PoolWriter<'a> {
    fn new(pool: &'a mut SendCopyPool) -> Self {
        PoolWriter {
            pool,
            current: None,
            filled: Vec::new(),
            exhausted: false,
        }
    }

    /// Seal the partially-filled tail slot (if any) into `filled`.
    fn seal_current(&mut self) {
        if let Some((slot, _, _, used)) = self.current.take() {
            if used > 0 {
                self.pool.set_filled(slot, used as u32);
                self.filled.push((slot, used as u32));
            } else {
                self.pool.release(slot);
            }
        }
    }

    /// Finish writing: seal the tail and return the filled slots in order.
    fn into_filled(mut self) -> Vec<(u16, u32)> {
        self.seal_current();
        std::mem::take(&mut self.filled)
    }

    /// Error path: return every allocated slot to the pool.
    fn release_all(mut self) {
        if let Some((slot, _, _, _)) = self.current.take() {
            self.pool.release(slot);
        }
        for (slot, _) in self.filled.drain(..) {
            self.pool.release(slot);
        }
    }
}

#[cfg(has_io_uring)]
impl io::Write for PoolWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.exhausted {
            return Err(io::Error::other("send copy pool exhausted for TLS"));
        }
        let mut copied = 0;
        while copied < buf.len() {
            if self.current.is_none() {
                match self.pool.alloc_raw() {
                    Some((slot, ptr, cap)) => {
                        self.current = Some((slot, ptr, cap as usize, 0));
                    }
                    None => {
                        self.exhausted = true;
                        if copied > 0 {
                            // Short write: rustls keeps the remainder
                            // buffered; the next call errors.
                            return Ok(copied);
                        }
                        return Err(io::Error::other("send copy pool exhausted for TLS"));
                    }
                }
            }
            let (_, ptr, cap, used) = self.current.as_mut().expect("just ensured");
            let n = (buf.len() - copied).min(*cap - *used);
            // Safety: `ptr` is the base of an in_use pool slot of size `cap`;
            // `used + n <= cap` by construction, and `buf` is a live slice.
            unsafe {
                std::ptr::copy_nonoverlapping(buf.as_ptr().add(copied), ptr.add(*used), n);
            }
            *used += n;
            copied += n;
            if *used == *cap {
                self.seal_current();
            }
        }
        Ok(copied)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Build a pool-backed send SQE entry without submitting it. The caller
/// routes it through the per-connection send queue (`submit_or_queue`) so
/// TLS ciphertext is serialized with every other send on the connection:
/// io_uring does not order independent SQEs, and a partial-send resubmit of
/// chunk A after chunk B already transmitted interleaves ciphertext on the
/// wire (bad_record_mac at the peer).
#[cfg(has_io_uring)]
fn build_pool_send(
    conn_index: u32,
    ptr: *const u8,
    len: u32,
    pool_slot: u16,
    tag: crate::completion::OpTag,
) -> crate::handler::BuiltSend {
    let user_data = crate::completion::UserData::encode(tag, conn_index, pool_slot as u32);
    let entry = io_uring::opcode::Send::new(io_uring::types::Fixed(conn_index), ptr, len)
        .flags(crate::completion::STREAM_SEND_FLAGS)
        .build()
        .user_data(user_data.raw());
    crate::handler::BuiltSend {
        entry,
        pool_slot,
        slab_idx: u16::MAX,
        total_len: len,
    }
}

/// Drain rustls' pending TLS output (handshake messages, alerts) into
/// pool-backed sends appended to `out`, tagged `TlsSend`.
///
/// Returns `false` on pool exhaustion or a `write_tls` error — the
/// connection should be considered broken. Sends already appended to `out`
/// must still be queued by the caller (their pool slots are otherwise
/// leaked); queueing a prefix of the output ahead of a close is safe.
#[cfg(has_io_uring)]
fn take_tls_output_sends(
    tls_conn: &mut TlsConn,
    send_copy_pool: &mut SendCopyPool,
    conn_index: u32,
    out: &mut Vec<crate::handler::BuiltSend>,
) -> bool {
    let mut writer = PoolWriter::new(send_copy_pool);
    while tls_conn.conn.wants_write() {
        match tls_conn.conn.write_tls(&mut writer) {
            Ok(0) | Err(_) => {
                // Pool exhaustion or writer error: release what this call
                // allocated. Sends appended to `out` by *earlier* calls are
                // untouched, preserving the caller contract.
                writer.release_all();
                return false;
            }
            Ok(_) => {}
        }
    }
    for (slot, len) in writer.into_filled() {
        let (ptr, _) = send_copy_pool.current_ptr_remaining(slot);
        out.push(build_pool_send(
            conn_index,
            ptr,
            len,
            slot,
            crate::completion::OpTag::TlsSend,
        ));
    }
    true
}

/// Flush close_notify ciphertext using linked SQEs (IOSQE_IO_LINK).
/// All TlsSend SQEs are linked so the caller's subsequent Close SQE
/// is chained — the kernel won't close the fd until the alert is sent.
#[cfg(has_io_uring)]
fn flush_close_notify_linked(
    tls_conn: &mut TlsConn,
    ring: &mut Ring,
    send_copy_pool: &mut SendCopyPool,
    conn_index: u32,
) {
    let mut writer = PoolWriter::new(send_copy_pool);
    while tls_conn.conn.wants_write() {
        match tls_conn.conn.write_tls(&mut writer) {
            Ok(0) | Err(_) => {
                // Pool exhausted or write error — the connection closes
                // without a complete alert, same as before.
                writer.release_all();
                return;
            }
            Ok(_) => {}
        }
    }
    for (slot, len) in writer.into_filled() {
        let (ptr, _) = send_copy_pool.current_ptr_remaining(slot);
        if ring
            .submit_tls_send_linked(conn_index, ptr, len, slot)
            .is_err()
        {
            send_copy_pool.release(slot);
            return;
        }
    }
}

/// Encrypt plaintext into pool-backed sends for the caller to route through
/// the per-connection send queue. The final chunk is tagged `OpTag::Send`
/// so its CQE wakes the send waiter; intermediates are `TlsSend`.
///
/// Ciphertext lands directly in pool slots via [`PoolWriter`]
/// (rustls buffer → pool), one copy fewer than the old
/// rustls → scratch → pool chain.
///
/// Encryption is interleaved with draining: rustls caps its ciphertext
/// buffer at 64 KiB (`DEFAULT_BUFFER_LIMIT`), so a single `write_all` of a
/// larger plaintext used to fail with `WriteZero` after the first 64 KiB
/// was already encrypted and queued (a truncated prefix could then reach
/// the wire). Writing and draining in a loop supports arbitrary sizes.
///
/// On error, pool slots already filled are released here — the caller sees
/// only `Err` and queues nothing.
#[cfg(has_io_uring)]
pub fn encrypt_to_sends(
    tls_table: &mut TlsTable,
    send_copy_pool: &mut SendCopyPool,
    conn_index: u32,
    plaintext: &[u8],
) -> io::Result<Vec<crate::handler::BuiltSend>> {
    let tls_conn = tls_table.get_mut(conn_index).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotConnected, "no TLS state for connection")
    })?;

    let mut writer = PoolWriter::new(send_copy_pool);
    let mut offset = 0;
    while offset < plaintext.len() {
        let n = match tls_conn.conn.writer().write(&plaintext[offset..]) {
            Ok(n) => n,
            Err(e) => {
                writer.release_all();
                return Err(io::Error::other(e));
            }
        };
        offset += n;

        // Drain whatever ciphertext this write produced.
        let mut drained = 0usize;
        while tls_conn.conn.wants_write() {
            match tls_conn.conn.write_tls(&mut writer) {
                Ok(0) => break,
                Ok(w) => drained += w,
                Err(e) => {
                    writer.release_all();
                    return Err(io::Error::other(e));
                }
            }
        }
        if n == 0 && drained == 0 {
            // rustls accepted nothing and produced nothing — no progress
            // (includes writer exhaustion with nothing drained).
            writer.release_all();
            return Err(io::Error::other("TLS encryption made no progress"));
        }
    }

    let filled = writer.into_filled();
    let mut built = Vec::with_capacity(filled.len());
    for (i, &(slot, len)) in filled.iter().enumerate() {
        let (ptr, _) = send_copy_pool.current_ptr_remaining(slot);
        // Final chunk completes the logical send (wakes the waiter, drives
        // the queue via handle_send); intermediates are TLS-internal.
        let tag = if i + 1 == filled.len() {
            crate::completion::OpTag::Send
        } else {
            crate::completion::OpTag::TlsSend
        };
        built.push(build_pool_send(conn_index, ptr, len, slot, tag));
    }
    Ok(built)
}

// ── Mio backend TLS helpers ─────────────────────────────────────────────

/// Feed received ciphertext into the TLS connection, decrypt plaintext into
/// the accumulator, and flush any TLS output (handshake responses, alerts).
///
/// Mio version: writes ciphertext directly to the TcpStream instead of
/// submitting io_uring SQEs.
#[cfg(not(has_io_uring))]
pub fn feed_tls_recv_mio(
    tls_table: &mut TlsTable,
    accumulators: &mut AccumulatorTable,
    pending: &mut std::collections::VecDeque<crate::backend::mio::driver::PendingSend>,
    conn_index: u32,
    ciphertext: &[u8],
) -> TlsRecvResult {
    let tls_conn = match tls_table.conns[conn_index as usize].as_mut() {
        Some(tc) => tc,
        None => return TlsRecvResult::Closed,
    };

    // The mio backend has no provided-buffer ring; segmented recv is io_uring
    // only. Plaintext always lands in the accumulator here.
    let mut sink = PlaintextSink::Accumulator(accumulators);

    let was_handshaking = !tls_conn.handshake_complete;
    let mut peer_closed = false;
    let mut remaining = ciphertext;

    // Feed ciphertext into rustls in a loop. `read_tls` may not consume all
    // input at once (rustls has an internal buffer limit, typically 4KB).
    // After each `read_tls` + `process_new_packets`, drain decrypted plaintext
    // and retry with remaining ciphertext.
    while !remaining.is_empty() {
        let mut cursor = io::Cursor::new(remaining);
        if let Err(e) = tls_conn.conn.read_tls(&mut cursor) {
            return TlsRecvResult::Error(rustls::Error::General(e.to_string()));
        }
        let consumed = cursor.position() as usize;
        if consumed == 0 {
            // read_tls consumed nothing — shouldn't happen with a non-empty
            // cursor, but guard against infinite loops.
            break;
        }
        remaining = &remaining[consumed..];

        // Drive the TLS state machine.
        let state = match tls_conn.conn.process_new_packets() {
            Ok(state) => state,
            Err(e) => {
                // Try to flush alert before returning error.
                if tls_conn.conn.wants_write() {
                    flush_tls_output_mio_inner(tls_conn, &mut tls_table.write_buf, pending);
                }
                return TlsRecvResult::Error(e);
            }
        };

        // Read decrypted plaintext into accumulator.
        if state.plaintext_bytes_to_read() > 0
            && !drain_tls_plaintext(tls_conn, &mut sink, conn_index)
        {
            return TlsRecvResult::Error(rustls::Error::General(
                "recv accumulator limit exceeded".into(),
            ));
        }

        // Queue any TLS output (handshake messages, alerts, etc.).
        if tls_conn.conn.wants_write() {
            flush_tls_output_mio_inner(tls_conn, &mut tls_table.write_buf, pending);
        }

        if state.peer_has_closed() {
            peer_closed = true;
            tls_conn.peer_sent_close_notify = true;
        }
    }

    // Check if handshake just completed.
    if was_handshaking && !tls_conn.conn.is_handshaking() {
        tls_conn.handshake_complete = true;
        return TlsRecvResult::HandshakeJustCompleted;
    }

    // Check for clean close.
    if peer_closed {
        return TlsRecvResult::Closed;
    }

    TlsRecvResult::Ok
}

/// Flush pending TLS output to the network via direct stream write.
/// Public entry point takes `&mut TlsTable`.
#[cfg(not(has_io_uring))]
pub fn flush_tls_output_mio_queued(
    tls_table: &mut TlsTable,
    pending: &mut std::collections::VecDeque<crate::backend::mio::driver::PendingSend>,
    conn_index: u32,
) {
    let (conn_slot, write_buf) = borrow_conn_and_buf(tls_table, conn_index);
    if let Some(tls_conn) = conn_slot {
        flush_tls_output_mio_inner(tls_conn, write_buf, pending);
    }
}

/// Inner flush for mio: queue ciphertext into the connection's pending-send
/// FIFO instead of writing to the stream directly. Direct writes dropped
/// the unwritten remainder on WouldBlock — losing handshake/alert bytes
/// with no retry (a truncated TLS record stalls the peer's handshake) —
/// and could reorder records around ciphertext already sitting in
/// pending_sends.
#[cfg(not(has_io_uring))]
fn flush_tls_output_mio_inner(
    tls_conn: &mut TlsConn,
    write_buf: &mut Vec<u8>,
    pending: &mut std::collections::VecDeque<crate::backend::mio::driver::PendingSend>,
) {
    write_buf.clear();
    if tls_conn.conn.write_tls(write_buf).is_err() {
        return;
    }

    if write_buf.is_empty() {
        return;
    }

    pending.push_back((std::mem::take(write_buf), 0, None));
}

/// Direct-write flush for close paths (close_notify): the connection is
/// being torn down, so best-effort nonblocking writes are appropriate —
/// there is no later flush opportunity.
#[cfg(not(has_io_uring))]
pub fn flush_tls_output_mio_direct(
    tls_table: &mut TlsTable,
    stream: &mut mio::net::TcpStream,
    conn_index: u32,
) {
    let (conn_slot, write_buf) = borrow_conn_and_buf(tls_table, conn_index);
    let Some(tls_conn) = conn_slot else { return };
    write_buf.clear();
    if tls_conn.conn.write_tls(write_buf).is_err() || write_buf.is_empty() {
        return;
    }
    let mut offset = 0;
    while offset < write_buf.len() {
        match stream.write(&write_buf[offset..]) {
            Ok(0) => break,
            Ok(n) => offset += n,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(_) => break,
        }
    }
}

/// Encrypt plaintext and return the ciphertext for buffered sending.
/// Mio version: encrypts data and returns ciphertext bytes. The caller
/// pushes the result into the pending_sends queue for the event loop to
/// flush when the socket is writable.
#[cfg(not(has_io_uring))]
pub fn encrypt_for_send_mio(
    tls_table: &mut TlsTable,
    conn_index: u32,
    plaintext: &[u8],
) -> io::Result<Vec<u8>> {
    let (conn_slot, _write_buf) = borrow_conn_and_buf(tls_table, conn_index);
    let tls_conn = conn_slot.as_mut().ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotConnected, "no TLS state for connection")
    })?;

    // Interleave writer().write with write_tls draining: rustls caps its
    // ciphertext buffer at 64 KiB, so a single write_all of a larger
    // plaintext fails with WriteZero after the first 64 KiB was already
    // encrypted (same fix as the io_uring path's encrypt_to_sends).
    let mut ciphertext = Vec::with_capacity(plaintext.len() + 128);
    let mut offset = 0;
    while offset < plaintext.len() {
        let n = tls_conn
            .conn
            .writer()
            .write(&plaintext[offset..])
            .map_err(io::Error::other)?;
        offset += n;
        let before = ciphertext.len();
        tls_conn
            .conn
            .write_tls(&mut ciphertext)
            .map_err(io::Error::other)?;
        if n == 0 && ciphertext.len() == before {
            return Err(io::Error::other("TLS encryption made no progress"));
        }
    }

    Ok(ciphertext)
}

/// Borrow a connection slot and the shared write_buf from a TlsTable simultaneously.
/// This is the borrow-splitting helper: `conns[i]` and `write_buf` are disjoint fields.
#[cfg(not(has_io_uring))]
fn borrow_conn_and_buf(
    table: &mut TlsTable,
    conn_index: u32,
) -> (&mut Option<TlsConn>, &mut Vec<u8>) {
    (&mut table.conns[conn_index as usize], &mut table.write_buf)
}

// ── Segmented recv routing (io_uring only) ──────────────────────────────

/// Focused unit tests for the TLS-plaintext → `segment_hold` routing added for
/// segmented recv over TLS. Drives a real in-memory rustls handshake (no
/// networking), then feeds the server ciphertext and drains its plaintext into a
/// [`PlaintextSink::Segments`], asserting decrypted plaintext lands as owned
/// segments and that the outstanding-plaintext bound kills an over-limit flood.
#[cfg(all(test, has_io_uring))]
mod segmented_tls_tests {
    use super::*;
    use crate::backend::HeldRecvBuf;
    use std::collections::VecDeque;
    use std::io::Cursor;

    fn test_certs() -> (
        Vec<rustls::pki_types::CertificateDer<'static>>,
        rustls::pki_types::PrivateKeyDer<'static>,
    ) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
        let cert_der = rustls::pki_types::CertificateDer::from(cert.cert);
        (vec![cert_der], key.into())
    }

    /// Move all of `from`'s pending TLS output into `to`, driving `to`'s state
    /// machine. Used to pump a handshake to completion.
    fn pump(from: &mut TlsConnKind, to: &mut TlsConnKind) {
        let mut buf = Vec::new();
        while from.wants_write() {
            from.write_tls(&mut buf).unwrap();
        }
        if buf.is_empty() {
            return;
        }
        let mut cursor = Cursor::new(&buf[..]);
        while (cursor.position() as usize) < buf.len() {
            let n = to.read_tls(&mut cursor).unwrap();
            if n == 0 {
                break;
            }
            to.process_new_packets().unwrap();
        }
    }

    /// A completed in-memory TLS session: (server, client), both past handshake.
    fn handshaked() -> (TlsConnKind, TlsConnKind) {
        let (certs, key) = test_certs();
        let server_config = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs.clone(), key)
                .unwrap(),
        );
        let mut roots = rustls::RootCertStore::empty();
        for c in &certs {
            roots.add(c.clone()).unwrap();
        }
        let client_config: Arc<rustls::ClientConfig> = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
            .into();
        let server_name: rustls::pki_types::ServerName<'_> = "localhost".try_into().unwrap();

        let mut server = TlsConnKind::Server(ServerConnection::new(server_config).unwrap());
        let mut client =
            TlsConnKind::Client(ClientConnection::new(client_config, server_name).unwrap());

        for _ in 0..30 {
            pump(&mut client, &mut server);
            pump(&mut server, &mut client);
            if !client.is_handshaking() && !server.is_handshaking() {
                break;
            }
        }
        assert!(
            !client.is_handshaking() && !server.is_handshaking(),
            "in-memory TLS handshake did not complete"
        );
        (server, client)
    }

    fn wrap_server(server: TlsConnKind) -> TlsConn {
        TlsConn {
            conn: server,
            handshake_complete: true,
            peer_sent_close_notify: false,
            close_notify_sent: false,
        }
    }

    fn held_len(hold: &VecDeque<HeldRecvBuf>) -> usize {
        hold.iter()
            .map(|h| match h {
                HeldRecvBuf::Owned(b) => b.len(),
                HeldRecvBuf::Pinned { len, .. } => *len as usize,
            })
            .sum()
    }

    // A multi-record plaintext value pushed over TLS is delivered to a Segments
    // sink as one-or-more OWNED segments that reassemble byte-exactly, and the
    // provided-buffer ring is never pinned (Owned entries only).
    #[test]
    fn plaintext_routes_to_owned_segments_and_reassembles() {
        let (server, mut client) = handshaked();
        let mut tls_conn = wrap_server(server);

        // Larger than one TLS record (~16 KiB plaintext) so the drain produces
        // several chunks/segments. Non-constant pattern flags mis-ordering.
        const SIZE: usize = 40 * 1024;
        let plaintext: Vec<u8> = (0..SIZE)
            .map(|i| (i as u32).wrapping_mul(2654435761) as u8)
            .collect();
        client.writer().write_all(&plaintext).unwrap();
        let mut cipher = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut cipher).unwrap();
        }

        // Feed ciphertext + drain plaintext into the segment hold, mirroring the
        // interleave in `feed_tls_recv` (drain after each read so rustls frees
        // buffer space for the next chunk).
        let mut hold: VecDeque<HeldRecvBuf> = VecDeque::new();
        let mut cursor = Cursor::new(&cipher[..]);
        while (cursor.position() as usize) < cipher.len() {
            let n = tls_conn.conn.read_tls(&mut cursor).unwrap();
            if n == 0 {
                break;
            }
            let pt = tls_conn
                .conn
                .process_new_packets()
                .unwrap()
                .plaintext_bytes_to_read();
            if pt > 0 {
                let outstanding = held_len(&hold);
                let mut sink = PlaintextSink::Segments {
                    hold: &mut hold,
                    outstanding,
                    max: usize::MAX,
                };
                assert!(
                    drain_tls_plaintext(&mut tls_conn, &mut sink, 0),
                    "drain must succeed under an unbounded sink"
                );
            }
        }
        // Final drain for any plaintext buffered by the last packet.
        {
            let outstanding = held_len(&hold);
            let mut sink = PlaintextSink::Segments {
                hold: &mut hold,
                outstanding,
                max: usize::MAX,
            };
            assert!(drain_tls_plaintext(&mut tls_conn, &mut sink, 0));
        }

        assert!(
            !hold.is_empty(),
            "expected at least one held segment for a {SIZE}-byte value"
        );
        // TLS is copy-per-chunk: every segment must be Owned (never pins the ring).
        assert!(
            hold.iter().all(|h| matches!(h, HeldRecvBuf::Owned(_))),
            "TLS plaintext segments must all be Owned (no ring pin)"
        );
        // Reassemble in arrival order and byte-compare.
        let mut reassembled = Vec::with_capacity(SIZE);
        for h in &hold {
            if let HeldRecvBuf::Owned(b) = h {
                reassembled.extend_from_slice(b);
            }
        }
        assert_eq!(reassembled, plaintext, "segmented TLS plaintext mismatch");
    }

    // The outstanding-plaintext bound is enforced: a chunk that would push held
    // plaintext past `max` is NOT consumed and the drain returns false (the
    // caller's connection-kill signal), mirroring the accumulator `append`
    // contract.
    #[test]
    fn plaintext_over_bound_returns_false() {
        let (server, mut client) = handshaked();
        let mut tls_conn = wrap_server(server);

        // One small record of plaintext (single chunk).
        let plaintext = vec![0x5Au8; 2048];
        client.writer().write_all(&plaintext).unwrap();
        let mut cipher = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut cipher).unwrap();
        }
        let mut cursor = Cursor::new(&cipher[..]);
        while (cursor.position() as usize) < cipher.len() {
            let n = tls_conn.conn.read_tls(&mut cursor).unwrap();
            if n == 0 {
                break;
            }
            tls_conn.conn.process_new_packets().unwrap();
        }

        // max below the chunk size: the first chunk breaches the bound, is left
        // unconsumed in rustls, and drain reports the flood.
        let mut hold: VecDeque<HeldRecvBuf> = VecDeque::new();
        let mut sink = PlaintextSink::Segments {
            hold: &mut hold,
            outstanding: 0,
            max: 1024,
        };
        assert!(
            !drain_tls_plaintext(&mut tls_conn, &mut sink, 0),
            "over-limit plaintext must return false (connection-kill signal)"
        );
        assert!(
            hold.is_empty(),
            "no segment should be held once the bound is breached"
        );
    }
}
