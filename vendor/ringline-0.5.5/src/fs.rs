//! Async filesystem I/O via io_uring.
//!
//! Provides buffered file read/write and metadata operations (stat, rename,
//! unlink, mkdir) using native io_uring opcodes — no blocking syscalls.

use std::ffi::CString;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::BytesMut;

use crate::runtime::CURRENT_TASK_ID;
use crate::runtime::io::{CURRENT_DRIVER, DiskIoFuture, with_state};

/// Opaque handle to an opened file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct File {
    pub(crate) index: u16,
    pub(crate) generation: u16,
}

impl File {
    /// Returns the file slot index.
    pub fn index(&self) -> usize {
        self.index as usize
    }
}

/// Flags for [`open()`].
#[derive(Debug, Clone, Copy)]
pub struct OpenFlags(pub(crate) i32);

impl OpenFlags {
    pub const READ: Self = Self(libc::O_RDONLY);
    pub const WRITE: Self = Self(libc::O_WRONLY);
    pub const READ_WRITE: Self = Self(libc::O_RDWR);
    pub const CREATE: Self = Self(libc::O_CREAT);
    pub const TRUNCATE: Self = Self(libc::O_TRUNC);
    pub const APPEND: Self = Self(libc::O_APPEND);
}

impl std::ops::BitOr for OpenFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// File metadata returned by [`stat()`].
#[derive(Debug, Clone)]
pub struct Metadata {
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub atime: Duration,
    pub mtime: Duration,
    pub ctime: Duration,
    pub is_file: bool,
    pub is_dir: bool,
    pub is_symlink: bool,
}

impl Metadata {
    #[cfg(target_os = "linux")]
    pub(crate) fn from_statx(stx: &libc::statx) -> Self {
        Metadata {
            size: stx.stx_size,
            mode: stx.stx_mode as u32,
            uid: stx.stx_uid,
            gid: stx.stx_gid,
            atime: Duration::new(stx.stx_atime.tv_sec as u64, stx.stx_atime.tv_nsec),
            mtime: Duration::new(stx.stx_mtime.tv_sec as u64, stx.stx_mtime.tv_nsec),
            ctime: Duration::new(stx.stx_ctime.tv_sec as u64, stx.stx_ctime.tv_nsec),
            is_file: (stx.stx_mode & libc::S_IFMT as u16) == libc::S_IFREG as u16,
            is_dir: (stx.stx_mode & libc::S_IFMT as u16) == libc::S_IFDIR as u16,
            is_symlink: (stx.stx_mode & libc::S_IFMT as u16) == libc::S_IFLNK as u16,
        }
    }

    /// Construct `Metadata` from a POSIX `libc::stat` result.
    ///
    /// Used by the mio backend's disk I/O pool for `fs_stat` on platforms
    /// that don't have `statx` (e.g., macOS).
    #[cfg(not(has_io_uring))]
    #[allow(clippy::unnecessary_cast)]
    pub(crate) fn from_stat(st: &libc::stat) -> Self {
        #[cfg(target_os = "macos")]
        let (atime, mtime, ctime) = (
            Duration::new(st.st_atime as u64, st.st_atime_nsec as u32),
            Duration::new(st.st_mtime as u64, st.st_mtime_nsec as u32),
            Duration::new(st.st_ctime as u64, st.st_ctime_nsec as u32),
        );
        #[cfg(target_os = "linux")]
        let (atime, mtime, ctime) = (
            Duration::new(st.st_atime as u64, st.st_atime_nsec as u32),
            Duration::new(st.st_mtime as u64, st.st_mtime_nsec as u32),
            Duration::new(st.st_ctime as u64, st.st_ctime_nsec as u32),
        );
        Metadata {
            size: st.st_size as u64,
            mode: st.st_mode as u32,
            uid: st.st_uid,
            gid: st.st_gid,
            atime,
            mtime,
            ctime,
            is_file: (st.st_mode & libc::S_IFMT) == libc::S_IFREG,
            is_dir: (st.st_mode & libc::S_IFMT) == libc::S_IFDIR,
            is_symlink: (st.st_mode & libc::S_IFMT) == libc::S_IFLNK,
        }
    }
}

/// Configuration for async filesystem I/O.
#[derive(Clone, Debug)]
pub struct FsConfig {
    /// Maximum number of files that can be opened simultaneously.
    pub max_files: u16,
    /// Maximum I/O commands in flight across all files per worker.
    pub max_commands_in_flight: u16,
}

impl Default for FsConfig {
    fn default() -> Self {
        FsConfig {
            max_files: 64,
            max_commands_in_flight: 256,
        }
    }
}

/// Operation type for tracking what kind of fs I/O was submitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum FsOp {
    Open,
    Read,
    Write,
    Fsync,
    Close,
    Statx,
    Rename,
    Unlink,
    Mkdir,
}

/// Per-file state tracked by the driver.
pub(crate) struct FsFileState {
    /// Index in the io_uring fixed file table.
    pub fd_index: u32,
    /// Whether this slot is in use.
    pub active: bool,
    /// Generation counter for stale-handle detection.
    pub generation: u16,
    /// Number of commands currently in flight.
    pub in_flight: u32,
}

impl FsFileState {
    pub fn new() -> Self {
        FsFileState {
            fd_index: u32::MAX,
            active: false,
            generation: 0,
            in_flight: 0,
        }
    }
}

/// Tracks filesystem file slots with allocation/release.
pub(crate) struct FsFileTable {
    slots: Vec<FsFileState>,
    free_list: Vec<u16>,
}

impl FsFileTable {
    pub fn new(max_files: u16) -> Self {
        let mut slots = Vec::with_capacity(max_files as usize);
        let mut free_list = Vec::with_capacity(max_files as usize);
        for i in (0..max_files).rev() {
            slots.push(FsFileState::new());
            free_list.push(i);
        }
        FsFileTable { slots, free_list }
    }

    pub fn allocate(&mut self) -> Option<u16> {
        let index = self.free_list.pop()?;
        let slot = &mut self.slots[index as usize];
        slot.active = true;
        Some(index)
    }

    pub fn release(&mut self, index: u16) {
        let slot = &mut self.slots[index as usize];
        slot.active = false;
        slot.fd_index = u32::MAX;
        slot.in_flight = 0;
        slot.generation = slot.generation.wrapping_add(1);
        self.free_list.push(index);
    }

    pub fn get(&self, index: u16) -> Option<&FsFileState> {
        self.slots.get(index as usize).filter(|s| s.active)
    }

    pub fn get_mut(&mut self, index: u16) -> Option<&mut FsFileState> {
        self.slots.get_mut(index as usize).filter(|s| s.active)
    }
}

/// Per-command tracking entry in the command slab.
pub(crate) struct FsCmdEntry {
    file_index: u16,
    pub(crate) op: FsOp,
    in_use: bool,
    /// Owned path for open/stat/unlink/mkdir (kept alive until CQE).
    pub path: Option<CString>,
    /// Second path for rename (kept alive until CQE).
    pub path2: Option<CString>,
    /// Heap-allocated statx buffer (kept alive until CQE).
    #[cfg(target_os = "linux")]
    pub statx_buf: Option<Box<libc::statx>>,
}

/// Tracks in-flight filesystem commands for resource cleanup on completion.
pub(crate) struct FsCmdSlab {
    entries: Vec<FsCmdEntry>,
    free_list: Vec<u16>,
}

impl FsCmdSlab {
    pub fn new(capacity: u16) -> Self {
        let mut entries = Vec::with_capacity(capacity as usize);
        let mut free_list = Vec::with_capacity(capacity as usize);
        for i in (0..capacity).rev() {
            entries.push(FsCmdEntry {
                file_index: 0,
                op: FsOp::Read,
                in_use: false,
                path: None,
                path2: None,
                #[cfg(target_os = "linux")]
                statx_buf: None,
            });
            free_list.push(i);
        }
        FsCmdSlab { entries, free_list }
    }

    /// Allocate a command slot. Returns the slab index.
    pub fn allocate(&mut self, file_index: u16, op: FsOp) -> Option<u16> {
        let idx = self.free_list.pop()?;
        let entry = &mut self.entries[idx as usize];
        entry.file_index = file_index;
        entry.op = op;
        entry.in_use = true;
        Some(idx)
    }

    /// Release a command slot. Returns (file_index, op).
    pub fn release(&mut self, idx: u16) -> (u16, FsOp) {
        let entry = &mut self.entries[idx as usize];
        let file_index = entry.file_index;
        let op = entry.op;
        entry.in_use = false;
        entry.path = None;
        entry.path2 = None;
        #[cfg(target_os = "linux")]
        {
            entry.statx_buf = None;
        }
        self.free_list.push(idx);
        (file_index, op)
    }

    pub fn in_use(&self, idx: u16) -> bool {
        self.entries.get(idx as usize).is_some_and(|e| e.in_use)
    }

    /// Get a reference to an entry (must be in use).
    pub fn get(&self, idx: u16) -> Option<&FsCmdEntry> {
        self.entries.get(idx as usize).filter(|e| e.in_use)
    }

    /// Get a mutable reference to an entry (must be in use).
    pub fn get_mut(&mut self, idx: u16) -> Option<&mut FsCmdEntry> {
        self.entries.get_mut(idx as usize).filter(|e| e.in_use)
    }
}

// ── Helper: convert Path to CString ───────────────────────────────────

pub(crate) fn path_to_cstring(path: &std::path::Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|_| io::Error::other("path contains null byte"))
}

// ── Async public API ──────────────────────────────────────────────────

/// Open a file asynchronously via io_uring.
///
/// Returns an [`OpenFuture`] that resolves to a [`File`] handle on success.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn open(
    path: impl AsRef<std::path::Path>,
    flags: OpenFlags,
    mode: u32,
) -> io::Result<OpenFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        let (file_index, generation, seq) = ctx.fs_open(path.as_ref(), flags, mode)?;
        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor.disk_io_waiters.insert(seq, task_id);
        Ok(OpenFuture {
            seq,
            file_index,
            generation,
        })
    })
}

/// Create a file (shorthand for open with CREATE | WRITE | TRUNCATE, mode 0o644).
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn create(path: impl AsRef<std::path::Path>) -> io::Result<OpenFuture> {
    open(
        path,
        OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::TRUNCATE,
        0o644,
    )
}

/// Future that resolves to a [`File`] handle when the open completes.
pub struct OpenFuture {
    seq: u32,
    file_index: u16,
    generation: u16,
}

impl Future for OpenFuture {
    type Output = io::Result<File>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<File>> {
        with_state(
            |_driver, executor| match executor.disk_io_results.remove(&self.seq) {
                Some(result) if result < 0 => {
                    Poll::Ready(Err(io::Error::from_raw_os_error(-result)))
                }
                Some(_) => Poll::Ready(Ok(File {
                    index: self.file_index,
                    generation: self.generation,
                })),
                None => {
                    let task_id = CURRENT_TASK_ID.with(|c| c.get());
                    executor.disk_io_waiters.insert(self.seq, task_id);
                    Poll::Pending
                }
            },
        )
    }
}

impl Drop for OpenFuture {
    fn drop(&mut self) {
        let opt_non_null = CURRENT_DRIVER.with(|c| c.get());
        if opt_non_null.is_none() {
            return;
        }
        let mut non_null = opt_non_null.unwrap();
        let state = unsafe { non_null.as_mut() };
        let executor = unsafe { &mut *state.executor.as_mut() };
        executor.disk_io_waiters.remove(&self.seq);
    }
}

/// Read from a file at the given offset.
///
/// Returns a [`DiskIoFuture`] whose output is the number of bytes read.
///
/// # Safety
///
/// `buf` must point to writable memory of at least `len` bytes that remains
/// valid until the returned future completes.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub unsafe fn read(file: File, offset: u64, buf: *mut u8, len: u32) -> io::Result<DiskIoFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        // Safety: the outer `read()` is already unsafe, and the caller
        // guarantees the buffer invariants.
        #[allow(unused_unsafe)]
        let seq = unsafe { ctx.fs_read(file, offset, buf, len)? };
        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor.disk_io_waiters.insert(seq, task_id);
        Ok(DiskIoFuture { seq })
    })
}

/// Write to a file at the given offset.
///
/// Returns a [`DiskIoFuture`] whose output is the number of bytes written.
///
/// # Safety
///
/// `buf` must point to readable memory of at least `len` bytes that remains
/// valid until the returned future completes.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub unsafe fn write(file: File, offset: u64, buf: *const u8, len: u32) -> io::Result<DiskIoFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        // Safety: the outer `write()` is already unsafe, and the caller
        // guarantees the buffer invariants.
        #[allow(unused_unsafe)]
        let seq = unsafe { ctx.fs_write(file, offset, buf, len)? };
        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor.disk_io_waiters.insert(seq, task_id);
        Ok(DiskIoFuture { seq })
    })
}

// ── Safe owned-buffer API ─────────────────────────────────────────────

/// Read from a file at the given offset into a [`BytesMut`].
///
/// The kernel writes into `buf`'s spare capacity (`buf.capacity() - buf.len()`).
/// On success, the returned future yields `(Ok(n), buf)` with `buf.len()` extended
/// by `n` bytes. On failure, the buffer is returned unchanged alongside the error.
///
/// If the future is dropped before the I/O completes, the buffer is parked
/// in the runtime until the kernel reports completion (a best-effort
/// `ASYNC_CANCEL` is also submitted). This avoids the use-after-free that
/// motivates the `unsafe` on [`read()`].
///
/// Returns [`io::ErrorKind::InvalidInput`] if `buf` has no spare capacity
/// or if spare capacity exceeds `u32::MAX`.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn read_into(file: File, offset: u64, mut buf: BytesMut) -> io::Result<ReadFuture> {
    let initial_len = buf.len();
    let cap = buf.capacity();
    if initial_len >= cap {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "BytesMut has no spare capacity",
        ));
    }
    let spare = cap - initial_len;
    if spare > u32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "spare capacity exceeds u32::MAX",
        ));
    }
    // Safety: `as_mut_ptr().add(initial_len)` points into the spare region of
    // the BytesMut's backing allocation. The future owns `buf` (or parks it on
    // drop), so the allocation outlives the kernel's use of the pointer.
    let ptr = unsafe { buf.as_mut_ptr().add(initial_len) };
    let len = spare as u32;
    let file_index = file.index;
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        // Safety: caller-side invariant is upheld by ReadFuture's ownership +
        // graveyard-on-drop (see ReadFuture::Drop).
        let seq = unsafe { ctx.fs_read(file, offset, ptr, len)? };
        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor.disk_io_waiters.insert(seq, task_id);
        Ok(ReadFuture {
            seq,
            file_index,
            initial_len,
            buf: Some(buf),
        })
    })
}

/// Write to a file at the given offset from a [`BytesMut`].
///
/// The kernel reads `buf.len()` bytes starting at `buf.as_ptr()`. The future
/// yields `(io::Result<usize>, buf)` — the buffer is returned unchanged.
///
/// If the future is dropped before the I/O completes, the buffer is parked
/// until the kernel reports completion (a best-effort `ASYNC_CANCEL` is also
/// submitted).
///
/// Returns [`io::ErrorKind::InvalidInput`] if `buf.len() > u32::MAX`.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn write_from(file: File, offset: u64, buf: BytesMut) -> io::Result<WriteFuture> {
    let len = buf.len();
    if len > u32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "buffer len exceeds u32::MAX",
        ));
    }
    let ptr = buf.as_ptr();
    let len_u32 = len as u32;
    let file_index = file.index;
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        // Safety: caller-side invariant is upheld by WriteFuture's ownership +
        // graveyard-on-drop (see WriteFuture::Drop).
        let seq = unsafe { ctx.fs_write(file, offset, ptr, len_u32)? };
        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor.disk_io_waiters.insert(seq, task_id);
        Ok(WriteFuture {
            seq,
            file_index,
            buf: Some(buf),
        })
    })
}

/// Future returned by [`read_into()`].
///
/// Resolves to `(io::Result<usize>, BytesMut)`. On success `buf.len()` is
/// extended by the number of bytes read.
pub struct ReadFuture {
    seq: u32,
    file_index: u16,
    initial_len: usize,
    buf: Option<BytesMut>,
}

impl Future for ReadFuture {
    type Output = (io::Result<usize>, BytesMut);

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        with_state(|_driver, executor| {
            match executor.disk_io_results.remove(&me.seq) {
                Some(result) if result < 0 => {
                    let buf = me.buf.take().expect("ReadFuture polled after completion");
                    Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buf))
                }
                Some(result) => {
                    let mut buf = me.buf.take().expect("ReadFuture polled after completion");
                    let n = result as usize;
                    let max_n = buf.capacity() - me.initial_len;
                    let n = n.min(max_n);
                    // Safety: kernel wrote `n` bytes into the spare region
                    // [initial_len, initial_len + n), initializing them.
                    unsafe { buf.set_len(me.initial_len + n) };
                    Poll::Ready((Ok(n), buf))
                }
                None => {
                    let task_id = CURRENT_TASK_ID.with(|c| c.get());
                    executor.disk_io_waiters.insert(me.seq, task_id);
                    Poll::Pending
                }
            }
        })
    }
}

impl Drop for ReadFuture {
    fn drop(&mut self) {
        let Some(buf) = self.buf.take() else { return };
        park_or_drop(self.seq, self.file_index, buf);
    }
}

/// Future returned by [`write_from()`].
///
/// Resolves to `(io::Result<usize>, BytesMut)`. The buffer is returned
/// unchanged regardless of success or failure.
pub struct WriteFuture {
    seq: u32,
    file_index: u16,
    buf: Option<BytesMut>,
}

impl Future for WriteFuture {
    type Output = (io::Result<usize>, BytesMut);

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        with_state(
            |_driver, executor| match executor.disk_io_results.remove(&me.seq) {
                Some(result) if result < 0 => {
                    let buf = me.buf.take().expect("WriteFuture polled after completion");
                    Poll::Ready((Err(io::Error::from_raw_os_error(-result)), buf))
                }
                Some(result) => {
                    let buf = me.buf.take().expect("WriteFuture polled after completion");
                    Poll::Ready((Ok(result as usize), buf))
                }
                None => {
                    let task_id = CURRENT_TASK_ID.with(|c| c.get());
                    executor.disk_io_waiters.insert(me.seq, task_id);
                    Poll::Pending
                }
            },
        )
    }
}

impl Drop for WriteFuture {
    fn drop(&mut self) {
        let Some(buf) = self.buf.take() else { return };
        park_or_drop(self.seq, self.file_index, buf);
    }
}

/// Drop-side handler shared by [`ReadFuture`] and [`WriteFuture`].
///
/// If the CQE has already arrived (result is sitting in `disk_io_results`),
/// the kernel is done with the buffer — drop it immediately. Otherwise park
/// the buffer in `disk_io_graveyard` keyed by `seq`; the fs CQE handler will
/// drop it when the op completes (with `-ECANCELED` if the cancel below
/// raced ahead, or with the natural result otherwise).
///
/// On the io_uring backend, also submit a best-effort `ASYNC_CANCEL` SQE
/// to speed up the inevitable completion. The mio backend has no equivalent
/// (the syscall is already in-flight on a worker thread); the buffer just
/// waits in the graveyard until the worker thread finishes.
fn park_or_drop(seq: u32, file_index: u16, buf: BytesMut) {
    let opt_non_null = CURRENT_DRIVER.with(|c| c.get());
    if opt_non_null.is_none() {
        // Outside the executor — leak the buffer rather than risk freeing
        // memory the kernel may still write into. This shouldn't happen in
        // practice (futures are owned by tasks pinned to a worker thread),
        // but `forget` is the safe fallback if it ever does.
        std::mem::forget(buf);
        return;
    }
    let mut non_null = opt_non_null.unwrap();
    let state = unsafe { non_null.as_mut() };
    let executor = unsafe { &mut *state.executor.as_mut() };
    executor.disk_io_waiters.remove(&seq);
    if executor.disk_io_results.remove(&seq).is_some() {
        // CQE already arrived — kernel released the buffer. Drop normally.
        drop(buf);
        let _ = file_index;
        return;
    }
    executor.disk_io_graveyard.insert(seq, buf);
    #[cfg(has_io_uring)]
    {
        let driver = unsafe { &mut *state.driver.as_mut() };
        let target = crate::completion::UserData::encode(
            crate::completion::OpTag::Fs,
            file_index as u32,
            seq,
        );
        // The cancel CQE handler is a no-op (event_loop.rs), so the
        // conn_index encoded into the cancel SQE's own user_data is
        // unused — pass 0 to satisfy `UserData::encode`'s 24-bit guard.
        let _ = driver.ring.submit_async_cancel(target.raw(), 0);
    }
}

/// Fsync a file, flushing all data and metadata to disk.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn fsync(file: File) -> io::Result<DiskIoFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        let seq = ctx.fs_fsync(file)?;
        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor.disk_io_waiters.insert(seq, task_id);
        Ok(DiskIoFuture { seq })
    })
}

/// Close a file handle.
///
/// This is synchronous — it deregisters the fd from the fixed file table
/// and releases the file slot. No SQE is needed.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn close(file: File) -> io::Result<()> {
    with_state(|driver, _| {
        let mut ctx = driver.make_ctx();
        ctx.fs_close(file)
    })
}

/// Get file metadata (stat) for a path.
///
/// Returns a [`StatFuture`] that resolves to [`Metadata`] on success.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn stat(path: impl AsRef<std::path::Path>) -> io::Result<StatFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        let seq = ctx.fs_stat(path.as_ref())?;
        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor.disk_io_waiters.insert(seq, task_id);
        Ok(StatFuture { seq })
    })
}

/// Future that resolves to [`Metadata`] when the stat completes.
///
/// The statx buffer is owned by the command slab entry. On CQE completion,
/// the handler converts it to [`Metadata`] and stores it in
/// `Executor::fs_stat_results` before releasing the slab entry.
pub struct StatFuture {
    seq: u32,
}

impl Future for StatFuture {
    type Output = io::Result<Metadata>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<Metadata>> {
        with_state(|_driver, executor| {
            // First check if there's a normal error result (negative).
            match executor.disk_io_results.remove(&self.seq) {
                Some(result) if result < 0 => {
                    // Also remove any stat result that might have been stored.
                    executor.fs_stat_results.remove(&self.seq);
                    Poll::Ready(Err(io::Error::from_raw_os_error(-result)))
                }
                Some(_) => {
                    // Success — read the Metadata from the stat results map.
                    match executor.fs_stat_results.remove(&self.seq) {
                        Some(metadata) => Poll::Ready(Ok(metadata)),
                        None => {
                            // Should not happen — handle_fs stores it before waking.
                            Poll::Ready(Err(io::Error::other("stat result missing")))
                        }
                    }
                }
                None => {
                    let task_id = CURRENT_TASK_ID.with(|c| c.get());
                    executor.disk_io_waiters.insert(self.seq, task_id);
                    Poll::Pending
                }
            }
        })
    }
}

impl Drop for StatFuture {
    fn drop(&mut self) {
        let opt_non_null = CURRENT_DRIVER.with(|c| c.get());
        if opt_non_null.is_none() {
            return;
        }
        let mut non_null = opt_non_null.unwrap();
        let state = unsafe { non_null.as_mut() };
        let executor = unsafe { &mut *state.executor.as_mut() };
        executor.disk_io_waiters.remove(&self.seq);
        executor.fs_stat_results.remove(&self.seq);
    }
}

/// Rename a file.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn rename(
    from: impl AsRef<std::path::Path>,
    to: impl AsRef<std::path::Path>,
) -> io::Result<DiskIoFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        let seq = ctx.fs_rename(from.as_ref(), to.as_ref())?;
        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor.disk_io_waiters.insert(seq, task_id);
        Ok(DiskIoFuture { seq })
    })
}

/// Remove a file (unlink).
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn remove(path: impl AsRef<std::path::Path>) -> io::Result<DiskIoFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        let seq = ctx.fs_unlink(path.as_ref())?;
        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor.disk_io_waiters.insert(seq, task_id);
        Ok(DiskIoFuture { seq })
    })
}

/// Create a directory.
///
/// # Panics
///
/// Panics if called outside the ringline async executor.
pub fn mkdir(path: impl AsRef<std::path::Path>, mode: u32) -> io::Result<DiskIoFuture> {
    with_state(|driver, executor| {
        let mut ctx = driver.make_ctx();
        let seq = ctx.fs_mkdir(path.as_ref(), mode)?;
        let task_id = CURRENT_TASK_ID.with(|c| c.get());
        executor.disk_io_waiters.insert(seq, task_id);
        Ok(DiskIoFuture { seq })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_table_alloc_release() {
        let mut table = FsFileTable::new(4);
        let a = table.allocate().unwrap();
        let b = table.allocate().unwrap();
        assert_ne!(a, b);
        assert!(table.get(a).is_some());

        table.release(a);
        assert!(table.get(a).is_none());

        // Re-allocate reuses the slot.
        let c = table.allocate().unwrap();
        assert_eq!(c, a);
        // Generation incremented.
        assert_eq!(table.get(c).unwrap().generation, 1);
    }

    #[test]
    fn cmd_slab_alloc_release() {
        let mut slab = FsCmdSlab::new(8);
        let a = slab.allocate(0, FsOp::Read).unwrap();
        let b = slab.allocate(1, FsOp::Write).unwrap();
        assert!(slab.in_use(a));
        assert!(slab.in_use(b));

        let (file, op) = slab.release(a);
        assert_eq!(file, 0);
        assert_eq!(op, FsOp::Read);
        assert!(!slab.in_use(a));

        let (file, op) = slab.release(b);
        assert_eq!(file, 1);
        assert_eq!(op, FsOp::Write);
    }

    #[test]
    fn cmd_slab_clears_owned_data_on_release() {
        let mut slab = FsCmdSlab::new(4);
        let a = slab.allocate(0, FsOp::Statx).unwrap();
        {
            let entry = slab.get_mut(a).unwrap();
            entry.path = Some(CString::new("/tmp/test").unwrap());
            #[cfg(target_os = "linux")]
            {
                entry.statx_buf = Some(Box::new(unsafe { std::mem::zeroed() }));
            }
        }
        slab.release(a);
        // After release, the entry is not in use but the path/statx_buf should be None.
        // We can verify by re-allocating and checking.
        let b = slab.allocate(0, FsOp::Read).unwrap();
        assert_eq!(b, a);
        let entry = slab.get(b).unwrap();
        assert!(entry.path.is_none());
        #[cfg(target_os = "linux")]
        assert!(entry.statx_buf.is_none());
    }

    #[test]
    fn file_table_exhaustion() {
        let mut table = FsFileTable::new(2);
        assert!(table.allocate().is_some());
        assert!(table.allocate().is_some());
        assert!(table.allocate().is_none());
    }
}
