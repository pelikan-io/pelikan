//! Direct I/O via io_uring (`O_DIRECT` + `IORING_OP_READ` / `IORING_OP_WRITE`).
//!
//! This module provides types for submitting asynchronous file I/O through
//! io_uring using `O_DIRECT`, bypassing the page cache while still going
//! through the kernel block layer. This is a more portable alternative to
//! NVMe passthrough — it works on any file or block device, not just NVMe
//! character devices.
//!
//! # Alignment requirements
//!
//! `O_DIRECT` requires that:
//! - Buffer addresses are aligned to the logical block size (typically 512 or 4096 bytes)
//! - I/O sizes are multiples of the logical block size
//! - File offsets are multiples of the logical block size
//!
//! The caller is responsible for meeting these alignment requirements.
//! Violating them results in `-EINVAL` from the kernel.
//!
//! # Kernel requirements
//!
//! - Linux 5.6+ for basic io_uring read/write
//! - Linux 6.0+ already required by ringline for `SendMsgZc`

/// Operation type for tracking what kind of I/O was submitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectIoOp {
    Read,
    Write,
    Fsync,
}

/// Opaque handle to an opened direct I/O file.
///
/// Returned by [`crate::DriverCtx::open_direct_io_file`] and used to identify the
/// file in subsequent read/write/fsync/close calls. The handle includes a
/// generation counter for stale-handle detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectIoFile {
    pub(crate) index: u16,
    pub(crate) generation: u16,
}

impl DirectIoFile {
    /// Returns the file slot index. Useful for indexing into per-file arrays.
    pub fn index(&self) -> usize {
        self.index as usize
    }
}

/// Per-file state tracked by the driver.
pub(crate) struct DirectIoFileState {
    /// Index in the io_uring fixed file table.
    pub fd_index: u32,
    /// Whether this slot is in use.
    pub active: bool,
    /// Generation counter for stale-handle detection.
    pub generation: u16,
    /// Number of commands currently in flight.
    pub in_flight: u32,
}

impl DirectIoFileState {
    pub fn new() -> Self {
        DirectIoFileState {
            fd_index: u32::MAX,
            active: false,
            generation: 0,
            in_flight: 0,
        }
    }
}

/// Tracks direct I/O file slots with allocation/release.
pub(crate) struct DirectIoFileTable {
    slots: Vec<DirectIoFileState>,
    free_list: Vec<u16>,
}

impl DirectIoFileTable {
    pub fn new(max_files: u16) -> Self {
        let mut slots = Vec::with_capacity(max_files as usize);
        let mut free_list = Vec::with_capacity(max_files as usize);
        for i in (0..max_files).rev() {
            slots.push(DirectIoFileState::new());
            free_list.push(i);
        }
        DirectIoFileTable { slots, free_list }
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

    pub fn get(&self, index: u16) -> Option<&DirectIoFileState> {
        self.slots.get(index as usize).filter(|s| s.active)
    }

    pub fn get_mut(&mut self, index: u16) -> Option<&mut DirectIoFileState> {
        self.slots.get_mut(index as usize).filter(|s| s.active)
    }
}

/// Per-command tracking entry in the command slab.
struct DirectIoCmdEntry {
    /// File slot index.
    file_index: u16,
    /// Operation type (read, write, fsync).
    op: DirectIoOp,
    /// Whether this slot is in use.
    in_use: bool,
}

/// Tracks in-flight direct I/O commands for resource cleanup on completion.
pub(crate) struct DirectIoCmdSlab {
    entries: Vec<DirectIoCmdEntry>,
    free_list: Vec<u16>,
}

impl DirectIoCmdSlab {
    pub fn new(capacity: u16) -> Self {
        let mut entries = Vec::with_capacity(capacity as usize);
        let mut free_list = Vec::with_capacity(capacity as usize);
        for i in (0..capacity).rev() {
            entries.push(DirectIoCmdEntry {
                file_index: 0,
                op: DirectIoOp::Read,
                in_use: false,
            });
            free_list.push(i);
        }
        DirectIoCmdSlab { entries, free_list }
    }

    /// Allocate a command slot. Returns the slab index.
    pub fn allocate(&mut self, file_index: u16, op: DirectIoOp) -> Option<u16> {
        let idx = self.free_list.pop()?;
        let entry = &mut self.entries[idx as usize];
        entry.file_index = file_index;
        entry.op = op;
        entry.in_use = true;
        Some(idx)
    }

    /// Release a command slot. Returns (file_index, op).
    pub fn release(&mut self, idx: u16) -> (u16, DirectIoOp) {
        let entry = &mut self.entries[idx as usize];
        let file_index = entry.file_index;
        let op = entry.op;
        entry.in_use = false;
        self.free_list.push(idx);
        (file_index, op)
    }

    pub fn in_use(&self, idx: u16) -> bool {
        self.entries.get(idx as usize).is_some_and(|e| e.in_use)
    }
}

/// Result of a direct I/O command completion.
///
/// Delivered to the direct I/O completion handler.
#[derive(Debug, Clone)]
pub struct DirectIoCompletion {
    /// The file this command was submitted to.
    pub file: DirectIoFile,
    /// The operation type (read, write, or fsync).
    pub op: DirectIoOp,
    /// Command slab sequence number (from the return value of the submit method).
    pub seq: u32,
    /// io_uring result code. For read/write: bytes transferred on success.
    /// Negative values are -errno.
    pub result: i32,
}

impl DirectIoCompletion {
    /// Whether the command completed successfully.
    pub fn is_success(&self) -> bool {
        self.result >= 0
    }

    /// Returns the number of bytes transferred, if the operation succeeded.
    pub fn bytes_transferred(&self) -> Option<u32> {
        if self.result >= 0 {
            Some(self.result as u32)
        } else {
            None
        }
    }
}

/// Configuration for direct I/O support.
///
/// When present in [`Config`](crate::config::Config), enables direct I/O file
/// management and allocates file/command tracking structures.
#[derive(Clone, Debug)]
pub struct DirectIoConfig {
    /// Maximum number of files that can be opened simultaneously.
    pub max_files: u16,
    /// Maximum I/O commands in flight across all files per worker.
    pub max_commands_in_flight: u16,
}

impl Default for DirectIoConfig {
    fn default() -> Self {
        DirectIoConfig {
            max_files: 8,
            max_commands_in_flight: 256,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_table_alloc_release() {
        let mut table = DirectIoFileTable::new(4);
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
        let mut slab = DirectIoCmdSlab::new(8);
        let a = slab.allocate(0, DirectIoOp::Read).unwrap();
        let b = slab.allocate(1, DirectIoOp::Write).unwrap();
        assert!(slab.in_use(a));
        assert!(slab.in_use(b));

        let (file, op) = slab.release(a);
        assert_eq!(file, 0);
        assert_eq!(op, DirectIoOp::Read);
        assert!(!slab.in_use(a));

        let (file, op) = slab.release(b);
        assert_eq!(file, 1);
        assert_eq!(op, DirectIoOp::Write);
    }

    #[test]
    fn cmd_slab_fsync() {
        let mut slab = DirectIoCmdSlab::new(4);
        let a = slab.allocate(2, DirectIoOp::Fsync).unwrap();
        let (file, op) = slab.release(a);
        assert_eq!(file, 2);
        assert_eq!(op, DirectIoOp::Fsync);
    }

    #[test]
    fn file_table_exhaustion() {
        let mut table = DirectIoFileTable::new(2);
        assert!(table.allocate().is_some());
        assert!(table.allocate().is_some());
        assert!(table.allocate().is_none());
    }

    #[test]
    fn completion_bytes_transferred() {
        let success = DirectIoCompletion {
            file: DirectIoFile {
                index: 0,
                generation: 0,
            },
            op: DirectIoOp::Read,
            seq: 0,
            result: 4096,
        };
        assert!(success.is_success());
        assert_eq!(success.bytes_transferred(), Some(4096));

        let failure = DirectIoCompletion {
            file: DirectIoFile {
                index: 0,
                generation: 0,
            },
            op: DirectIoOp::Write,
            seq: 1,
            result: -libc::EINVAL,
        };
        assert!(!failure.is_success());
        assert_eq!(failure.bytes_transferred(), None);
    }
}
