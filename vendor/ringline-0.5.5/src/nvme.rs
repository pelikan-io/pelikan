//! NVMe io_uring passthrough types and helpers.
//!
//! This module provides the types needed to submit NVMe commands via
//! `IORING_OP_URING_CMD` (io_uring passthrough). Commands are submitted
//! on NVMe-generic character devices (`/dev/ng<X>n<Y>`), bypassing the
//! filesystem and block layer entirely.
//!
//! # Kernel requirements
//!
//! - Linux 5.19+ for `IORING_OP_URING_CMD`
//! - Linux 6.0+ already required by ringline for `SendMsgZc`
//! - NVMe device accessible via `/dev/ng*` (requires `CAP_SYS_ADMIN` or udev rules)

/// NVMe I/O command opcodes (NVM command set).
pub const NVME_CMD_FLUSH: u8 = 0x00;
pub const NVME_CMD_WRITE: u8 = 0x01;
pub const NVME_CMD_READ: u8 = 0x02;

/// NVMe uring_cmd opcode passed as `cmd_op` to `IORING_OP_URING_CMD`.
///
/// This is the kernel's `NVME_URING_CMD_IO = _IOWR('N', 0x80, struct
/// nvme_uring_cmd)` ioctl encoding: dir=RW (3) << 30 | size (72) << 16 |
/// 'N' (0x4E) << 8 | nr (0x80). The NVMe driver dispatches `uring_cmd` on
/// this value; anything else (e.g. a literal 0) returns `ENOTTY`.
pub const NVME_URING_CMD_IO: u32 = 0xC048_4E80;

/// NVMe command structure for io_uring passthrough.
///
/// This matches the kernel's `struct nvme_uring_cmd` (72 bytes).
/// It is embedded in the 80-byte command area of a Big SQE (`UringCmd80`).
///
/// # Layout
///
/// The structure maps directly to NVMe command dwords, with `addr` and
/// `data_len` specifying the data buffer for read/write commands.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NvmeUringCmd {
    /// NVMe command opcode (e.g., 0x02 for Read, 0x01 for Write).
    pub opcode: u8,
    /// Command flags.
    pub flags: u8,
    /// Reserved.
    pub rsvd1: u16,
    /// Namespace ID (usually 1).
    pub nsid: u32,
    /// Command dword 2.
    pub cdw2: u32,
    /// Command dword 3.
    pub cdw3: u32,
    /// Metadata buffer address (0 if unused).
    pub metadata: u64,
    /// Data buffer address (userspace virtual address).
    pub addr: u64,
    /// Metadata length in bytes.
    pub metadata_len: u32,
    /// Data length in bytes.
    pub data_len: u32,
    /// Command dword 10 (start LBA low 32 bits for read/write).
    pub cdw10: u32,
    /// Command dword 11 (start LBA high 32 bits for read/write).
    pub cdw11: u32,
    /// Command dword 12 (number of logical blocks - 1, plus flags).
    pub cdw12: u32,
    /// Command dword 13.
    pub cdw13: u32,
    /// Command dword 14.
    pub cdw14: u32,
    /// Command dword 15.
    pub cdw15: u32,
    /// Timeout in milliseconds (0 = default).
    pub timeout_ms: u32,
    /// Reserved.
    pub rsvd2: u32,
}

const _: () = assert!(std::mem::size_of::<NvmeUringCmd>() == 72);
const _: () = assert!(std::mem::size_of::<NvmeUringCmd>() <= 80);

impl NvmeUringCmd {
    /// Build an NVMe Read command.
    ///
    /// # Arguments
    /// - `nsid`: Namespace ID (usually 1)
    /// - `lba`: Starting logical block address
    /// - `num_blocks`: Number of blocks to read (1-based; 1 = one block)
    /// - `buf_addr`: Userspace buffer address for the read data
    /// - `buf_len`: Buffer length in bytes
    pub fn read(nsid: u32, lba: u64, num_blocks: u16, buf_addr: u64, buf_len: u32) -> Self {
        NvmeUringCmd {
            opcode: NVME_CMD_READ,
            flags: 0,
            rsvd1: 0,
            nsid,
            cdw2: 0,
            cdw3: 0,
            metadata: 0,
            addr: buf_addr,
            metadata_len: 0,
            data_len: buf_len,
            cdw10: lba as u32,         // SLBA low
            cdw11: (lba >> 32) as u32, // SLBA high
            cdw12: num_blocks.checked_sub(1).expect("num_blocks must be >= 1") as u32, // NLB (0-based)
            cdw13: 0,
            cdw14: 0,
            cdw15: 0,
            timeout_ms: 0,
            rsvd2: 0,
        }
    }

    /// Build an NVMe Write command.
    ///
    /// # Arguments
    /// - `nsid`: Namespace ID (usually 1)
    /// - `lba`: Starting logical block address
    /// - `num_blocks`: Number of blocks to write (1-based; 1 = one block)
    /// - `buf_addr`: Userspace buffer address containing write data
    /// - `buf_len`: Buffer length in bytes
    pub fn write(nsid: u32, lba: u64, num_blocks: u16, buf_addr: u64, buf_len: u32) -> Self {
        NvmeUringCmd {
            opcode: NVME_CMD_WRITE,
            flags: 0,
            rsvd1: 0,
            nsid,
            cdw2: 0,
            cdw3: 0,
            metadata: 0,
            addr: buf_addr,
            metadata_len: 0,
            data_len: buf_len,
            cdw10: lba as u32,
            cdw11: (lba >> 32) as u32,
            cdw12: num_blocks.checked_sub(1).expect("num_blocks must be >= 1") as u32,
            cdw13: 0,
            cdw14: 0,
            cdw15: 0,
            timeout_ms: 0,
            rsvd2: 0,
        }
    }

    /// Build an NVMe Flush command.
    pub fn flush(nsid: u32) -> Self {
        NvmeUringCmd {
            opcode: NVME_CMD_FLUSH,
            flags: 0,
            rsvd1: 0,
            nsid,
            cdw2: 0,
            cdw3: 0,
            metadata: 0,
            addr: 0,
            metadata_len: 0,
            data_len: 0,
            cdw10: 0,
            cdw11: 0,
            cdw12: 0,
            cdw13: 0,
            cdw14: 0,
            cdw15: 0,
            timeout_ms: 0,
            rsvd2: 0,
        }
    }

    /// Serialize to a byte array for embedding in `UringCmd80`.
    pub fn to_bytes(&self) -> [u8; 80] {
        let mut buf = [0u8; 80];
        // Safety: NvmeUringCmd is repr(C) and 72 bytes, fits in 80.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self as *const Self as *const u8,
                buf.as_mut_ptr(),
                std::mem::size_of::<Self>(),
            );
        }
        buf
    }
}

/// Opaque handle to an opened NVMe device.
///
/// Returned by [`crate::DriverCtx::open_nvme_device`] and used to identify the
/// device in subsequent read/write/close calls. The handle includes a
/// generation counter for stale-handle detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NvmeDevice {
    pub(crate) index: u16,
    pub(crate) generation: u16,
}

impl NvmeDevice {
    /// Returns the device slot index. Useful for indexing into per-device arrays.
    pub fn index(&self) -> usize {
        self.index as usize
    }
}

/// Per-device state tracked by the driver.
pub(crate) struct NvmeDeviceState {
    /// Index in the io_uring fixed file table.
    pub fd_index: u32,
    /// NVMe namespace ID (usually 1).
    pub nsid: u32,
    /// Whether this slot is in use.
    pub active: bool,
    /// Generation counter for stale-handle detection.
    pub generation: u16,
    /// Number of commands currently in flight.
    pub in_flight: u32,
}

impl NvmeDeviceState {
    pub fn new() -> Self {
        NvmeDeviceState {
            fd_index: u32::MAX,
            nsid: 1,
            active: false,
            generation: 0,
            in_flight: 0,
        }
    }
}

/// Tracks NVMe device slots with allocation/release.
pub(crate) struct NvmeDeviceTable {
    slots: Vec<NvmeDeviceState>,
    free_list: Vec<u16>,
}

impl NvmeDeviceTable {
    pub fn new(max_devices: u16) -> Self {
        let mut slots = Vec::with_capacity(max_devices as usize);
        let mut free_list = Vec::with_capacity(max_devices as usize);
        for i in (0..max_devices).rev() {
            slots.push(NvmeDeviceState::new());
            free_list.push(i);
        }
        NvmeDeviceTable { slots, free_list }
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

    pub fn get(&self, index: u16) -> Option<&NvmeDeviceState> {
        self.slots.get(index as usize).filter(|s| s.active)
    }

    pub fn get_mut(&mut self, index: u16) -> Option<&mut NvmeDeviceState> {
        self.slots.get_mut(index as usize).filter(|s| s.active)
    }
}

/// Tracks in-flight NVMe commands for resource cleanup on completion.
pub(crate) struct NvmeCmdSlab {
    entries: Vec<NvmeCmdEntry>,
    free_list: Vec<u16>,
}

/// Per-command tracking entry.
struct NvmeCmdEntry {
    /// Device slot index.
    device_index: u16,
    /// Whether this slot is in use.
    in_use: bool,
}

impl NvmeCmdSlab {
    pub fn new(capacity: u16) -> Self {
        let mut entries = Vec::with_capacity(capacity as usize);
        let mut free_list = Vec::with_capacity(capacity as usize);
        for i in (0..capacity).rev() {
            entries.push(NvmeCmdEntry {
                device_index: 0,
                in_use: false,
            });
            free_list.push(i);
        }
        NvmeCmdSlab { entries, free_list }
    }

    /// Allocate a command slot. Returns the slab index.
    pub fn allocate(&mut self, device_index: u16) -> Option<u16> {
        let idx = self.free_list.pop()?;
        let entry = &mut self.entries[idx as usize];
        entry.device_index = device_index;
        entry.in_use = true;
        Some(idx)
    }

    /// Release a command slot. Returns the device index.
    pub fn release(&mut self, idx: u16) -> u16 {
        let entry = &mut self.entries[idx as usize];
        let device_index = entry.device_index;
        entry.in_use = false;
        self.free_list.push(idx);
        device_index
    }

    #[cfg(test)]
    pub fn device_index(&self, idx: u16) -> u16 {
        self.entries[idx as usize].device_index
    }

    pub fn in_use(&self, idx: u16) -> bool {
        self.entries.get(idx as usize).is_some_and(|e| e.in_use)
    }
}

/// Result of an NVMe command completion.
///
/// Delivered to the NVMe completion handler.
#[derive(Debug, Clone)]
pub struct NvmeCompletion {
    /// The device this command was submitted to.
    pub device: NvmeDevice,
    /// Command slab sequence number (from the payload of `nvme_read`/`nvme_write`).
    pub seq: u32,
    /// io_uring result code. Negative values are -errno.
    pub result: i32,
}

impl NvmeCompletion {
    /// Whether the command completed successfully.
    pub fn is_success(&self) -> bool {
        // 0 is success. Positive values are the NVMe status word (SCT/SC,
        // e.g. 0x281 media error) placed in cqe->res by the kernel's
        // passthrough completion; negative values are transport errnos.
        self.result == 0
    }
}

/// Configuration for NVMe passthrough support.
///
/// When present in [`Config`](crate::config::Config), enables NVMe device
/// management and allocates device/command tracking structures.
#[derive(Clone, Debug)]
pub struct NvmeConfig {
    /// Maximum number of NVMe devices that can be opened simultaneously.
    pub max_devices: u16,
    /// Maximum NVMe commands in flight across all devices per worker.
    pub max_commands_in_flight: u16,
}

impl Default for NvmeConfig {
    fn default() -> Self {
        NvmeConfig {
            max_devices: 4,
            max_commands_in_flight: 256,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nvme_uring_cmd_size() {
        assert_eq!(std::mem::size_of::<NvmeUringCmd>(), 72);
    }

    #[test]
    fn nvme_uring_cmd_fits_in_80_bytes() {
        assert!(std::mem::size_of::<NvmeUringCmd>() <= 80);
    }

    #[test]
    fn read_command_fields() {
        let cmd = NvmeUringCmd::read(1, 0x1000, 8, 0xDEAD_BEEF, 4096);
        assert_eq!(cmd.opcode, NVME_CMD_READ);
        assert_eq!(cmd.nsid, 1);
        assert_eq!(cmd.cdw10, 0x1000); // LBA low
        assert_eq!(cmd.cdw11, 0); // LBA high
        assert_eq!(cmd.cdw12, 7); // NLB = num_blocks - 1
        assert_eq!(cmd.addr, 0xDEAD_BEEF);
        assert_eq!(cmd.data_len, 4096);
    }

    #[test]
    fn write_command_fields() {
        let cmd = NvmeUringCmd::write(1, 0x2000_0000_0000, 1, 0xCAFE, 512);
        assert_eq!(cmd.opcode, NVME_CMD_WRITE);
        assert_eq!(cmd.cdw10, 0); // LBA low (0x2000_0000_0000 & 0xFFFF_FFFF)
        assert_eq!(cmd.cdw11, 0x2000); // LBA high
        assert_eq!(cmd.cdw12, 0); // NLB = 0 (one block)
    }

    #[test]
    fn flush_command() {
        let cmd = NvmeUringCmd::flush(1);
        assert_eq!(cmd.opcode, NVME_CMD_FLUSH);
        assert_eq!(cmd.nsid, 1);
        assert_eq!(cmd.addr, 0);
        assert_eq!(cmd.data_len, 0);
    }

    #[test]
    fn to_bytes_roundtrip() {
        let cmd = NvmeUringCmd::read(1, 42, 1, 0x1234, 512);
        let bytes = cmd.to_bytes();
        let recovered: NvmeUringCmd =
            unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const _) };
        assert_eq!(recovered.opcode, NVME_CMD_READ);
        assert_eq!(recovered.nsid, 1);
        assert_eq!(recovered.cdw10, 42);
        assert_eq!(recovered.addr, 0x1234);
    }

    #[test]
    fn device_table_alloc_release() {
        let mut table = NvmeDeviceTable::new(4);
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
        let mut slab = NvmeCmdSlab::new(8);
        let a = slab.allocate(0).unwrap();
        let b = slab.allocate(1).unwrap();
        assert!(slab.in_use(a));
        assert!(slab.in_use(b));
        assert_eq!(slab.device_index(a), 0);
        assert_eq!(slab.device_index(b), 1);

        let dev = slab.release(a);
        assert_eq!(dev, 0);
        assert!(!slab.in_use(a));
    }

    #[test]
    #[should_panic(expected = "num_blocks must be >= 1")]
    fn read_zero_blocks_panics() {
        NvmeUringCmd::read(1, 0, 0, 0x1234, 0);
    }

    #[test]
    #[should_panic(expected = "num_blocks must be >= 1")]
    fn write_zero_blocks_panics() {
        NvmeUringCmd::write(1, 0, 0, 0x1234, 0);
    }

    #[test]
    fn read_one_block_has_nlb_zero() {
        let cmd = NvmeUringCmd::read(1, 42, 1, 0x1234, 512);
        assert_eq!(cmd.cdw12, 0, "NLB for 1 block should be 0 (0-based)");
    }

    #[test]
    fn write_max_blocks_has_correct_nlb() {
        let cmd = NvmeUringCmd::write(1, 0, u16::MAX, 0x1234, 512);
        assert_eq!(cmd.cdw12, (u16::MAX - 1) as u32);
    }
}
