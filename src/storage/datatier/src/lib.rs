// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use blake3::Hash;
use core::ops::Range;
use std::fs::{File, OpenOptions};
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::path::Path;

#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;

#[cfg(target_os = "macos")]
use std::os::unix::io::AsRawFd;

use memmap2::{MmapMut, MmapOptions};

const PAGE_SIZE: usize = 4096;
const HEADER_SIZE: usize = core::mem::size_of::<Header>();
const MAGIC: [u8; 8] = *b"PELIKAN!";

// NOTE: this must be incremented if there are breaking changes to the on-disk
// format
const VERSION: u64 = 0;

mod direct_file;
mod file_backed_memory;
mod memory;
mod mmap_file;

pub use direct_file::DirectFile;
pub use file_backed_memory::FileBackedMemory;
pub use memory::Memory;
pub use mmap_file::MmapFile;

/// The datapool trait defines the abstraction that each datapool implementation
/// should conform to.
#[allow(clippy::len_without_is_empty)]
pub trait Datapool: Send {
    /// Immutable borrow of the data within the datapool
    fn as_slice(&self) -> &[u8];

    /// Mutable borrow of the data within the datapool
    fn as_mut_slice(&mut self) -> &mut [u8];

    /// Performs any actions necessary to persist the data to the backing store.
    /// This may be a no-op for datapools which cannot persist data.
    fn flush(&mut self) -> Result<(), std::io::Error>;

    fn len(&self) -> usize {
        self.as_slice().len()
    }
}

// NOTE: make sure this is a whole number of pages and that all fields which are
// accessed are properly aligned to avoid undefined behavior.
#[repr(C, packed)]
pub struct Header {
    checksum: [u8; 32],
    magic: [u8; 8],
    version: u64,
    time_monotonic_s: clocksource::coarse::Instant,
    time_unix_s: clocksource::coarse::UnixInstant,
    time_monotonic_ns: clocksource::precise::Instant,
    time_unix_ns: clocksource::precise::UnixInstant,
    user_version: u64,
    options: u64,
    _pad: [u8; 4008],
}

impl Header {
    fn new() -> Self {
        Self {
            checksum: [0; 32],
            magic: MAGIC,
            version: VERSION,
            time_monotonic_s: clocksource::coarse::Instant::now(),
            time_unix_s: clocksource::coarse::UnixInstant::now(),
            time_monotonic_ns: clocksource::precise::Instant::now(),
            time_unix_ns: clocksource::precise::UnixInstant::now(),
            user_version: 0,
            options: 0,
            _pad: [0; 4008],
        }
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts((self as *const Header) as *const u8, HEADER_SIZE) }
    }

    fn checksum(&self) -> &[u8; 32] {
        &self.checksum
    }

    fn set_checksum(&mut self, hash: Hash) {
        for (idx, byte) in hash.as_bytes()[0..32].iter().enumerate() {
            self.checksum[idx] = *byte;
        }
    }

    fn zero_checksum(&mut self) {
        for byte in self.checksum.iter_mut() {
            *byte = 0;
        }
    }

    fn check(&self) -> Result<(), std::io::Error> {
        self.check_magic()?;
        self.check_version()
    }

    fn check_version(&self) -> Result<(), std::io::Error> {
        if self.version != VERSION {
            Err(Error::other(
                "file has incompatible version",
            ))
        } else {
            Ok(())
        }
    }

    fn check_magic(&self) -> Result<(), std::io::Error> {
        if self.magic[0..8] == MAGIC[0..8] {
            Ok(())
        } else {
            Err(Error::other("header is not recognized"))
        }
    }

    fn user_version(&self) -> u64 {
        self.user_version
    }

    fn set_user_version(&mut self, user_version: u64) {
        self.user_version = user_version;
    }

    pub fn options(&self) -> u64 {
        self.options
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_size() {
        // NOTE: make sure this is an even multiple of the page size
        assert_eq!(std::mem::size_of::<Header>(), PAGE_SIZE);
    }
}
