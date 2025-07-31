use super::*;

/// Represents storage that primarily exists in a file. This is best used in
/// combination with a DAX-aware filesystem on persistent memory to avoid page
/// cache pollution and interference. It can be used for volatile storage or
/// allow to resume from a clean shutdown.
pub struct MmapFile {
    mmap: MmapMut,
    data: Range<usize>,
    user_version: u64,
}

impl MmapFile {
    /// Open an existing `MmapFile` datapool at the given path and with the
    /// specified size (in bytes). Returns an error if the file does not exist,
    /// does not match the expected size, could not be mmap'd, or is otherwise
    /// determined to be corrupt.
    pub fn open<T: AsRef<Path>>(
        path: T,
        data_size: usize,
        user_version: u64,
    ) -> Result<Self, std::io::Error> {
        // we need the data size to be a whole number of pages
        let pages = ((HEADER_SIZE + data_size) as f64 / PAGE_SIZE as f64).ceil() as usize;

        let total_size = pages * PAGE_SIZE;

        // open an existing file for read and write access
        let file = OpenOptions::new()
            .create_new(false)
            .read(true)
            .write(true)
            .open(path)?;

        // make sure the file size matches the expected size
        if file.metadata()?.len() != total_size as u64 {
            return Err(Error::new(ErrorKind::Other, "filesize mismatch"));
        }

        // data resides after a small header
        let data = Range {
            start: HEADER_SIZE,
            end: HEADER_SIZE + data_size,
        };

        // mmap the file
        let mmap = unsafe { MmapOptions::new().populate().map_mut(&file)? };

        // load copy the header from the mmap'd file
        let mut header = [0; HEADER_SIZE];
        header.copy_from_slice(&mmap[0..HEADER_SIZE]);

        // convert the header to a struct so we can check and manipulate it
        let header = unsafe { &mut *(header.as_ptr() as *mut Header) };

        // check the header
        header.check()?;

        // check the user version
        if header.user_version() != user_version {
            return Err(Error::new(ErrorKind::Other, "user version mismatch"));
        }

        // zero out the checksum in the header copy
        header.zero_checksum();

        // create a hasher
        let mut hasher = blake3::Hasher::new();

        // hash the header with a zero'd checksum
        hasher.update(header.as_bytes());

        // calculates the hash of the data region, as a side effect this
        // prefaults all the pages
        hasher.update(&mmap[data.start..data.end]);

        // finalize the hash
        let hash = hasher.finalize();

        // compare the stored checksum in the file to the calculated checksum
        if mmap[0..32] != hash.as_bytes()[0..32] {
            return Err(Error::new(ErrorKind::Other, "checksum mismatch"));
        }

        // return the loaded datapool
        Ok(Self {
            mmap,
            data,
            user_version,
        })
    }

    /// Create a new `File` datapool at the given path and with the specified
    /// size (in bytes). Returns an error if the file already exists, could not
    /// be created, couldn't be extended to the requested size, or couldn't be
    /// mmap'd.
    pub fn create<T: AsRef<Path>>(
        path: T,
        data_size: usize,
        user_version: u64,
    ) -> Result<Self, std::io::Error> {
        // we need the data size to be a whole number of pages
        let pages = ((HEADER_SIZE + data_size) as f64 / PAGE_SIZE as f64).ceil() as usize;

        let total_size = pages * PAGE_SIZE;

        // data resides after a small header
        let data = Range {
            start: HEADER_SIZE,
            end: total_size,
        };

        // create a new file with read and write access
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;

        // grow the file to match the total size
        file.set_len(total_size as u64)?;

        // mmap the file
        let mut mmap = unsafe { MmapOptions::new().populate().map_mut(&file)? };

        // causes the mmap'd region to be prefaulted by writing a zero at the
        // start of each page
        let mut offset = 0;
        while offset < total_size {
            mmap[offset] = 0;
            offset += PAGE_SIZE;
        }
        mmap.flush()?;

        Ok(Self {
            mmap,
            data,
            user_version,
        })
    }

    pub fn header(&self) -> &Header {
        // load copy the header from the mmap'd file
        let mut header = [0; HEADER_SIZE];
        header.copy_from_slice(&self.mmap[0..HEADER_SIZE]);

        // convert the header to a struct
        unsafe { &*(header.as_ptr() as *const Header) }
    }

    pub fn time_monotonic_s(&self) -> clocksource::coarse::Instant {
        self.header().time_monotonic_s
    }

    pub fn time_monotonic_ns(&self) -> clocksource::precise::Instant {
        self.header().time_monotonic_ns
    }

    pub fn time_unix_s(&self) -> clocksource::coarse::UnixInstant {
        self.header().time_unix_s
    }

    pub fn time_unix_ns(&self) -> clocksource::precise::UnixInstant {
        self.header().time_unix_ns
    }
}

impl Datapool for MmapFile {
    fn as_slice(&self) -> &[u8] {
        &self.mmap[self.data.start..self.data.end]
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.mmap[self.data.start..self.data.end]
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        // flush everything to the underlying file
        self.mmap.flush()?;

        // initialize the hasher
        let mut hasher = blake3::Hasher::new();

        // prepare the header
        let mut header = Header::new();

        // set the user version
        header.set_user_version(self.user_version);

        // hash the header
        hasher.update(header.as_bytes());

        // calculate the number of data pages to be copied
        let data_pages = (self.mmap.len() - HEADER_SIZE) / PAGE_SIZE;

        // hash the data region
        for page in 0..data_pages {
            let start = page * PAGE_SIZE + HEADER_SIZE;
            let end = start + PAGE_SIZE;
            hasher.update(&self.mmap[start..end]);
        }

        // finalize the hash
        let hash = hasher.finalize();

        // set the header checksum with the calculated hash
        header.set_checksum(hash);

        // write the header to the file using memcpy
        // SAFETY: we know the source is exactly HEADER_SIZE and that the
        // destination is at least as large. We also know that they are both
        // properly aligned and do not overlap.
        unsafe {
            let src = header.as_bytes().as_ptr();
            let dst = self.mmap.as_mut_ptr();
            std::ptr::copy_nonoverlapping(src, dst, HEADER_SIZE);
        }

        // flush again
        self.mmap.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn mmapfile_datapool() {
        let tempdir = TempDir::new().expect("failed to generate tempdir");
        let mut path = tempdir.into_path();
        path.push("mmap_test.data");

        let magic_a = [0xDE, 0xCA, 0xFB, 0xAD];
        let magic_b = [0xBA, 0xDC, 0x0F, 0xFE, 0xEB, 0xAD, 0xCA, 0xFE];

        // create a datapool, write some content to it, and close it
        {
            let mut datapool =
                MmapFile::create(&path, 2 * PAGE_SIZE, 0).expect("failed to create pool");
            assert_eq!(datapool.len(), 2 * PAGE_SIZE);
            datapool.flush().expect("failed to flush");

            for (i, byte) in magic_a.iter().enumerate() {
                datapool.as_mut_slice()[i] = *byte;
            }
            datapool.flush().expect("failed to flush");
        }

        // open the datapool and check the content, then update it
        {
            let mut datapool =
                MmapFile::open(&path, 2 * PAGE_SIZE, 0).expect("failed to create pool");
            assert_eq!(datapool.len(), 2 * PAGE_SIZE);
            assert_eq!(datapool.as_slice()[0..4], magic_a[0..4]);
            assert_eq!(datapool.as_slice()[4..8], [0; 4]);

            for (i, byte) in magic_b.iter().enumerate() {
                datapool.as_mut_slice()[i] = *byte;
            }
            datapool.flush().expect("failed to flush");
        }

        // open the datapool again, and check that it has the new data
        {
            let datapool = MmapFile::open(&path, 2 * PAGE_SIZE, 0).expect("failed to create pool");
            assert_eq!(datapool.len(), 2 * PAGE_SIZE);
            assert_eq!(datapool.as_slice()[0..8], magic_b[0..8]);
        }

        // check that the datapool does not open if the user version is incorrect
        {
            assert!(MmapFile::open(&path, 2 * PAGE_SIZE, 1).is_err());
        }
    }
}
