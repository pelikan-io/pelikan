use super::*;

/// Storage that is primarily in-memory, but has an associated file which backs
/// it onto more durable storage media. This allows us to use DRAM to provide
/// fast access to the storage region but with the ability to save to and
/// restore from some file. It is recommended this file be kept on a fast local
/// disk (eg: NVMe), but it is not strictly required. Unlike simply using mmap
/// on the file, this ensures all the data is kept resident in-memory.
///
/// Uses `DirectFile` for on-disk storage to attempt to bypass pagecache when
/// reading from / writing to disk.
pub struct FileBackedMemory {
    memory: Memory,
    header: Box<[u8]>,
    file: DirectFile,
    file_data: Range<usize>,
    user_version: u64,
}

impl FileBackedMemory {
    pub fn open<T: AsRef<Path>>(
        path: T,
        data_size: usize,
        user_version: u64,
    ) -> Result<Self, std::io::Error> {
        // we need the data size to be a whole number of pages for direct io
        let pages = ((HEADER_SIZE + data_size) as f64 / PAGE_SIZE as f64).ceil() as usize;

        // total size must be larger than the requested size to allow for the
        // header
        let file_total_size = Range {
            start: 0,
            end: pages * PAGE_SIZE,
        };

        // data resides after a small header
        let file_data = Range {
            start: HEADER_SIZE,
            end: HEADER_SIZE + data_size,
        };

        // open file with direct I/O if supported
        let mut file = DirectFile::open(path)?;

        // make sure the file size matches the expected size
        if file.file().metadata()?.len() != file_total_size.end as u64 {
            return Err(Error::other("filesize mismatch"));
        }

        // calculate the page range for the data region
        let data_pages = (file_data.end - file_data.start) / PAGE_SIZE;

        // reserve memory for the data
        let mut memory = Memory::create(data_size)?;

        // seek to start of header
        file.seek(SeekFrom::Start(0))?;

        // prepare the header to read from disk
        let mut header = [0; HEADER_SIZE];

        // read the header from disk
        loop {
            if file.read(&mut header[0..PAGE_SIZE])? == PAGE_SIZE {
                break;
            }
            file.seek(SeekFrom::Start(0))?;
        }

        // create a new hasher to checksum the file content, including the
        // header with a zero'd checksum
        let mut hasher = blake3::Hasher::new();

        // turn the raw header into the struct
        let header = unsafe { &mut *(header.as_ptr() as *mut Header) };

        // check the header
        header.check()?;

        // check the user version
        if header.user_version() != user_version {
            return Err(Error::other("user version mismatch"));
        }

        // copy the checksum out of the header and zero it in the header
        let file_checksum = header.checksum().to_owned();
        header.zero_checksum();

        // hash the header with the zero'd checksum
        hasher.update(header.as_bytes());

        // seek to start of the data
        file.seek(SeekFrom::Start(file_data.start as u64))?;

        // read the data region from the file, copy it into memory and hash it
        // in a single pass
        for page in 0..data_pages {
            // retry the read until a complete page is read
            loop {
                let start = page * PAGE_SIZE;
                let end = start + PAGE_SIZE;

                if file.read(&mut memory.as_mut_slice()[start..end])? == PAGE_SIZE {
                    hasher.update(&memory.as_slice()[start..end]);
                    break;
                }
                // if the read was incomplete, we seek back to the right spot in
                // the file
                file.seek(SeekFrom::Start((HEADER_SIZE + start) as u64))?;
            }
        }

        // finalize the hash
        let hash = hasher.finalize();

        // compare the checksum agaianst what's in the header
        if file_checksum[0..32] != hash.as_bytes()[0..32] {
            return Err(Error::other("checksum mismatch"));
        }

        // return the loaded datapool
        Ok(Self {
            memory,
            header: header.as_bytes().to_owned().into_boxed_slice(),
            file,
            file_data,
            user_version,
        })
    }

    pub fn create<T: AsRef<Path>>(
        path: T,
        data_size: usize,
        user_version: u64,
    ) -> Result<Self, std::io::Error> {
        // we need the data size to be a whole number of pages for direct io
        let pages = ((HEADER_SIZE + data_size) as f64 / PAGE_SIZE as f64).ceil() as usize;

        // total size must be larger than the requested size to allow for the
        // header
        let file_total_size = Range {
            start: 0,
            end: pages * PAGE_SIZE,
        };

        // data resides after a small header
        let file_data = Range {
            start: HEADER_SIZE,
            end: pages * PAGE_SIZE,
        };

        // create a new file with direct I/O if supported
        let mut file = DirectFile::create(path)?;

        // grow the file to match the total size
        file.file_mut().set_len(file_total_size.end as u64)?;

        // causes file to be zeroed out
        let zero_page = vec![0u8; PAGE_SIZE];
        for page in 0..pages {
            loop {
                if file.write(&zero_page)? == PAGE_SIZE {
                    break;
                }
                file.seek(SeekFrom::Start((page * PAGE_SIZE) as u64))?;
            }
        }
        file.file_mut().sync_all()?;

        let memory = Memory::create(data_size)?;

        Ok(Self {
            memory,
            header: vec![0; HEADER_SIZE].into_boxed_slice(),
            file,
            file_data,
            user_version,
        })
    }

    pub fn header(&self) -> &Header {
        unsafe { &*(self.header.as_ptr() as *const Header) }
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

impl Datapool for FileBackedMemory {
    fn as_slice(&self) -> &[u8] {
        self.memory.as_slice()
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.memory.as_mut_slice()
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        // initialize the hasher
        let mut hasher = blake3::Hasher::new();

        // prepare the header
        let mut header = Header::new();

        // set the user version
        header.set_user_version(self.user_version);

        // hash the header with a zero'd checksum
        hasher.update(header.as_bytes());

        // calculate the number of data pages to be copied
        let data_pages = (self.file_data.end - self.file_data.start) / PAGE_SIZE;

        // write the data region to the file and hash it in one pass
        self.file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
        for page in 0..data_pages {
            loop {
                let start = page * PAGE_SIZE;
                let end = start + PAGE_SIZE;
                if self.file.write(&self.memory.as_slice()[start..end])? == PAGE_SIZE {
                    hasher.update(&self.memory.as_slice()[start..end]);
                    break;
                }
                self.file
                    .seek(SeekFrom::Start((HEADER_SIZE + start) as u64))?;
            }
        }

        // finalize the hash
        let hash = hasher.finalize();

        // set the checksum in the header to the calculated hash
        header.set_checksum(hash);

        // write the header to the file
        self.file.seek(SeekFrom::Start(0))?;
        loop {
            if self.file.write(header.as_bytes())? == HEADER_SIZE {
                break;
            }
            self.file.seek(SeekFrom::Start(0))?;
        }

        self.file.file_mut().sync_all()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn filebackedmemory_datapool() {
        let tempdir = TempDir::new().expect("failed to generate tempdir");
        let mut path = tempdir.into_path();
        path.push("filebacked_test.data");

        let magic_a = [0xDE, 0xCA, 0xFB, 0xAD];
        let magic_b = [0xBA, 0xDC, 0x0F, 0xFE, 0xEB, 0xAD, 0xCA, 0xFE];

        // create a datapool, write some content to it, and close it
        {
            let mut datapool =
                FileBackedMemory::create(&path, 2 * PAGE_SIZE, 0).expect("failed to create pool");
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
                FileBackedMemory::open(&path, 2 * PAGE_SIZE, 0).expect("failed to open pool");
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
            let datapool =
                FileBackedMemory::open(&path, 2 * PAGE_SIZE, 0).expect("failed to create pool");
            assert_eq!(datapool.len(), 2 * PAGE_SIZE);
            assert_eq!(datapool.as_slice()[0..8], magic_b[0..8]);
        }

        // check that the datapool does not open if the user version is incorrect
        {
            assert!(FileBackedMemory::open(&path, 2 * PAGE_SIZE, 1).is_err());
        }
    }
}
