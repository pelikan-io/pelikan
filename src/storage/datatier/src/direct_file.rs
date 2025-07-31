use super::*;

/// A wrapper around std::fs::File that attempts to use O_DIRECT on Linux
/// and F_NOCACHE on macOS to bypass the page cache. Falls back to regular
/// file I/O if direct I/O is not supported.
pub struct DirectFile {
    file: File,
    direct_io: bool,
}

impl DirectFile {
    /// Opens an existing file with direct I/O if supported
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let path = path.as_ref();

        // Try O_DIRECT on Linux
        #[cfg(target_os = "linux")]
        {
            match OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .open(path)
            {
                Ok(file) => {
                    return Ok(Self {
                        file,
                        direct_io: true,
                    })
                }
                Err(_) => {
                    // Fall back to regular I/O
                }
            }
        }

        // Open normally
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        // Apply F_NOCACHE on macOS
        #[cfg(target_os = "macos")]
        {
            let fd = file.as_raw_fd();
            unsafe {
                libc::fcntl(fd, libc::F_NOCACHE, 1);
            }
        }

        Ok(Self {
            file,
            direct_io: false,
        })
    }

    /// Creates a new file with direct I/O if supported
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let path = path.as_ref();

        // Try O_DIRECT on Linux
        #[cfg(target_os = "linux")]
        {
            match OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .open(path)
            {
                Ok(file) => {
                    return Ok(Self {
                        file,
                        direct_io: true,
                    })
                }
                Err(_) => {
                    // Fall back to regular I/O
                }
            }
        }

        // Create normally
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;

        // Apply F_NOCACHE on macOS
        #[cfg(target_os = "macos")]
        {
            let fd = file.as_raw_fd();
            unsafe {
                libc::fcntl(fd, libc::F_NOCACHE, 1);
            }
        }

        Ok(Self {
            file,
            direct_io: false,
        })
    }

    /// Returns true if direct I/O is enabled
    pub fn is_direct(&self) -> bool {
        self.direct_io
    }

    /// Get a reference to the underlying file
    pub fn file(&self) -> &File {
        &self.file
    }

    /// Get a mutable reference to the underlying file
    pub fn file_mut(&mut self) -> &mut File {
        &mut self.file
    }
}

impl Read for DirectFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl Write for DirectFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl Seek for DirectFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use tempfile::TempDir;

    #[test]
    fn direct_file() {
        let tempdir = TempDir::new().expect("failed to generate tempdir");
        let mut path = tempdir.into_path();
        path.push("direct_test.data");

        // Test create
        {
            let mut file = DirectFile::create(&path).expect("failed to create direct file");
            assert!(file.is_direct() || !cfg!(any(target_os = "linux", target_os = "macos")));

            // Write some data
            file.write_all(b"Hello, Direct I/O!")
                .expect("failed to write");
            file.flush().expect("failed to flush");
        }

        // Test open
        {
            let mut file = DirectFile::open(&path).expect("failed to open direct file");
            assert!(file.is_direct() || !cfg!(any(target_os = "linux", target_os = "macos")));

            // Read the data back
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).expect("failed to read");
            assert_eq!(buffer, b"Hello, Direct I/O!");
        }
    }
}
