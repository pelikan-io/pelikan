use super::*;

/// A wrapper around std::fs::File that attempts to use O_DIRECT on Linux
/// and F_NOCACHE on macOS to bypass the page cache. Falls back to regular
/// file I/O if direct I/O is not supported.
/// 
/// IMPORTANT: O_DIRECT has strict requirements:
/// - Buffer memory must be aligned (typically 512 bytes)
/// - I/O size must be a multiple of the block size
/// - File offset must be aligned
/// 
/// This implementation does NOT support read-modify-write for partial blocks.
/// If you attempt to write data that isn't block-aligned in size, it will
/// automatically fall back to buffered I/O. This is suitable for use cases
/// like datatier where we're typically writing full pages.
pub struct DirectFile {
    file: File,
    direct_io: bool,
    // Reusable aligned buffer for O_DIRECT operations
    aligned_buffer: Option<AlignedBuffer>,
}

// Alignment requirement for O_DIRECT
// Use 4KB which is safe for all modern systems
const ALIGNMENT: usize = 4096;

// A 4KB-aligned buffer for O_DIRECT operations
#[repr(align(4096))]
struct AlignedBuffer {
    data: [u8; ALIGNMENT],
}

impl AlignedBuffer {
    fn new() -> Self {
        let buffer = Self {
            data: [0; ALIGNMENT],
        };
        // Verify alignment at runtime in debug builds
        debug_assert_eq!(buffer.data.as_ptr() as usize % ALIGNMENT, 0, "Buffer not properly aligned");
        buffer
    }
    
    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl DirectFile {
    /// Get the current file position
    fn current_position(&mut self) -> std::io::Result<u64> {
        self.file.stream_position()
    }

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
                        aligned_buffer: Some(AlignedBuffer::new()),
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::InvalidInput => {
                    // O_DIRECT not supported, fall back to regular I/O
                    let file = OpenOptions::new().read(true).write(true).open(path)?;

                    return Ok(Self {
                        file,
                        direct_io: false,
                        aligned_buffer: None,
                    });
                }
                Err(e) => {
                    // Some other error occurred, propagate it
                    return Err(e);
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            // Open normally
            let file = OpenOptions::new().read(true).write(true).open(path)?;

            #[allow(unused_assignments)]
            let mut direct_io = false;

            // Apply F_NOCACHE on macOS
            #[cfg(target_os = "macos")]
            {
                let fd = file.as_raw_fd();
                direct_io = unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) } != -1;
            }

            Ok(Self { 
                file, 
                direct_io,
                aligned_buffer: if direct_io { Some(AlignedBuffer::new()) } else { None },
            })
        }
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
                        aligned_buffer: Some(AlignedBuffer::new()),
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::InvalidInput => {
                    // O_DIRECT not supported, fall back to regular I/O
                    let file = OpenOptions::new()
                        .create_new(true)
                        .read(true)
                        .write(true)
                        .open(path)?;

                    return Ok(Self {
                        file,
                        direct_io: false,
                        aligned_buffer: None,
                    });
                }
                Err(e) => {
                    // Some other error occurred, propagate it
                    return Err(e);
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            // Create normally
            let file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(path)?;

            #[allow(unused_assignments)]
            let mut direct_io = false;

            // Apply F_NOCACHE on macOS
            #[cfg(target_os = "macos")]
            {
                let fd = file.as_raw_fd();
                direct_io = unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) } != -1;
            }

            Ok(Self { 
                file, 
                direct_io,
                aligned_buffer: if direct_io { Some(AlignedBuffer::new()) } else { None },
            })
        }
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
    
    // Write a buffer with proper alignment handling for O_DIRECT
    // This handles read-modify-write for partial blocks when necessary
    fn write_aligned(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if !self.direct_io || data.is_empty() {
            // Regular write for non-O_DIRECT cases
            return self.file.write(data);
        }
        
        // Get current file position
        let file_pos = self.current_position()?;
        
        // Calculate aligned boundaries
        let start_offset = (file_pos % ALIGNMENT as u64) as usize;
        
        let buffer = self.aligned_buffer.as_mut()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "No aligned buffer available"))?;
        let aligned_buffer = buffer.as_mut_slice();
        
        let mut bytes_written = 0;
        let mut data_offset = 0;
        
        // Handle first partial block if needed
        if start_offset != 0 {
            // Read the existing block
            let block_start = file_pos - start_offset as u64;
            self.file.seek(SeekFrom::Start(block_start))?;
            
            let mut bytes_read = 0;
            while bytes_read < ALIGNMENT {
                match self.file.read(&mut aligned_buffer[bytes_read..]) {
                    Ok(0) => break, // EOF
                    Ok(n) => bytes_read += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            }
            
            // Zero out any unread bytes
            if bytes_read < ALIGNMENT {
                aligned_buffer[bytes_read..].fill(0);
            }
            
            // Copy user data into buffer
            let bytes_in_first_block = (ALIGNMENT - start_offset).min(data.len());
            aligned_buffer[start_offset..start_offset + bytes_in_first_block]
                .copy_from_slice(&data[..bytes_in_first_block]);
            
            // Write the block back
            self.file.seek(SeekFrom::Start(block_start))?;
            self.file.write_all(aligned_buffer)?;
            
            bytes_written += bytes_in_first_block;
            data_offset += bytes_in_first_block;
        }
        
        // Handle complete middle blocks with cut-through writes
        while data_offset + ALIGNMENT <= data.len() {
            // For complete aligned blocks, write directly from user buffer
            self.file.write_all(&data[data_offset..data_offset + ALIGNMENT])?;
            bytes_written += ALIGNMENT;
            data_offset += ALIGNMENT;
        }
        
        // Handle last partial block if needed
        if data_offset < data.len() {
            let remaining = data.len() - data_offset;
            let block_start = file_pos + data_offset as u64;
            
            // Read existing block
            self.file.seek(SeekFrom::Start(block_start))?;
            let mut bytes_read = 0;
            while bytes_read < ALIGNMENT {
                match self.file.read(&mut aligned_buffer[bytes_read..]) {
                    Ok(0) => break, // EOF
                    Ok(n) => bytes_read += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            }
            
            // Zero out any unread bytes
            if bytes_read < ALIGNMENT {
                aligned_buffer[bytes_read..].fill(0);
            }
            
            // Copy remaining user data
            aligned_buffer[..remaining].copy_from_slice(&data[data_offset..]);
            
            // Write the block back
            self.file.seek(SeekFrom::Start(block_start))?;
            self.file.write_all(aligned_buffer)?;
            
            bytes_written += remaining;
        }
        
        // Restore file position
        self.file.seek(SeekFrom::Start(file_pos + bytes_written as u64))?;
        
        Ok(bytes_written)
    }
    
    // Read into a buffer with proper alignment handling for O_DIRECT
    fn read_aligned(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if !self.direct_io || buf.is_empty() {
            // Regular read for non-O_DIRECT cases
            return self.file.read(buf);
        }
        
        // Get current file position
        let file_pos = self.current_position()?;
        
        // Calculate aligned boundaries
        let start_offset = (file_pos % ALIGNMENT as u64) as usize;
        
        let buffer = self.aligned_buffer.as_mut()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "No aligned buffer available"))?;
        let aligned_buffer = buffer.as_mut_slice();
        
        let mut total_read = 0;
        let mut buf_offset = 0;
        
        // If we're not at a block boundary, handle the first partial block
        if start_offset != 0 {
            // Read the block containing our starting position
            let block_start = file_pos - start_offset as u64;
            self.file.seek(SeekFrom::Start(block_start))?;
            
            let mut bytes_read = 0;
            while bytes_read < ALIGNMENT {
                match self.file.read(&mut aligned_buffer[bytes_read..]) {
                    Ok(0) => break, // EOF
                    Ok(n) => bytes_read += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            }
            
            if bytes_read > start_offset {
                // Copy available data from the block to user buffer
                let available = bytes_read - start_offset;
                let to_copy = available.min(buf.len());
                buf[..to_copy].copy_from_slice(&aligned_buffer[start_offset..start_offset + to_copy]);
                total_read += to_copy;
                buf_offset += to_copy;
                
                if to_copy < available || buf_offset >= buf.len() {
                    // We've read enough or filled the buffer
                    self.file.seek(SeekFrom::Start(file_pos + total_read as u64))?;
                    return Ok(total_read);
                }
            } else {
                // EOF before our start offset
                return Ok(0);
            }
        }
        
        // Read complete blocks directly into user buffer where possible
        while buf_offset + ALIGNMENT <= buf.len() {
            let mut bytes_read = 0;
            while bytes_read < ALIGNMENT {
                match self.file.read(&mut aligned_buffer[bytes_read..]) {
                    Ok(0) => {
                        // EOF
                        if total_read > 0 {
                            self.file.seek(SeekFrom::Start(file_pos + total_read as u64))?;
                        }
                        return Ok(total_read);
                    }
                    Ok(n) => bytes_read += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            }
            
            // Copy the full block to user buffer
            buf[buf_offset..buf_offset + ALIGNMENT].copy_from_slice(aligned_buffer);
            total_read += ALIGNMENT;
            buf_offset += ALIGNMENT;
        }
        
        // Handle last partial block if needed
        if buf_offset < buf.len() {
            let mut bytes_read = 0;
            while bytes_read < ALIGNMENT {
                match self.file.read(&mut aligned_buffer[bytes_read..]) {
                    Ok(0) => break, // EOF
                    Ok(n) => bytes_read += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            }
            
            if bytes_read > 0 {
                let to_copy = bytes_read.min(buf.len() - buf_offset);
                buf[buf_offset..buf_offset + to_copy].copy_from_slice(&aligned_buffer[..to_copy]);
                total_read += to_copy;
            }
        }
        
        // Update file position
        if total_read > 0 {
            self.file.seek(SeekFrom::Start(file_pos + total_read as u64))?;
        }
        
        Ok(total_read)
    }
}

impl Read for DirectFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.read_aligned(buf)
    }
}

impl Write for DirectFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_aligned(buf)
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
    fn direct_file_partial_writes() {
        let tempdir = TempDir::new().expect("failed to generate tempdir");
        let mut path = tempdir.into_path();
        path.push("direct_partial_test.data");

        // Test small, unaligned writes
        {
            let mut file = DirectFile::create(&path).expect("failed to create direct file");
            
            // Write some small chunks
            file.write_all(b"Hello").expect("failed to write Hello");
            file.write_all(b", ").expect("failed to write comma");
            file.write_all(b"World!").expect("failed to write World");
            file.flush().expect("failed to flush");
        }

        // Read back and verify
        {
            let mut file = DirectFile::open(&path).expect("failed to open direct file");
            let mut buffer = vec![0u8; 13];
            file.read_exact(&mut buffer).expect("failed to read");
            assert_eq!(&buffer, b"Hello, World!");
            
            // Test reading at non-aligned positions
            file.seek(SeekFrom::Start(6)).expect("failed to seek");
            let mut buffer = vec![0u8; 6];
            file.read_exact(&mut buffer).expect("failed to read");
            assert_eq!(&buffer, b" World");
        }
    }

    #[test]
    fn direct_file() {
        let tempdir = TempDir::new().expect("failed to generate tempdir");
        let mut path = tempdir.into_path();
        path.push("direct_test.data");

        // Test with aligned buffer sizes for O_DIRECT compatibility
        const BLOCK_SIZE: usize = 4096;
        let mut write_buffer = vec![0u8; BLOCK_SIZE];
        let message = b"Hello, Direct I/O!";
        write_buffer[..message.len()].copy_from_slice(message);
        
        // Test create
        {
            let mut file = DirectFile::create(&path).expect("failed to create direct file");
            
            // Write a full block (DirectFile handles alignment internally)
            file.write_all(&write_buffer).expect("failed to write");
            file.flush().expect("failed to flush");
        }

        // Test open
        {
            let mut file = DirectFile::open(&path).expect("failed to open direct file");
            
            // Read back the data
            let mut read_buffer = vec![0u8; BLOCK_SIZE];
            file.read_exact(&mut read_buffer).expect("failed to read");
            assert_eq!(&read_buffer[..message.len()], message);
        }
    }
}
