use super::*;

/// Represents volatile in-memory storage.
pub struct Memory {
    mmap: MmapMut,
    size: usize,
}

impl Memory {
    pub fn create(size: usize) -> Result<Self, std::io::Error> {
        // mmap an anonymous region
        let mut mmap = MmapOptions::new().populate().len(size).map_anon()?;

        // causes the mmap'd region to be prefaulted by writing a zero at the
        // start of each page
        let mut offset = 0;
        while offset < size {
            mmap[offset] = 0;
            offset += PAGE_SIZE;
        }

        Ok(Self { mmap, size })
    }
}

impl Datapool for Memory {
    fn as_slice(&self) -> &[u8] {
        &self.mmap[..self.size]
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.mmap[..self.size]
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.mmap.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_datapool() {
        let datapool = Memory::create(2 * PAGE_SIZE).expect("failed to create pool");
        assert_eq!(datapool.len(), 2 * PAGE_SIZE);
    }
}
