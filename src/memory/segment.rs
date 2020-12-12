use std::fmt::Debug;

/// Segment of a disk. Writes go through a buffer writers to
/// reduce number of system calls. Reads are directly read from
/// the file as seek on buffer reader will dump the buffer anyway
/// Also multiple readers might be operating on a given segment
/// which makes the cursor movement very dynamic
pub struct Segment<T> {
    base_offset: u64,
    size: usize,
    pub(crate) file: Vec<T>,
}

impl<T: Debug + Clone> Segment<T> {
    pub fn new(base_offset: u64) -> Segment<T> {
        let file = Vec::with_capacity(10000);

        Segment {
            base_offset,
            file,
            size: 0,
        }
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn len(&self) -> usize {
        self.file.len()
    }

    /// Appends record to the file and return next offset
    pub fn append(&mut self, record: T, len: usize) -> u64 {
        self.file.push(record);
        self.size += len;

        // return current offset after incrementing next offset
        self.base_offset + self.file.len() as u64
    }

    /// Reads to fill the complete buffer. Returns number of bytes reamodd
    pub fn read(&mut self, offset: usize) -> Option<T> {
        match self.file.get(offset) {
            Some(record) => Some(record.clone()),
            None => None,
        }
    }

    /// Reads multiple data from an offset to the end of segment
    pub fn readv(&self, offset: usize, out: &mut Vec<T>) {
        out.extend_from_slice(&self.file[offset..])
    }
}
