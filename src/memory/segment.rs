use std::fmt::Debug;

/// Segment of a disk. Writes go through a buffer writers to
/// reduce number of system calls. Reads are directly read from
/// the file as seek on buffer reader will dump the buffer anyway
/// Also multiple readers might be operating on a given segment
/// which makes the cursor movement very dynamic
#[derive(Debug)]
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

    /// Reads at an absolute offset
    pub fn read(&self, offset: u64) -> Option<T> {
        if offset < self.base_offset {
            return None;
        }

        let offset = offset - self.base_offset;
        match self.file.get(offset as usize) {
            Some(record) => Some(record.clone()),
            None => None,
        }
    }

    /// Reads multiple data from an offset to the end of segment
    pub fn readv(&self, offset: u64, out: &mut Vec<T>) -> usize {
        println!(
            "Sweep. Offset = {} Base offset = {}",
            offset, self.base_offset
        );

        if offset < self.base_offset {
            return 0;
        }

        if offset > self.base_offset + self.file.len() {
            return 0;
        }

        let offset = offset - self.base_offset;
        let slice = &self.file[offset as usize..];
        out.extend_from_slice(slice);
        slice.len()
    }
}
