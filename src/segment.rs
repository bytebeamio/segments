use std::io;

use bytes::Bytes;

/// A struct for keeping Bytes in memory.
#[derive(Debug)]
pub(super) struct Segment {
    data: Vec<Bytes>,
    size: u64,
}

impl Segment {
    /// Create a new segment with given capacity.
    #[inline]
    pub(super) fn with_capacity(capacity: u64) -> Self {
        Self {
            data: Vec::with_capacity(capacity as usize),
            size: 0,
        }
    }

    /// Push a new `Bytes` in the segment.
    #[inline]
    pub(super) fn push(&mut self, byte: Bytes) {
        self.size += byte.len() as u64;
        self.data.push(byte);
    }

    /// Get `Bytes` at the given index.
    #[inline]
    pub(super) fn at(&self, index: u64) -> io::Result<Bytes> {
        if index > self.len() {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("byte at offset {} not found", index).as_str(),
            ))
        } else {
            Ok(self.data[index as usize].clone())
        }
    }

    /// Get the number of `Bytes` in the segment.
    #[inline]
    pub(super) fn len(&self) -> u64 {
        self.data.len() as u64
    }

    /// Get the total size in bytes of the segment.
    #[inline]
    pub(super) fn size(&self) -> u64 {
        self.size
    }

    /// Convert the segment into `Vec<Bytes>`, consuming `self`.
    #[inline]
    pub(super) fn into_data(self) -> Vec<Bytes> {
        self.data
    }

    /// Read a range of data into `out`.
    #[inline]
    pub(super) fn readv(&self, index: u64, len: u64, out: &mut Vec<Bytes>) -> io::Result<u64> {
        if index >= self.len() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("byte at offset {} not found", index).as_str(),
            ));
        }

        let mut limit = (index + len) as usize;
        let mut left = 0;
        if limit > self.data.len() {
            left = limit - self.data.len();
            limit = self.data.len();
        }
        out.extend_from_slice(&self.data[index as usize..limit]);
        Ok(left as u64)
    }
}
