use std::{
    io,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;

/// A struct for keeping Bytes in memory.
#[derive(Debug)]
pub(super) struct Segment {
    data: Vec<(Bytes, u64)>,
    size: u64,
    start_time: u64,
    end_time: u64,
}

// TODO: verify that unwraps for system time are fine.
impl Segment {
    /// Create a new segment with given capacity.
    #[inline]
    pub(super) fn with_capacity(capacity: u64) -> Self {
        Self {
            data: Vec::with_capacity(capacity as usize),
            size: 0,
            start_time: 0,
            end_time: 0,
        }
    }

    /// Create a new segment with given capacity, and the given `Bytes` and `timestamp` as the
    /// first element.
    #[allow(dead_code)]
    #[inline]
    pub(super) fn new(capacity: u64, byte: Bytes, timestamp: u64) -> Self {
        let size = byte.len() as u64;
        let mut data = Vec::with_capacity(capacity as usize);
        data.push((byte, timestamp));

        Self {
            data,
            size,
            start_time: timestamp,
            end_time: timestamp,
        }
    }

    /// Push a new `Bytes` in the segment.
    #[inline]
    pub(super) fn push(&mut self, byte: Bytes) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // ASSUMPTION: we don't do any inserts at 1970 according to system time.
        if self.start_time == 0 {
            self.start_time = now;
        }

        self.end_time = now;
        self.size += byte.len() as u64;
        self.data.push((byte, now));
    }

    /// Push a new element with the given timestamp.
    #[allow(dead_code)]
    #[inline]
    pub(super) fn push_with_timestamp(&mut self, byte: Bytes, timestamp: u64) {
        // ASSUMPTION: we don't do any inserts at 1970 according to system time.
        if self.start_time == 0 {
            self.start_time = timestamp;
        }

        self.end_time = timestamp;
        self.size += byte.len() as u64;
        self.data.push((byte, timestamp));
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
            Ok(self.data[index as usize].0.clone())
        }
    }

    /// Get `Bytes` and the timestamp at the given index.
    #[inline]
    pub(super) fn at_with_timestamp(&self, index: u64) -> io::Result<(Bytes, u64)> {
        if index > self.len() {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("byte at offset {} not found", index).as_str(),
            ))
        } else {
            Ok(self.data[index as usize].clone())
        }
    }

    /// Retrieve the index which either matches the given timestamp or is the immediate next one.
    #[inline]
    pub(super) fn index_from_timestamp(&self, timestamp: u64) -> u64 {
        match self.data.binary_search_by(|a| a.1.cmp(&timestamp)) {
            Ok(idx) => idx as u64,
            Err(idx) => idx as u64,
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
    pub(super) fn into_data(self) -> Vec<(Bytes, u64)> {
        self.data
    }

    /// Get the smallest timestamp of any packet in the segment.
    #[inline]
    pub(super) fn start_time(&self) -> u64 {
        self.start_time
    }

    /// Get the largest timestamp of any packet in the segment.
    #[inline]
    pub(super) fn end_time(&self) -> u64 {
        self.end_time
    }

    /// Read a range of data into `out`, doesn't add timestamp.
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
        out.extend(self.data[index as usize..limit].iter().map(|x| x.0.clone()));
        Ok(left as u64)
    }

    /// Read a range of data into `out`, along with timestamp.
    #[inline]
    pub(super) fn readv_with_timestamps(
        &self,
        index: u64,
        len: u64,
        out: &mut Vec<(Bytes, u64)>,
    ) -> io::Result<u64> {
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
