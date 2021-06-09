use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom},
    path::Path,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::{debug, info, warn};

/// Size of the offset of packet, in bytes.
pub const OFFSET_SIZE: u64 = 8;
/// Size of the len of packet, in bytes.
pub const LEN_SIZE: u64 = 8;
/// Size of entry, in bytes.
pub const ENTRY_SIZE: u64 = OFFSET_SIZE + LEN_SIZE;

/// Wrapper around a index file for convenient reading of bytes sizes.
///
/// Does **not** check any of the constraint enforced by user, or that the index being read from/
/// written to is valid. Simply performs what asked.
///
/// #### Note
/// It is the duty of the handler of this struct to ensure index file's size does not exceed the
/// specified limit.
pub(super) struct Index {
    /// The opened index file.
    file: File,
    /// Index at which next call to [`Index::append`] will append to.
    tail: u64,
    /// The last entry that was appended.
    last_entry: (u64, u64)
}

impl Index {
    /// Open/create a new index file.
    #[inline]
    pub(super) fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        // TODO: maybe memory map files?

        let file = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(path)?;
        let tail = file.metadata()?.len() / ENTRY_SIZE;
        let mut index = Self { file, tail, last_entry: (0, 0) };

        if tail == 0 {
            Ok(index)
        } else {
            let [offset, len] = index.read(tail - 1)?;
            index.last_entry = (offset, len);
            Ok(index)
        }

    }

    /// Return the index at which next call to [`Index::append`] will append to.
    #[inline(always)]

    pub(super) fn append_index(&self) -> u64 {
        self.tail
    }

    /// Get the size of packet at the given index, using the index file.
    #[inline]
    pub(super) fn read(&mut self, index: u64) -> io::Result<[u64; 2]> {
        self.file.seek(SeekFrom::Start(index * ENTRY_SIZE))?;
        // SAFETY: if it is safe to read 2 u64s one after the another, then it is also safe to read
        // a single u128 in one go and parse it as [u64; 2]. Not using tuples as they don't have
        // any guarantee about their layout.
        // See: https://doc.rust-lang.org/reference/type-layout.html#array-layout
        //      https://doc.rust-lang.org/reference/type-layout.html#tuple-layout
        Ok(unsafe { std::mem::transmute(self.file.read_u128::<BigEndian>()?) })
    }

    /// Get the sizes of packets, starting from the given index upto the given lenght
    #[inline]
    pub(super) fn readv(&mut self, index: u64, len: u64) -> io::Result<Vec<[u64; 2]>> {
        let len = len as usize;
        let mut buf = Vec::with_capacity(len);
        self.file.seek(SeekFrom::Start(index * ENTRY_SIZE))?;

        // SAFETY: We have already preallocated the capacity, and
        //         ENTRY_SIZE = 16 = size of [u64; 2] in bytes
        unsafe {
            self.file
                .read_exact(std::mem::transmute(std::slice::from_raw_parts_mut(
                    buf.as_mut_ptr(),
                    len * ENTRY_SIZE as usize,
                )))?;
            buf.set_len(len);
        }

        Ok(buf)
    }

    /// Append a new value to the index file.
    #[inline]
    pub(super) fn append(&mut self, value: u64) -> io::Result<()> {
        let offset = self.last_entry.0 + self.last_entry.1;
        self.file.seek(SeekFrom::End(0))?;
        self.tail += 1;
        self.last_entry = (offset, value);
        // SAFETY: equivalent to writing u64 twice.
        self.file
            .write_u128::<BigEndian>(unsafe { std::mem::transmute([offset, value]) })
    }
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn append_and_read() {
        let dir = tempdir().unwrap();
        let mut index = Index::new(dir.path().join(&format!("{:020}", 1))).unwrap();
        assert_eq!(index.append_index(), 0);

        // Adding 10 len entries of 100 size each. results in:
        //  - tail = 10
        //  - offset[tail - 1] = 900
        //  - len[tail - 1] = 100
        for _ in 0..10 {
            index.append(100).unwrap();
        }
        assert_eq!(index.append_index(), 10);
        assert_eq!(index.read(9).unwrap(), [900, 100]);

        // Adding 10 len entries of 200 size each. results in:
        //  - tail = 20
        //  - offset[tail - 1] = 2800
        //  - len[tail - 1] = 200
        for _ in 0..10 {
            index.append(200).unwrap();
        }
        assert_eq!(index.append_index(), 20);
        assert_eq!(index.read(19).unwrap(), [2800, 200]);
    }

    #[test]
    fn append_and_read_after_saving_on_disk() {
        let dir = tempdir().unwrap();
        let mut index = Index::new(dir.path().join(&format!("{:020}", 1))).unwrap();
        assert_eq!(index.append_index(), 0);

        // Adding 10 len entries of 100 size each. results in:
        //  - tail = 10
        //  - offset[tail - 1] = 900
        //  - len[tail - 1] = 100
        for _ in 0..10 {
            index.append(100).unwrap();
        }
        assert_eq!(index.append_index(), 10);

        // Adding 10 len entries of 200 size each. results in:
        //  - tail = 20
        //  - offset[tail - 1] = 2800
        //  - len[tail - 1] = 200
        for _ in 0..10 {
            index.append(200).unwrap();
        }
        assert_eq!(index.append_index(), 20);

        drop(index);
        let mut index = Index::new(dir.path().join(&format!("{:020}", 1))).unwrap();

        assert_eq!(index.read(9).unwrap(), [900, 100]);
        assert_eq!(index.read(19).unwrap(), [2800, 200]);
    }
}
