use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom},
    path::Path,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

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
pub(super) struct Index(File);

impl Index {
    /// Open/create a new index file.
    #[inline]
    pub(super) fn new<P: AsRef<Path>>(path: P) -> io::Result<(Self, u64)> {
        let file = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(path)?;
        let tail = file.metadata()?.len() / ENTRY_SIZE;
        Ok((Self(file), tail))
    }

    /// Get the size of packet at the given index, using the index file.
    #[inline]
    pub(super) fn read(&mut self, index: u64) -> io::Result<[u64; 2]> {
        self.0.seek(SeekFrom::Start(index * ENTRY_SIZE))?;
        // SAFETY: if it is safe to read 2 u64s one after the another, then it is also safe to read
        // a single u128 in one go and parse it as [u64; 2]. Not using tuples as they don't have
        // any guarantee about their layout.
        // See: https://doc.rust-lang.org/reference/type-layout.html#array-layout
        //      https://doc.rust-lang.org/reference/type-layout.html#tuple-layout
        Ok(unsafe { std::mem::transmute(self.0.read_u128::<BigEndian>()?) })
    }

    /// Get the sizes of packets, starting from the given index upto the given lenght
    #[inline]
    pub(super) fn readv(&mut self, index: u64, len: u64) -> io::Result<Vec<[u64; 2]>> {
        let len = len as usize;
        let mut buf = Vec::with_capacity(len);

        // SAFETY: We have already preallocated the capacity, and
        //         ENTRY_SIZE = 16 = size of [u64; 2] in bytes
        unsafe {
            self.0
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
        self.0.seek(SeekFrom::End(0))?;
        self.0.write_u64::<BigEndian>(value)
    }
}
