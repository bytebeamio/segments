use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom},
    path::Path,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

/// Size of entry, in bytes.
pub const ENTRY_SIZE: u64 = 8;

/// Wrapper around index files for convenient reading of bytes sizes.
///
/// Does **not** check any of the constraint enforced by user, or that the index being read from/
/// written to is valid. Simply performs what asked.
pub(super) struct Index(File);

impl Index {
    /// Open a new index file, or create one if doesn't exist.
    pub(super) fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Ok(Self(
            OpenOptions::new()
                .append(true)
                .read(true)
                .create(true)
                .open(path)?,
        ))
    }

    /// Get the size of packet at the given index, using the index file.
    pub(super) fn read(&mut self, index: u64) -> io::Result<u64> {
        self.0.seek(SeekFrom::Start(index * ENTRY_SIZE))?;
        self.0.read_u64::<BigEndian>()
    }

    /// Append a new value to the index file. Increases `self.index` by one, and returns the index
    /// at which new value was appened.
    ///
    /// #### Note
    /// It is the duty of the handler of this struct to ensure index file's size does not exceed
    /// the specified limit.
    pub(super) fn append(&mut self, value: u64) -> io::Result<()> {
        self.0.seek(SeekFrom::End(0))?;
        self.0.write_u64::<BigEndian>(value)
    }
}
