use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

use bytes::{Bytes, BytesMut};

pub(super) struct Segment(File);

/// A wrapper around a single segment file for convenient reading of bytes. Does **not** enforce
/// any contraints and simply does what asked. Handler should enforce the contraints.
///
/// #### Note
/// It is the duty of the handler of this struct to ensure index file's size does not exceed the
/// specified limit.
impl Segment {
    /// Open/create a new segment file.
    #[inline]
    pub(super) fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Ok(Self(
            OpenOptions::new()
                .append(true)
                .read(true)
                .create(true)
                .open(path)?,
        ))
    }

    /// Reads `len` bytes from given `offset` in the file.
    #[inline]
    pub(super) fn read(&mut self, offset: u64, len: u64) -> io::Result<Bytes> {
        self.0.seek(SeekFrom::Start(offset))?;
        let mut bytes = BytesMut::with_capacity(len as usize);
        self.0.read(&mut bytes)?;
        Ok(bytes.into())
    }
}
