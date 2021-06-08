use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

use bytes::{Bytes, BytesMut};

use super::index::Index;

/// Wrapper around the segment file.
pub(super) struct Segment {
    /// A buffered reader for the segment file.
    reader: BufReader<File>,
    /// A buffered writer for the segment file.
    writer: BufWriter<File>,
    /// The total size of segment file in bytes.
    size: u64,
}

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
        let file = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(path)?;
        let size = file.metadata()?.len();
        let reader = BufReader::new(file.try_clone()?);
        let writer = BufWriter::new(file);
        Ok(Self {
            reader,
            writer,
            size,
        })
    }

    #[inline]
    /// Returns the size of the file the segment is holding.
    pub(super) fn size(&self) -> u64 {
        self.size
    }

    /// Reads `len` bytes from given `offset` in the file.
    #[inline]
    pub(super) fn read(&mut self, offset: u64, len: u64) -> io::Result<Bytes> {
        let len = len as usize;
        if offset >= self.size {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("given offset {} when file size is {}", offset, self.size).as_str(),
            ));
        }
        let mut bytes = BytesMut::with_capacity(len);
        // SAFETY: We fill it with the contents later on, and has already been allocated.
        unsafe { bytes.set_len(len) };

        self.reader.seek(SeekFrom::Start(offset))?;
        self.reader.read_exact(&mut bytes)?;

        Ok(bytes.freeze())
    }

    /// Get packets from given vector of indices and corresponding lens.
    #[inline]
    pub(super) fn readv(&mut self, offsets: Vec<[u64; 2]>) -> io::Result<Vec<Bytes>> {
        let len = offsets.len();
        let total = if let Some(first) = offsets.first() {
            let mut total = first[1];
            for offset in offsets.iter().skip(1) {
                total += offset[1];
            }
            total
        } else {
            return Ok(vec![Bytes::new()])
        };

        let mut buf = self.read(offsets[0][0], total)?;
        let mut v = Vec::with_capacity(len);

        for offset in offsets.into_iter() {
            v.push(buf.split_to(offset[1] as usize));
        }

        Ok(v)
    }

    /// Append a packet to the segment.
    #[inline]
    pub(super) fn append(&mut self, bytes: Bytes) -> io::Result<u64> {
        let index = self.size;
        self.writer.seek(SeekFrom::End(0))?;
        self.writer.write_all(&bytes)?;
        self.size += bytes.len() as u64;
        Ok(index)
    }
}
