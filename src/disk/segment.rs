use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

use bytes::{Bytes, BytesMut};

/// Wrapper around the segment file.
#[derive(Debug)]
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

    #[cfg(test)]
    #[inline]
    /// Returns the size of the file the segment is holding.
    pub(super) fn size(&self) -> u64 {
        self.size
    }

    /// Reads `len` bytes from given `offset` in the file.
    #[inline]
    pub(super) fn read(&mut self, offset: u64, len: u64) -> io::Result<Bytes> {
        if offset + len > self.size {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "given offset + len = {} when file size is {}",
                    offset + len,
                    self.size
                )
                .as_str(),
            ));
        }
        let len = len as usize;
        let mut bytes = BytesMut::with_capacity(len);
        // SAFETY: We fill it with the contents later on, and has already been allocated.
        unsafe { bytes.set_len(len) };

        self.reader.seek(SeekFrom::Start(offset))?;
        self.reader.read_exact(&mut bytes)?;

        Ok(bytes.freeze())
    }

    /// Get packets from given vector of indices and corresponding lens.
    #[inline]
    pub(super) fn readv(&mut self, offsets: Vec<[u64; 2]>, out: &mut Vec<Bytes>) -> io::Result<()> {
        let total = if let Some(first) = offsets.first() {
            let mut total = first[1];
            for offset in offsets.iter().skip(1) {
                total += offset[1];
            }
            total
        } else {
            return Ok(());
        };

        let mut buf = self.read(offsets[0][0], total)?;

        for offset in offsets.into_iter() {
            out.push(buf.split_to(offset[1] as usize));
        }

        Ok(())
    }

    /// Append a packet to the segment. Note that appending is buffered, thus always call
    /// [`Segment::flush`] before reading. Returns offset at which bytes were appended.
    #[inline]
    pub(super) fn append(&mut self, bytes: Bytes) -> io::Result<u64> {
        let offset = self.size;
        self.writer.seek(SeekFrom::End(0))?;
        self.writer.write_all(&bytes)?;
        self.size += bytes.len() as u64;
        Ok(offset)
    }

    /// Flush the contents to disk.
    #[inline(always)]
    pub(super) fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    #[cfg(test)]
    fn actual_size(self) -> io::Result<(Self, u64)> {
        let Self {
            reader,
            writer,
            size,
        } = self;
        let file = reader.into_inner();
        let actual_len = file.metadata()?.len();
        Ok((
            Self {
                reader: BufReader::new(file),
                writer,
                size,
            },
            actual_len,
        ))
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn append_and_read_segment() {
        let dir = tempdir().unwrap();
        let mut segment = Segment::new(dir.path().join(&format!("{:020}", 1))).unwrap();
        assert_eq!(segment.size(), 0);

        // appending 20 x 1KB to segment. results in:
        // - size = 20KB = 20 * 1024
        // - segment[0..1023] = 0, segment[1024..2047] = 1 and so on
        for i in 0..20u8 {
            segment.append(Bytes::from(vec![i; 1024])).unwrap();
        }
        assert_eq!(segment.size(), 20 * 1024);
        segment.flush().unwrap();

        let (mut segment, actual_len) = segment.actual_size().unwrap();
        assert_eq!(actual_len, 20 * 1024);
        for i in 0..20u8 {
            let byte = segment.read(i as u64 * 1024, 1024).unwrap();
            assert_eq!(byte.len(), 1024);
            assert_eq!(byte[0], i);
            assert_eq!(byte[1023], i);
        }

        let mut offsets = Vec::with_capacity(20);
        for i in 0..20 {
            offsets.push([i * 1024, 1024]);
        }
        let mut out = Vec::with_capacity(20);
        segment.readv(offsets, &mut out).unwrap();
        for (i, byte) in out.into_iter().enumerate() {
            assert_eq!(byte.len(), 1024);
            assert_eq!(byte[0], i as u8);
            assert_eq!(byte[1023], i as u8);
        }
    }

    #[test]
    fn append_and_read_segment_after_saving_on_disk() {
        let dir = tempdir().unwrap();
        let mut segment = Segment::new(dir.path().join(&format!("{:020}", 1))).unwrap();
        assert_eq!(segment.size(), 0);

        // appending 20 x 1KB to segment. results in:
        // - size = 20KB = 20 * 1024
        // - segment[0..1023] = 0, segment[1024..2047] = 1 and so on
        for i in 0..20u8 {
            segment.append(Bytes::from(vec![i; 1024])).unwrap();
        }

        drop(segment);

        let segment = Segment::new(dir.path().join(&format!("{:020}", 1))).unwrap();
        assert_eq!(segment.size(), 20 * 1024);
        let (mut segment, actual_len) = segment.actual_size().unwrap();

        assert_eq!(actual_len, 20 * 1024);
        for i in 0..20u8 {
            let byte = segment.read(i as u64 * 1024, 1024).unwrap();
            assert_eq!(byte.len(), 1024);
            assert_eq!(byte[0], i);
            assert_eq!(byte[1023], i);
        }

        let mut offsets = Vec::with_capacity(20);
        for i in 0..20 {
            offsets.push([i * 1024, 1024]);
        }
        let mut out = Vec::with_capacity(20);
        segment.readv(offsets, &mut out).unwrap();
        for (i, byte) in out.into_iter().enumerate() {
            assert_eq!(byte.len(), 1024);
            assert_eq!(byte[0], i as u8);
            assert_eq!(byte[1023], i as u8);
        }
    }
}
