use std::{
    fs::{File, OpenOptions},
    io,
    path::Path,
};

use bytes::{Bytes, BytesMut};

/// Wrapper around the segment file.
#[derive(Debug)]
pub(super) struct Segment {
    /// A buffered reader for the segment file.
    file: File,
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
    /// Open a new segment file. Will throw an error if file does not exist.
    #[inline]
    pub(super) fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let size = file.metadata()?.len();
        Ok(Self { file, size })
    }

    /// Create a new segment file. Will throw an error if file already exists.
    #[inline]
    pub(super) fn new<P: AsRef<Path>>(path: P, bytes: Bytes) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let size = bytes.len() as u64;
        let mut ret = Self { file, size };
        ret.write_at(&bytes, 0)?;
        Ok(ret)
    }

    #[inline]
    /// Returns the size of the file the segment is holding.
    pub(super) fn size(&self) -> u64 {
        self.size
    }

    /// Reads `len` bytes from given `offset` in the file.
    #[inline]
    pub(super) fn read(&self, offset: u64, len: u64) -> io::Result<Bytes> {
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
        self.read_at(&mut bytes, offset)?;

        Ok(bytes.freeze())
    }

    /// Get packets from given vector of indices and corresponding lens.
    #[inline]
    pub(super) fn readv(&self, offsets: Vec<[u64; 2]>, out: &mut Vec<Bytes>) -> io::Result<()> {
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

    /// Takes in the vector of 3-arrays, whose elements are timestamp, offset, len in this order.
    /// Returns a vector of 2-tuples containing `(packet_data, timestamp)`
    #[inline]
    pub(super) fn readv_with_timestamps(
        &self,
        offsets: Vec<[u64; 3]>,
        out: &mut Vec<(Bytes, u64)>,
    ) -> io::Result<()> {
        let total = if let Some(first) = offsets.first() {
            let mut total = first[2];
            for offset in offsets.iter().skip(1) {
                total += offset[2];
            }
            total
        } else {
            return Ok(());
        };

        let mut buf = self.read(offsets[0][1], total)?;

        for offset in offsets.into_iter() {
            out.push((buf.split_to(offset[2] as usize), offset[0]));
        }

        Ok(())
    }

    /// Get the actual size of the file by reading it's metadata. Used only for testing.
    #[cfg(test)]
    #[inline]
    fn actual_size(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    #[allow(unused_mut)]
    #[inline]
    fn read_at(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        #[cfg(target_family = "unix")]
        {
            use std::os::unix::prelude::FileExt;
            self.file.read_exact_at(buf, offset)
        }
        #[cfg(target_family = "windows")]
        {
            use std::os::windows::fs::FileExt;
            while !buf.is_empty() {
                match self.seek_write(buf, offset) {
                    Ok(0) => return Ok(()),
                    Ok(n) => {
                        buf = &buf[n..];
                        offset += n as u64;
                    }
                    Err(e) => return Err(e),
                }
            }
            if !buf.is_empty() {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to write whole buffer",
                ))
            } else {
                Ok(())
            }
        }
    }

    #[allow(unused_mut)]
    #[inline]
    fn write_at(&mut self, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
        #[cfg(target_family = "unix")]
        {
            use std::os::unix::prelude::FileExt;
            self.file.write_all_at(buf, offset)
        }
        #[cfg(target_family = "windows")]
        {
            use std::os::windows::fs::FileExt;
            while !buf.is_empty() {
                match self.seek_read(buf, offset) {
                    Ok(0) => return Ok(()),
                    Ok(n) => {
                        buf = &mut buf[n..];
                        offset += n as u64;
                    }
                    Err(e) => return Err(e),
                }
            }
            if !buf.is_empty() {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ))
            } else {
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use bytes::{BufMut, Bytes, BytesMut};
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn new_and_read_segment() {
        let dir = tempdir().unwrap();

        let mut buf = BytesMut::new();
        for i in 0..20u8 {
            buf.put(Bytes::from(vec![i; 1024]));
        }
        let segment = Segment::new(dir.path().join(&format!("{:020}", 1)), buf.freeze()).unwrap();
        assert_eq!(segment.size(), 20 * 1024);

        assert_eq!(segment.actual_size().unwrap(), 20 * 1024);
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
    fn open_and_read_segment() {
        let dir = tempdir().unwrap();

        let mut buf = BytesMut::new();
        for i in 0..20u8 {
            buf.put(Bytes::from(vec![i; 1024]));
        }
        let segment = Segment::new(dir.path().join(&format!("{:020}", 1)), buf.freeze()).unwrap();
        assert_eq!(segment.size(), 20 * 1024);

        assert_eq!(segment.actual_size().unwrap(), 20 * 1024);
        for i in 0..20u8 {
            let byte = segment.read(i as u64 * 1024, 1024).unwrap();
            assert_eq!(byte.len(), 1024);
            assert_eq!(byte[0], i);
            assert_eq!(byte[1023], i);
        }

        drop(segment);

        let segment = Segment::open(dir.path().join(&format!("{:020}", 1))).unwrap();
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
