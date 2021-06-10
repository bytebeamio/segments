use std::{io, path::Path};

use bytes::{Bytes, BytesMut};

use super::{index::Index, segment::Segment};

/// The handler for a segment file which is on the disk, and it's corresponding index file.
#[derive(Debug)]
pub(super) struct Chunk {
    /// The handle for index file.
    index: Index,
    /// The handle for segment file.
    segment: Segment,
}

impl Chunk {
    /// Create a new segment on the disk.
    #[inline]
    pub(super) fn new<P: AsRef<Path>>(dir: P, index: u64) -> io::Result<Self> {
        let index_path = dir.as_ref().join(&format!("{:020}.index", index));
        let segment_path = dir.as_ref().join(&format!("{:020}.segment", index));

        // PROBLEM: We don't verify whether index file's offsets make sense, for example, the max
        // length in index file might be larger than the file, or offsets are beyond the file etc.
        // SAFETY: We are the ones to write to both segment as well as index files, and assume no
        // external interference.
        //
        // TODO: maybe we should verify?
        let index = Index::new(index_path)?;
        let segment = Segment::new(segment_path)?;

        Ok(Self { index, segment })
    }

    /// Read a packet from the disk segment at the particular index.
    #[inline]
    pub(super) fn read(&mut self, index: u64) -> io::Result<Bytes> {
        let [offset, len] = self.index.read(index)?;
        self.segment.read(offset, len)
    }

    /// Read `len` packets from disk starting at `index`. If it is not possible to read `len`, it
    /// returns the number of bytes still left to read.
    #[inline]
    pub(super) fn readv(&mut self, index: u64, len: u64, out: &mut Vec<Bytes>) -> io::Result<u64> {
        let (offsets, left) = self.index.readv(index, len)?;
        self.segment.readv(offsets, out)?;
        Ok(left)
    }

    /// Appned a packet to the disk segment. Does not check for any size limit.
    #[cfg(test)]
    #[inline]
    pub(super) fn append(&mut self, bytes: Bytes) -> io::Result<u64> {
        self.index.append(bytes.len() as u64)?;
        self.segment.append(bytes)
    }

    /// And multiple packets at once. Returns offset at which bytes were appended.
    pub(super) fn appendv(&mut self, bytes: Vec<Bytes>) -> io::Result<u64> {
        let mut total = 0;
        for byte in bytes.iter() {
            self.index.append(byte.len() as u64)?;
            total += byte.len();
        }
        let mut buf = BytesMut::with_capacity(total);
        let t: Vec<u8> = bytes.into_iter().flatten().collect();
        buf.extend_from_slice(&t[..]);
        self.segment.append(buf.freeze())
    }

    /// Total number of packet appended.
    #[inline(always)]
    pub(super) fn entries(&self) -> u64 {
        self.index.entries()
    }

    /// Flush the contents to disk.
    #[inline(always)]
    pub(super) fn flush(&mut self) -> io::Result<()> {
        self.segment.flush()
    }
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn append_and_read_chunk() {
        let dir = tempdir().unwrap();
        let mut chunk = Chunk::new(dir.path(), 2).unwrap();

        // appending 20 x 1KB to segment. results in:
        // - segment.size = 20KB = 20 * 1024
        // - segment[0..1023] = 0, segment[1024..2047] = 1 and so on
        // - index.tail = 20
        // - index.tail[offset - 1] = 1024 * 19
        // - index.len[offset - 1] = 1024
        for i in 0..20u8 {
            chunk.append(Bytes::from(vec![i; 1024])).unwrap();
        }

        chunk.flush().unwrap();

        for i in 0..20u8 {
            let byte = chunk.read(i as u64).unwrap();
            assert_eq!(byte.len(), 1024);
            assert_eq!(byte[0], i);
            assert_eq!(byte[1023], i);
        }
    }

    #[test]
    fn append_and_read_chunk_after_saving_to_disk() {
        let dir = tempdir().unwrap();
        let mut chunk = Chunk::new(dir.path(), 2).unwrap();

        // appending 20 x 1KB to segment. results in:
        // - segment.size = 20KB = 20 * 1024
        // - segment[0..1023] = 0, segment[1024..2047] = 1 and so on
        // - index.tail = 20
        // - index.tail[offset - 1] = 1024 * 19
        // - index.len[offset - 1] = 1024
        for i in 0..20u8 {
            chunk.append(Bytes::from(vec![i; 1024])).unwrap();
        }

        drop(chunk);

        let mut chunk = Chunk::new(dir.path(), 2).unwrap();

        for i in 0..20u8 {
            let byte = chunk.read(i as u64).unwrap();
            assert_eq!(byte.len(), 1024);
            assert_eq!(byte[0], i);
            assert_eq!(byte[1023], i);
        }
    }
}
