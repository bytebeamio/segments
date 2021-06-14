use std::{io, path::Path};

use bytes::Bytes;
use sha2::Digest;

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
    /// Opens an existing segment-index pair from the disk. Will throw error if either does not
    /// exist. Note that this does not verify the checksum. Call [`Chunk::verify`] to do so
    /// manually.
    ///
    /// This only opens them immutably.
    #[inline]
    pub(super) fn open<P: AsRef<Path>>(dir: P, index: u64) -> io::Result<Self> {
        let index_path = dir.as_ref().join(&format!("{:020}.index", index));
        let segment_path = dir.as_ref().join(&format!("{:020}.segment", index));

        let index = Index::open(index_path)?;
        let segment = Segment::open(segment_path)?;

        Ok(Self { index, segment })
    }

    /// Creates a new segment-index pair onto the disk, and throws error if they already exist. The
    /// given hasher is used to calculate the the checksum of the given bytes. The given bytes are
    /// stored as 1 single segment.
    ///
    /// This only opens them immutably, after writing the given data.
    pub(super) fn new<P: AsRef<Path>>(
        dir: P,
        index: u64,
        bytes: Vec<(Bytes, u64)>,
        hasher: &mut impl Digest,
    ) -> io::Result<Self> {
        let index_path = dir.as_ref().join(&format!("{:020}.index", index));
        let segment_path = dir.as_ref().join(&format!("{:020}.segment", index));

        let mut lens = Vec::with_capacity(bytes.len());
        for (byte, timestamp) in &bytes {
            lens.push((byte.len() as u64, *timestamp));
        }

        let bytes: Vec<u8> = bytes.into_iter().map(|x| x.0).flatten().collect();
        let bytes = Bytes::from(bytes);
        hasher.update(&bytes);
        let hash = hasher.finalize_reset();

        let segment = Segment::new(segment_path, bytes)?;
        // SAFETY: the length is already this, but AsRef for this length not implemented.
        let index = Index::new(index_path, hash.as_ref(), lens)?;

        Ok(Self { index, segment })
    }

    /// Get the size of the segment.
    #[allow(dead_code)]
    #[inline]
    pub(super) fn segment_size(&self) -> u64 {
        self.segment.size()
    }

    /// Verify the checksum by reading the checksum from the start of the index file, calcuating
    /// the checksum of segment file and then comparing those two.
    pub(super) fn verify(&self, hasher: &mut impl Digest) -> io::Result<bool> {
        let read_hash = self.index.read_hash()?;
        let read_segment = self.segment.read(0, self.segment.size())?;
        hasher.update(&read_segment);
        let calculated_hash = hasher.finalize_reset();
        Ok(calculated_hash.len() == read_hash.len()
            && read_hash
                .iter()
                .enumerate()
                .all(|(i, x)| *x == calculated_hash[i]))
    }

    /// Read a packet from the disk segment at the particular index.
    #[inline]
    pub(super) fn read(&self, index: u64) -> io::Result<Bytes> {
        let [offset, len] = self.index.read(index)?;
        self.segment.read(offset, len)
    }

    /// Read a packet from the disk segment at the particular index.
    #[inline]
    pub(super) fn read_with_timestamps(&self, index: u64) -> io::Result<(Bytes, u64)> {
        let [timestamp, offset, len] = self.index.read_with_timestamps(index)?;
        Ok((self.segment.read(offset, len)?, timestamp))
    }

    /// Read `len` packets from disk starting at `index`. If it is not possible to read `len`, it
    /// returns the number of bytes still left to read.
    #[inline]
    pub(super) fn readv(
        &self,
        index: u64,
        len: u64,
        out: &mut Vec<Bytes>,
    ) -> io::Result<u64> {
        let (offsets, left) = self.index.readv(index, len)?;
        self.segment.readv(offsets, out)?;
        Ok(left)
    }

    #[inline]
    pub(super) fn readv_with_timestamps(
        &self,
        index: u64,
        len: u64,
        out: &mut Vec<(Bytes, u64)>,
    ) -> io::Result<u64> {
        let (offsets, left) = self.index.readv_with_timestamps(index, len)?;
        self.segment.readv_with_timestamps(offsets, out)?;
        Ok(left)
    }

    /// Total number of packet appended.
    #[inline(always)]
    pub(super) fn entries(&self) -> u64 {
        self.index.entries()
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use pretty_assertions::assert_eq;
    use sha2::Sha256;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn new_and_read_chunk() {
        let dir = tempdir().unwrap();
        let mut hasher = Sha256::new();

        let mut v = Vec::with_capacity(20);
        for i in 0..20u8 {
            v.push(( Bytes::from(vec![i; 1024]), i as u64 * 100 ));
        }

        let chunk = Chunk::new(dir.path(), 0, v, &mut hasher).unwrap();
        assert!(chunk.verify(&mut hasher).unwrap());

        for i in 0..20u8 {
            let byte = chunk.read(i as u64).unwrap();
            assert_eq!(byte.len(), 1024);
            assert_eq!(byte[0], i);
            assert_eq!(byte[1023], i);
        }
    }

    #[test]
    fn open_and_read_chunk() {
        let dir = tempdir().unwrap();
        let mut hasher = Sha256::new();

        let mut v = Vec::with_capacity(20);
        for i in 0..20u8 {
            v.push(( Bytes::from(vec![i; 1024]), i as u64 * 100 ));
        }

        let chunk = Chunk::new(dir.path(), 0, v, &mut hasher).unwrap();
        assert!(chunk.verify(&mut hasher).unwrap());

        drop(chunk);

        let chunk = Chunk::open(dir.path(), 0).unwrap();
        assert!(chunk.verify(&mut hasher).unwrap());

        for i in 0..20u8 {
            let byte = chunk.read(i as u64).unwrap();
            assert_eq!(byte.len(), 1024);
            assert_eq!(byte[0], i);
            assert_eq!(byte[1023], i);
        }
    }
}
