use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::Path,
};

use bytes::Bytes;

mod index;
use index::Index;
mod segment;
use segment::Segment;

/// The handler for a segment file which is on the disk, and it's corresponding index file.
pub(super) struct DiskSegment {
    /// The handle for index file.
    index: Index,
    /// The handle for segment file.
    segment: Segment,
    /// The index at which we add stuff to. It is 1 beyond the actual value stored.
    tail: u64,
}

impl DiskSegment {
    #[inline]
    pub(super) fn new<P: AsRef<Path>>(dir: P, index: u64) -> io::Result<Self> {
        let commit_path = dir.as_ref().join(&format!("{:020}", index));
        let index_path = commit_path.join(".index");
        let segment_path = commit_path.join(".segment");

        // PROBLEM: We don't verify whether index file's offsets make sense, for example, the max
        // length in index file might be larger than the file, or offsets are beyond the file etc.
        // SAFETY: We are the ones to write to both segment as well as index files, and assume no
        // external interference.
        //
        // TODO: maybe we should verify?
        let (index, tail) = Index::new(index_path)?;
        let segment = Segment::new(segment_path)?;

        Ok(Self {
            index,
            segment,
            tail,
        })
    }

    #[inline]
    pub(super) fn read(&mut self, index: u64) -> io::Result<Bytes> {
        let [offset, len] = self.index.read(index)?;
        self.segment.read(offset, len)
    }

    #[inline]
    pub(super) fn readv(&mut self, index: u64, len: u64) -> io::Result<Vec<Bytes>> {
        let offsets = self.index.readv(index, len)?;
        self.segment.readv(offsets)
    }

    #[inline]
    pub(super) fn append(&mut self, bytes: Bytes) -> io::Result<u64> {
        self.tail += 1;
        self.index.append(bytes.len() as u64)?;
        self.segment.append(bytes)
    }
}
