#![allow(dead_code, unused_imports, unused_variables)]

use std::{
    fs::{self, File, OpenOptions},
    io,
    iter::Iterator,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use fnv::FnvHashMap;

mod disk;
use disk::DiskHandler;
mod iter;

/// The log which can store commits in memory, and push them onto disk when needed, as well as read
/// from disk any valid segment.
/// ### Invariants
/// - The active segment should have index `tail`.
/// - The segments in memory should have contiguous indices, though this need not be the case for
///   segment stored on disk.
/// - The total size in bytes for each segment in memory should not increase beyond the
///   max_segment_size by more than the overflowing bytes of the last packet.
pub struct CommitLog {
    /// The index at which segments of memory start.
    pub(crate) head: u64,
    /// The index at which the current active segment is, and also marks the last valid segment as
    /// well as last segment in memory.
    pub(crate) tail: u64,
    /// Maximum size of any segment in memory.
    pub(crate) max_segment_size: u64,
    /// Maximum number of segments in memory, apart from the active segment.
    pub(crate) max_segments: u64,
    /// The active segment, to which incoming [`Bytes`] are appended to. Note that the bytes are
    /// themselves not mutable.
    pub(crate) active_segment: Vec<Bytes>,
    /// Total size of active segment, used for enforcing the contraints.
    pub(crate) active_segment_size: u64,
    /// The collection of segments on disk which are not actively being modified.
    pub(crate) segments: FnvHashMap<u64, Vec<Bytes>>,
    /// Total size of segments in memory apart from active_segment, used for enforcing the
    /// contraints.
    pub(crate) segments_size: u64,
    /// A set of opened file handles to all the segments stored onto the disk. This is optional.
    pub(crate) files: Option<DiskHandler>,
}

impl CommitLog {
    pub fn new<P: AsRef<Path>>(
        max_segment_size: u64,
        max_segments: u64,
        dir: Option<P>,
    ) -> io::Result<Self> {
        if let Some(dir) = dir {
            let (head, files) = DiskHandler::new(dir)?;

            Ok(Self {
                head,
                tail: head,
                max_segment_size,
                max_segments,
                active_segment: Vec::new(),
                active_segment_size: 0,
                segments: FnvHashMap::default(),
                segments_size: 0,
                files: Some(files),
            })
        } else {
            Ok(Self {
                head: 0,
                tail: 0,
                max_segment_size,
                max_segments,
                active_segment: Vec::new(),
                active_segment_size: 0,
                segments: FnvHashMap::default(),
                segments_size: 0,
                files: None,
            })
        }
    }

    #[inline]
    pub fn append(&mut self, bytes: Bytes) -> io::Result<(u64, u64)> {
        self.apply_retention()?;
        self.active_segment.push(bytes);
        Ok((self.tail, self.active_segment.len() as u64))
    }

    fn apply_retention(&mut self) -> io::Result<()> {
        if self.active_segment_size > self.max_segment_size {
            if self.segments_size > self.max_segment_size {
                let removed_segment = self.segments.remove(&self.head).unwrap();

                if let Some(files) = self.files.as_mut() {
                    files.push(self.head, removed_segment)?;
                }

                self.head += 1;
            }

            // this replace is cheap as we only swap the 3 pointer that are held by Vec<T>
            let old_segment = std::mem::replace(
                &mut self.active_segment,
                Vec::with_capacity(self.max_segment_size as usize),
            );
            self.segments.insert(self.tail, old_segment);
            self.tail += 1;
        }

        Ok(())
    }

    fn read(&mut self, segment: u64, index: u64) -> io::Result<iter::Iter> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::{
        fs::{File, OpenOptions},
        io::{Read, Write},
    };

    use mqttbytes::v5;
    use simplelog::{CombinedLogger, Level, TermLogger};

    use super::*;

    fn active_segment_store() {
        todo!()
    }
    fn memory_segment_store() {
        todo!()
    }
    fn disk_segment_store() {
        todo!()
    }

    fn active_segment_store_packet() {
        todo!()
    }
    fn memory_segment_store_packet() {
        todo!()
    }
    fn disk_segment_store_packet() {
        todo!()
    }

    fn active_segment_read() {
        todo!()
    }
    fn memory_segment_read() {
        todo!()
    }
    fn disk_segment_read() {
        todo!()
    }

    fn active_segment_read_packet() {
        todo!()
    }
    fn memory_segment_read_packet() {
        todo!()
    }
    fn disk_segment_read_packet() {
        todo!()
    }
}
