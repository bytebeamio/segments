#![allow(dead_code, unused_imports, unused_variables)]

use std::{fs::{File, OpenOptions}, io::{Read, Write}};

use bytes::Bytes;
use fnv::FnvHashMap;

mod disk;
use disk::DiskSegment;

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
    head: u64,
    /// The index at which the current active segment is, and also marks the last valid segment as
    /// well as last segment in memory.
    tail: u64,
    /// The active segment, to which incoming [`Bytes`] are appended to. Note that the bytes are
    /// themselves not mutable.
    active_segment: Vec<Bytes>,
    /// The collection of segments on disk which are not actively being modified.
    segments: FnvHashMap<u64, Vec<Bytes>>,
    /// A set of opened file handles to all the segments stored onto the disk. This is optional.
    files: Option<FnvHashMap<u64, DiskSegment>>
}

#[cfg(test)]
mod test {
    use std::{fs::{File, OpenOptions}, io::{Read, Write}};

    use simplelog::{Level, CombinedLogger, TermLogger};
    use mqttbytes::v5;

    use super::*;

    fn active_segment_store() {}
    fn memory_segment_store() {}
    fn disk_segment_store() {}

    fn active_segment_store_packet() {}
    fn memory_segment_store_packet() {}
    fn disk_segment_store_packet() {}

    fn active_segment_read() {}
    fn memory_segment_read() {}
    fn disk_segment_read() {}

    fn active_segment_read_packet() {}
    fn memory_segment_read_packet() {}
    fn disk_segment_read_packet() {}
}
