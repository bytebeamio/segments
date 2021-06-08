use std::{fs::{OpenOptions, File}, path::Path, io::{self, Read, Write}};

mod index;
use index::Index;
mod segment;
use segment::Segment;

pub(super) struct DiskSegment {
    index: Index,
    segment: Segment,
}

impl DiskSegment {
    pub(super) fn new<P: AsRef<Path>>(dir: P, index: u64) -> io::Result<Self> {
        let commit_path = dir.as_ref().join(&format!("{:020}", index));
        let index_path = commit_path.join(".index");
        let segment_path = commit_path.join(".segment");

        let index = Index::new(index_path);

        todo!()
    }
}
