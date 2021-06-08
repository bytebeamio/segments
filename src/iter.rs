use std::iter::Iterator;

use bytes::Bytes;

use super::{CommitLog, disk::DiskHandler};

pub struct Iter<'a>(&'a CommitLog);

struct ActiveSegmentIter<'a>(&'a Vec<Bytes>);

struct SegmentsIter<'a, I: Iterator<Item = &'a Vec<Bytes>>>(&'a I);

struct DiskIter<'a>(&'a DiskHandler);
