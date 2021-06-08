use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use bytes::Bytes;
use fnv::FnvHashMap;

mod index;
mod segment;
mod chunk;
use chunk::Chunk;

// TODO: document everything in here

pub(super) struct DiskHandler {
    segments: FnvHashMap<u64, Chunk>,
    dir: PathBuf,
}

impl DiskHandler {
    pub(super) fn new<P: AsRef<Path>>(dir: P) -> io::Result<(u64, Self)> {
        let _ = fs::create_dir_all(&dir)?;

        let files = fs::read_dir(&dir)?;
        let mut base_offsets = Vec::new();
        let mut segments = FnvHashMap::default();
        for file in files {
            let path = file?.path();
            let offset = path.file_stem().unwrap().to_str().unwrap();
            let offset = offset.parse::<u64>().unwrap();
            segments.insert(offset, Chunk::new(&dir, offset)?);
            base_offsets.push(offset);
        }
        base_offsets.sort_unstable();

        let head = if let Some(head) = base_offsets.last() {
            head + 1
        } else {
            0
        };

        Ok((
            head,
            Self {
                segments,
                dir: dir.as_ref().into(),
            },
        ))
    }

    #[inline]
    pub(super) fn read(&mut self, index: u64, offset: u64) -> io::Result<Bytes> {
        if let Some(disk_segment) = self.segments.get_mut(&index) {
            disk_segment.read(offset)
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("given index {} does not exists on disk", index).as_str(),
            ))
        }
    }

    #[inline]
    pub(super) fn readv(&mut self, index: u64, offset: u64, len: u64) -> io::Result<Vec<Bytes>> {
        if let Some(disk_segment) = self.segments.get_mut(&index) {
            disk_segment.readv(offset, len)
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("given index {} does not exists on disk", index).as_str(),
            ))
        }
    }

    #[inline]
    pub(super) fn push(&mut self, index: u64, data: Vec<Bytes>) -> io::Result<u64> {
        let mut disk_segment = Chunk::new(&self.dir, index)?;
        disk_segment.appendv(data)
    }
}
