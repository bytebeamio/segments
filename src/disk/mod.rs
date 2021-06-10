use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use bytes::Bytes;
use fnv::FnvHashMap;
use log::debug;

mod chunk;
mod index;
mod segment;

use chunk::Chunk;

/// A wrapper around all index and segment files on the disk.
pub(super) struct DiskHandler {
    /// Hashmap for file handlers of index and segment files.
    chunks: FnvHashMap<u64, Chunk>,
    /// The directory in which to store files in.
    dir: PathBuf,
    /// The indices of the open files.
    indices: Vec<u64>,
}

impl DiskHandler {
    /// Create a new disk handler which saves files in the given directory.
    pub(super) fn new<P: AsRef<Path>>(dir: P) -> io::Result<(u64, Self)> {
        let _ = fs::create_dir_all(&dir)?;

        let files = fs::read_dir(&dir)?;
        let mut indices = Vec::new();
        let mut segments = FnvHashMap::default();
        for file in files {
            let path = file?.path();
            let offset = path.file_stem().unwrap().to_str().unwrap();
            let offset = offset.parse::<u64>().unwrap();
            segments.insert(offset, Chunk::new(&dir, offset)?);
            indices.push(offset);
        }
        indices.sort_unstable();

        let head = if let Some(head) = indices.last() {
            head + 1
        } else {
            0
        };

        Ok((
            head,
            Self {
                chunks: segments,
                dir: dir.as_ref().into(),
                indices,
            },
        ))
    }

    /// Return the total number of segments.
    #[inline]
    pub(super) fn len(&self) -> u64 {
        self.chunks.len() as u64
    }

    /// Read a single packet from given offset in segment at given index.
    #[inline]
    pub(super) fn read(&mut self, index: u64, offset: u64) -> io::Result<Bytes> {
        if let Some(chunk) = self.chunks.get_mut(&index) {
            chunk.read(offset)
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("given index {} does not exists on disk", index).as_str(),
            ))
        }
    }

    /// Read `len` packets, starting from the given offset in segment at given index. Does not care
    /// about segment boundaries, and will keep on reading until length is met or we run out of
    /// packets. Returns the number of packets left to read, but were not found, and the index of
    /// next segment if exists.
    #[inline]
    pub(super) fn readv(
        &mut self,
        index: u64,
        offset: u64,
        mut len: u64,
        out: &mut Vec<Bytes>,
    ) -> io::Result<(u64, Option<u64>)> {
        let chunk = if let Some(disk_segment) = self.chunks.get_mut(&index) {
            disk_segment
        } else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("given index {} does not exists on disk", index).as_str(),
            ));
        };
        len = chunk.readv(offset, len, out)?;

        // as we find segment at `index` in self.chunks, it must exist in self.indices
        let mut segment_idx = self.indices.binary_search(&index).unwrap();

        if len == 0 {
            segment_idx += 1;
            if segment_idx >= self.indices.len() {
                return Ok((0, None));
            } else {
                return Ok((0, Some(segment_idx as u64)));
            }
        }

        while len > 0 {
            segment_idx += 1;
            if segment_idx >= self.indices.len() {
                return Ok((len, None));
            }
            len = self
                .chunks
                .get_mut(&self.indices[segment_idx])
                .unwrap()
                .readv(0, len, out)?;
        }

        Ok((0, Some(self.indices[segment_idx])))

        // There are three possible cases for return of Ok(_):
        // 1.) len = 0, next = Some(_)
        //     => we still have segment left to read, but len reached
        // 2.) len = 0, next = None
        //     => len reached but no more segments, we were just able to fill it
        // 3.) len > 0, next = None
        //     => let left, but we ran out of segments
    }

    /// Store a vector of bytes to the disk. Returns offset at which bytes were appended to the
    /// segment at the given index.
    #[inline]
    pub(super) fn insert(&mut self, index: u64, data: Vec<Bytes>) -> io::Result<u64> {
        let mut chunk = Chunk::new(&self.dir, index)?;
        let res = chunk.appendv(data)?;
        self.chunks.insert(index, chunk);
        self.indices.push(index);
        Ok(res)
    }

    /// Flush all the segments files.
    pub(super) fn flush(&mut self) -> io::Result<()> {
        for chunk in self.chunks.values_mut() {
            chunk.flush()?;
        }
        Ok(())
    }

    /// Flush the segment file at the given index.
    #[inline]
    pub(super) fn flush_at(&mut self, index: u64) -> io::Result<()> {
        self.chunks
            .get_mut(&index)
            .ok_or(io::Error::new(
                io::ErrorKind::NotFound,
                format!("flushing at invalid index {}", index).as_str(),
            ))?
            .flush()
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use log::debug;
    use mqttbytes::v5;
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;
    use crate::test::{init_logging, random_packets_as_bytes, verify_bytes_as_random_packets};

    #[test]
    fn push_and_read_handler() {
        let dir = tempdir().unwrap();
        let (_, mut handler) = DiskHandler::new(dir.path()).unwrap();
        let (ranpack_bytes, _) = random_packets_as_bytes();

        // results in:
        // - ( 0, [len] *  1 packets)
        // - ( 1, [len] *  2 packets)
        //   ...
        // - (19, [len] * 20 packets)
        //
        // where [len] = ranpack_bytes.len()
        for i in 0..20 {
            let mut v = Vec::with_capacity((i + 1) * ranpack_bytes.len());
            for _ in 0..=i {
                v.extend(ranpack_bytes.clone().into_iter());
            }
            handler.insert(i as u64, v).unwrap();
        }

        handler.flush().unwrap();

        for i in 0..20 {
            let mut v = Vec::new();
            handler
                .readv(i, 0, ranpack_bytes.len() as u64 * (i + 1), &mut v)
                .unwrap();
            for j in 0..=i {
                let u = v.split_off(ranpack_bytes.len());
                verify_bytes_as_random_packets(u, ranpack_bytes.len());
            }
        }
    }

    #[test]
    fn push_and_read_handler_after_drop() {
        let dir = tempdir().unwrap();
        let (_, mut handler) = DiskHandler::new(dir.path()).unwrap();
        let (ranpack_bytes, _) = random_packets_as_bytes();

        // results in:
        // - ( 0, [len] *  1 packets)
        // - ( 1, [len] *  2 packets)
        //   ...
        // - (19, [len] * 20 packets)
        //
        // where [len] = ranpack_bytes.len()
        for i in 0..20 {
            let mut v = Vec::with_capacity((i + 1) * ranpack_bytes.len());
            for _ in 0..=i {
                v.extend(ranpack_bytes.clone().into_iter());
            }
            handler.insert(i as u64, v).unwrap();
        }

        drop(handler);

        let (_, mut handler) = DiskHandler::new(dir.path()).unwrap();
        for i in 0..20 {
            let mut v = Vec::new();
            handler
                .readv(i, 0, ranpack_bytes.len() as u64 * (i + 1), &mut v)
                .unwrap();
            for j in 0..=i {
                let u = v.split_off(ranpack_bytes.len());
                verify_bytes_as_random_packets(u, ranpack_bytes.len());
            }
        }
    }

    #[test]
    fn read_handler_from_returned_index() {
        let dir = tempdir().unwrap();
        let (_, mut handler) = DiskHandler::new(dir.path()).unwrap();
        let (ranpack_bytes, _) = random_packets_as_bytes();

        // results in:
        // - ( 0, [len] *  1 packets)
        // - ( 1, [len] *  2 packets)
        //   ...
        // - (14, [len] * 15 packets)
        //
        // where [len] = ranpack_bytes.len()
        for i in 0..15 {
            let mut v = Vec::with_capacity((i + 1) * ranpack_bytes.len());
            for _ in 0..=i {
                v.extend(ranpack_bytes.clone().into_iter());
            }
            handler.insert(i as u64, v).unwrap();
        }

        handler.flush().unwrap();

        let mut v = Vec::new();
        let (mut left, mut ret) = handler.readv(0, 0, 10, &mut v).unwrap();
        verify_bytes_as_random_packets(v, 10);
        let mut offset = 0;
        let mut v: Vec<Bytes> = Vec::new();

        while let Some(seg) = ret {
            v.clear();
            offset = if left > 0 { 0 } else { offset + 10 };
            let (new_left, new_ret) = handler.readv(seg, offset, 10, &mut v).unwrap();
            left = new_left;
            ret = new_ret;
        }

        assert_eq!(left, 0);
    }
}
