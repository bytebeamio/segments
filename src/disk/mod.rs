use std::{
    fs, io,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use fnv::FnvHashMap;
use sha2::{Digest, Sha256};

mod chunk;
mod index;
mod segment;

use chunk::Chunk;

/// A wrapper around all index and segment files on the disk.
#[allow(dead_code)]
pub(super) struct DiskHandler {
    /// Hashmap for file handlers of index and segment files.
    chunks: FnvHashMap<u64, Chunk>,
    /// Directory in which to store files in.
    dir: PathBuf,
    /// Starting index of segment files.
    head: u64,
    /// Ending index of segment files.
    tail: u64,
    /// Invalid files.
    invalid_files: Vec<InvalidType>,
    /// The hasher for segment files
    hasher: Sha256,
}

// TODO: document this, also also the hierarchy or InvalidType.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum InvalidType {
    InvalidName(PathBuf),
    NoIndex(u64),
    NoSegment(u64),
    InvalidChecksum(u64),
}

//TODO: Review all unwraps
impl DiskHandler {
    pub(super) fn new<P: AsRef<Path>>(dir: P) -> io::Result<(u64, Self)> {
        struct FileStatus {
            index_found: bool,
            segment_found: bool,
        }

        // creating and reading given dir
        let _ = fs::create_dir_all(&dir)?;
        let files = fs::read_dir(&dir)?;

        let mut indices = Vec::new();
        let mut statuses: FnvHashMap<u64, FileStatus> = FnvHashMap::default();
        let mut invalid_files = Vec::new();
        let mut hasher = Sha256::new();

        // checking status of files in dir, storing valid index in `indices`
        for file in files {
            let path = file?.path();

            let file_index = match path.file_stem() {
                // TODO: is this unwrap fine?
                Some(s) => s.to_str().unwrap(),
                None => {
                    invalid_files.push(InvalidType::InvalidName(path));
                    continue;
                }
            };

            let offset = match file_index.parse::<u64>() {
                Ok(n) => n,
                Err(_) => {
                    invalid_files.push(InvalidType::InvalidName(path));
                    continue;
                }
            };

            // TODO: is this unwrap fine?
            match path.extension().map(|s| s.to_str().unwrap()) {
                Some("index") => {
                    if let Some(status) = statuses.get_mut(&offset) {
                        // TODO: also verify checksum here
                        status.index_found = true;
                    } else {
                        statuses.insert(
                            offset,
                            FileStatus {
                                index_found: true,
                                segment_found: false,
                            },
                        );
                    }
                }
                Some("segment") => {
                    if let Some(status) = statuses.get_mut(&offset) {
                        // TODO: also verify checksum here
                        status.segment_found = true;
                    } else {
                        statuses.insert(
                            offset,
                            FileStatus {
                                index_found: false,
                                segment_found: true,
                            },
                        );
                    }
                }
                _ => invalid_files.push(InvalidType::InvalidName(path)),
            }

            indices.push(offset);
        }

        // getting the head and tail
        indices.sort_unstable();
        let (inmemory_head, head, tail) = if let Some(tail) = indices.last() {
            // unwrap fine as if last exists then first exists as well, even if they are the same
            (*tail + 1, *indices.first().unwrap(), *tail)
        } else {
            (0, 0, 0)
        };

        // opening valid files, sorting the invalid ones
        let mut chunks = FnvHashMap::default();
        for (
            index,
            FileStatus {
                index_found,
                segment_found,
            },
        ) in statuses.into_iter()
        {
            if !index_found {
                invalid_files.push(InvalidType::NoIndex(index));
            } else if !segment_found {
                invalid_files.push(InvalidType::NoSegment(index));
            } else {
                let chunk = Chunk::open(&dir, index)?;
                if !chunk.verify(&mut hasher)? {
                    invalid_files.push(InvalidType::InvalidChecksum(index))
                } else {
                    chunks.insert(index, chunk);
                }
            }
        }

        Ok((
            inmemory_head,
            Self {
                chunks,
                dir: dir.as_ref().into(),
                head,
                tail,
                invalid_files,
                hasher,
            },
        ))
    }

    #[allow(dead_code)]
    #[inline]
    pub(super) fn head_idx(&self) -> u64 {
        self.head
    }

    #[allow(dead_code)]
    #[inline]
    pub(super) fn tail_idx(&self) -> u64 {
        self.tail
    }

    /// Returns the total number of segments.
    #[inline]
    pub(super) fn len(&self) -> u64 {
        self.chunks.len() as u64
    }

    // /// Returns the number of entries for a particular segment.
    // #[inline]
    // pub(super) fn len_at(&self, index: u64) -> io::Result<u64> {
    //     Ok(self.chunks.get(&index).ok_or(io::Error::new(
    //             io::ErrorKind::Other,
    //             "No elemt at the given index",
    //         ))?.entries())
    // }

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
    /// packets. Returns the number of packets left to read (which can be 0), but were not found,
    /// and the index of next segment if exists.
    #[inline]
    pub(super) fn readv(
        &self,
        index: u64,
        offset: u64,
        len: u64,
        out: &mut Vec<Bytes>,
    ) -> io::Result<(u64, Option<u64>)> {
        let chunk = if let Some(disk_segment) = self.chunks.get(&index) {
            disk_segment
        } else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("given index {} does not exists on disk", index).as_str(),
            ));
        };
        let mut left = chunk.readv(offset, len, out)?;

        let mut segment_idx = index;

        if left == 0 {
            // if no more packets left in `chunk`, move onto next
            if offset + len >= chunk.entries() {
                segment_idx += 1;
                while self.chunks.get(&segment_idx).is_none() {
                    segment_idx += 1;
                    if segment_idx > self.tail {
                        return Ok((left, None));
                    }
                }
            }

            return Ok((0, Some(segment_idx as u64)));
        }

        while left > 0 {
            segment_idx += 1;
            while self.chunks.get(&segment_idx).is_none() {
                segment_idx += 1;
                if segment_idx > self.tail {
                    return Ok((left, None));
                }
            }

            // unwrap fine as we already validated the index in the while loop
            left = self
                .chunks
                .get(&segment_idx)
                .unwrap()
                .readv(0, left, out)?;
        }

        Ok((0, Some(segment_idx)))

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
    pub(super) fn insert(&mut self, index: u64, data: Vec<Bytes>) -> io::Result<()> {
        let chunk = Chunk::new(&self.dir, index, data, &mut self.hasher)?;
        self.chunks.insert(index, chunk);

        if index > self.tail {
            self.tail = index;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;
    use crate::test::{random_packets_as_bytes, verify_bytes_as_random_packets};

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

        for i in 0..20 {
            let mut v = Vec::new();
            handler
                .readv(i, 0, ranpack_bytes.len() as u64 * (i + 1), &mut v)
                .unwrap();
            for _ in 0..=i {
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

        let (_, handler) = DiskHandler::new(dir.path()).unwrap();
        for i in 0..20 {
            let mut v = Vec::new();
            handler
                .readv(i, 0, ranpack_bytes.len() as u64 * (i + 1), &mut v)
                .unwrap();
            for _ in 0..=i {
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
