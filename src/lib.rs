use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    mem::{self, MaybeUninit},
    path::PathBuf,
};

use fnv::FnvHashMap;
#[allow(unused_imports)]
use log::{debug, info};

/// CommitLog is an in-memory log which splits data into segments. It maintains
/// an active segment to which data is appended, and im-memory list of segments
/// which act as buffer, and file handlers to all the files in the specified
/// directory, all of which can be read from.
#[derive(Debug)]
pub struct CommitLog {
    /// Offset of the first segment.
    pub head_offset: u64,
    /// Offset of the last segment.
    pub tail_offset: u64,
    /// Maximum size of a segment.
    pub max_segment_size: usize,
    /// Maximum number of segments.
    pub max_segments: usize,
    /// Current active chunk to append.
    pub active_segment: Vec<u8>,
    /// All the segments in a ringbuffer.
    pub segments: FnvHashMap<u64, Vec<u8>>,
    /// The directory where all logs are stored.
    pub dir: PathBuf,
    /// Optionally open file handles for the log files.
    pub files: Option<FnvHashMap<u64, File>>,
}

impl CommitLog {
    /// Create a new log. `open_files` specifies whether all the exisiting files
    /// in the directory should be opened for reading or not.
    pub fn new<P: Into<PathBuf>>(
        max_segment_size: usize,
        max_segments: usize,
        dir: P,
        open_files: bool,
    ) -> io::Result<Self> {
        let dir = dir.into();
        let _ = fs::create_dir_all(&dir);
        if max_segment_size < 1024 {
            panic!("size should be at least 1KB")
        }

        let files = fs::read_dir(&dir)?;
        let mut base_offsets = Vec::new();
        for file in files {
            let path = file?.path();
            let offset = path.file_stem().unwrap().to_str().unwrap();
            let offset = offset.parse::<u64>().unwrap();
            base_offsets.push(offset);
        }
        // this is fine as files have to unique
        base_offsets.sort_unstable();

        let head_offset = match base_offsets.last() {
            Some(offset) => offset + 1,
            None => 0,
        };

        let files = if open_files {
            let mut files = FnvHashMap::default();
            for offset in base_offsets.into_iter() {
                let file_path = dir.join(format!("{:020}", offset));
                files.insert(offset, OpenOptions::new().read(true).open(file_path)?);
            }
            Some(files)
        } else {
            None
        };

        Ok(Self {
            head_offset,
            tail_offset: head_offset,
            max_segment_size,
            max_segments,
            active_segment: Vec::with_capacity(max_segment_size),
            segments: FnvHashMap::default(),
            dir,
            files,
        })
    }

    #[inline]
    /// Get the starting end ending indices of the segments that are currently
    /// in memory.
    pub fn head_and_tail(&self) -> (u64, u64) {
        (self.head_offset, self.tail_offset)
    }

    /// Append a new set of bytes to the log.
    pub fn append(&mut self, record: &[u8]) -> io::Result<(u64, u64)> {
        // if record.len() + self.active_segment.len() > self.max_segment_size {
        //     let limit = self.max_segment_size as usize - self.active_segment.len();
        //     let (current, next) = record.split_at(limit);
        //     self.active_segment.extend_from_slice(current);
        //     self.apply_retention()?; // this should empty active_segment
        //     record = next;

        //     while record.len() > self.max_segment_size {
        //         let (current, next) = record.split_at(self.max_segment_size);
        //         self.active_segment.extend_from_slice(current);
        //         self.apply_retention()?; // this should empty active_segment
        //         record = next;
        //     }
        // }

        self.apply_retention()?;
        let segment_id = self.tail_offset;
        // NOTE: allows 1 packet to overflow the size contraint
        self.active_segment.extend_from_slice(record);
        let offset = self.active_segment.len() as u64;

        Ok((segment_id, offset))
    }

    fn apply_retention(&mut self) -> io::Result<()> {
        if self.active_segment.len() >= self.max_segment_size {
            if self.segments.len() >= self.max_segments {
                let mut file = OpenOptions::new()
                    .read(true)
                    .append(true)
                    .create(true)
                    .open(self.dir.join(format!("{:020}", self.head_offset)))?;

                let removed_segment = self.segments.remove(&self.head_offset).unwrap();
                file.write_all(&removed_segment)?;

                if let Some(ref mut files) = self.files {
                    files.insert(self.head_offset, file);
                }
                self.head_offset += 1;
            }

            let old_segment =
                mem::replace(&mut self.active_segment, Vec::with_capacity(self.max_segment_size));
            self.segments.insert(self.tail_offset, old_segment);
            self.tail_offset += 1;
        }

        Ok(())
    }

    /// Get the next offset to which to append data to.
    #[inline]
    pub fn next_offset(&self) -> (u64, u64) {
        (self.tail_offset, self.active_segment.len() as u64)
    }

    /// Read the data at given segment and offset into the given vector. Will fill the vector
    /// will all the contents of the segment filled into the vector.
    pub fn readv(&self, cursor: (u64, u64), out: &mut Vec<u8>) -> io::Result<Option<(u64, u64)>> {
        info!("head {} tail {}", self.head_offset, self.tail_offset);
        if cursor.0 < self.head_offset {
            if let Some(Some(mut file)) = self.files.as_ref().map(|map| map.get(&cursor.0)) {
                let file_len = file.metadata()?.len();
                if cursor.1 > file_len {
                    info!("1 invalid offset: segment {} offset {}", cursor.0, cursor.1);
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid offset"));
                }
                file.seek(SeekFrom::Start(cursor.1))?;
                let fill_len = file_len - cursor.1;
                out.extend(&vec![
                    unsafe { MaybeUninit::uninit().assume_init() };
                    fill_len as usize
                ]);
                file.read(&mut out[..])?;
                let mut next_file = cursor.0 + 1;
                if next_file == self.head_offset {
                    return Ok(Some((self.head_offset, 0)));
                }
                let files = self.files.as_ref().unwrap();
                while files.get(&next_file).is_none() {
                    next_file += 1;
                }
                return Ok(Some((next_file, 0)));
            }

            info!(
                "2 invalid segment: file segment {} offset {}, max {:?}",
                cursor.0,
                cursor.1,
                self.files.as_ref().unwrap().keys().collect::<Vec<&u64>>()
            );
            Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid segment"))
        } else if cursor.0 < self.tail_offset {
            if cursor.1 > self.max_segment_size as u64 {
                info!("3 invalid offset: file segment {} offset {}", cursor.0, cursor.1);
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid offset"));
            }
            out.extend_from_slice(&self.segments.get(&cursor.0).unwrap()[cursor.1 as usize..]);
            Ok(Some((cursor.0 + 1, 0)))
        } else if cursor.0 == self.tail_offset {
            if cursor.1 >= self.active_segment.len() as u64 {
                info!("4 invalid offset: file segment {} offset {}", cursor.0, cursor.1);
                Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid offset"))
            } else {
                out.extend_from_slice(&self.active_segment[cursor.1 as usize..]);
                Ok(None)
            }
        } else {
            info!("5 invalid segment: file segment {} offset {}", cursor.0, cursor.1);
            Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid segment"))
        }
    }
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn append_creates_and_deletes_segments_correctly() {
        // remove previous stuff as interferes with tests
        let dir = tempdir().unwrap();

        // segment size max 10K, and max 10 segments in memory.
        let mut log = CommitLog::new(10 * 1024, 10, dir.path(), true).unwrap();

        // 200 iterations appending 1K data each. results in [0..8] in disk, [9..18]
        // in memory, and [19] completely filled and currently the active segment.
        for i in 0..200 {
            let payload = vec![i; 1024];
            log.append(&payload).unwrap();
        }
        assert_eq!(log.active_segment.len(), 10_240);
        assert_eq!(log.files.as_ref().unwrap().len(), 9);
        assert_eq!(log.segments.len(), 10);

        // add 5K more elements. results in [0..9] in disk, [10..19] in memory, and
        // [20] active and half filled.
        for i in 200..205 {
            let payload = vec![i; 1024];
            log.append(&payload).unwrap();
        }
        assert_eq!(log.active_segment.len(), 5120);
        assert_eq!(log.files.as_ref().unwrap().len(), 10);
        assert_eq!(log.segments.len(), 10);

        // data in last file starts with 1024 '90' written, then 1024 '91' and so on.
        let mut data = Vec::new();
        let res = log.readv((9, 0), &mut data).unwrap().unwrap();
        assert_eq!(res.0, 10);
        assert_eq!(data[0], 90);
        assert_eq!(data[1023], 90);
        assert_eq!(data[2047], 91);
        assert_eq!(data[3071], 92);
        assert_eq!(data[4095], 93);
        assert_eq!(data[5119], 94);
        assert_eq!(data[6143], 95);
        assert_eq!(data[7167], 96);
        assert_eq!(data[8191], 97);
        assert_eq!(data[9215], 98);
        assert_eq!(data[10239], 99);
    }

    #[test]
    fn vectored_read_works_as_expected() {
        // remove previous stuff as interferes with tests
        let dir = tempdir().unwrap();

        // segment size max 10K, and max 10 segments in memory.
        let mut log = CommitLog::new(10 * 1024, 10, dir.path(), true).unwrap();

        // 200 iterations appending 1K data each. results in [0..8] in disk, [9..18]
        // in memory, and [19] completely filled and currently the active segment.
        for i in 0..200 {
            let payload = vec![i; 1024];
            log.append(&payload).unwrap();
        }
        assert_eq!(log.active_segment.len(), 10_240);
        assert_eq!(log.files.as_ref().unwrap().len(), 9);
        assert_eq!(log.segments.len(), 10);

        let mut data = Vec::new();
        let next = log.readv((19, 5120), &mut data).unwrap();
        assert_eq!(data.len(), 5120);
        assert_eq!(next, None);
    }

    #[test]
    fn vectored_reads_from_active_segment_resumes_after_empty_reads_correctly() {
        let dir = tempdir().unwrap();

        // segment size max 10K, and max 10 segments in memory.
        let mut log = CommitLog::new(10 * 1024, 10, dir.path(), true).unwrap();

        // [0..7] in memory, active_segment half filled.
        for i in 0..85 {
            let payload = vec![i; 1024];
            log.append(&payload).unwrap();
        }

        // read active segment
        let mut data = Vec::new();
        let _ = log.readv((8, 3072), &mut data).unwrap();
        assert_eq!(data.len(), 2048);
        assert_eq!(data[0], 83);

        data.clear();
        let _ = log.readv((8, 4096), &mut data).unwrap();
        assert_eq!(data.len(), 1024);
        assert_eq!(data[0], 84);

        data.clear();
        let next = log.readv((8, 5119), &mut data).unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0], 84);
        assert_eq!(next, None);

        assert!(log.readv((9, 5120), &mut data).is_err());
    }

    #[test]
    fn last_active_segment_read_jumps_to_next_segment_read_correctly() {
        let dir = tempdir().unwrap();

        // segment size max 10K, and max 10 segments in memory.
        let mut log = CommitLog::new(10 * 1024, 10, dir.path(), true).unwrap();

        // [0..7] in memory, active_segment completely filled.
        for i in 0..90 {
            let payload = vec![i; 1024];
            log.append(&payload).unwrap();
        }

        // read active segment
        let mut data = Vec::new();
        let _ = log.readv((8, 10_239), &mut data).unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0], 89);

        // [0..9] in memory, active_segment completely filled.
        for i in 90..110 {
            let payload = vec![i; 1024];
            log.append(&payload).unwrap();
        }

        data.clear();
        let _ = log.readv((10, 0), &mut data).unwrap();
        assert_eq!(data.len(), 10240);
        assert_eq!(data[0], 100);

        data.clear();
        let next = log.readv((10, 1024 * 9), &mut data).unwrap();
        assert_eq!(data.len(), 1024);
        assert_eq!(data[0], 109);
        assert_eq!(next, None);
    }
}
