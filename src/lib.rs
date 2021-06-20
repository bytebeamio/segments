use std::{collections::VecDeque, io, path::PathBuf};

use bytes::Bytes;

mod disk;
mod segment;
use disk::DiskHandler;
use segment::Segment;

// asdsa
/// asdsadsa

/// The log which can store commits in memory, and push them onto disk when needed, as well as read
/// from disk any valid segment. See [`Self::new`] for more information on how exactly log is
/// stored onto disk.
///
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
    /// Maximum size of any segment in memory.
    max_segment_size: usize,
    /// Maximum number of segments in memory, apart from the active segment.
    max_segments: usize,
    /// The active segment, to which incoming [`Bytes`] are appended to. Note that the bytes are
    /// themselves not mutable.
    active_segment: Segment,
    /// Total size of active segment, used for enforcing the contraints.
    segments: VecDeque<Segment>,
    /// Total size of segments in memory apart from active_segment, used for enforcing the
    /// contraints.
    segments_size: usize,
    /// A set of opened file handles to all the segments stored onto the disk. This is optional.
    disk_handler: Option<DiskHandler>,
}

impl CommitLog {
    /// Create a new `CommitLog` with given contraints. If `None` is passed in for `dir` argument,
    /// there will be no logs on the disk, and when memory limit is reached the segment at
    /// `self.head` will be removed. If a valid path is passed, the directory will be created if
    /// does not exist, and the segment at `self.head` will be stored onto disk instead of simply
    /// being deleted.
    pub fn new(
        max_segment_size: usize,
        max_segments: usize,
        dir: Option<PathBuf>,
    ) -> io::Result<Self> {
        if max_segment_size < 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "minimum 'max_segment_size' should be 1KB, {} given",
                    max_segment_size,
                )
                .as_str(),
            ));
        }

        if let Some(dir) = dir {
            let (head, files) = DiskHandler::new(dir)?;

            return Ok(Self {
                head,
                tail: head,
                max_segment_size,
                max_segments,
                active_segment: Segment::with_capacity(max_segment_size),
                segments: VecDeque::with_capacity(max_segments as usize),
                segments_size: 0,
                disk_handler: Some(files),
            });
        }

        Ok(Self {
            head: 0,
            tail: 0,
            max_segment_size,
            max_segments,
            active_segment: Segment::with_capacity(max_segment_size),
            segments: VecDeque::with_capacity(max_segments as usize),
            segments_size: 0,
            disk_handler: None,
        })
    }

    #[inline]
    pub fn next_offset(&self) -> (u64, u64) {
        if self.active_segment.len() >= self.max_segment_size as u64 {
            (self.tail + 1, 0)
        } else {
            (self.tail, self.active_segment.len())
        }
    }

    /// Get the number of segment on the disk.
    #[inline]
    pub fn disk_len(&self) -> io::Result<u64> {
        Ok(self
            .disk_handler
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "disk log was never opened"))?
            .len())
    }

    #[inline]
    pub fn head_and_tail(&self) -> (u64, u64) {
        (
            match &self.disk_handler {
                Some(handler) => handler.head(),
                None => self.head,
            },
            self.tail,
        )
    }

    /// Append a new [`Bytes`] to the active segment.
    #[inline]
    pub fn append(&mut self, bytes: Bytes) -> io::Result<(u64, u64)> {
        self.apply_retention()?;
        self.active_segment.push(bytes);
        Ok((self.tail, self.active_segment.len() as u64))
    }

    /// Append a new [`Bytes`] to the active segment, with timestamp as given.
    #[inline]
    pub fn append_with_timestamp(
        &mut self,
        bytes: Bytes,
        timestamp: u64,
    ) -> io::Result<(u64, u64)> {
        self.apply_retention()?;
        self.active_segment.push_with_timestamp(bytes, timestamp);
        Ok((self.tail, self.active_segment.len() as u64))
    }

    fn apply_retention(&mut self) -> io::Result<()> {
        if self.active_segment.size() >= self.max_segment_size {
            if self.segments.len() >= self.max_segments {
                // TODO: unwrap might cause error if self.max_segments == 0
                let removed_segment = self.segments.pop_front().unwrap();
                self.segments_size -= removed_segment.size();

                if let Some(files) = self.disk_handler.as_mut() {
                    files.insert(self.head, removed_segment.into_data())?;
                }

                self.head += 1;
            }

            // this replace is cheap as we only swap the 3 pointer that are held by Vec<T>
            let old_segment = std::mem::replace(
                &mut self.active_segment,
                Segment::with_capacity(self.max_segment_size),
            );
            self.segments_size += old_segment.size();
            self.segments.push_back(old_segment);
            self.tail += 1;
        }

        Ok(())
    }

    /// Read a single [`Bytes`] from the logs.
    pub fn read(&self, index: u64, offset: u64) -> io::Result<Bytes> {
        if index > self.tail {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("segment with given index {} not found", index).as_str(),
            ));
        }

        // in disk
        if index < self.head {
            if let Some(handler) = self.disk_handler.as_ref() {
                return handler.read(index, offset);
            }

            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("segment with given index {} not found", index).as_str(),
            ));
        }

        // in memory segment
        if index < self.tail {
            let segment = &self.segments[(index - self.head) as usize];
            return segment.at(index);
        }

        // in active segment
        self.active_segment.at(index)
    }

    pub fn read_with_timestamps(&self, index: u64, offset: u64) -> io::Result<(Bytes, u64)> {
        if index > self.tail {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("segment with given index {} not found", index).as_str(),
            ));
        }

        // in disk
        if index < self.head {
            if let Some(handler) = self.disk_handler.as_ref() {
                return handler.read_with_timestamps(index, offset);
            }

            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("segment with given index {} not found", index).as_str(),
            ));
        }

        // in memory segment
        if index < self.tail {
            let segment = &self.segments[(index - self.head) as usize];
            return segment.at_with_timestamp(index);
        }

        // in active segment
        self.active_segment.at_with_timestamp(index)
    }

    /// Read vector of [`Bytes`] from the logs. Returns a tuple as follows:
    ///
    /// `(data, remaining_len, index, offset)`
    ///
    /// - `data` is the vector of `Bytes` which were read.
    /// - `remaining_len` is the length left from the provided length which we were not able to
    ///   read.
    /// - `index` is the next segment to start reading at.
    /// - `offset` is the next offset within that segment to start reading at.
    ///
    /// Note that `index` and `offset` might currently not exist, and might be corresponding to
    /// future data which will be put in log.
    pub fn readv(
        &self,
        mut index: u64,
        mut offset: u64,
        len: u64,
    ) -> io::Result<(Vec<Bytes>, u64, u64, u64)> {
        if index > self.tail {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("segment with given index {} not found", index).as_str(),
            ));
        }

        let mut remaining_len = len;
        let mut out = Vec::with_capacity(remaining_len as usize);

        if index < self.head {
            if let Some(handler) = self.disk_handler.as_ref() {
                let (new_len, next_index) =
                    handler.readv(index, offset, remaining_len, &mut out)?;

                remaining_len = new_len;
                // start reading from memory in next iteration if no segment left to read on
                // disk
                index = next_index.unwrap_or(self.head);
                // start from beginning of next segment
                offset = 0;
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("segment with given index {} not found", index).as_str(),
                ));
            }
        }

        if remaining_len == 0 {
            return Ok((out, remaining_len, index, offset));
        }

        if index < self.tail {
            let segment = &self.segments[index as usize];
            remaining_len = segment.readv(offset, remaining_len, &mut out)?;
            // read the next segment, or move onto the active segment
            index += 1;
            // start from beginning of next segment
            offset = 0;
        }

        if remaining_len == 0 {
            return Ok((out, remaining_len, index, offset));
        }

        remaining_len = self.active_segment.readv(offset, remaining_len, &mut out)?;

        Ok((out, remaining_len, index, offset))
    }

    pub fn readv_with_timestamps(
        &self,
        mut index: u64,
        mut offset: u64,
        len: u64,
    ) -> io::Result<(Vec<(Bytes, u64)>, u64, u64, u64)> {
        if index > self.tail {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("segment with given index {} not found", index).as_str(),
            ));
        }

        let mut remaining_len = len;
        let mut out = Vec::with_capacity(remaining_len as usize);

        if index < self.head {
            if let Some(handler) = self.disk_handler.as_ref() {
                let (new_len, next_index) =
                    handler.readv_with_timestamps(index, offset, remaining_len, &mut out)?;

                remaining_len = new_len;
                // start reading from memory in next iteration if no segment left to read on
                // disk
                index = next_index.unwrap_or(self.head);
                // start from beginning of next segment
                offset = 0;
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("segment with given index {} not found", index).as_str(),
                ));
            }
        }

        if remaining_len == 0 {
            return Ok((out, remaining_len, index, offset));
        }

        if index < self.tail {
            let segment = &self.segments[index as usize];
            remaining_len = segment.readv_with_timestamps(offset, remaining_len, &mut out)?;
            // read the next segment, or move onto the active segment
            index += 1;
            // start from beginning of next segment
            offset = 0;
        }

        if remaining_len == 0 {
            return Ok((out, remaining_len, index, offset));
        }

        remaining_len =
            self.active_segment
                .readv_with_timestamps(offset, remaining_len, &mut out)?;

        Ok((out, remaining_len, index, offset))
    }

    pub fn index_from_timestamp(&self, timestamp: u64) -> io::Result<(u64, u64)> {
        // beyond even active segment
        if self.active_segment.end_time() < timestamp {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("timestamp {} not contained by any segment", timestamp).as_str(),
            ));
        }

        if self.active_segment.start_time() <= timestamp {
            // found within active segment
            return Ok((
                self.tail,
                self.active_segment.index_from_timestamp(timestamp),
            ));
        }

        if self.segments.len() > 0 && self.segments.front().unwrap().start_time() <= timestamp {
            for (i, segment) in self.segments.iter().enumerate() {
                if segment.start_time() <= timestamp && timestamp <= segment.end_time() {
                    // found within segment in memory
                    return Ok((
                        i as u64 + self.head,
                        segment.index_from_timestamp(timestamp),
                    ));
                }
            }
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("timestamp {} not contained by any segment", timestamp).as_str(),
            ));
        }

        let disk_handler = match self.disk_handler.as_ref() {
            Some(disk_handler) => disk_handler,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("timestamp {} not contained by any segment", timestamp).as_str(),
                ))
            }
        };

        if !disk_handler.is_timestamp_contained(timestamp) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("timestamp {} not contained by any segment", timestamp).as_str(),
            ));
        }

        disk_handler.index_from_timestamp(timestamp)
    }

    pub fn read_from_timestamp(&self, timestamp: u64) -> io::Result<(Bytes, u64)> {
        // beyond even active segment
        if self.active_segment.end_time() < timestamp {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("timestamp {} not contained by any segment", timestamp).as_str(),
            ));
        }

        if self.active_segment.start_time() <= timestamp {
            // found within active segment
            let idx = self.active_segment.index_from_timestamp(timestamp);
            return self.active_segment.at_with_timestamp(idx);
        }

        if self.segments.len() > 0 && self.segments[0].start_time() >= timestamp {
            for segment in self.segments.iter() {
                if segment.start_time() <= timestamp && timestamp <= segment.end_time() {
                    // found within segment in memory
                    let idx = segment.index_from_timestamp(timestamp);
                    return segment.at_with_timestamp(idx);
                }
            }
        }

        let disk_handler = match self.disk_handler.as_ref() {
            Some(disk_handler) => disk_handler,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("timestamp {} not contained by any segment", timestamp).as_str(),
                ))
            }
        };

        if !disk_handler.is_timestamp_contained(timestamp) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("timestamp {} not contained by any segment", timestamp).as_str(),
            ));
        }

        let (segment_idx, offset) = disk_handler.index_from_timestamp(timestamp)?;
        disk_handler.read_with_timestamps(segment_idx, offset)
    }
}

#[cfg(test)]
mod test {
    use bytes::{Bytes, BytesMut};
    use mqttbytes::{
        v4::{read, ConnAck, ConnectReturnCode::Success, Packet, Publish, Subscribe},
        QoS,
    };
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn init_logging() {
        use simplelog::{
            ColorChoice, CombinedLogger, Config, LevelFilter, TermLogger, TerminalMode,
        };
        // Error ignored as will be called in multiple functions, and this causes error if called
        // multiple times.
        let _ = CombinedLogger::init(vec![TermLogger::new(
            LevelFilter::Trace,
            Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        )]);
    }

    // Total size of all packets = 197 bytes
    #[rustfmt::skip]
    #[inline]
    pub(crate) fn random_packets() -> Vec<Packet> {
        vec![
            Packet::Publish  (Publish::new  ("broker1", QoS::AtMostOnce , "ayoad1" )),
            Packet::Publish  (Publish::new  ("brker2" , QoS::AtMostOnce , "pyload2")),
            Packet::Subscribe(Subscribe::new("broker1", QoS::AtMostOnce            )),
            Packet::Publish  (Publish::new  ("brr3"   , QoS::AtMostOnce , "pyload3")),
            Packet::Publish  (Publish::new  ("bruuuu4", QoS::AtMostOnce , "pyload4")),
            Packet::ConnAck  (ConnAck::new  (Success  , true                       )),
            Packet::ConnAck  (ConnAck::new  (Success  , true                       )),
            Packet::Publish  (Publish::new  ("brrrr5" , QoS::AtMostOnce , "paylad5")),
            Packet::ConnAck  (ConnAck::new  (Success  , true                       )),
            Packet::Publish  (Publish::new  ("bro44r6", QoS::AtMostOnce , "aylad6" )),
            Packet::Subscribe(Subscribe::new("broker7", QoS::AtMostOnce            )),
            Packet::Publish  (Publish::new  ("broker7", QoS::AtMostOnce , "paylad7")),
            Packet::Publish  (Publish::new  ("b8"     , QoS::AtMostOnce , "payl8"  )),
            Packet::Subscribe(Subscribe::new("b8"     , QoS::AtMostOnce            )),
            Packet::Subscribe(Subscribe::new("bro44r6", QoS::AtMostOnce            )),
            Packet::ConnAck  (ConnAck::new  (Success  , true                       )),
        ]
    }

    pub(crate) fn random_packets_as_bytes() -> (Vec<Bytes>, usize) {
        let ranpacks = random_packets();
        let mut bytes = Vec::with_capacity(ranpacks.len());
        let mut total_len = 0;
        for packet in ranpacks.into_iter() {
            let mut byte = BytesMut::default();
            match packet {
                Packet::Publish(p) => {
                    p.write(&mut byte).unwrap();
                }
                Packet::Subscribe(p) => {
                    p.write(&mut byte).unwrap();
                }
                Packet::ConnAck(p) => {
                    p.write(&mut byte).unwrap();
                }
                _ => panic!("unexpected packet type"),
            }
            total_len += byte.len();
            bytes.push(byte.freeze());
        }
        (bytes, total_len)
    }

    pub(crate) fn verify_bytes_as_random_packets(bytes: Vec<Bytes>, take: usize) {
        let ranpacks = random_packets();
        for (ranpack, byte) in ranpacks.into_iter().zip(bytes.into_iter()).take(take) {
            let readpack = read(&mut BytesMut::from(byte.as_ref()), byte.len()).unwrap();
            assert_eq!(readpack, ranpack);
        }
    }

    #[test]
    fn active_segment() {
        let (ranpack_bytes, len) = random_packets_as_bytes();
        let mut log = CommitLog::new(len * 10, 10, None).unwrap();

        for _ in 0..5 {
            for byte in ranpack_bytes.clone() {
                log.append(byte).unwrap();
            }
        }

        assert_eq!(log.active_segment.len() as usize, ranpack_bytes.len() * 5);
        assert_eq!(log.active_segment.size() as usize, len * 5);

        for _ in 0..5 {
            for byte in ranpack_bytes.clone() {
                log.append(byte).unwrap();
            }
        }

        assert_eq!(log.active_segment.len() as usize, ranpack_bytes.len() * 10);
        assert_eq!(log.active_segment.size() as usize, len * 10);
    }

    #[test]
    fn memory_segment() {
        let (ranpack_bytes, len) = random_packets_as_bytes();
        let mut log = CommitLog::new(len * 10, 10, None).unwrap();

        for _ in 0..7 {
            for byte in ranpack_bytes.clone() {
                log.append(byte).unwrap();
            }
        }

        assert_eq!(log.active_segment.len() as usize, ranpack_bytes.len() * 7);
        assert_eq!(log.active_segment.size() as usize, len * 7);

        for _ in 0..70 {
            for byte in ranpack_bytes.clone() {
                log.append(byte).unwrap();
            }
        }

        assert_eq!(log.active_segment.len() as usize, ranpack_bytes.len() * 7);
        assert_eq!(log.active_segment.size() as usize, len * 7);
        assert_eq!(log.segments[0].size() as usize, len * 10);
        assert_eq!(log.segments[0].len() as usize, ranpack_bytes.len() * 10);
        assert_eq!(log.segments.len(), 7);
    }

    #[test]
    fn disk_segment() {
        let (ranpack_bytes, len) = random_packets_as_bytes();
        let dir = tempdir().unwrap();
        let mut log = CommitLog::new(len * 10, 5, Some(dir.path().into())).unwrap();

        for _ in 0..5 {
            for byte in ranpack_bytes.clone() {
                log.append(byte).unwrap();
            }
        }

        assert_eq!(log.active_segment.len() as usize, ranpack_bytes.len() * 5);
        assert_eq!(log.active_segment.size() as usize, len * 5);

        for _ in 0..70 {
            for byte in ranpack_bytes.clone() {
                log.append(byte).unwrap();
            }
        }

        assert_eq!(log.active_segment.size() as usize, 5 * len);
        assert_eq!(log.active_segment.len() as usize, 5 * ranpack_bytes.len());
        assert_eq!(log.segments_size, len * 10 * 5);
        assert_eq!(log.disk_handler.unwrap().len(), 2);
    }

    #[test]
    fn read_from_everywhere() {
        let (ranpack_bytes, len) = random_packets_as_bytes();
        let dir = tempdir().unwrap();
        let mut log = CommitLog::new(len * 10, 5, Some(dir.path().into())).unwrap();

        // 160 packets in active_segment, 800 packets in segment, 640 packets in disk
        for _ in 0..100 {
            for byte in ranpack_bytes.clone() {
                log.append(byte).unwrap();
            }
        }

        assert_eq!(log.active_segment.len() as usize, ranpack_bytes.len() * 10);
        assert_eq!(log.segments.len(), 5);
        assert_eq!(log.disk_handler.as_ref().unwrap().len(), 4);
        assert_eq!(log.head_and_tail(), (0, 9));

        let mut offset = 0;
        let mut index = 0;
        for _ in 0..100 {
            let v = log.readv(index, offset, 16).unwrap();
            index = v.1;
            offset = v.2;
            verify_bytes_as_random_packets(v.0, 16);
        }
    }

    #[test]
    fn read_and_append_with_timestamps() {
        let (ranpack_bytes, len) = random_packets_as_bytes();
        let dir = tempdir().unwrap();
        let mut log = CommitLog::new(len * 10, 5, Some(dir.path().into())).unwrap();

        // 160 packets in active_segment, 800 packets in segment, 640 packets in disk = total of
        // 1600 packes.
        // timestamps = segment_id * 1000 + offset * 10;
        for i in 0..100 {
            for (j, byte) in ranpack_bytes.clone().into_iter().enumerate() {
                log.append_with_timestamp(byte, i * 1000 + j as u64 * 10)
                    .unwrap();
            }
        }

        assert_eq!(log.active_segment.len() as usize, ranpack_bytes.len() * 10);
        assert_eq!(log.segments.len(), 5);
        let disk_handler = log.disk_handler.as_ref().unwrap();
        assert_eq!(disk_handler.len(), 4);

        // the segment
        for i in 0..10 {
            for j in 0..10 {
                for k in 0..ranpack_bytes.len() as u64 - 1 {
                    let idx = log
                        .index_from_timestamp(i * 10000 + j * 1000 + k * 10 + 5)
                        .unwrap();
                    assert_eq!(idx.0, i);
                    assert_eq!(idx.1, j * 16 + k + 1);
                }
            }
        }
    }
}
