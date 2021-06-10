use std::{io, path::PathBuf};

use bytes::Bytes;
use fnv::FnvHashMap;

mod disk;
mod segment;
use disk::DiskHandler;
use log::debug;
use segment::Segment;

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
    max_segment_size: u64,
    /// Maximum number of segments in memory, apart from the active segment.
    max_segments: u64,
    /// The active segment, to which incoming [`Bytes`] are appended to. Note that the bytes are
    /// themselves not mutable.
    active_segment: Segment,
    /// Total size of active segment, used for enforcing the contraints.
    segments: FnvHashMap<u64, Segment>,
    /// Total size of segments in memory apart from active_segment, used for enforcing the
    /// contraints.
    segments_size: u64,
    /// A set of opened file handles to all the segments stored onto the disk. This is optional.
    disk_handler: Option<DiskHandler>,
    // TODO: add max_index_file_size?
}

impl CommitLog {
    /// Create a new `CommitLog` with given contraints. If `None` is passed in for `dir` argument,
    /// there will be no logs on the disk, and when memory limit is reached the segment at
    /// `self.head` will be removed. If a valid path is passed, the directory will be created if
    /// does not exist, and the segment at `self.head` will be stored onto disk instead of simply
    /// being deleted.
    pub fn new(max_segment_size: u64, max_segments: u64, dir: Option<PathBuf>) -> io::Result<Self> {
        if max_segment_size < 1024 {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "minimum 'max_segment_size' should be 1KB, {} given",
                    max_segment_size,
                )
                .as_str(),
            ))
        } else if let Some(dir) = dir {
            let (head, files) = DiskHandler::new(dir)?;

            Ok(Self {
                head,
                tail: head,
                max_segment_size,
                max_segments,
                active_segment: Segment::with_capacity(max_segment_size),
                segments: FnvHashMap::default(),
                segments_size: 0,
                disk_handler: Some(files),
            })
        } else {
            Ok(Self {
                head: 0,
                tail: 0,
                max_segment_size,
                max_segments,
                active_segment: Segment::with_capacity(max_segment_size),
                segments: FnvHashMap::default(),
                segments_size: 0,
                disk_handler: None,
            })
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

    /// Append a new [`Bytes`] to the active segment.
    #[inline]
    pub fn append(&mut self, bytes: Bytes) -> io::Result<(u64, u64)> {
        self.apply_retention()?;
        self.active_segment.push(bytes);
        Ok((self.tail, self.active_segment.len() as u64))
    }

    /// Flush the contents onto the disk. Call this before any reads if disk was opened for logs to
    /// avoid missing data.
    #[inline]
    pub fn flush(&mut self) -> io::Result<()> {
        self.disk_handler
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "disk log was never opened"))?
            .flush()
    }

    /// Flush the contents onto the disk for a particular segment.
    #[inline]
    pub fn flush_at(&mut self, index: u64) -> io::Result<()> {
        self.disk_handler
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "disk log was never opened"))?
            .flush_at(index)
    }

    fn apply_retention(&mut self) -> io::Result<()> {
        if self.active_segment.size() >= self.max_segment_size {
            if self.segments.len() as u64 >= self.max_segments {
                let removed_segment = self.segments.remove(&self.head).unwrap();
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
            self.segments.insert(self.tail, old_segment);
            self.tail += 1;
        }

        Ok(())
    }

    /// Read a single [`Bytes`] from the logs.
    ///
    /// #### Note
    /// `read` requires a mutable reference to self as we might need to push data to disk, which
    /// requires mutable access to corresponding file handler.
    pub fn read(&mut self, index: u64, offset: u64) -> io::Result<Bytes> {
        if index < self.head {
            if let Some(handler) = self.disk_handler.as_mut() {
                handler.read(index, offset)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("segment with given index {} not found", index).as_str(),
                ))
            }
        } else if index < self.tail {
            if let Some(segment) = self.segments.get(&index) {
                if index > segment.len() as u64 {
                    Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "byte at offset {} not found for segment at {}",
                            offset, index
                        )
                        .as_str(),
                    ))
                } else {
                    Ok(segment.at(index))
                }
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("segment with given index {} not found", index).as_str(),
                ))
            }
        } else if index == self.tail {
            if index > self.active_segment.len() as u64 {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "byte at offset {} not found for segment at {}",
                        offset, index
                    )
                    .as_str(),
                ))
            } else {
                Ok(self.active_segment.at(index))
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("segment with given index {} not found", index).as_str(),
            ))
        }
    }

    /// Read vector of [`Bytes`] from the logs.
    ///
    /// #### Note
    /// `readv` requires a mutable reference to self as we might need to push data to disk, which
    /// requires mutable access to corresponding file handler.
    pub fn readv(
        &mut self,
        mut index: u64,
        mut offset: u64,
        mut len: u64,
    ) -> io::Result<(Vec<Bytes>, u64, u64, u64)> {
        let mut out = Vec::with_capacity(len as usize);
        loop {
            if index < self.head {
                debug!("disk called");
                if let Some(handler) = self.disk_handler.as_mut() {
                    let (new_len, next_index) = handler.readv(index, offset, len, &mut out)?;
                    len = new_len;
                    // start reading from memory in next iteration if no segment left to read on
                    // disk
                    index = next_index.unwrap_or(self.head);
                    // start from beginning of next segment
                    offset = 0;
                    debug!("disk fine");
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("segment with given index {} not found", index).as_str(),
                    ));
                }
            } else if index < self.tail {
                debug!("segment called");
                let segment = self.segments.get(&index).unwrap();
                if offset >= segment.len() as u64 {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "byte at offset {} not found for segment at {}",
                            offset, index
                        )
                        .as_str(),
                    ));
                }
                len = segment.readv(index, len, &mut out);
                // read the next segment, or move onto the active segment
                index += 1;
                // start from beginning of next segment
                offset = 0;
                debug!("segment fine");
            } else if index == self.tail {
                debug!("active_segment called");
                if offset > self.active_segment.len() as u64 {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "byte at offset {} not found for segment at {}",
                            offset, index
                        )
                        .as_str(),
                    ));
                }
                len = self.active_segment.readv(offset, len, &mut out);
                debug!("active_segment fine");
                // we have read from active segment as well. even if len not satisfied, we can not
                // read further so break anyway.
                break;
            } else {
                // this case only reached when initially provided index was beyond active segment
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("segment with given index {} not found", index).as_str(),
                ));
            }

            // meaning we satisfied the len
            if len == 0 {
                break;
            }
        }

        Ok((out, len, index, offset))
    }
}

#[cfg(test)]
mod test {
    use bytes::{Bytes, BytesMut};
    use log::debug;
    use mqttbytes::{
        v4::{read, ConnAck, ConnectReturnCode::Success, Packet, Publish, Subscribe},
        QoS,
    };
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

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
        let mut log = CommitLog::new(len as u64 * 10, 10, None).unwrap();

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
        let mut log = CommitLog::new(len as u64 * 10, 10, None).unwrap();

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
        assert_eq!(log.segments.get_mut(&0).unwrap().size() as usize, len * 10);
        assert_eq!(
            log.segments.get_mut(&0).unwrap().len() as usize,
            ranpack_bytes.len() * 10
        );
        assert_eq!(log.segments.len(), 7);
    }

    #[test]
    fn disk_segment() {
        let (ranpack_bytes, len) = random_packets_as_bytes();
        let dir = tempdir().unwrap();
        let mut log = CommitLog::new(len as u64 * 10, 5, Some(dir.path().into())).unwrap();

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
        assert_eq!(log.segments_size, len as u64 * 10 * 5);
        assert_eq!(log.disk_handler.unwrap().len(), 2);
    }

    #[test]
    fn read_from_everywhere() {
        init_logging();
        let (ranpack_bytes, len) = random_packets_as_bytes();
        let dir = tempdir().unwrap();
        let mut log = CommitLog::new(len as u64 * 10, 5, Some(dir.path().into())).unwrap();

        // 160 packets in active_segment, 800 packets in segment, 640 packets in disk
        for _ in 0..100 {
            for byte in ranpack_bytes.clone() {
                log.append(byte).unwrap();
            }
        }

        log.disk_handler.as_mut().unwrap().flush().unwrap();

        assert_eq!(log.active_segment.len() as usize, ranpack_bytes.len() * 10);
        assert_eq!(log.segments.len(), 5);
        assert_eq!(log.disk_handler.as_ref().unwrap().len(), 4);

        let mut offset = 0;
        let mut index = 0;
        for i in 0..100 {
            debug!("{}", i);
            let v = log.readv(index, offset, 16).unwrap();
            index = v.1;
            offset = v.2;
            verify_bytes_as_random_packets(v.0, 16);
        }
    }
}
