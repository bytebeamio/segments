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
mod segment;
use disk::DiskHandler;
use segment::Segment;

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
    pub(crate) active_segment: Segment,
    /// Total size of active segment, used for enforcing the contraints.
    pub(crate) segments: FnvHashMap<u64, Segment>,
    /// Total size of segments in memory apart from active_segment, used for enforcing the
    /// contraints.
    pub(crate) segments_size: u64,
    /// A set of opened file handles to all the segments stored onto the disk. This is optional.
    pub(crate) disk_handler: Option<DiskHandler>,
    // TODO: add max_index_file_size?
}

impl CommitLog {
    pub fn new<P: AsRef<Path>>(
        max_segment_size: u64,
        max_segments: u64,
        dir: Option<P>,
    ) -> io::Result<Self> {
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

    #[inline]
    pub fn append(&mut self, bytes: Bytes) -> io::Result<(u64, u64)> {
        self.apply_retention()?;
        self.active_segment.push(bytes);
        Ok((self.tail, self.active_segment.len() as u64))
    }

    fn apply_retention(&mut self) -> io::Result<()> {
        if self.active_segment.len() > self.max_segment_size {
            if self.segments_size > self.max_segment_size {
                let removed_segment = self.segments.remove(&self.head).unwrap();

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
            self.segments.insert(self.tail, old_segment);
            self.tail += 1;
        }

        Ok(())
    }

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

    pub fn readv(
        &mut self,
        mut index: u64,
        mut offset: u64,
        mut len: u64,
    ) -> io::Result<(Vec<Bytes>, u64, u64, u64)> {
        let mut out = Vec::with_capacity(len as usize);
        loop {
            if index < self.head {
                if let Some(handler) = self.disk_handler.as_mut() {
                    let (new_len, next_index) = handler.readv(index, offset, len, &mut out)?;
                    len = new_len;
                    // start reading from memory in next iteration if no segment left to read on
                    // disk
                    let index = next_index.unwrap_or(self.head);
                    // start from beginning of next segment
                    offset = 0;
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("segment with given index {} not found", index).as_str(),
                    ));
                }
            } else if index < self.tail {
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
            } else if index == self.tail {
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
    use std::{
        fs::{File, OpenOptions},
        io::{Read, Write},
    };

    use mqttbytes::{v4::Publish, QoS};

    use super::*;

    pub fn init_logging() {
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

    #[rustfmt::skip]
    pub fn random_packets() -> Vec<Publish> {
        vec![
            Publish::new("broker1", QoS::AtLeastOnce, "payload1".to_string()),
            Publish::new("broker2", QoS::ExactlyOnce, "payload2".to_string()),
            Publish::new("broker3", QoS::AtMostOnce , "payload3".to_string()),
            Publish::new("broker4", QoS::AtMostOnce , "payload4".to_string()),
            Publish::new("broker5", QoS::AtLeastOnce, "payload5".to_string()),
        ]
    }

    // TODO: write these tests

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
