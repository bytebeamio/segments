mod segment;

use fnv::FnvHashMap;
use segment::Segment;
use std::fmt::Debug;
use std::mem;

/// Log is an inmemory commitlog (per topic) which splits data in segments.
/// It drops the oldest segment when retention policies are crossed.
/// Each segment is identified by base offset and a new segment is created
/// when ever current segment crosses disk limit
#[derive(Debug)]
pub struct MemoryLog<T> {
    /// First segment
    head: (u64, u64),
    /// Last segment
    tail: (u64, u64),
    /// Maximum size of a segment
    max_segment_size: usize,
    /// Maximum number of segments
    max_segments: usize,
    /// Current active chunk to append
    active_segment: Segment<T>,
    /// All the segments in a ringbuffer
    segments: FnvHashMap<u64, Segment<T>>,
}

impl<T: Debug + Clone> MemoryLog<T> {
    /// Create a new log
    pub fn new(max_segment_size: usize, max_segments: usize) -> MemoryLog<T> {
        if max_segment_size < 1024 {
            panic!("size should be at least 1KB")
        }

        MemoryLog {
            head: (0, 0),
            tail: (0, 0),
            max_segment_size,
            max_segments,
            segments: FnvHashMap::default(),
            active_segment: Segment::new(0),
        }
    }

    pub fn head_and_tail(&self) -> (u64, u64) {
        (self.head.0, self.tail.0)
    }

    /// Appends this record to the tail and returns the offset of this append.
    /// When the current segment is full, this also create a new segment and
    /// writes the record to it.
    /// This function also handles retention by removing head segment
    pub fn append(&mut self, size: usize, record: T) -> (u64, u64) {
        let switch = self.apply_retention();
        let segment_id = self.tail.0;
        let offset = self.active_segment.append(record, size);

        // For debugging during flux. Will be removed later
        if switch {
            // println!("swch. segment = {}, next_offset = {}", segment_id, offset);
        }

        (segment_id, offset)
    }

    fn apply_retention(&mut self) -> bool {
        if self.active_segment.size() >= self.max_segment_size {
            let next_offset = self.active_segment.base_offset() + self.active_segment.len() as u64;
            let last_active = mem::replace(&mut self.active_segment, Segment::new(next_offset));
            self.segments.insert(self.tail.0, last_active);

            // Next tail
            self.tail.0 += 1;
            self.tail.1 = next_offset;

            // if backlog + active segment count is greater than max segments,
            // delete first segment and update head
            if self.segments.len() + 1 > self.max_segments {
                if let Some(segment) = self.segments.remove(&self.head.0) {
                    let next_offset = segment.base_offset() + segment.len() as u64;

                    // Next head
                    self.head.0 += 1;
                    self.head.1 = next_offset;
                }
            }

            return true;
        }

        false
    }

    pub fn next_offset(&self) -> (u64, u64) {
        let segment_id = self.tail.0;
        let next_offset = self.active_segment.base_offset() + self.active_segment.len() as u64;
        (segment_id, next_offset)
    }

    /// Read a record from correct segment
    pub fn read(&mut self, cursor: (u64, u64)) -> Option<T> {
        if cursor.0 == self.tail.0 {
            return self.active_segment.read(cursor.1);
        }

        match self.segments.get(&cursor.0) {
            Some(segment) => segment.read(cursor.1),
            None => None,
        }
    }

    /// Reads multiple packets from the disk and returns base offset and
    /// offset of the next log.
    /// When data of deleted segment is asked, returns data of the current head
    /// **Note**: segment id is used to be able to pull directly from correct segment
    /// **Note**: This method also returns full segment data when requested
    /// data is not of active segment. Set your max_segment size keeping tail
    /// latencies of all the concurrent connections mind
    /// (some runtimes support internal preemption using await points)
    pub fn readv(&mut self, cursor: (u64, u64), out: &mut Vec<T>) -> Option<(u64, u64)> {
        let mut progress = cursor;

        // TODO Fix usize to u64 conversions
        // jump to head if the caller is trying to read deleted segment
        if progress.0 < self.head.0 {
            warn!("Trying to read a deleted segment. Jumping");
            progress = self.head;
        }

        // TODO Cover case where progress.0 is > self.tail.0

        // read from active segment if base offset matches active segment's base offset
        if progress.0 == self.tail.0 {
            let count = self.active_segment.readv(progress.1, out);
            if count == 0 {
                return None;
            }

            progress.1 += count as u64;
            return Some(progress);
        }

        let mut reset_offset = false;
        loop {
            // read from backlog segments
            let segment = match self.segments.get(&progress.0) {
                Some(s) => s,
                None if progress.0 == self.tail.0 => {
                    // If we are jumping to active segment reset offset to start of the segment
                    reset_offset = true;
                    &self.active_segment
                }
                None => {
                    // If we are jumping to new segment reset offset to start of the segment
                    reset_offset = true;
                    progress.0 += 1;
                    continue;
                }
            };

            if reset_offset {
                reset_offset = false;
                progress.1 = segment.base_offset();
            }

            let count = segment.readv(progress.1, out);
            if count > 0 {
                // We always read full segment. So we can always jump to next segment
                progress.0 += 1;
                progress.1 += count as u64;
                return Some(progress);
            }

            // If count is zero and current segment is tail
            if progress.0 == self.tail.0 {
                return None;
            }

            // Jump to the next segment if the above readv return 0 element
            // because of just being at the edge before next segment got
            // added
            // NOTE: This jump is necessary because, readv should always
            // return data if there is data. Or else router registers this
            // for notification even though there is data (which might
            // cause a block)
            progress.0 += 1;
            continue;
        }
    }
}

#[cfg(test)]
mod test {
    use super::MemoryLog;
    use pretty_assertions::assert_eq;

    #[test]
    fn append_creates_and_deletes_segments_correctly() {
        let mut log = MemoryLog::new(10 * 1024, 10);

        // 200 1K iterations. 10 1K records per file. 20 files ignoring deletes.
        // segments: 0.segment (0 - 9), 1.segment (10 -19) .... 19.segment (190 - 200)
        // considering deletes: 10.segment, 11.segment .. 19.segment
        for i in 0..200 {
            let payload = vec![i; 1024];
            log.append(payload.len(), payload);
        }

        // Semi fill 200.segment. Deletes 100.segment
        // considering deletes: 110.segment .. 190.segment
        for i in 200..205 {
            let payload = vec![i; 1024];
            log.append(payload.len(), payload);
        }

        let data = log.read((9, 0));
        assert!(data.is_none());

        // considering: 10.segment (100-109) .. 19.segment (190-199)
        // read segment with base offset 11
        let segment_id = 11;
        let base_offset = 110;
        for i in 0..10 {
            let data = log.read((segment_id, base_offset + i)).unwrap();
            let d = base_offset as u8 + i as u8;
            assert_eq!(data[0], d);
        }

        // read segment with base offset 190 (1 last segment before
        // semi filled segment)
        let segment_id = 19;
        let base_offset = 190;
        for i in 0..10 {
            let data = log.read((segment_id, base_offset + i)).unwrap();
            let d = base_offset as u8 + i as u8;
            assert_eq!(data[0], d);
        }

        // read 200.segment which is semi filled with 5 records
        let segment_id = 20;
        let base_offset = 200;
        for i in 0..5 {
            let data = log.read((segment_id, base_offset + i)).unwrap();
            let d = base_offset as u8 + i as u8;
            assert_eq!(data[0], d);
        }

        let data = log.read((20, base_offset + 5));
        assert!(data.is_none());
    }

    #[test]
    fn vectored_read_works_as_expected() {
        let mut log = MemoryLog::new(10 * 1024, 10);

        // 90 1K iterations. 10 files
        // 0.segment (data with 0 - 9), 1.segment (10 - 19) .... 8.segment (80 - 89)
        // 10K per segment = 10 records per segment
        for i in 0..90 {
            let payload = vec![i; 1024];
            log.append(payload.len(), payload);
        }

        let mut data = Vec::new();
        // Read a segment from start. This returns full segment
        let next = log.readv((0, 0), &mut data);
        assert_eq!(data.len(), 10);
        assert_eq!(next, Some((1, 10)));
        assert_eq!(data[0][0], 0);
        assert_eq!(data[data.len() - 1][0], 9);

        // Read 5.segment
        let data = log.read((5, 50)).unwrap();
        assert_eq!(data[0], 50);

        // Read a segment from the middle. This returns all the remaining elements
        let mut data = Vec::new();
        let next = log.readv((1, 15), &mut data);
        assert_eq!(data.len(), 5);
        assert_eq!(next, Some((2, 20)));
        assert_eq!(data[0][0], 15);
        assert_eq!(data[data.len() - 1][0], 19);

        // Read a segment from scratch. gets full segment
        let mut data = Vec::new();
        let next = log.readv((1, 10), &mut data);
        assert_eq!(data.len(), 10);
        assert_eq!(next, Some((2, 20)));
    }

    #[test]
    fn vectored_reads_from_active_segment_works_as_expected() {
        let mut log = MemoryLog::new(10 * 1024, 10);

        // 200 1K iterations. 10 1K records per file. 20 files ignoring deletes.
        // segments: 0.segment, 1.segment .... 19.segment
        // considering deletes: 10.segment .. 19.segment
        for i in 0..200 {
            let payload = vec![i; 1024];
            log.append(payload.len(), payload);
        }

        // Read active segment. Next shouldn't jump to next segment
        let mut data = Vec::new();
        let next = log.readv((19, 190), &mut data);
        assert_eq!(data.len(), 10);
        assert_eq!(next, Some((19, 200)));
    }

    #[test]
    fn vectored_reads_from_active_segment_resumes_after_empty_reads_correctly() {
        let mut log = MemoryLog::new(10 * 1024, 10);

        // 85 1K iterations. 10 files
        // 0.segment (data with 0 - 9), 1.segment (10 - 19) .... 8.segment (80 - 84)
        // 10 records per segment (1K each)
        // 8.segment is semi filled
        for i in 0..85 {
            let payload = vec![i; 1024];
            log.append(payload.len(), payload);
        }

        // read active segment
        let mut data = Vec::new();
        let next = log.readv((8, 80), &mut data);
        assert_eq!(data.len(), 5);
        assert_eq!(next, Some((8, 85)));

        // fill active segment more
        for i in 85..90 {
            let payload = vec![i; 1024];
            log.append(payload.len(), payload);
        }

        // read active segment
        let mut data = Vec::new();
        let next = log.readv(next.unwrap(), &mut data);
        assert_eq!(data.len(), 5);
        assert_eq!(next, Some((8, 90)));

        let mut data = Vec::new();
        let next = log.readv(next.unwrap(), &mut data);
        assert_eq!(data.len(), 0);
        assert!(next.is_none());
    }

    #[test]
    fn last_active_segment_read_jumps_to_next_segment_read_correctly() {
        let mut log = MemoryLog::new(10 * 1024, 10);

        // 90 1K iterations. 9 files ignoring deletes.
        // 0.segment (data with 0 - 9), 1.segment .... 8.segment (80 - 89)
        // 10K per segment = 10 records per segment
        for i in 0..90 {
            let payload = vec![i; 1024];
            log.append(payload.len(), payload);
        }

        // read active segment. there's no next segment. so active segment
        // is not done yet
        let mut data = Vec::new();
        let next = log.readv((8, 80), &mut data);
        assert_eq!(data.len(), 10);
        assert_eq!(next, Some((8, 90)));

        // append more which also changes active segment to 100.segment
        for i in 90..110 {
            let payload = vec![i; 1024];
            log.append(payload.len(), payload);
        }

        // read from the next offset of previous active segment
        let mut data = Vec::new();
        let next = log.readv(next.unwrap(), &mut data);
        assert_eq!(data.len(), 10);
        assert_eq!(next, Some((10, 100)));

        // read active segment again
        let mut data = Vec::new();
        let next = log.readv(next.unwrap(), &mut data);
        assert_eq!(data.len(), 10);
        assert_eq!(next, Some((10, 110)));

        // read again when there is no more data
        let mut data = Vec::new();
        let next = log.readv(next.unwrap(), &mut data);
        assert_eq!(data.len(), 0);
        assert!(next.is_none());
    }

    #[test]
    fn vectored_read_works_as_expected_2() {
        let mut log = MemoryLog::new(10 * 1024, 100);

        // 15 1K iterations. 1.5 files
        // 0.segment (data with 0 - 9), 1.segment (10 - 15)
        for i in 0..15 {
            let payload = vec![i; 1024];
            log.append(payload.len(), payload);
        }

        let mut data = Vec::new();

        // Read a segment from start. This returns full segment
        let next = log.readv((0, 0), &mut data).unwrap();
        assert_eq!(next, (1, 10));

        let next = log.readv(next, &mut data).unwrap();
        assert_eq!(next, (1, 15));

        // Write again
        for i in 15..50 {
            let payload = vec![i; 1024];
            log.append(payload.len(), payload);
        }

        let next = log.readv(next, &mut data).unwrap();
        assert_eq!(next, (2, 20));

        let next = log.readv(next, &mut data).unwrap();
        assert_eq!(next, (3, 30));

        let next = log.readv(next, &mut data).unwrap();
        assert_eq!(next, (4, 40));

        let next = log.readv(next, &mut data).unwrap();
        assert_eq!(next, (4, 50));

        let next = log.readv(next, &mut data);
        assert!(next.is_none());
    }
}
