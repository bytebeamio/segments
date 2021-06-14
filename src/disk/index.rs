use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    mem::{transmute, MaybeUninit},
    path::Path,
};

/// Size of the offset of packet, in bytes.
const OFFSET_SIZE: u64 = 8;
/// Size of the len of packet, in bytes.
const LEN_SIZE: u64 = 8;
/// Size of timestamp appended to each entry, in bytes.
const TIMESTAMP_SIZE: u64 = 8;
/// Size of the hash of segment file, stored at the start of index file.
const HASH_SIZE: u64 = 32;
/// Size of entry, in bytes.
const ENTRY_SIZE: u64 = TIMESTAMP_SIZE + OFFSET_SIZE + LEN_SIZE;

/// Wrapper around a index file for convenient reading of bytes sizes.
///
/// Does **not** check any of the constraint enforced by user, or that the index being read from/
/// written to is valid. Simply performs what asked.
///
///
///### Index file format
///
///The index file starts with the 32-bytes hash of the segment file, followed by entries. Each
///entry consists of 3 u64s, [ timestamp |   offset  |    len    ].
///
/// #### Note
/// It is the duty of the handler of this struct to ensure index file's size does not exceed the
/// specified limit.
#[derive(Debug)]
pub(super) struct Index {
    /// The opened index file.
    file: File,
    /// Number of entries in the index file.
    entries: u64,
}

impl Index {
    /// Open a new index file. Does not create a new one, and throws error if does not exist.
    ///
    /// Note that index file is opened immutably, after writing the given data.
    #[inline]
    pub(super) fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let entries = (file.metadata()?.len() - HASH_SIZE) / ENTRY_SIZE;

        Ok(Self { file, entries })
    }

    /// Create a new index file. Throws error if does not exist. The `info` vector has 2-tuples as
    /// elements, whose 1st element is the length of the packet inserted in segment file, and 2nd
    /// element is timestap in format of time since epoch. The hash may be of any len, but only
    /// starting 32 bytes will be taken.
    ///
    /// Note that index file is opened immutably, after writing the given data.
    pub(super) fn new<P: AsRef<Path>>(
        path: P,
        hash: &[u8],
        info: Vec<(u64, u64)>,
    ) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let tail = info.len() as u64;
        let mut offset = 0;

        let entries: Vec<u8> = info
            .into_iter()
            .map(|(len, timestamp)| {
                let ret = [timestamp, offset, len];
                offset += len;
                // SAFETY: we will read back from file in exact same manner. as representation will
                // remain same, we don't need to change the length of vec either.
                unsafe { transmute::<[u64; 3], [u8; 24]>(ret) }
            })
            .flatten()
            .collect();
        file.write_all(&hash[..32])?;
        file.write_all(&entries[..])?;

        Ok(Self {
            file,
            entries: tail,
        })
    }

    /// Return the index at which next call to [`Index::append`] will append to.
    #[inline(always)]
    pub(super) fn entries(&self) -> u64 {
        self.entries
    }

    /// Read the hash stored in the index file, which is the starting 32 bytes of the file.
    #[inline]
    pub(super) fn read_hash(&self) -> io::Result<[u8; 32]> {
        let mut buf: [u8; 32] = unsafe { MaybeUninit::uninit().assume_init() };
        self.read_at(&mut buf, 0)?;
        Ok(buf)
    }

    /// Get the offset, size of packet at the given index, using the index file.
    #[inline]
    pub(super) fn read(&self, index: u64) -> io::Result<[u64; 2]> {
        let mut buf: [u8; 16] = unsafe { MaybeUninit::uninit().assume_init() };
        self.read_at(&mut buf, HASH_SIZE + ENTRY_SIZE * index + TIMESTAMP_SIZE)?;
        // SAFETY: we are reading the same number of bytes, and we write in exact same manner.
        Ok(unsafe { transmute::<[u8; 16], [u64; 2]>(buf) })
    }

    /// Get the offset, size and the index of the packet at the given index.
    #[inline]
    pub(super) fn read_with_timestamps(&self, index: u64) -> io::Result<[u64; 3]> {
        let mut buf: [u8; 24] = unsafe { MaybeUninit::uninit().assume_init() };
        self.read_at(&mut buf, HASH_SIZE + ENTRY_SIZE * index)?;
        // SAFETY: we are reading the same number of bytes, and we write in exact same manner.
        Ok(unsafe { transmute::<[u8; 24], [u64; 3]>(buf) })
    }

    #[inline]
    pub(super) fn readv(&self, index: u64, len: u64) -> io::Result<(Vec<[u64; 2]>, u64)> {
        self.readv_with_timestamps(index, len).map(|(v, left)| {
            (
                v.into_iter()
                    .map(|reads| [reads[1], reads[2]])
                    .collect::<Vec<[u64; 2]>>(),
                left,
            )
        })
    }

    /// Get the sizes of packets, starting from the given index upto the given length. If `len` is
    /// larger than number of packets stored in segment, it will return as the 2nd element of the
    /// return tuple the number of packets still left to read.
    #[inline]
    pub(super) fn readv_with_timestamps(
        &self,
        index: u64,
        len: u64,
    ) -> io::Result<(Vec<[u64; 3]>, u64)> {
        let limit = index + len;
        let (left, len) = if limit > self.entries {
            (
                limit - self.entries,
                ((self.entries - index) * ENTRY_SIZE) as usize,
            )
        } else {
            (0, (len * ENTRY_SIZE) as usize)
        };

        let mut buf = Vec::with_capacity(len);
        // SAFETY: we have already preallocated the capacity. needed so that `read_at` fills it
        // completely with u8.
        unsafe {
            buf.set_len(len);
        }

        self.read_at(buf.as_mut(), HASH_SIZE + ENTRY_SIZE * index)?;

        // SAFETY: needed beacuse of transmute. As new transmuted type is of different length, we
        // need to make sure the length stored in vec also matches.
        unsafe {
            buf.set_len(len / ENTRY_SIZE as usize);
        }

        // SAFETY: we have written to disk in exact same manner.
        Ok((unsafe { transmute::<Vec<u8>, Vec<[u64; 3]>>(buf) }, left))
    }

    #[allow(unused_mut)]
    #[inline]
    fn read_at(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        #[cfg(target_family = "unix")]
        {
            use std::os::unix::prelude::FileExt;
            self.file.read_exact_at(buf, offset)
        }
        #[cfg(target_family = "windows")]
        {
            use std::os::windows::fs::FileExt;
            while !buf.is_empty() {
                match self.seek_read(buf, offset) {
                    Ok(0) => return Ok(()),
                    Ok(n) => {
                        buf = &mut buf[n..];
                        offset += n as u64;
                    }
                    Err(e) => return Err(e),
                }
            }
            if !buf.is_empty() {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ))
            } else {
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn new_and_read_index() {
        let dir = tempdir().unwrap();

        #[rustfmt::skip]
        let index = Index::new(
            dir.path().join(format!("{:020}", 2).as_str()),
            &[2; 32],
            vec![(100,  1), (100,  2), (100,  3), (100,  4), (100,  5), (100,  6), (100,  7), (100,  8), (100,  9), (100, 10),
                 (200, 11), (200, 12), (200, 13), (200, 14), (200, 15), (200, 16), (200, 17), (200, 18), (200, 19), (200, 20),]
            ).unwrap();

        assert_eq!(index.entries(), 20);
        assert_eq!(index.read(9).unwrap(), [900, 100]);
        assert_eq!(index.read(19).unwrap(), [2800, 200]);
        assert_eq!(index.read_hash().unwrap(), [2; 32]);

        let (v, _) = index.readv_with_timestamps(0, 20).unwrap();
        for i in 0..10 {
            assert_eq!(v[i][0] as usize, (i + 1));
            assert_eq!(v[i][1] as usize, 100 * i);
            assert_eq!(v[i][2], 100);
        }
        for i in 10..20 {
            assert_eq!(v[i][0] as usize, (i + 1));
            assert_eq!(v[i][1] as usize, 1000 + 200 * (i - 10));
            assert_eq!(v[i][2], 200);
        }
    }

    #[test]
    fn open_and_read_index() {
        let dir = tempdir().unwrap();

        #[rustfmt::skip]
        let index = Index::new(
            dir.path().join(format!("{:020}", 2).as_str()),
            &[2; 32],
            vec![(100,  1), (100,  2), (100,  3), (100,  4), (100,  5), (100,  6), (100,  7), (100,  8), (100,  9), (100, 10),
                 (200, 11), (200, 12), (200, 13), (200, 14), (200, 15), (200, 16), (200, 17), (200, 18), (200, 19), (200, 20),]
            ).unwrap();

        assert_eq!(index.entries(), 20);
        assert_eq!(index.read_hash().unwrap(), [2; 32]);

        drop(index);

        let index = Index::open(dir.path().join(format!("{:020}", 2).as_str())).unwrap();
        assert_eq!(index.read(19).unwrap(), [2800, 200]);
        assert_eq!(index.read_hash().unwrap(), [2; 32]);

        let (v, _) = index.readv_with_timestamps(0, 20).unwrap();
        for i in 0..10 {
            assert_eq!(v[i][0] as usize, (i + 1));
            assert_eq!(v[i][1] as usize, 100 * i);
            assert_eq!(v[i][2], 100);
        }
        for i in 10..20 {
            assert_eq!(v[i][0] as usize, (i + 1));
            assert_eq!(v[i][1] as usize, 1000 + 200 * (i - 10));
            assert_eq!(v[i][2], 200);
        }
    }
}
