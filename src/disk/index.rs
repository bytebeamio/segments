use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    mem::{transmute, MaybeUninit},
    os::unix::prelude::FileExt,
    path::Path,
};

/// Size of the offset of packet, in bytes.
const OFFSET_SIZE: u64 = 8;
/// Size of the len of packet, in bytes.
const LEN_SIZE: u64 = 8;
/// Size of the hash of segment file, stored at the start of index file.
const HASH_SIZE: u64 = 32;
/// Size of entry, in bytes.
const ENTRY_SIZE: u64 = OFFSET_SIZE + LEN_SIZE;

/// Wrapper around a index file for convenient reading of bytes sizes.
///
/// Does **not** check any of the constraint enforced by user, or that the index being read from/
/// written to is valid. Simply performs what asked.
///
/// #### Note
/// It is the duty of the handler of this struct to ensure index file's size does not exceed the
/// specified limit.
#[derive(Debug)]
pub(super) struct Index {
    /// The opened index file.
    file: File,
    /// Index at which next call to [`Index::append`] will append to.
    tail: u64,
}

impl Index {
    /// Open a new index file. Does not create a new one.
    #[inline]
    pub(super) fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let tail = file.metadata()?.len() / ENTRY_SIZE;

        Ok(Self { file, tail })
    }

    pub(super) fn new<P: AsRef<Path>>(path: P, hash: &[u8], lens: Vec<u64>) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let tail = lens.len() as u64;
        let mut offset = 0;

        let entries: Vec<u8> = lens
            .into_iter()
            .map(|x| {
                let ret = [offset, x];
                offset += x;
                // SAFETY: we will read back from file in exact same manner. as representation will
                // remain same, we don't need to change the length of vec either.
                unsafe { transmute::<[u64; 2], [u8; 16]>(ret) }
            })
            .flatten()
            .collect();
        file.write_all(&hash[..32])?;
        file.write_all(&entries[..])?;

        Ok(Self { file, tail })
    }

    /// Return the index at which next call to [`Index::append`] will append to.
    #[inline(always)]
    pub(super) fn entries(&self) -> u64 {
        self.tail
    }

    #[inline]
    pub(super) fn read_hash(&self) -> io::Result<[u8; 32]> {
        let mut buf: [u8; 32] = unsafe { MaybeUninit::uninit().assume_init() };
        self.file.read_at(&mut buf, 0)?;
        Ok(buf)
    }

    /// Get the size of packet at the given index, using the index file.
    #[inline]
    pub(super) fn read(&self, index: u64) -> io::Result<[u64; 2]> {
        let mut buf: [u8; 16] = unsafe { MaybeUninit::uninit().assume_init() };
        self.file
            .read_at(&mut buf, HASH_SIZE + ENTRY_SIZE * index)?;
        // SAFETY: we are reading the same number of bytes, and we write in exact same manner.
        Ok(unsafe { transmute::<[u8; 16], [u64; 2]>(buf) })
    }

    /// Get the sizes of packets, starting from the given index upto the given length. If `len` is
    /// larger than number of packets stored in segment, it will return as the 2nd element of the
    /// return tuple the number of packets still left to read.
    #[inline]
    pub(super) fn readv(&self, index: u64, len: u64) -> io::Result<(Vec<[u64; 2]>, u64)> {
        let limit = index + len;
        let (left, len) = if limit > self.tail {
            (
                limit - self.tail,
                ((self.tail - index) * ENTRY_SIZE) as usize,
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

        self.file
            .read_at(buf.as_mut(), HASH_SIZE + ENTRY_SIZE * index)?;

        // SAFETY: needed beacuse of transmute. As new transmuted type is of different length, we
        // need to make sure the length stored in vec also matches.
        unsafe {
            buf.set_len(len / ENTRY_SIZE as usize);
        }

        // SAFETY: we have written to disk in exact same manner.
        Ok((unsafe { transmute::<Vec<u8>, Vec<[u64; 2]>>(buf) }, left))
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
        let index = Index::new(
            dir.path().join(format!("{:020}", 1).as_str()),
            &[1; 32],
            vec![100; 10],
        )
        .unwrap();
        assert_eq!(index.entries(), 10);
        assert_eq!(index.read(9).unwrap(), [900, 100]);
        assert_eq!(index.read_hash().unwrap(), [1; 32]);

        #[rustfmt::skip]
        let index = Index::new(
            dir.path().join(format!("{:020}", 2).as_str()),
            &[2; 32],
            vec![100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                 200, 200, 200, 200, 200, 200, 200, 200, 200, 200,]
            ).unwrap();

        assert_eq!(index.entries(), 20);
        assert_eq!(index.read(19).unwrap(), [2800, 200]);
        assert_eq!(index.read_hash().unwrap(), [2; 32]);

        let (v, _) = index.readv(0, 20).unwrap();
        for i in 0..10 {
            assert_eq!(v[i][0] as usize, 100 * i);
            assert_eq!(v[i][1], 100);
        }
        for i in 10..20 {
            assert_eq!(v[i][0] as usize, 1000 + 200 * (i - 10));
            assert_eq!(v[i][1], 200);
        }
    }

    #[test]
    fn open_and_read_index() {
        let dir = tempdir().unwrap();

        #[rustfmt::skip]
        let index = Index::new(
            dir.path().join(format!("{:020}", 2).as_str()),
            &[2; 32],
            vec![100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                 200, 200, 200, 200, 200, 200, 200, 200, 200, 200,]
            ).unwrap();

        assert_eq!(index.entries(), 20);
        assert_eq!(index.read_hash().unwrap(), [2; 32]);

        drop(index);

        let index = Index::open(dir.path().join(format!("{:020}", 2).as_str())).unwrap();
        assert_eq!(index.read(19).unwrap(), [2800, 200]);
        assert_eq!(index.read_hash().unwrap(), [2; 32]);

        let (v, _) = index.readv(0, 20).unwrap();
        for i in 0..10 {
            assert_eq!(v[i][0] as usize, 100 * i);
            assert_eq!(v[i][1], 100);
        }
        for i in 10..20 {
            assert_eq!(v[i][0] as usize, 1000 + 200 * (i - 10));
            assert_eq!(v[i][1], 200);
        }
    }
}
