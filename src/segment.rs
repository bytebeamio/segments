use bytes::Bytes;

pub(super) struct Segment {
    data: Vec<Bytes>,
    size: u64
}

impl Segment {
    #[inline]
    pub(super) fn with_capacity(capacity: u64) -> Self {
        Self { data: Vec::with_capacity(capacity as usize), size: 0 }
    }

    #[inline]
    pub(super) fn push(&mut self, byte: Bytes) {
        self.size += byte.len() as u64;
        self.data.push(byte);
    }

    #[inline]
    pub(super) fn at(&self, index: u64) -> Bytes {
        self.data[index as usize].clone()
    }

    #[inline]
    pub(super) fn len(&self) -> u64 {
        self.data.len() as u64
    }

    #[inline]
    pub(super) fn size(&self) -> u64 {
        self.size
    }

    #[inline]
    pub(super) fn into_data(self) -> Vec<Bytes> {
        self.data
    }

    #[inline]
    pub(super) fn readv(&self, index: u64, len: u64, out: &mut Vec<Bytes>) -> u64 {
        let mut limit = (index + len) as usize;
        let mut left = 0;
        if limit > self.data.len() {
            left = limit - self.data.len();
            limit = self.data.len();
        }
        out.extend_from_slice(&self.data[index as usize..limit]);
        left as u64
    }
}
