use bytes::Bytes;

// TODO: document all this

#[derive(Debug)]
pub struct Segment {
    pub data: Vec<Bytes>,
    pub size: u64
}

impl Segment {
    #[inline]
    pub fn with_capacity(capacity: u64) -> Self {
        Self { data: Vec::with_capacity(capacity as usize), size: 0 }
    }

    #[inline]
    pub fn push(&mut self, byte: Bytes) {
        self.size += byte.len() as u64;
        self.data.push(byte);
    }

    #[inline]
    pub fn at(&self, index: u64) -> Bytes {
        self.data[index as usize].clone()
    }

    #[inline]
    pub fn len(&self) -> u64 {
        self.data.len() as u64
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.size
    }

    #[inline]
    pub fn into_data(self) -> Vec<Bytes> {
        self.data
    }

    #[inline]
    pub fn readv(&self, index: u64, len: u64, out: &mut Vec<Bytes>) -> u64 {
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
