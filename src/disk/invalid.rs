use std::path::PathBuf;

// TODO: document this, also also the hierarchy or InvalidType.

#[derive(Debug, Clone, Copy)]
pub enum InvalidType {
    InvalidName,
    NoIndex(u64),
    NoSegment(u64),
    InvalidChecksum(u64),
}

pub(super) struct InvalidFile {
    path: PathBuf,
    error_type: InvalidType,
}
