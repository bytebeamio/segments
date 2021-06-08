use std::{fs::{File, OpenOptions}, io::{self, Read, Write, Seek, SeekFrom}};

pub struct Segment {
    file: File,
}
