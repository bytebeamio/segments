#[macro_use]
extern crate log;

mod disk;
mod memory;

pub use disk::DiskLog;
pub use memory::MemoryLog;
