//! Segment files: the on-disk form of a contiguous run of a stream's records.
//!
//! A replica stores each segment as one file. This crate owns that file's
//! byte layout, its sparse index, the scan that recovers it after a crash and
//! the scrub that rereads it later. It makes no decisions about ordering,
//! membership or leadership (design, section 7).

mod disk;
pub mod format;
mod local_disk;
mod memory_disk;

pub use disk::{Disk, DiskFile, DiskSpace};
pub use local_disk::LocalDisk;
pub use memory_disk::{DEFAULT_MEMORY_DISK_CAPACITY_BYTES, MemoryDisk};
