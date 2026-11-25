//! Segment files: the on-disk form of a contiguous run of a stream's records.
//!
//! A replica stores each segment as one file. This crate owns that file's
//! byte layout, its sparse index, the scan that recovers it after a crash and
//! the scrub that rereads it later. It makes no decisions about ordering,
//! membership or leadership (design, section 7).

mod disk;
mod file;
pub mod format;
mod index;
mod local_disk;
mod memory_disk;
mod state;

pub use disk::{Disk, DiskFile, DiskSpace};
pub use file::{SegmentError, SegmentFile};
pub use local_disk::LocalDisk;
pub use memory_disk::{DEFAULT_MEMORY_DISK_CAPACITY_BYTES, MemoryDisk};
pub use state::{STATE_BYTES, SegmentState, StateError};
