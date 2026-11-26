//! Segment files: the stored form of a contiguous run of a stream's records.
//!
//! # Why this crate exists
//!
//! A stream is a chain of segments, and a segment is what actually holds records: as a file on each
//! replica's disk while it is recent, and as one object in the object store once it is sealed and tiered
//! (design, sections 7 and 10). The same batch bytes also travel on the wire. One crate has to own those
//! bytes, so that the writer, the replicas, the readers and the tiering task cannot disagree about them,
//! and so that the rules which make a copy survive a crash are written, and tested, once.
//!
//! This crate is that owner. It defines the byte layouts, keeps one copy of a segment on a disk, recovers
//! it after a crash and rereads it to find silent damage. It makes no decision about ordering, membership
//! or leadership: it stores what it is told to store and says truthfully what it holds.
//!
//! # What it contains
//!
//! - [`format`]: the layout of a segment file and of a batch frame, with the strict decoder for messages
//!   and the forgiving scan for recovery.
//! - [`SegmentFile`]: one replica's copy of a segment, with [`SegmentError`].
//! - [`SegmentState`]: the fencing and sealing state kept in a small file next to the copy, with
//!   [`StateError`] and [`STATE_BYTES`].
//! - [`tiered`]: the layout of a sealed segment in the object store.
//! - [`Disk`], [`DiskFile`] and [`DiskSpace`]: the interface every file is reached through.
//! - [`LocalDisk`]: that interface on a directory of the local file system.
//! - [`MemoryDisk`]: that interface in memory, with a crash model and fault switches for tests and the
//!   simulator; [`CrashMode`] says what a crash leaves behind.
//!
//! # How it works
//!
//! The layouts are pure functions over bytes, so they are tested exhaustively without a disk. Everything
//! that touches a file goes through [`Disk`], so the code that orders writes and flushes runs unchanged
//! on a real directory and on the in-memory disk, where a test can crash it at any point and look at what
//! is left.
//!
//! A batch is checksummed once, by the writer, with the segment's address mixed in. The frame is then
//! stored, sent and uploaded as the same bytes, and verified at every hop.
//!
//! # Invariants
//!
//! - The byte layouts are format version 1. `tests/format_golden_test.rs` holds them to bytes written by
//!   that version, and `docs/protocol` describes them for other languages.
//! - No decoder panics on any input; `tests/decoder_fuzz_test.rs` feeds them noise.
//! - Nothing in this crate reads a clock or draws a random number of its own, and only `local_disk.rs`
//!   touches the file system.
//!
//! # See also
//!
//! - `README.md` in this crate: the design, and which file holds what.
//! - `docs/protocol/disk-format.md` and `docs/protocol/tiered-object.md`: the layouts.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

mod disk;
mod file;
pub mod format;
mod index;
mod local_disk;
mod memory_disk;
mod state;
pub mod tiered;

pub use disk::{Disk, DiskFile, DiskSpace};
pub use file::{SegmentError, SegmentFile};
pub use local_disk::LocalDisk;
pub use memory_disk::{CrashMode, DEFAULT_MEMORY_DISK_CAPACITY_BYTES, MemoryDisk};
pub use state::{STATE_BYTES, SegmentState, StateError};
