//! One replica's copy of one segment: what it holds, and whether it can be trusted.
//!
//! # Why this file exists
//!
//! A replica makes no decisions about ordering, membership or leadership, but it does enforce the handful
//! of local rules that make those decisions safe: accept only batches that continue the copy, refuse
//! writers that were fenced, never serve a record before the writer says it is durable, never forget a
//! fence or a seal (design, sections 5, 7 and 9; `docs/protocol/replication.md`, "A replica"). Those rules
//! are about one copy of one segment, and they need one place that owns the copy's files and everything
//! the copy remembers. [`SegmentCopy`] is that place.
//!
//! # What it contains
//!
//! - [`SegmentCopy`]: the copy, with how it is created, loaded after a restart, described, verified and
//!   deleted.
//! - `Health`: whether the copy is sound, damaged, or was refused a write by its disk.
//! - The operations themselves, one file each in `copy/`: `append.rs` (appends and heartbeats, with
//!   [`Admission`]), `fence_seal.rs`, `read.rs` and `repair.rs` (with [`RepairPace`]).
//!
//! # How it works
//!
//! A copy is up to two files: the records (`SegmentFile`, from the segment crate) and a small state file
//! with the fenced epoch and the sealed end. Either can exist without the other: a replica that is fenced
//! or told of a seal before it ever received a record remembers that, and a copy that was never fenced or
//! sealed has no state file.
//!
//! Besides the files, the copy remembers two things in memory only. The durable sequence it last heard
//! from the writer limits what readers are served; it is safe to lose, because a restarted copy serves
//! nothing until the writer speaks again. Its health says whether the copy may still answer: once a
//! checksum fails or the disk refuses a write, the copy refuses every request and waits to be replaced or
//! for the process to restart.
//!
//! After a restart [`SegmentCopy::load`] reads both files back. A file that cannot be read or verified
//! makes the copy damaged, not absent: it is reported and repaired instead of silently forgotten.
//!
//! # Invariants
//!
//! - A copy has one owner, the task in `actor.rs`, and every operation takes `&mut self` and runs to
//!   completion before the next starts. Nothing here locks, and a request that is dropped by its caller
//!   can never leave a half-applied change.
//! - The fence is never forgotten: every path that rewrites the state keeps or raises the fenced epoch,
//!   also when the records are replaced by a repair.
//! - A copy that is not sound answers nothing but its status. In particular a damaged copy never reports
//!   a local end to a recovering writer, which would count on it to hold everything committed.
//! - Once the disk refused a write, the copy stays refused until the process restarts: what the process
//!   sees of the file may be pages that never reached the disk.
//! - The records file is never deleted or rewritten to repair it. A replacement is built beside it and
//!   moved over it only when complete (`copy/repair.rs`).
//! - The append, heartbeat and read paths run for every request. The helpers in this file that they use
//!   (`fenced_epoch`, `local_end_or`, `fail_if_unsound`) allocate nothing and await nothing.
//!
//! # See also
//!
//! - `copy/append.rs`, `copy/fence_seal.rs`, `copy/read.rs`, `copy/repair.rs`: the operations.
//! - `actor.rs`: the task that owns a copy and runs its operations in arrival order.
//! - `paths.rs`: where the copy's files are.
//! - `bitacora-segment`: the records file, the state file and the disk they are on.

use std::sync::Arc;

use bitacora_segment::{Disk, SegmentError, SegmentFile, SegmentState};
use bitacora_types::replica_api::{ReplicaError, SegmentDescriptor, SegmentStatus};
use bitacora_types::{Epoch, SegmentAddress, SequenceNumber};
use bytes::Bytes;

use crate::paths::{segment_file_path, segment_repair_path, segment_state_path};

mod append;
mod fence_seal;
mod read;
mod repair;

pub(crate) use append::Admission;
pub(crate) use repair::RepairPace;

/// Whether a copy can still be trusted, and if not, why.
///
/// One value instead of a damage reason and a separate flag: a copy that was refused a write is always
/// also unfit to answer, so the two could never disagree, and now cannot.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Health {
    /// Nothing is known to be wrong with the copy.
    Sound,
    /// A checksum failed or a file could not be read. The copy refuses every request and waits to be
    /// replaced by a repair, which makes it sound again.
    Damaged {
        /// What was found wrong, as reported to callers and in the copy's status.
        reason: String,
    },
    /// The disk refused a write or a flush. The copy refuses every request, a repair included, and the
    /// process must stop: after a refused flush the kernel may have dropped the unwritten pages, so the
    /// process can no longer trust what it sees of the file (design, section 7, "Flush policy").
    WriteRefused {
        /// What the disk reported, or what was already wrong with the copy when the disk refused.
        reason: String,
    },
}

/// One replica's copy of one segment.
///
/// Created with [`SegmentCopy::absent`] for a segment the replica has nothing stored for, or with
/// [`SegmentCopy::load`] for what a restarted replica finds on its disk. Its operations are spread over
/// this file and the files in `copy/`.
pub(crate) struct SegmentCopy {
    /// The segment this is a copy of.
    address: SegmentAddress,
    /// The disk the copy's files are on. Shared with the replica's other copies.
    disk: Arc<dyn Disk>,
    /// How much disk the records file reserves when it is created, and grows by when full.
    preallocated_bytes: u64,
    /// The records file. Absent while the replica only holds fencing or sealing state for a segment it
    /// never received records for.
    file: Option<SegmentFile>,
    /// The fenced epoch and the sealed end. In memory this always has a value; whether it is also on the
    /// disk is what `is_state_stored` says.
    state: SegmentState,
    /// Whether `state` has ever been written to the state file. Independent of `file`: all four
    /// combinations of the two happen.
    is_state_stored: bool,
    /// The latest durable sequence heard from the writer. In memory only: after a restart the copy serves
    /// nothing until it hears again.
    known_durable: SequenceNumber,
    /// Whether the copy may still answer requests.
    health: Health,
}

impl SegmentCopy {
    /// Creates the copy of a segment the replica has nothing on disk for yet.
    ///
    /// Touches no disk. The copy's files are created by the first operation that needs them.
    ///
    /// # Arguments
    ///
    /// * `address` - The segment this is a copy of.
    /// * `disk` - The disk the copy's files will be on.
    /// * `preallocated_bytes` - How much disk the records file reserves, and grows by when full.
    ///
    /// # Returns
    ///
    /// A sound copy without records, not fenced and not sealed.
    pub(crate) fn absent(address: SegmentAddress, disk: Arc<dyn Disk>, preallocated_bytes: u64) -> Self {
        Self {
            address,
            disk,
            preallocated_bytes,
            file: None,
            state: SegmentState::default(),
            is_state_stored: false,
            known_durable: SequenceNumber::ZERO,
            health: Health::Sound,
        }
    }

    /// Loads a copy from the disk, as a restarted replica does.
    ///
    /// Reads the state file, then opens the records file, which scans and verifies it. A copy whose files
    /// cannot be read or verified is loaded as damaged, so that it is reported and replaced instead of
    /// silently forgotten.
    ///
    /// # Arguments
    ///
    /// * `address` - The segment to load the copy of.
    /// * `disk` - The disk the copy's files are on.
    /// * `preallocated_bytes` - How much disk the records file grows by when full.
    ///
    /// # Returns
    ///
    /// The copy as found: with or without records, with or without stored state, sound or damaged. It
    /// knows no durable sequence, so it serves no reader until the writer speaks again.
    pub(crate) async fn load(address: SegmentAddress, disk: Arc<dyn Disk>, preallocated_bytes: u64) -> Self {
        let mut copy = Self::absent(address, disk, preallocated_bytes);
        copy.load_state().await;
        copy.load_records().await;
        copy
    }

    /// Reads the state file into the copy. An unreadable or invalid state file makes the copy damaged; an
    /// absent one leaves it never fenced and never sealed.
    async fn load_state(&mut self) {
        let stored_state = match self.disk.read_file(&segment_state_path(self.address)).await {
            Ok(Some(stored_state)) => stored_state,
            Ok(None) => return,
            Err(io_error) => {
                self.mark_damaged(io_error.to_string());
                return;
            }
        };
        match SegmentState::decode(&stored_state) {
            Ok(state) => {
                self.state = state;
                self.is_state_stored = true;
            }
            Err(state_error) => self.mark_damaged(state_error.to_string()),
        }
    }

    /// Opens the records file into the copy. A file that cannot be opened or verified makes the copy
    /// damaged; an absent one leaves it without records.
    async fn load_records(&mut self) {
        let path = segment_file_path(self.address);
        let opened = SegmentFile::open(self.disk.as_ref(), &path, self.address, self.preallocated_bytes).await;
        match opened {
            Ok((file, _why_the_scan_stopped)) => {
                // Without a state file the copy was never fenced or sealed, so the lowest epoch it accepts
                // is the one that created it.
                self.state.fenced_epoch = self.state.fenced_epoch.max(file.header().epoch);
                self.file = Some(file);
            }
            Err(SegmentError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(segment_error) => self.mark_damaged(segment_error.to_string()),
        }
    }

    /// Tells whether anything about this segment is stored on the replica's disk.
    ///
    /// # Returns
    ///
    /// `true` if the copy has a records file or a state file that this process read or wrote. A copy for
    /// which this is `false` is not listed among the replica's segments.
    pub(crate) const fn is_stored(&self) -> bool {
        self.file.is_some() || self.is_state_stored
    }

    /// Reports the last sequence number readers may be served.
    ///
    /// For a sealed copy that is the sealed end; for an open one, the durable sequence last heard from
    /// the writer. Either is capped at what the copy actually holds.
    ///
    /// # Returns
    ///
    /// The sequence number, or `None` if the copy has no records file.
    pub(crate) fn readable_end(&self) -> Option<SequenceNumber> {
        let local_end = self.local_end()?;
        let limit = self.state.sealed_end.unwrap_or(self.known_durable);
        Some(local_end.min(limit))
    }

    /// Reports the segment's final sequence number, if the copy knows the segment is sealed.
    ///
    /// # Returns
    ///
    /// The sealed end, or `None` while the copy has not been told of a seal.
    pub(crate) const fn sealed_end(&self) -> Option<SequenceNumber> {
        self.state.sealed_end
    }

    /// Rereads the whole records file and marks the copy damaged if anything fails its checksum, as the
    /// background scrubber does (design, section 7, "Integrity").
    ///
    /// Reads the file from the disk in chunks. A copy without a records file has nothing to verify.
    pub(crate) async fn verify(&mut self) {
        let Some(file) = &self.file else {
            return;
        };
        if let Err(segment_error) = file.verify().await {
            self.damaged_by(&segment_error);
        }
    }

    /// Reports what the replica holds for this segment.
    ///
    /// Touches no disk: everything reported is kept in memory.
    ///
    /// # Returns
    ///
    /// The copy's end, fenced epoch, sealed end, size, whether it is damaged, and what the header of its
    /// records file says. A copy without a records file reports no end, no header and a size of zero.
    pub(crate) fn status(&self) -> SegmentStatus {
        let mut descriptor = None;
        let mut size_bytes = 0;
        if let Some(file) = &self.file {
            let header = file.header();
            descriptor = Some(SegmentDescriptor {
                address: header.address,
                epoch: header.epoch,
                first_sequence: header.first_sequence,
            });
            size_bytes = file.content_bytes();
        }
        SegmentStatus {
            address: self.address,
            local_end: self.local_end(),
            fenced_epoch: self.fenced_epoch(),
            sealed_end: self.state.sealed_end,
            size_bytes,
            is_damaged: self.health != Health::Sound,
            descriptor,
        }
    }

    /// Removes everything the replica stores for this segment, leaving the copy as if the replica had
    /// never seen the segment.
    ///
    /// Removes the records file, the state file and the scratch file of an interrupted repair, each
    /// durably. The caller stops using the copy afterwards.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaError::Storage`] if the disk refuses to remove a file. The files removed before it
    /// stay removed.
    pub(crate) async fn delete(&mut self) -> Result<(), ReplicaError> {
        self.file = None;
        for path in [
            segment_file_path(self.address),
            segment_state_path(self.address),
            segment_repair_path(self.address),
        ] {
            if let Err(io_error) = self.disk.remove_file(&path).await {
                return Err(ReplicaError::Storage {
                    reason: format!("cannot remove {path}: {io_error}"),
                });
            }
        }
        self.state = SegmentState::default();
        self.is_state_stored = false;
        self.known_durable = SequenceNumber::ZERO;
        self.health = Health::Sound;
        Ok(())
    }

    /// Reports why the disk refused a write or a flush to this copy, if it ever did.
    ///
    /// The task that owns the copy asks after every command, and tells the replica's host, which must
    /// stop the process.
    ///
    /// # Returns
    ///
    /// The reason, or `None` if the disk has refused nothing.
    pub(crate) fn refused_write(&self) -> Option<&str> {
        match &self.health {
            Health::WriteRefused { reason } => Some(reason),
            Health::Sound | Health::Damaged { .. } => None,
        }
    }

    /// The lowest epoch the copy accepts a writer from: the highest of the fence it was given and the
    /// epoch that created its records file, which nothing older may write.
    fn fenced_epoch(&self) -> Epoch {
        self.state.fenced_epoch.max(self.file_epoch())
    }

    /// The epoch that created the records file, or the lowest epoch there is if the copy has none.
    fn file_epoch(&self) -> Epoch {
        match &self.file {
            Some(file) => file.header().epoch,
            None => Epoch::ZERO,
        }
    }

    /// The last sequence number the copy holds, or `None` if it has no records file.
    fn local_end(&self) -> Option<SequenceNumber> {
        self.file.as_ref().map(SegmentFile::local_end)
    }

    /// The last sequence number the copy holds, or `end_of_an_empty_copy` if it has no records file.
    /// Callers pass the position before the segment's first record.
    fn local_end_or(&self, end_of_an_empty_copy: SequenceNumber) -> SequenceNumber {
        match &self.file {
            Some(file) => file.local_end(),
            None => end_of_an_empty_copy,
        }
    }

    /// Refuses the request at hand if the copy is damaged or was refused a write, with the reason.
    fn fail_if_unsound(&self) -> Result<(), ReplicaError> {
        match &self.health {
            Health::Sound => Ok(()),
            Health::Damaged { reason } | Health::WriteRefused { reason } => {
                Err(ReplicaError::Storage { reason: reason.clone() })
            }
        }
    }

    /// Records that something is wrong with the copy. A copy whose disk refused a write stays one: only
    /// its reason is replaced.
    fn mark_damaged(&mut self, reason: String) {
        self.health = match self.health {
            Health::WriteRefused { .. } => Health::WriteRefused { reason },
            Health::Sound | Health::Damaged { .. } => Health::Damaged { reason },
        };
    }

    /// Marks the copy damaged by an error of its records file, logs it, and returns the error to answer
    /// the request with.
    fn damaged_by(&mut self, segment_error: &SegmentError) -> ReplicaError {
        let reason = segment_error.to_string();
        tracing::error!(segment = %self.address, %reason, "segment copy is damaged");
        self.mark_damaged(reason.clone());
        ReplicaError::Storage { reason }
    }

    /// Marks the copy as refused a write by an error of its records file, logs it, and returns the error
    /// to answer the request with. The process must stop.
    fn write_refused_by(&mut self, segment_error: &SegmentError) -> ReplicaError {
        let reason = segment_error.to_string();
        tracing::error!(segment = %self.address, %reason, "segment copy is damaged");
        self.health = Health::WriteRefused { reason: reason.clone() };
        ReplicaError::Storage { reason }
    }

    /// Replaces the state file durably, then adopts the new state in memory.
    ///
    /// The state file is written whole and renamed into place, so after a crash it holds the old state or
    /// the new one. If the disk refuses, the copy is marked as refused a write and keeps its old state.
    async fn store_state(&mut self, new_state: SegmentState) -> Result<(), ReplicaError> {
        let encoded_state = Bytes::copy_from_slice(&new_state.encode());
        let stored = self
            .disk
            .write_file_atomically(&segment_state_path(self.address), encoded_state)
            .await;
        if let Err(io_error) = stored {
            let reason = format!("cannot store segment state: {io_error}");
            self.health = Health::WriteRefused { reason: reason.clone() };
            return Err(ReplicaError::Storage { reason });
        }
        self.state = new_state;
        self.is_state_stored = true;
        Ok(())
    }
}

/// Builds the answer to a request that is wrong in itself, whatever the copy holds.
fn invalid_request(reason: &str) -> ReplicaError {
    ReplicaError::InvalidRequest {
        reason: reason.to_owned(),
    }
}

#[cfg(test)]
#[path = "copy_test.rs"]
mod tests;
