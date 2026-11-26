//! What a stream's writer does in the metadata store: epochs, leases and segments.
//!
//! # Why this file exists
//!
//! A stream has one writer at a time, and the metadata store is where that is decided. A writer claims the stream by
//! raising its epoch, and from then on every change it makes to the segment list is valid only while the epoch is still
//! its own. That single condition is what makes a writer that was paused, partitioned or simply slow harmless when it
//! comes back: its writes fail, and it learns that it must stop (design, sections 5 and 9). This file holds the
//! writer's operations and the rules they enforce.
//!
//! # What it contains
//!
//! - `MetadataStore::claim_epoch`: becoming the writer.
//! - `MetadataStore::renew_lease`, `MetadataStore::release_lease`: the courtesy lease.
//! - `MetadataStore::seal_segment`, `MetadataStore::open_segment`, `MetadataStore::roll_segment`: ending a segment,
//!   starting the next, and both in one write.
//! - `require_epoch`, `seal_last_segment`, `open_next_segment`: the rules, as pure functions over a stream record, with
//!   the small checks they are made of (`is_rolled_already`, `require_same_sealed_end`, `unverified_copies`).
//!
//! # How it works
//!
//! Every operation here is `MetadataStore::update_stream` with one of the pure functions as the change. The function
//! first checks that the stream's epoch is the caller's and fails with [`MetaError::Fenced`] otherwise; the conditional
//! write then guarantees that this was still true when the change was applied.
//!
//! **Taking over** is three calls (design, section 9): `claim_epoch` raises the epoch from the value the candidate read
//! to the next one, so of two candidates that read the same epoch only one succeeds; `seal_segment` records where the
//! previous writer's segment ends; `open_segment` starts a new segment on the replica set the controller placed in
//! advance.
//!
//! **Rolling** is one call (design, section 5): `roll_segment` seals the open segment and opens the next in the same
//! write, so there is no moment at which the stream has no open segment and no moment at which it has two.
//!
//! **Repeating** any of them is safe. Sealing again at the same end succeeds, opening again under the same epoch
//! returns the segment that is open, and rolling again returns the segment the first roll opened. A writer that did not
//! hear the answer simply asks again.
//!
//! # Invariants
//!
//! - A stream's epoch only grows, and only by a claim.
//! - Only the holder of the current epoch changes the segment list for writing.
//! - At most the last segment is open, and a segment is authored by exactly one epoch.
//! - A sealed end, once written, never changes.
//! - A new segment starts right after the previous sealed end, on the standby replica set, under an identifier that was
//!   never used before.
//!
//! # See also
//!
//! - `streams.rs`: `MetadataStore::update_stream`, which every operation here goes through.
//! - `docs/protocol/replication.md`: the protocol these operations are the metadata half of.
//! - `bitacora-client/src/writer`: the caller.

use bitacora_types::{Epoch, ReplicaId, SegmentId, SequenceNumber, Timestamp};

use super::MetadataStore;
use crate::error::MetaError;
use crate::records::{CopyRecord, CopyState, SealRecord, SegmentRecord, StreamRecord, WriterLease};

impl MetadataStore {
    /// Claims the stream for a new writer by raising its epoch from `expected_epoch` to the next one (design, section
    /// 9, step 1).
    ///
    /// The previous writer, if any, is fenced from that moment: every later change it attempts fails. Of two candidates
    /// that both read `expected_epoch`, exactly one succeeds.
    ///
    /// # Arguments
    ///
    /// * `name` - The stream's name.
    /// * `expected_epoch` - The epoch the candidate read. The claim is valid only if it is still current.
    /// * `writer_identity` - A name for the new writer, recorded with its lease for operators.
    /// * `lease_expires_at` - When the new writer's lease lapses unless it is renewed.
    ///
    /// # Returns
    ///
    /// The stream's record after the claim, with the epoch the caller now holds.
    ///
    /// # Errors
    ///
    /// * [`MetaError::Fenced`] if the epoch is no longer `expected_epoch`: someone else is ahead, and the caller must
    ///   not proceed.
    /// * [`MetaError::StreamNotFound`] if no stream has this name.
    /// * [`MetaError::TooMuchContention`] or [`MetaError::Unavailable`] if the store could not do it now.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetaError, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::{Epoch, ReplicationSettings, Timestamp};
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # metadata
    /// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    /// #     .await?;
    /// let lease_expires_at = Timestamp::from_unix_millis(10_000);
    ///
    /// let first_claim = metadata
    ///     .claim_epoch("orders", Epoch::ZERO, "compute-1", lease_expires_at)
    ///     .await?;
    /// let late_claim = metadata
    ///     .claim_epoch("orders", Epoch::ZERO, "compute-2", lease_expires_at)
    ///     .await;
    ///
    /// assert_eq!(first_claim.epoch, Epoch::new(1));
    /// assert!(matches!(late_claim, Err(MetaError::Fenced { .. })));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn claim_epoch(
        &self,
        name: &str,
        expected_epoch: Epoch,
        writer_identity: &str,
        lease_expires_at: Timestamp,
    ) -> Result<StreamRecord, MetaError> {
        let (record, ()) = self
            .update_stream(name, |record| {
                require_epoch(record, expected_epoch)?;
                record.epoch = expected_epoch.next();
                record.writer = Some(WriterLease {
                    writer_identity: writer_identity.to_owned(),
                    expires_at: lease_expires_at,
                });
                Ok(())
            })
            .await?;
        Ok(record)
    }

    /// Extends the writer's lease.
    ///
    /// The lease tells other candidates that a writer is alive, so that they wait instead of taking a healthy stream.
    /// If the lease was released, renewing changes nothing.
    ///
    /// # Arguments
    ///
    /// * `name` - The stream's name.
    /// * `epoch` - The epoch the writer holds.
    /// * `lease_expires_at` - The new moment at which the lease lapses.
    ///
    /// # Errors
    ///
    /// * [`MetaError::Fenced`] if `epoch` is no longer current. The writer has lost the stream.
    /// * [`MetaError::StreamNotFound`] if no stream has this name.
    /// * [`MetaError::TooMuchContention`] or [`MetaError::Unavailable`] if the store could not do it now.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::{Epoch, ReplicationSettings, Timestamp};
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # metadata
    /// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    /// #     .await?;
    /// # let first_expiry = Timestamp::from_unix_millis(10_000);
    /// # metadata.claim_epoch("orders", Epoch::ZERO, "compute-1", first_expiry).await?;
    /// let later_expiry = Timestamp::from_unix_millis(20_000);
    ///
    /// metadata.renew_lease("orders", Epoch::new(1), later_expiry).await?;
    ///
    /// let lease = metadata.get_stream("orders").await?.writer.expect("the writer holds a lease");
    /// assert_eq!(lease.expires_at, later_expiry);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn renew_lease(&self, name: &str, epoch: Epoch, lease_expires_at: Timestamp) -> Result<(), MetaError> {
        self.update_stream(name, |record| {
            require_epoch(record, epoch)?;
            if let Some(lease) = &mut record.writer {
                lease.expires_at = lease_expires_at;
            }
            Ok(())
        })
        .await?;
        Ok(())
    }

    /// Gives the lease up, so that the next writer does not wait for it to lapse.
    ///
    /// The epoch stays as it is: it only ever grows.
    ///
    /// # Arguments
    ///
    /// * `name` - The stream's name.
    /// * `epoch` - The epoch the writer holds.
    ///
    /// # Errors
    ///
    /// * [`MetaError::Fenced`] if `epoch` is no longer current.
    /// * [`MetaError::StreamNotFound`] if no stream has this name.
    /// * [`MetaError::TooMuchContention`] or [`MetaError::Unavailable`] if the store could not do it now.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::{Epoch, ReplicationSettings, Timestamp};
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # metadata
    /// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    /// #     .await?;
    /// # let lease_expires_at = Timestamp::from_unix_millis(10_000);
    /// # metadata.claim_epoch("orders", Epoch::ZERO, "compute-1", lease_expires_at).await?;
    /// metadata.release_lease("orders", Epoch::new(1)).await?;
    ///
    /// let stream = metadata.get_stream("orders").await?;
    /// assert_eq!(stream.writer, None);
    /// assert_eq!(stream.epoch, Epoch::new(1));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn release_lease(&self, name: &str, epoch: Epoch) -> Result<(), MetaError> {
        self.update_stream(name, |record| {
            require_epoch(record, epoch)?;
            record.writer = None;
            Ok(())
        })
        .await?;
        Ok(())
    }

    /// Records that the stream's last segment ends at `sealed_end` (design, section 9, step 7).
    ///
    /// Sealing again at the same end succeeds and changes nothing, so a writer that did not hear the answer can ask
    /// again.
    ///
    /// # Arguments
    ///
    /// * `name` - The stream's name.
    /// * `epoch` - The epoch the caller holds.
    /// * `segment_id` - The segment to seal. It must be the stream's last segment.
    /// * `sealed_end` - The segment's final sequence number. One below the segment's first sequence number seals it
    ///   empty; anything lower is refused.
    /// * `sealed_at` - The current time, recorded with the seal. Retention and tiering count from it.
    ///
    /// # Returns
    ///
    /// The stream's record after the seal.
    ///
    /// # Errors
    ///
    /// * [`MetaError::Fenced`] if `epoch` is no longer current.
    /// * [`MetaError::InvalidState`] if `segment_id` is not the last segment, if the segment is already sealed at
    ///   another end, or if `sealed_end` lies more than one below the segment's start.
    /// * [`MetaError::StreamNotFound`] if no stream has this name.
    /// * [`MetaError::TooMuchContention`] or [`MetaError::Unavailable`] if the store could not do it now.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::{Epoch, ReplicaId, ReplicationSettings, SegmentId, SequenceNumber, Timestamp};
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # let standby_replicas = vec![ReplicaId::new("replica-a").expect("a valid identifier")];
    /// # metadata
    /// #     .create_stream(
    /// #         "orders",
    /// #         ReplicationSettings::DEFAULT,
    /// #         RetentionPolicy::default(),
    /// #         Some(standby_replicas),
    /// #     )
    /// #     .await?;
    /// # let lease_expires_at = Timestamp::from_unix_millis(10_000);
    /// # metadata.claim_epoch("orders", Epoch::ZERO, "compute-1", lease_expires_at).await?;
    /// # metadata.open_segment("orders", Epoch::new(1)).await?;
    /// let now = Timestamp::from_unix_millis(5_000);
    ///
    /// let stream = metadata
    ///     .seal_segment("orders", Epoch::new(1), SegmentId::FIRST, SequenceNumber::new(50), now)
    ///     .await?;
    ///
    /// assert_eq!(stream.segments[0].sealed_end(), Some(SequenceNumber::new(50)));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn seal_segment(
        &self,
        name: &str,
        epoch: Epoch,
        segment_id: SegmentId,
        sealed_end: SequenceNumber,
        sealed_at: Timestamp,
    ) -> Result<StreamRecord, MetaError> {
        let (record, ()) = self
            .update_stream(name, |record| {
                require_epoch(record, epoch)?;
                seal_last_segment(record, segment_id, sealed_end, sealed_at)
            })
            .await?;
        Ok(record)
    }

    /// Opens the next segment on the standby replica set (design, section 9, step 8).
    ///
    /// The new segment starts right after the previous sealed end, is authored by `epoch`, and takes the replica set
    /// the controller placed in advance, which is then used up. If the last segment is already open under `epoch`, it
    /// is returned as it is, so a repeated call opens only one segment.
    ///
    /// # Arguments
    ///
    /// * `name` - The stream's name.
    /// * `epoch` - The epoch the caller holds.
    ///
    /// # Returns
    ///
    /// The stream's record after the call, and the segment that is now open.
    ///
    /// # Errors
    ///
    /// * [`MetaError::Fenced`] if `epoch` is no longer current.
    /// * [`MetaError::NoStandbySegment`] if the controller has not placed the next segment yet. Trying again after its
    ///   next pass succeeds.
    /// * [`MetaError::InvalidState`] if the last segment is still open under an older epoch: it must be sealed first.
    /// * [`MetaError::StreamNotFound`] if no stream has this name.
    /// * [`MetaError::TooMuchContention`] or [`MetaError::Unavailable`] if the store could not do it now.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetaError, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::{Epoch, ReplicationSettings, Timestamp};
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # let lease_expires_at = Timestamp::from_unix_millis(10_000);
    /// metadata
    ///     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    ///     .await?;
    /// metadata.claim_epoch("orders", Epoch::ZERO, "compute-1", lease_expires_at).await?;
    ///
    /// let before_the_controller_placed_a_standby = metadata.open_segment("orders", Epoch::new(1)).await;
    ///
    /// assert!(matches!(
    ///     before_the_controller_placed_a_standby,
    ///     Err(MetaError::NoStandbySegment { .. })
    /// ));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open_segment(&self, name: &str, epoch: Epoch) -> Result<(StreamRecord, SegmentRecord), MetaError> {
        self.update_stream(name, |record| {
            require_epoch(record, epoch)?;
            open_next_segment(record, epoch)
        })
        .await
    }

    /// Seals the last segment at `sealed_end` and opens the next one, in one write (design, section 5, "A slow or
    /// failed replica", step 2).
    ///
    /// Either both happen or neither: if no standby replica set is placed, the segment stays open. A repeated call
    /// returns the segment the first one opened.
    ///
    /// # Arguments
    ///
    /// * `name` - The stream's name.
    /// * `epoch` - The epoch the writer holds.
    /// * `segment_id` - The open segment the writer is rolling away from.
    /// * `sealed_end` - Where that segment ends: the writer's durable sequence.
    /// * `sealed_at` - The current time, recorded with the seal.
    ///
    /// # Returns
    ///
    /// The stream's record after the roll, and the segment that is now open.
    ///
    /// # Errors
    ///
    /// As [`MetadataStore::seal_segment`] and [`MetadataStore::open_segment`]. After any of them the stream record is
    /// unchanged.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::{Epoch, ReplicaId, ReplicationSettings, SegmentId, SequenceNumber, Timestamp};
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # let standby_replicas = vec![ReplicaId::new("replica-a").expect("a valid identifier")];
    /// # metadata
    /// #     .create_stream(
    /// #         "orders",
    /// #         ReplicationSettings::DEFAULT,
    /// #         RetentionPolicy::default(),
    /// #         Some(standby_replicas.clone()),
    /// #     )
    /// #     .await?;
    /// # let lease_expires_at = Timestamp::from_unix_millis(10_000);
    /// # metadata.claim_epoch("orders", Epoch::ZERO, "compute-1", lease_expires_at).await?;
    /// # metadata.open_segment("orders", Epoch::new(1)).await?;
    /// # metadata
    /// #     .update_stream("orders", |record| {
    /// #         record.standby_replicas = Some(standby_replicas.clone());
    /// #         Ok(())
    /// #     })
    /// #     .await?;
    /// let now = Timestamp::from_unix_millis(5_000);
    /// let durable_sequence = SequenceNumber::new(50);
    ///
    /// let (_stream, next_segment) = metadata
    ///     .roll_segment("orders", Epoch::new(1), SegmentId::FIRST, durable_sequence, now)
    ///     .await?;
    ///
    /// assert_eq!(next_segment.segment_id, SegmentId::new(2));
    /// assert_eq!(next_segment.first_sequence, SequenceNumber::new(51));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn roll_segment(
        &self,
        name: &str,
        epoch: Epoch,
        segment_id: SegmentId,
        sealed_end: SequenceNumber,
        sealed_at: Timestamp,
    ) -> Result<(StreamRecord, SegmentRecord), MetaError> {
        self.update_stream(name, |record| {
            require_epoch(record, epoch)?;
            if !is_rolled_already(record, segment_id, epoch) {
                seal_last_segment(record, segment_id, sealed_end, sealed_at)?;
            }
            open_next_segment(record, epoch)
        })
        .await
    }
}

/// Checks that the stream's epoch is the one the caller holds.
///
/// # Errors
///
/// Returns [`MetaError::Fenced`], with both epochs, if it is not.
fn require_epoch(record: &StreamRecord, held_epoch: Epoch) -> Result<(), MetaError> {
    if record.epoch == held_epoch {
        Ok(())
    } else {
        Err(MetaError::Fenced {
            held_epoch,
            current_epoch: record.epoch,
        })
    }
}

/// Says whether an earlier roll away from `segment_id` under `epoch` has already been applied: the stream's last
/// segment is then the one that roll opened.
fn is_rolled_already(record: &StreamRecord, segment_id: SegmentId, epoch: Epoch) -> bool {
    let Some(last_segment) = record.last_segment() else {
        return false;
    };
    last_segment.segment_id == segment_id.next() && last_segment.epoch == epoch && last_segment.is_open()
}

/// Seals the stream's last segment at `sealed_end`, in the record given.
///
/// A segment that is already sealed at `sealed_end` is left as it is.
///
/// # Errors
///
/// Returns [`MetaError::InvalidState`] if `segment_id` is not the last segment, if the segment is already sealed at
/// another end, or if `sealed_end` lies more than one below the segment's first sequence number. One below is allowed:
/// that is a segment sealed empty.
fn seal_last_segment(
    record: &mut StreamRecord,
    segment_id: SegmentId,
    sealed_end: SequenceNumber,
    sealed_at: Timestamp,
) -> Result<(), MetaError> {
    let last_segment = match record.segments.last_mut() {
        Some(last_segment) if last_segment.segment_id == segment_id => last_segment,
        _another_segment_or_none => {
            return Err(MetaError::InvalidState {
                reason: format!("segment {segment_id} is not the stream's last segment"),
            });
        }
    };
    if let Some(existing_seal) = last_segment.sealed {
        return require_same_sealed_end(segment_id, existing_seal, sealed_end);
    }
    let end_of_an_empty_segment = last_segment.first_sequence.preceding();
    if sealed_end < end_of_an_empty_segment {
        return Err(MetaError::InvalidState {
            reason: format!(
                "segment {segment_id} starts at {} and cannot end at {sealed_end}",
                last_segment.first_sequence
            ),
        });
    }
    last_segment.sealed = Some(SealRecord {
        end: sealed_end,
        at: sealed_at,
    });
    Ok(())
}

/// Checks a request to seal, at `sealed_end`, a segment that is sealed already.
///
/// # Errors
///
/// Returns [`MetaError::InvalidState`] if the segment was sealed at another end: a sealed end never changes. Sealing
/// again at the same end is the repetition of a request that was already applied.
fn require_same_sealed_end(
    segment_id: SegmentId,
    existing_seal: SealRecord,
    sealed_end: SequenceNumber,
) -> Result<(), MetaError> {
    if existing_seal.end == sealed_end {
        return Ok(());
    }
    Err(MetaError::InvalidState {
        reason: format!(
            "segment {segment_id} is already sealed at {}, not {sealed_end}",
            existing_seal.end
        ),
    })
}

/// Builds the copies of a segment that was just opened on `replicas`: one per replica, none of them verified yet.
fn unverified_copies(replicas: Vec<ReplicaId>) -> Vec<CopyRecord> {
    let mut copies = Vec::with_capacity(replicas.len());
    for replica_id in replicas {
        copies.push(CopyRecord {
            replica_id,
            state: CopyState::Unverified,
        });
    }
    copies
}

/// Opens the stream's next segment under `epoch`, in the record given, and returns it.
///
/// If the last segment is already open under `epoch`, that segment is returned and nothing changes. Otherwise the new
/// segment takes the standby replica set, which is used up, starts right after the previous sealed end, and gets the
/// stream's next unused segment identifier.
///
/// # Errors
///
/// Returns [`MetaError::InvalidState`] if the last segment is still open under another epoch, and
/// [`MetaError::NoStandbySegment`] if no replica set was placed for the next segment.
fn open_next_segment(record: &mut StreamRecord, epoch: Epoch) -> Result<SegmentRecord, MetaError> {
    if let Some(open_segment) = record.open_segment() {
        if open_segment.epoch == epoch {
            return Ok(open_segment.clone());
        }
        return Err(MetaError::InvalidState {
            reason: format!(
                "segment {} is still open under epoch {}",
                open_segment.segment_id, open_segment.epoch
            ),
        });
    }
    let Some(standby_replicas) = record.standby_replicas.take() else {
        return Err(MetaError::NoStandbySegment {
            name: record.name.clone(),
        });
    };
    let opened_segment = SegmentRecord {
        segment_id: record.next_segment_id,
        epoch,
        first_sequence: record.next_segment_first_sequence(),
        sealed: None,
        copies: unverified_copies(standby_replicas),
        tiered: None,
    };
    record.next_segment_id = opened_segment.segment_id.next();
    record.segments.push(opened_segment.clone());
    Ok(opened_segment)
}

#[cfg(test)]
#[path = "writers_test.rs"]
mod tests;
