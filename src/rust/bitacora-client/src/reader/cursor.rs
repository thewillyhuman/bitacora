//! A reader's position in a stream, and how the records at it are fetched.
//!
//! # Why this file exists
//!
//! Both ways of reading, a bounded range and following the tail, are the same step repeated: fetch the committed
//! records at a position, then move past them (design, section 6). Everything that is difficult about reading is inside
//! that step: finding the segment that holds the position, choosing a replica, switching when one fails or holds a
//! short copy, falling back to the object store for a tiered segment, and noticing seals, rolls and truncation. This
//! file is that step, so that the handles in `../reader.rs` are only loops around it.
//!
//! # What it contains
//!
//! - [`Cursor`]: the position, the stream's metadata as last read, and the connections kept between fetches.
//! - [`Fetched`]: what one fetch brings back.
//! - `SegmentAnswer`, `Round`: what the replicas of a segment answered together, and how one round of a fetch ended.
//! - The decisions that need no I/O, as plain functions: `known_durable_sequence`, `are_the_replicas_of`,
//!   `reason_to_try_again`.
//!
//! # How it works
//!
//! A fetch is a loop of rounds. One round:
//!
//! 1. Fails if the position is below the low-water mark.
//! 2. Looks for the segment that holds the position in the metadata as last read. If there is none, the metadata is
//!    read again, once; if there is still none, nothing is committed at the position yet.
//! 3. Asks the segment's replicas, starting with the one that had records last time. Any single replica can serve a
//!    read, because a replica only serves records at or below the durable sequence, and within one segment there is
//!    only one possible record at each sequence number. The first replica with records ends the fetch.
//! 4. If no replica had records and the segment was uploaded, reads from the object store.
//! 5. If that brought nothing either, reads the metadata again, because the segment may have been sealed, repaired,
//!    tiered or followed by another, and reports why the round failed.
//!
//! A failed round is followed by a pause of one retry interval and another round, up to `max_attempts_per_read`.
//!
//! A follower passes a tail wait: how long a replica may hold the request when nothing is committed yet. It then asks
//! one replica per fetch. When that replica has nothing new at the tail of the open segment the fetch ends, and the
//! next one starts with its neighbour, so that a replica cut off from the writer cannot starve the follower.
//!
//! # Invariants
//!
//! - The position only moves forward, and only past records that were returned.
//! - The records of one fetch are consecutive and start at the position (`answer.rs` and `../tiered.rs` enforce it).
//! - `highest_readable_end` never moves backwards.
//! - A copy the metadata marks as bad is never asked.
//! - The common fetch is short on purpose: when the preferred replica has records, it is one request, with no look at
//!   the metadata store and no new connection. It clones the segment's record and the list of connections once per
//!   fetch, never per record. Reading the metadata again, reconnecting and the object store happen only when that
//!   request brings nothing or the cursor crosses into another segment.
//!
//! # See also
//!
//! - `../reader.rs`: the handles that drive the cursor.
//! - `answer.rs`: what one replica's answer means.
//! - `../tiered.rs`: reading a sealed segment from the object store.
//! - `docs/protocol/replication.md`, "A reader".

use std::sync::Arc;
use std::time::Duration;

use bitacora_artifact::ArtifactStore;
use bitacora_meta::{CopyRecord, CopyState, SegmentRecord, StreamRecord, TieredRecord};
use bitacora_types::replica_api::{ReadLimit, ReadRequest};
use bitacora_types::{LogRecord, SegmentAddress, SegmentId, SequenceNumber};
use tokio::time::{sleep, timeout};

use super::answer::{ReplicaAnswer, classify_response};
use crate::context::{ClientContext, ConnectedReplica};
use crate::error::ReadError;
use crate::settings::ReaderSettings;
use crate::tiered::TieredSegment;

/// What one fetch brought back.
pub(super) enum Fetched {
    /// One or more records, starting at the position the cursor had. The cursor is now past them.
    Records(Vec<LogRecord>),
    /// No committed record exists at the cursor's position yet.
    NothingNew,
}

/// What the replicas of a segment answered, together, for the cursor's position.
enum SegmentAnswer {
    /// One of them had records.
    Records(Vec<LogRecord>),
    /// A follower's replica of the open segment has nothing new yet.
    NothingNewAtTheTail,
    /// The segment is sealed and the cursor is past its end.
    SegmentFinished,
    /// None of them had records.
    NoRecords {
        /// Why the last replica that failed did; `None` if they all answered and simply held nothing.
        last_failure: Option<String>,
    },
}

/// How one round of a fetch ended.
enum Round {
    /// The fetch is over.
    Done(Fetched),
    /// The metadata was read again because what the cursor held did not lead to records. The next round looks for the
    /// segment once more.
    MetadataRefreshed,
    /// Neither the replicas nor the object store had records for the position.
    Failed {
        /// The segment that holds the position.
        segment: SegmentAddress,
        /// Why the last source that was asked failed.
        reason: String,
    },
}

/// The replicas of the segment a cursor is reading, connected.
struct ConnectedSegment {
    /// The segment.
    segment_id: SegmentId,
    /// Handles to its replicas, in the order of the segment's copies.
    replicas: Vec<ConnectedReplica>,
}

/// The object of the tiered segment a cursor is reading from the object store.
struct OpenTieredSegment {
    /// The segment.
    segment_id: SegmentId,
    /// Its object, opened: the footer and the index were fetched and verified.
    object: TieredSegment,
}

/// A position in a stream, with what is needed to read on from it.
pub(super) struct Cursor {
    /// The injected interfaces of the client.
    context: Arc<ClientContext>,
    /// The reader's settings.
    settings: ReaderSettings,
    /// The stream's name.
    stream_name: String,
    /// The sequence number of the next record to hand out.
    next_sequence: SequenceNumber,
    /// The stream's metadata as last read. Sealed segments never change, so only what it says about the open segment
    /// can be stale.
    stream: StreamRecord,
    /// The replicas of the segment being read, kept so that a fetch does not look them up again.
    connected_segment: Option<ConnectedSegment>,
    /// Which replica of the segment is asked first.
    preferred_replica: usize,
    /// The object of the tiered segment being read from the object store, kept open so that its footer and index are
    /// fetched once.
    open_tiered_segment: Option<OpenTieredSegment>,
    /// The highest position a replica has said it may serve: committed for certain, though the stream may be further
    /// along.
    highest_readable_end: SequenceNumber,
}

impl Cursor {
    /// Places a cursor at `from_sequence` of a stream.
    ///
    /// Reads the stream's metadata once. A position before the first record is moved to the first record, which is the
    /// earliest thing anyone can ask for.
    ///
    /// # Arguments
    ///
    /// * `context` - The injected interfaces of the client.
    /// * `settings` - The reader's settings.
    /// * `stream_name` - The stream to read.
    /// * `from_sequence` - The sequence number of the first record wanted.
    ///
    /// # Returns
    ///
    /// The cursor, connected to nothing yet.
    ///
    /// # Errors
    ///
    /// Returns [`ReadError::Metadata`] if the stream does not exist or the metadata store cannot be reached.
    pub(super) async fn open(
        context: &Arc<ClientContext>,
        settings: &ReaderSettings,
        stream_name: &str,
        from_sequence: SequenceNumber,
    ) -> Result<Self, ReadError> {
        let stream = context.metadata.get_stream(stream_name).await?;
        Ok(Self {
            context: Arc::clone(context),
            settings: settings.clone(),
            stream_name: stream_name.to_owned(),
            next_sequence: from_sequence.max(SequenceNumber::FIRST),
            stream,
            connected_segment: None,
            preferred_replica: 0,
            open_tiered_segment: None,
            highest_readable_end: SequenceNumber::ZERO,
        })
    }

    /// The cursor's position.
    ///
    /// # Returns
    ///
    /// The sequence number of the next record the cursor will hand out.
    pub(super) const fn next_sequence(&self) -> SequenceNumber {
        self.next_sequence
    }

    /// The settings the cursor reads with.
    ///
    /// # Returns
    ///
    /// The reader's settings, as the cursor was opened with them.
    pub(super) const fn settings(&self) -> &ReaderSettings {
        &self.settings
    }

    /// The stream the cursor reads.
    ///
    /// # Returns
    ///
    /// The stream's name.
    pub(super) fn stream_name(&self) -> &str {
        &self.stream_name
    }

    /// The highest sequence number known to be committed.
    ///
    /// # Returns
    ///
    /// The furthest a replica has served up to, or the end of the last sealed segment in the metadata as last read,
    /// whichever is later. The position before the first record if the cursor has learned neither yet.
    pub(super) fn known_durable_sequence(&self) -> SequenceNumber {
        known_durable_sequence(self.highest_readable_end, &self.stream.segments)
    }

    /// Fetches the records at the cursor's position and moves past them.
    ///
    /// See the file header for the rounds a fetch goes through.
    ///
    /// # Arguments
    ///
    /// * `tail_wait` - How long a replica may hold the request when nothing is committed yet. `None` asks every replica
    ///   once without waiting.
    ///
    /// # Returns
    ///
    /// The records, or [`Fetched::NothingNew`] if no committed record exists at the position yet.
    ///
    /// # Errors
    ///
    /// * [`ReadError::Truncated`] if the position is below the stream's low-water mark.
    /// * [`ReadError::Unavailable`] if the records exist but neither a replica nor the object store served them in
    ///   `max_attempts_per_read` rounds.
    /// * [`ReadError::Metadata`] if the metadata store failed.
    pub(super) async fn fetch(&mut self, tail_wait: Option<Duration>) -> Result<Fetched, ReadError> {
        let mut has_refreshed = false;
        let mut failed_rounds = 0;
        loop {
            match self.fetch_round(tail_wait, has_refreshed).await? {
                Round::Done(fetched) => return Ok(fetched),
                Round::MetadataRefreshed => has_refreshed = true,
                Round::Failed { segment, reason } => {
                    failed_rounds += 1;
                    if failed_rounds >= self.settings.max_attempts_per_read {
                        return Err(ReadError::Unavailable { segment, reason });
                    }
                    sleep(self.settings.retry_interval).await;
                }
            }
        }
    }

    /// One round of a fetch: finds the segment that holds the position and asks its replicas, then the object store.
    ///
    /// `has_refreshed` says whether an earlier round of this fetch already read the metadata again; a position that no
    /// segment holds even then has nothing committed at it.
    async fn fetch_round(&mut self, tail_wait: Option<Duration>, has_refreshed: bool) -> Result<Round, ReadError> {
        if self.next_sequence < self.stream.low_water_mark {
            return Err(ReadError::Truncated {
                low_water_mark: self.stream.low_water_mark,
            });
        }
        let Some(segment) = self.stream.segment_containing(self.next_sequence).cloned() else {
            // No known segment holds the position: either the stream ends here for now, or a segment was opened since.
            if has_refreshed {
                return Ok(Round::Done(Fetched::NothingNew));
            }
            self.refresh().await?;
            return Ok(Round::MetadataRefreshed);
        };
        let replicas = self.replicas_of(&segment).await?;
        match self.ask_replicas(&segment, &replicas, tail_wait).await {
            SegmentAnswer::Records(records) => Ok(Round::Done(Fetched::Records(records))),
            SegmentAnswer::NothingNewAtTheTail => {
                // The refresh notices a seal.
                self.refresh().await?;
                Ok(Round::Done(Fetched::NothingNew))
            }
            SegmentAnswer::SegmentFinished => {
                self.refresh().await?;
                Ok(Round::MetadataRefreshed)
            }
            SegmentAnswer::NoRecords { last_failure } => self.fall_back(&segment, last_failure).await,
        }
    }

    /// What a round does when no replica of `segment` produced records: reads from the object store if the segment was
    /// uploaded, and otherwise reads the metadata again and says why the round failed.
    ///
    /// `replicas_failure` is why the last replica that failed did, if any did.
    async fn fall_back(
        &mut self,
        segment: &SegmentRecord,
        replicas_failure: Option<String>,
    ) -> Result<Round, ReadError> {
        let mut last_failure = replicas_failure;
        if let Some(tiered) = &segment.tiered {
            match self.fetch_tiered(segment, tiered).await {
                Ok(records) => return Ok(Round::Done(Fetched::Records(records))),
                Err(reason) => {
                    tracing::debug!(segment = %segment.segment_id, %reason, "read from the object store failed");
                    last_failure = Some(reason);
                }
            }
        }

        // Look at the metadata again: the segment may have been sealed, repaired, tiered or followed by another.
        self.refresh().await?;
        let is_still_open = self
            .stream
            .segment_containing(self.next_sequence)
            .is_some_and(SegmentRecord::is_open);
        match reason_to_try_again(last_failure, is_still_open) {
            Some(reason) => Ok(Round::Failed {
                segment: segment.address(self.stream.stream_id),
                reason,
            }),
            None => Ok(Round::Done(Fetched::NothingNew)),
        }
    }

    /// Asks the replicas of `segment` for the records at the cursor's position, starting with the preferred one, until
    /// one has them. Moves the cursor past the records it returns.
    async fn ask_replicas(
        &mut self,
        segment: &SegmentRecord,
        replicas: &[ConnectedReplica],
        tail_wait: Option<Duration>,
    ) -> SegmentAnswer {
        let mut last_failure = None;
        for attempt in 0..replicas.len() {
            let replica_index = (self.preferred_replica + attempt) % replicas.len();
            if segment.copies[replica_index].state == CopyState::Bad {
                continue;
            }
            match self.ask(&replicas[replica_index], segment, tail_wait).await {
                ReplicaAnswer::Records(records) => {
                    self.preferred_replica = replica_index;
                    if let Some(last_record) = records.last() {
                        self.next_sequence = last_record.sequence.next();
                    }
                    return SegmentAnswer::Records(records);
                }
                ReplicaAnswer::SegmentFinished => return SegmentAnswer::SegmentFinished,
                ReplicaAnswer::Nothing if segment.is_open() && tail_wait.is_some() => {
                    // A follower asks one replica per wait. If this one is cut off from the writer, the next call asks
                    // its neighbour.
                    self.preferred_replica = (replica_index + 1) % replicas.len();
                    return SegmentAnswer::NothingNewAtTheTail;
                }
                ReplicaAnswer::Nothing => {}
                ReplicaAnswer::Failed(reason) => {
                    let replica = &replicas[replica_index].replica_id;
                    tracing::debug!(%replica, %reason, "read failed, trying another replica");
                    last_failure = Some(reason);
                }
            }
        }
        SegmentAnswer::NoRecords { last_failure }
    }

    /// Sends one read for the cursor's position to one replica and decides what its answer means.
    ///
    /// The request is bounded by the request timeout, plus the tail wait when the replica may hold it. What the replica
    /// says it may serve is remembered as known to be committed.
    async fn ask(
        &mut self,
        replica: &ConnectedReplica,
        segment: &SegmentRecord,
        tail_wait: Option<Duration>,
    ) -> ReplicaAnswer {
        let address = segment.address(self.stream.stream_id);
        // A sealed segment has nothing to wait for.
        let wait_for_records = if segment.is_open() { tail_wait } else { None };
        let request = ReadRequest {
            segment: address,
            from_sequence: self.next_sequence,
            max_bytes: self.settings.max_response_bytes,
            wait_for_records,
            limit: ReadLimit::DurableSequence,
        };
        let longest_answer_time = self.settings.request_timeout + wait_for_records.unwrap_or_default();
        let response = match timeout(longest_answer_time, replica.client.read(request)).await {
            Ok(Ok(response)) => response,
            Ok(Err(replica_error)) => return ReplicaAnswer::Failed(replica_error.to_string()),
            Err(_elapsed) => return ReplicaAnswer::Failed("request timed out".to_owned()),
        };
        self.highest_readable_end = self.highest_readable_end.max(response.readable_end);
        classify_response(address, &response, self.next_sequence, segment.sealed_end())
    }

    /// Reads the records at the cursor's position from the object store and moves past them.
    ///
    /// Fails with the reason if the object store cannot serve them. After a failed read the object is opened afresh
    /// next time, because it may have been replaced by a new upload.
    async fn fetch_tiered(&mut self, segment: &SegmentRecord, tiered: &TieredRecord) -> Result<Vec<LogRecord>, String> {
        let Some(artifact_store) = self.context.artifact_store.clone() else {
            return Err("the segment is in the object store, and this client was given none".to_owned());
        };
        let Some(sealed_end) = segment.sealed_end() else {
            return Err("the segment is recorded as tiered but not as sealed".to_owned());
        };
        self.open_tiered_object(artifact_store.as_ref(), segment, sealed_end, tiered)
            .await?;
        let Some(open_tiered_segment) = &self.open_tiered_segment else {
            return Err("the tiered segment was not opened".to_owned());
        };
        let fetched = open_tiered_segment
            .object
            .read_from(
                artifact_store.as_ref(),
                self.next_sequence,
                self.settings.max_response_bytes,
            )
            .await;
        let records = match fetched {
            Ok(records) => records,
            Err(tiered_error) => {
                self.open_tiered_segment = None;
                return Err(tiered_error.to_string());
            }
        };
        let Some(last_record) = records.last() else {
            return Err(format!("the object holds no record at {}", self.next_sequence));
        };
        self.next_sequence = last_record.sequence.next();
        Ok(records)
    }

    /// Opens the object of `segment` in `artifact_store`, unless it is the one the cursor already holds open.
    ///
    /// Fails with the reason if the object cannot be opened as this segment sealed at `sealed_end`.
    async fn open_tiered_object(
        &mut self,
        artifact_store: &dyn ArtifactStore,
        segment: &SegmentRecord,
        sealed_end: SequenceNumber,
        tiered: &TieredRecord,
    ) -> Result<(), String> {
        let is_open = self
            .open_tiered_segment
            .as_ref()
            .is_some_and(|open_tiered_segment| open_tiered_segment.segment_id == segment.segment_id);
        if is_open {
            return Ok(());
        }
        let opened = TieredSegment::open(
            artifact_store,
            segment.address(self.stream.stream_id),
            sealed_end,
            tiered,
            self.settings.request_timeout,
        )
        .await;
        match opened {
            Ok(object) => {
                self.open_tiered_segment = Some(OpenTieredSegment {
                    segment_id: segment.segment_id,
                    object,
                });
                Ok(())
            }
            Err(tiered_error) => Err(tiered_error.to_string()),
        }
    }

    /// Returns handles to the replicas of `segment`, connecting when the cursor moves to another segment or the
    /// segment's replica set changed. On another segment the first replica is preferred again.
    async fn replicas_of(&mut self, segment: &SegmentRecord) -> Result<Vec<ConnectedReplica>, ReadError> {
        if let Some(connected_segment) = &self.connected_segment
            && connected_segment.segment_id == segment.segment_id
            && are_the_replicas_of(&connected_segment.replicas, &segment.copies)
        {
            return Ok(connected_segment.replicas.clone());
        }
        let is_another_segment = self
            .connected_segment
            .as_ref()
            .is_none_or(|connected_segment| connected_segment.segment_id != segment.segment_id);
        if is_another_segment {
            self.preferred_replica = 0;
        }
        let replicas = self.context.connect_copies(&segment.copies).await?;
        self.connected_segment = Some(ConnectedSegment {
            segment_id: segment.segment_id,
            replicas: replicas.clone(),
        });
        Ok(replicas)
    }

    /// Reads the stream's metadata again.
    async fn refresh(&mut self) -> Result<(), ReadError> {
        self.stream = self.context.metadata.get_stream(&self.stream_name).await?;
        Ok(())
    }
}

/// The highest sequence number known to be committed, given the furthest a replica has served up to and the segments of
/// the stream as last read: the later of `highest_readable_end` and the end of the last sealed segment.
fn known_durable_sequence(highest_readable_end: SequenceNumber, segments: &[SegmentRecord]) -> SequenceNumber {
    let mut known_durable = highest_readable_end;
    for segment in segments {
        if let Some(sealed_end) = segment.sealed_end() {
            known_durable = known_durable.max(sealed_end);
        }
    }
    known_durable
}

/// Whether `replicas` are connections to exactly the replicas that hold `copies`, in the same order.
fn are_the_replicas_of(replicas: &[ConnectedReplica], copies: &[CopyRecord]) -> bool {
    if replicas.len() != copies.len() {
        return false;
    }
    for (replica, copy) in replicas.iter().zip(copies) {
        if replica.replica_id != copy.replica_id {
            return false;
        }
    }
    true
}

/// Why a round that brought no records counts as failed, or `None` if it did not fail: every source answered, and the
/// position is in the open segment, where nothing is committed yet.
///
/// `last_failure` is why the last source that failed did. Without a failure, a position in a sealed segment means that
/// every copy asked was short: the reader waits for repair like for a failed replica.
fn reason_to_try_again(last_failure: Option<String>, is_position_in_open_segment: bool) -> Option<String> {
    match last_failure {
        Some(reason) => Some(reason),
        None if is_position_in_open_segment => None,
        None => Some("every reachable copy of the sealed segment is incomplete".to_owned()),
    }
}

#[cfg(test)]
#[path = "cursor_test.rs"]
mod tests;
