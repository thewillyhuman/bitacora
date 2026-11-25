//! The stream reader: the read path of the design (section 6).
//!
//! Any single replica can serve a read, because a replica only serves
//! records at or below the durable sequence, and within one segment there is
//! only one possible record at each sequence number. A reader therefore
//! picks one replica per segment and switches only when that replica fails,
//! falls short or has nothing new.
//!
//! A sealed segment that was uploaded to the object store is read from
//! there when no replica serves it: after the replicas dropped their copies,
//! or while none of them can be reached. The reader's caller cannot tell the
//! difference, apart from latency.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use bitacora_meta::{CopyState, MetaError, SegmentRecord, StreamRecord, TieredRecord};
use bitacora_segment::format::{Batch, decode_frames};
use bitacora_types::replica_api::{ReadLimit, ReadRequest};
use bitacora_types::{LogRecord, SegmentId, SequenceNumber, SequenceRange};
use tokio::time::{Instant, sleep, timeout};

use crate::context::{ClientContext, ConnectedReplica};
use crate::error::ReadError;
use crate::settings::ReaderSettings;
use crate::tiered::TieredSegment;

/// Reads the committed records of one stream. Any number of readers can
/// read the same stream; they never coordinate.
#[derive(Clone)]
pub struct StreamReader {
    context: Arc<ClientContext>,
    settings: ReaderSettings,
    stream_name: String,
}

impl StreamReader {
    pub(crate) fn new(
        context: Arc<ClientContext>,
        settings: ReaderSettings,
        stream_name: &str,
    ) -> Self {
        Self {
            context,
            settings,
            stream_name: stream_name.to_owned(),
        }
    }

    /// Reads the committed records of `range`, in order.
    ///
    /// The result is shorter than the range only if the range extends past
    /// the last committed record at the time of the call.
    ///
    /// # Errors
    ///
    /// Returns [`ReadError::Truncated`] if the range starts below the
    /// low-water mark, or [`ReadError::Unavailable`] if no replica holding
    /// the records could be reached.
    pub async fn read_range(&self, range: SequenceRange) -> Result<Vec<LogRecord>, ReadError> {
        let mut cursor = Cursor::open(self, range.first()).await?;
        let mut records = Vec::new();
        while cursor.next_sequence <= range.last() {
            match cursor.fetch(None).await? {
                Fetched::Records(fetched_records) => records.extend(
                    fetched_records
                        .into_iter()
                        .filter(|record| record.sequence <= range.last()),
                ),
                Fetched::NothingNew => break,
            }
        }
        Ok(records)
    }

    /// Follows the stream from `from_sequence`: the returned stream yields
    /// every committed record from there on, as soon as it commits, and
    /// keeps going across segment rolls and writer changes.
    ///
    /// # Errors
    ///
    /// Returns [`ReadError::Metadata`] if the stream does not exist or the
    /// metadata store cannot be reached.
    pub async fn follow_from(
        &self,
        from_sequence: SequenceNumber,
    ) -> Result<RecordStream, ReadError> {
        Ok(RecordStream {
            cursor: Cursor::open(self, from_sequence).await?,
            buffered_records: VecDeque::new(),
            has_ended: false,
        })
    }

    /// The first sequence number that can still be read. Everything below
    /// it has been truncated.
    ///
    /// # Errors
    ///
    /// Returns [`ReadError::Metadata`] if the stream does not exist or the
    /// metadata store cannot be reached.
    pub async fn low_water_mark(&self) -> Result<SequenceNumber, ReadError> {
        let stream = self.context.metadata.get_stream(&self.stream_name).await?;
        Ok(stream.low_water_mark)
    }
}

/// The committed records of a stream, in order, as they commit.
///
/// Obtained from [`StreamReader::follow_from`].
pub struct RecordStream {
    cursor: Cursor,
    buffered_records: VecDeque<LogRecord>,
    has_ended: bool,
}

impl RecordStream {
    /// Waits for the next committed record.
    ///
    /// Trouble that can pass, such as an unreachable replica or metadata
    /// store, is retried without bothering the caller. An error is yielded
    /// only when following cannot continue: the position was truncated or
    /// the stream was deleted. After that the stream yields `None`.
    pub async fn next(&mut self) -> Option<Result<LogRecord, ReadError>> {
        loop {
            if let Some(record) = self.buffered_records.pop_front() {
                return Some(Ok(record));
            }
            if self.has_ended {
                return None;
            }
            let tail_wait = self.cursor.settings.tail_wait;
            let asked_at = Instant::now();
            match self.cursor.fetch(Some(tail_wait)).await {
                Ok(Fetched::Records(records)) => self.buffered_records.extend(records),
                Ok(Fetched::NothingNew) => {
                    // Usually a replica held the request for the tail wait.
                    // When there was no replica to ask, for example between
                    // two writers, nothing has waited yet: pause here, or
                    // the follower would poll the metadata store in a loop.
                    let still_to_wait = self
                        .cursor
                        .settings
                        .retry_interval
                        .saturating_sub(asked_at.elapsed());
                    if !still_to_wait.is_zero() {
                        sleep(still_to_wait).await;
                    }
                }
                Err(
                    fatal @ (ReadError::Truncated { .. }
                    | ReadError::Metadata(MetaError::StreamNotFound { .. })),
                ) => {
                    self.has_ended = true;
                    return Some(Err(fatal));
                }
                Err(passing_trouble) => {
                    tracing::warn!(
                        stream = %self.cursor.stream_name,
                        error = %passing_trouble,
                        "follower is retrying"
                    );
                    sleep(self.cursor.settings.retry_interval).await;
                }
            }
        }
    }
}

/// What one fetch brought back.
enum Fetched {
    /// One or more records, starting at the cursor's position.
    Records(Vec<LogRecord>),
    /// No committed record exists at the cursor's position yet.
    NothingNew,
}

/// What one replica answered for the cursor's position.
enum ReplicaAnswer {
    Records(Vec<LogRecord>),
    /// The segment is sealed and the cursor is past its end.
    SegmentFinished,
    /// The replica has nothing at the position: it is caught up, behind, or
    /// holds a short copy.
    Nothing,
    /// The replica failed or sent something invalid.
    Failed(String),
}

/// What the replicas of a segment answered, together, for the cursor's
/// position.
enum ReplicasAnswer {
    /// One of them had records.
    Records(Vec<LogRecord>),
    /// A follower's replica of the open segment has nothing new yet.
    NothingNewAtTheTail,
    /// The segment is sealed and the cursor is past its end.
    SegmentFinished,
    /// None of them had records. `last_failure` is why the last one that
    /// failed did; `None` if they all answered and simply held nothing.
    NoRecords { last_failure: Option<String> },
}

/// A position in a stream, with what is needed to read on from it.
struct Cursor {
    context: Arc<ClientContext>,
    settings: ReaderSettings,
    stream_name: String,
    next_sequence: SequenceNumber,
    /// The stream's metadata as last read. Sealed segments never change, so
    /// only what it says about the open segment can be stale.
    stream: StreamRecord,
    /// The replicas of the segment being read, and which segment that is.
    connected_segment: Option<(SegmentId, Vec<ConnectedReplica>)>,
    /// Which replica of the segment is asked first.
    preferred_replica: usize,
    /// The object of the tiered segment being read from the object store,
    /// kept open so that its footer and index are fetched once.
    open_tiered_segment: Option<(SegmentId, TieredSegment)>,
}

impl Cursor {
    async fn open(reader: &StreamReader, from_sequence: SequenceNumber) -> Result<Self, ReadError> {
        let stream = reader
            .context
            .metadata
            .get_stream(&reader.stream_name)
            .await?;
        Ok(Self {
            context: Arc::clone(&reader.context),
            settings: reader.settings.clone(),
            stream_name: reader.stream_name.clone(),
            // The first record is the earliest thing anyone can ask for.
            next_sequence: from_sequence.max(SequenceNumber::FIRST),
            stream,
            connected_segment: None,
            preferred_replica: 0,
            open_tiered_segment: None,
        })
    }

    /// Fetches the records at the cursor's position and moves past them.
    ///
    /// `tail_wait` is how long a replica may hold the request when nothing
    /// is committed yet; `None` asks every replica once without waiting.
    async fn fetch(&mut self, tail_wait: Option<Duration>) -> Result<Fetched, ReadError> {
        let mut has_refreshed = false;
        let mut failed_rounds = 0;
        loop {
            if self.next_sequence < self.stream.low_water_mark {
                return Err(ReadError::Truncated {
                    low_water_mark: self.stream.low_water_mark,
                });
            }
            let Some(segment) = self.stream.segment_containing(self.next_sequence).cloned() else {
                // No known segment holds the position: either the stream
                // ends here for now, or a segment was opened since.
                if has_refreshed {
                    return Ok(Fetched::NothingNew);
                }
                self.refresh().await?;
                has_refreshed = true;
                continue;
            };
            let replicas = self.replicas_of(&segment).await?;
            let mut last_failure = match self.ask_replicas(&segment, &replicas, tail_wait).await {
                ReplicasAnswer::Records(records) => return Ok(Fetched::Records(records)),
                ReplicasAnswer::NothingNewAtTheTail => {
                    // The refresh notices a seal.
                    self.refresh().await?;
                    return Ok(Fetched::NothingNew);
                }
                ReplicasAnswer::SegmentFinished => {
                    self.refresh().await?;
                    has_refreshed = true;
                    continue;
                }
                ReplicasAnswer::NoRecords { last_failure } => last_failure,
            };

            // No replica produced records. A segment that was uploaded can
            // still be read from the object store.
            if let Some(tiered) = &segment.tiered {
                match self.fetch_tiered(&segment, tiered).await {
                    Ok(records) => return Ok(Fetched::Records(records)),
                    Err(reason) => {
                        tracing::debug!(
                            segment = %segment.segment_id,
                            %reason,
                            "read from the object store failed"
                        );
                        last_failure = Some(reason);
                    }
                }
            }

            // Look at the metadata again: the segment may have been sealed,
            // repaired, tiered or followed by another.
            self.refresh().await?;
            let is_still_open = self
                .stream
                .segment_containing(self.next_sequence)
                .is_some_and(SegmentRecord::is_open);
            let reason = match last_failure {
                Some(reason) => reason,
                None if is_still_open => return Ok(Fetched::NothingNew),
                // A sealed segment holds the position but every copy asked
                // was short: wait for repair like for a failed replica.
                None => "every reachable copy of the sealed segment is incomplete".to_owned(),
            };
            failed_rounds += 1;
            if failed_rounds >= self.settings.max_attempts_per_read {
                return Err(ReadError::Unavailable {
                    segment: segment.address(self.stream.stream_id),
                    reason,
                });
            }
            sleep(self.settings.retry_interval).await;
        }
    }

    /// Asks the replicas of `segment` for the records at the cursor's
    /// position, starting with the preferred one, until one has them.
    /// Moves the cursor past the records it returns.
    async fn ask_replicas(
        &mut self,
        segment: &SegmentRecord,
        replicas: &[ConnectedReplica],
        tail_wait: Option<Duration>,
    ) -> ReplicasAnswer {
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
                    return ReplicasAnswer::Records(records);
                }
                ReplicaAnswer::SegmentFinished => return ReplicasAnswer::SegmentFinished,
                ReplicaAnswer::Nothing if segment.is_open() && tail_wait.is_some() => {
                    // A follower asks one replica per wait. If this one is
                    // cut off from the writer, the next call asks its
                    // neighbour.
                    self.preferred_replica = (replica_index + 1) % replicas.len();
                    return ReplicasAnswer::NothingNewAtTheTail;
                }
                ReplicaAnswer::Nothing => {}
                ReplicaAnswer::Failed(reason) => {
                    tracing::debug!(
                        replica = %replicas[replica_index].replica_id,
                        %reason,
                        "read failed, trying another replica"
                    );
                    last_failure = Some(reason);
                }
            }
        }
        ReplicasAnswer::NoRecords { last_failure }
    }

    /// Reads the records at the cursor's position from the object store
    /// and moves past them. Fails with the reason if the object store
    /// cannot serve them.
    async fn fetch_tiered(
        &mut self,
        segment: &SegmentRecord,
        tiered: &TieredRecord,
    ) -> Result<Vec<LogRecord>, String> {
        let Some(artifact_store) = self.context.artifact_store.clone() else {
            return Err(
                "the segment is in the object store, and this client was given none".to_owned(),
            );
        };
        let Some(sealed_end) = segment.sealed_end() else {
            return Err("the segment is recorded as tiered but not as sealed".to_owned());
        };
        let is_open = self
            .open_tiered_segment
            .as_ref()
            .is_some_and(|(open_segment_id, _object)| *open_segment_id == segment.segment_id);
        if !is_open {
            let opened = TieredSegment::open(
                artifact_store.as_ref(),
                segment.address(self.stream.stream_id),
                sealed_end,
                tiered,
                self.settings.request_timeout,
            )
            .await
            .map_err(|tiered_error| tiered_error.to_string())?;
            self.open_tiered_segment = Some((segment.segment_id, opened));
        }
        let Some((_segment_id, tiered_segment)) = &self.open_tiered_segment else {
            return Err("the tiered segment was not opened".to_owned());
        };
        let fetched = tiered_segment
            .read_from(
                artifact_store.as_ref(),
                self.next_sequence,
                self.settings.max_response_bytes,
            )
            .await;
        let records = match fetched {
            Ok(records) => records,
            Err(tiered_error) => {
                // Open the object afresh next time: it may have been
                // replaced by a new upload.
                self.open_tiered_segment = None;
                return Err(tiered_error.to_string());
            }
        };
        let Some(last_record) = records.last() else {
            return Err(format!(
                "the object holds no record at {}",
                self.next_sequence
            ));
        };
        self.next_sequence = last_record.sequence.next();
        Ok(records)
    }

    /// Sends one read to one replica and classifies its answer.
    async fn ask(
        &self,
        replica: &ConnectedReplica,
        segment: &SegmentRecord,
        tail_wait: Option<Duration>,
    ) -> ReplicaAnswer {
        let address = segment.address(self.stream.stream_id);
        // A sealed segment has nothing to wait for.
        let wait_for_records = tail_wait.filter(|_| segment.is_open());
        let request = ReadRequest {
            segment: address,
            from_sequence: self.next_sequence,
            max_bytes: self.settings.max_response_bytes,
            wait_for_records,
            limit: ReadLimit::DurableSequence,
        };
        let longest_answer_time =
            self.settings.request_timeout + wait_for_records.unwrap_or_default();
        let response = match timeout(longest_answer_time, replica.client.read(request)).await {
            Ok(Ok(response)) => response,
            Ok(Err(replica_error)) => return ReplicaAnswer::Failed(replica_error.to_string()),
            Err(_elapsed) => return ReplicaAnswer::Failed("request timed out".to_owned()),
        };
        // The reader verifies every batch checksum itself (design, section
        // 6, step 4): the replica is not trusted to have done it.
        let batches = match decode_frames(address, &response.batch_frames) {
            Ok(batches) => batches,
            Err(frames_error) => return ReplicaAnswer::Failed(frames_error.to_string()),
        };
        let records: Vec<LogRecord> = batches
            .iter()
            .flat_map(Batch::records)
            .filter(|record| record.sequence >= self.next_sequence)
            .collect();
        if let Some(first_record) = records.first() {
            if first_record.sequence != self.next_sequence {
                return ReplicaAnswer::Failed(format!(
                    "answer starts at {} instead of {}",
                    first_record.sequence, self.next_sequence
                ));
            }
            return ReplicaAnswer::Records(records);
        }
        let sealed_end = segment.sealed_end().or(response.sealed_end);
        if sealed_end.is_some_and(|sealed_end| self.next_sequence > sealed_end) {
            ReplicaAnswer::SegmentFinished
        } else {
            ReplicaAnswer::Nothing
        }
    }

    /// Returns handles to the replicas of `segment`, connecting when the
    /// cursor moves to another segment.
    async fn replicas_of(
        &mut self,
        segment: &SegmentRecord,
    ) -> Result<Vec<ConnectedReplica>, ReadError> {
        if let Some((connected_segment_id, replicas)) = &self.connected_segment
            && *connected_segment_id == segment.segment_id
            && replicas
                .iter()
                .map(|replica| &replica.replica_id)
                .eq(segment.copies.iter().map(|copy| &copy.replica_id))
        {
            return Ok(replicas.clone());
        }
        let is_another_segment =
            self.connected_segment
                .as_ref()
                .is_none_or(|(connected_segment_id, _replicas)| {
                    *connected_segment_id != segment.segment_id
                });
        if is_another_segment {
            self.preferred_replica = 0;
        }
        let replicas = self.context.connect_copies(&segment.copies).await?;
        self.connected_segment = Some((segment.segment_id, replicas.clone()));
        Ok(replicas)
    }

    /// Reads the stream's metadata again.
    async fn refresh(&mut self) -> Result<(), ReadError> {
        self.stream = self.context.metadata.get_stream(&self.stream_name).await?;
        Ok(())
    }
}
