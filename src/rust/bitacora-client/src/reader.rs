//! The stream reader: the read path of the log.
//!
//! # Why this file exists
//!
//! Everything that is not the writer reads the log: materializers that turn it into pages, replicas of a database that
//! follow their primary, recovery, backups. They need two things only (design, section 6): the committed records of a
//! range, and the committed records from a position on, for as long as the stream lives. This file is the handle that
//! offers both.
//!
//! # What it contains
//!
//! - [`StreamReader`]: a reader of one stream, with `read_range`, `follow_from` and `low_water_mark`.
//! - [`RecordStream`]: what `follow_from` returns: the committed records in order, as they commit.
//! - The submodules: `cursor` (a position, and fetching the records at it) and `answer` (what one replica's answer
//!   means).
//!
//! # How it works
//!
//! Both operations are loops around one cursor. A range read fetches until the cursor passes the end of the range or
//! reaches the last committed record. A follower fetches for ever, letting the replica hold each request for a while
//! when nothing is committed yet, so that a new record arrives as soon as its replica learns it is durable.
//!
//! Any single replica can serve a read, because a replica only serves records at or below the durable sequence, and
//! within one segment there is only one possible record at each sequence number. A reader therefore picks one replica
//! per segment and switches only when that replica fails, falls short or has nothing new.
//!
//! A sealed segment that was uploaded to the object store is read from there when no replica serves it: after the
//! replicas dropped their copies, or while none of them can be reached. The reader's caller cannot tell the difference,
//! apart from latency.
//!
//! # Invariants
//!
//! - A reader only ever returns committed records, in sequence order, without gaps and without repeats.
//! - Readers never coordinate, and never write anything: any number of them can read the same stream.
//! - A follower reports only what ends it: its position was truncated, or the stream was deleted. Everything that can
//!   pass, such as a replica or a metadata store that cannot be reached, is retried.
//! - A follower that has nothing to ask waits one retry interval between looks at the metadata store. It never polls in
//!   a tight loop.
//!
//! # See also
//!
//! - `reader/cursor.rs`: how the records at a position are fetched.
//! - `tiered.rs`: reading a sealed segment from the object store.
//! - `docs/protocol/replication.md`, "A reader".

mod answer;
mod cursor;

use std::collections::VecDeque;
use std::sync::Arc;

use bitacora_meta::MetaError;
use bitacora_types::{LogRecord, SequenceNumber, SequenceRange};
use tokio::time::{Instant, sleep};

use crate::context::ClientContext;
use crate::error::ReadError;
use crate::settings::ReaderSettings;
use cursor::{Cursor, Fetched};

/// Reads the committed records of one stream.
///
/// Obtained from [`crate::BitacoraClient::open_reader`]. Any number of readers can read the same stream; they never
/// coordinate. A reader holds no connection of its own: each read and each follower opens what it needs. Cloning a
/// reader is cheap.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions, TakeoverPolicy};
/// use bitacora_types::{SequenceNumber, SequenceRange};
/// # let cluster = bitacora_testkit::Cluster::start(3).await;
/// # let client = BitacoraClient::new(
/// #     cluster.metadata().clone(),
/// #     cluster.connector(),
/// #     cluster.clock(),
/// #     ClientSettings::default(),
/// # );
/// # client.admin().create_stream("orders", StreamOptions::default()).await?;
/// # let writer = client.open_writer("orders", TakeoverPolicy::Force).await?;
/// # writer.append("order 1 placed".into()).await?;
/// # writer.append("order 1 paid".into()).await?;
/// # // Lets the replicas learn that both records are committed.
/// # tokio::time::sleep(std::time::Duration::from_millis(200)).await;
///
/// let reader = client.open_reader("orders");
/// let whole_stream = SequenceRange::new(SequenceNumber::FIRST, SequenceNumber::new(100))?;
/// let records = reader.read_range(whole_stream).await?;
///
/// assert_eq!(records.len(), 2);
/// assert_eq!(records[1].payload, "order 1 paid");
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct StreamReader {
    /// The injected interfaces of the client.
    context: Arc<ClientContext>,
    /// The reader's settings.
    settings: ReaderSettings,
    /// The stream this reader reads.
    stream_name: String,
}

impl StreamReader {
    /// Creates a reader of `stream_name`. Nothing is contacted until the reader is used.
    ///
    /// # Arguments
    ///
    /// * `context` - The injected interfaces of the client.
    /// * `settings` - The reader's settings.
    /// * `stream_name` - The stream to read.
    ///
    /// # Returns
    ///
    /// The reader.
    pub(crate) fn new(context: Arc<ClientContext>, settings: ReaderSettings, stream_name: &str) -> Self {
        Self {
            context,
            settings,
            stream_name: stream_name.to_owned(),
        }
    }

    /// Reads the committed records of `range`, in order.
    ///
    /// Fetches from the replicas of each segment the range touches, or from the object store for a segment only it
    /// holds, until the range is covered or the last committed record is reached. Does not wait for records that are
    /// not committed yet.
    ///
    /// # Arguments
    ///
    /// * `range` - The sequence numbers wanted, both ends included.
    ///
    /// # Returns
    ///
    /// The records of the range. Shorter than the range only if the range extends past the last committed record at the
    /// time of the call; empty if it starts past it.
    ///
    /// # Errors
    ///
    /// * [`ReadError::Truncated`] if the range starts below the low-water mark.
    /// * [`ReadError::Unavailable`] if neither a replica holding the records nor the object store could serve them.
    /// * [`ReadError::Metadata`] if the stream does not exist or the metadata store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions, TakeoverPolicy};
    /// use bitacora_types::{SequenceNumber, SequenceRange};
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("orders", StreamOptions::default()).await?;
    /// # let writer = client.open_writer("orders", TakeoverPolicy::Force).await?;
    /// # for order in 1..=5 {
    /// #     writer.append(format!("order {order} placed").into()).await?;
    /// # }
    /// # // Lets the replicas learn that the records are committed.
    /// # tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    /// # let reader = client.open_reader("orders");
    /// let second_to_fourth = SequenceRange::new(SequenceNumber::new(2), SequenceNumber::new(4))?;
    ///
    /// let records = reader.read_range(second_to_fourth).await?;
    ///
    /// assert_eq!(records.len(), 3);
    /// assert_eq!(records[0].payload, "order 2 placed");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_range(&self, range: SequenceRange) -> Result<Vec<LogRecord>, ReadError> {
        let mut cursor = self.open_cursor(range.first()).await?;
        let mut records = Vec::new();
        while cursor.next_sequence() <= range.last() {
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

    /// Follows the stream from `from_sequence`.
    ///
    /// The returned stream yields every committed record from there on, as soon as it commits, and keeps going across
    /// segment rolls and writer changes. Reads the stream's metadata once; records are fetched as the stream is
    /// consumed.
    ///
    /// # Arguments
    ///
    /// * `from_sequence` - The sequence number of the first record wanted. A position before the first record means the
    ///   first record.
    ///
    /// # Returns
    ///
    /// The stream of records. It may start below the low-water mark; it then yields [`ReadError::Truncated`] first.
    ///
    /// # Errors
    ///
    /// Returns [`ReadError::Metadata`] if the stream does not exist or the metadata store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions, TakeoverPolicy};
    /// use bitacora_types::SequenceNumber;
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("orders", StreamOptions::default()).await?;
    /// # let writer = client.open_writer("orders", TakeoverPolicy::Force).await?;
    /// # let reader = client.open_reader("orders");
    /// let mut records = reader.follow_from(SequenceNumber::FIRST).await?;
    ///
    /// writer.append("order 1 placed".into()).await?;
    ///
    /// let first_record = records.next().await.expect("a followed stream does not end")?;
    /// assert_eq!(first_record.payload, "order 1 placed");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn follow_from(&self, from_sequence: SequenceNumber) -> Result<RecordStream, ReadError> {
        Ok(RecordStream {
            cursor: self.open_cursor(from_sequence).await?,
            buffered_records: VecDeque::new(),
            has_ended: false,
        })
    }

    /// The first sequence number that can still be read. Everything below it has been truncated.
    ///
    /// Reads the stream's metadata once.
    ///
    /// # Returns
    ///
    /// The stream's low-water mark. The first sequence number on a stream that was never truncated.
    ///
    /// # Errors
    ///
    /// Returns [`ReadError::Metadata`] if the stream does not exist or the metadata store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions};
    /// use bitacora_types::SequenceNumber;
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("orders", StreamOptions::default()).await?;
    /// let reader = client.open_reader("orders");
    ///
    /// assert_eq!(reader.low_water_mark().await?, SequenceNumber::FIRST);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn low_water_mark(&self) -> Result<SequenceNumber, ReadError> {
        let stream = self.context.metadata.get_stream(&self.stream_name).await?;
        Ok(stream.low_water_mark)
    }

    /// Places a cursor at `from_sequence` of this reader's stream.
    async fn open_cursor(&self, from_sequence: SequenceNumber) -> Result<Cursor, ReadError> {
        Cursor::open(&self.context, &self.settings, &self.stream_name, from_sequence).await
    }
}

/// The committed records of a stream, in order, as they commit.
///
/// Obtained from [`StreamReader::follow_from`]. The stream has no end of its own: it ends only after yielding an error
/// that makes following impossible.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions, TakeoverPolicy};
/// use bitacora_types::SequenceNumber;
/// # let cluster = bitacora_testkit::Cluster::start(3).await;
/// # let client = BitacoraClient::new(
/// #     cluster.metadata().clone(),
/// #     cluster.connector(),
/// #     cluster.clock(),
/// #     ClientSettings::default(),
/// # );
/// # client.admin().create_stream("orders", StreamOptions::default()).await?;
/// # let writer = client.open_writer("orders", TakeoverPolicy::Force).await?;
/// # writer.append("order 1 placed".into()).await?;
/// # writer.append("order 1 paid".into()).await?;
/// let mut records = client.open_reader("orders").follow_from(SequenceNumber::FIRST).await?;
///
/// let mut payloads = Vec::new();
/// while payloads.len() < 2 {
///     let record = records.next().await.expect("a followed stream does not end")?;
///     payloads.push(record.payload);
/// }
///
/// assert_eq!(payloads, ["order 1 placed", "order 1 paid"]);
/// # Ok(())
/// # }
/// ```
pub struct RecordStream {
    /// The position in the stream, and how to read on from it.
    cursor: Cursor,
    /// Records already fetched and not handed out yet.
    buffered_records: VecDeque<LogRecord>,
    /// Whether the stream has yielded the error that ends it.
    has_ended: bool,
}

impl RecordStream {
    /// Waits for the next committed record.
    ///
    /// Trouble that can pass, such as an unreachable replica or metadata store, is retried without bothering the
    /// caller. An error is yielded only when following cannot continue.
    ///
    /// # Returns
    ///
    /// The next record, or `None` once the stream has ended, which it does only after yielding an error.
    ///
    /// # Errors
    ///
    /// Yielded once, as the last item of the stream:
    ///
    /// * [`ReadError::Truncated`] if the follower's position was truncated.
    /// * [`ReadError::Metadata`] if the stream was deleted.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions, TakeoverPolicy};
    /// use bitacora_types::SequenceNumber;
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("orders", StreamOptions::default()).await?;
    /// # let writer = client.open_writer("orders", TakeoverPolicy::Force).await?;
    /// # writer.append("order 1 placed".into()).await?;
    /// # let mut records = client.open_reader("orders").follow_from(SequenceNumber::FIRST).await?;
    /// let record = records.next().await.expect("a followed stream does not end")?;
    ///
    /// assert_eq!(record.sequence, SequenceNumber::FIRST);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next(&mut self) -> Option<Result<LogRecord, ReadError>> {
        loop {
            if let Some(record) = self.buffered_records.pop_front() {
                return Some(Ok(record));
            }
            if self.has_ended {
                return None;
            }
            if let Err(fatal) = self.fetch_more().await {
                self.has_ended = true;
                return Some(Err(fatal));
            }
        }
    }

    /// Waits for the next committed records and returns all that are at hand.
    ///
    /// Waits like [`RecordStream::next`] for the first record, then adds whatever the same answer of a replica brought
    /// with it. A materializer applies the result as one slice.
    ///
    /// # Returns
    ///
    /// At least one record, in order, or `None` once the stream has ended.
    ///
    /// # Errors
    ///
    /// As [`RecordStream::next`]: the error that ends the stream, yielded once.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions, TakeoverPolicy};
    /// use bitacora_types::SequenceNumber;
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("orders", StreamOptions::default()).await?;
    /// # let writer = client.open_writer("orders", TakeoverPolicy::Force).await?;
    /// # writer.append("order 1 placed".into()).await?;
    /// # writer.append("order 1 paid".into()).await?;
    /// # // Lets the replicas learn that both records are committed.
    /// # tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    /// # let mut records = client.open_reader("orders").follow_from(SequenceNumber::FIRST).await?;
    /// let batch = records.next_batch().await.expect("a followed stream does not end")?;
    ///
    /// assert_eq!(batch.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next_batch(&mut self) -> Option<Result<Vec<LogRecord>, ReadError>> {
        let first_record = match self.next().await? {
            Ok(first_record) => first_record,
            Err(read_error) => return Some(Err(read_error)),
        };
        let mut records = Vec::with_capacity(1 + self.buffered_records.len());
        records.push(first_record);
        records.extend(self.buffered_records.drain(..));
        Some(Ok(records))
    }

    /// The highest sequence number this follower knows to be committed.
    ///
    /// It is what the replicas the follower asked and the stream's sealed segments have told it so far. The gap between
    /// it and the last record handed out is how far the follower knows it lags. Reads the follower's own state; nothing
    /// is contacted.
    ///
    /// # Returns
    ///
    /// The known durable sequence. It never moves backwards, and it is the position before the first record for a
    /// follower that has learned nothing yet.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions, TakeoverPolicy};
    /// use bitacora_types::SequenceNumber;
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("orders", StreamOptions::default()).await?;
    /// # let writer = client.open_writer("orders", TakeoverPolicy::Force).await?;
    /// # writer.append("order 1 placed".into()).await?;
    /// # writer.append("order 1 paid".into()).await?;
    /// # writer.append("order 1 shipped".into()).await?;
    /// # // Lets the replicas learn that the records are committed.
    /// # tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    /// let mut records = client.open_reader("orders").follow_from(SequenceNumber::FIRST).await?;
    /// assert_eq!(records.durable_sequence(), SequenceNumber::ZERO);
    ///
    /// records.next().await.expect("a followed stream does not end")?;
    ///
    /// // One record was handed out, and the follower knows that two more are committed.
    /// assert_eq!(records.durable_sequence(), SequenceNumber::new(3));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn durable_sequence(&self) -> SequenceNumber {
        self.cursor.known_durable_sequence()
    }

    /// Fetches the records after the ones handed out into the buffer, waiting where there are none yet.
    ///
    /// Returns without records when nothing new was committed or when trouble that can pass was met; the caller asks
    /// again. Fails only with an error that ends the stream.
    async fn fetch_more(&mut self) -> Result<(), ReadError> {
        let tail_wait = self.cursor.settings().tail_wait;
        let retry_interval = self.cursor.settings().retry_interval;
        let asked_at = Instant::now();
        match self.cursor.fetch(Some(tail_wait)).await {
            Ok(Fetched::Records(records)) => self.buffered_records.extend(records),
            Ok(Fetched::NothingNew) => {
                // Usually a replica held the request for the tail wait. When there was no replica to ask, for example
                // between two writers, nothing has waited yet: pause here, or the follower would poll the metadata
                // store in a loop.
                let still_to_wait = retry_interval.saturating_sub(asked_at.elapsed());
                if !still_to_wait.is_zero() {
                    sleep(still_to_wait).await;
                }
            }
            Err(read_error) if ends_a_followed_stream(&read_error) => return Err(read_error),
            Err(passing_trouble) => {
                tracing::warn!(
                    stream = %self.cursor.stream_name(),
                    error = %passing_trouble,
                    "follower is retrying"
                );
                sleep(retry_interval).await;
            }
        }
        Ok(())
    }
}

/// Whether `read_error` makes following impossible: the follower's position was truncated, or its stream was deleted.
/// Every other error can pass, and a follower retries after it.
const fn ends_a_followed_stream(read_error: &ReadError) -> bool {
    matches!(
        read_error,
        ReadError::Truncated { .. } | ReadError::Metadata(MetaError::StreamNotFound { .. })
    )
}

#[cfg(test)]
#[path = "reader_test.rs"]
mod tests;
