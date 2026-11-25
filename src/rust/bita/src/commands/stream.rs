//! The `bita stream` commands.
//!
//! # Why this file exists
//!
//! Streams are what a cluster is for, so most of what an operator does starts with one: create it, see
//! what state it is in, cut its history, remove it. An operator also needs a way to prove that a cluster
//! works end to end without writing a program, which is what `append`, `read` and `tail` are for.
//!
//! # What it contains
//!
//! - Administration: [`create`], [`list`], [`describe`], [`delete`], [`truncate_before`],
//!   [`register_materializer`], [`unregister_materializer`].
//! - Smoke tests: [`append`], [`read`], [`tail`].
//! - Their results: [`StreamSummary`], [`StreamList`], [`StreamDetails`], [`MaterializerDetails`],
//!   [`AppendedRecords`], [`ReadRecords`], [`RecordLine`].
//!
//! # How it works
//!
//! Administration goes through the client library's `StreamAdmin`, the same interface an application
//! uses, so the tool cannot do anything to a stream that the API does not allow. `append` becomes the
//! stream's writer, which runs the takeover protocol and fences any writer that was there: it is a test
//! tool, not a way to add records to a stream an application is writing. `read` and `tail` are ordinary
//! readers and disturb nobody.
//!
//! # Invariants
//!
//! - `delete` does nothing without its confirmation.
//! - A writer opened by `append` is closed before the command returns, on success and on failure of an
//!   append alike, so the tool never leaves a lease behind on purpose.
//!
//! # See also
//!
//! - `bitacora-client/src/admin.rs`: what each administration call does.
//! - `commands/segment.rs`: the segments of a stream, which `describe` only counts.

use std::io::Write;
use std::time::Duration;

use bitacora::{LogRecord, SequenceNumber, SequenceRange, StreamOptions, TakeoverPolicy};
use bitacora_meta::{CheckpointRecord, RetentionPolicy, StreamRecord};
use bitacora_types::ReplicationSettings;
use bytes::Bytes;
use serde::Serialize;

use super::{Acknowledgement, CommandError};
use crate::cluster::Cluster;
use crate::output::{OutputFormat, Table, format_payload, format_timestamp, hex_of, key_value_lines};

/// The name the tool's writers record with their lease.
const WRITER_IDENTITY: &str = "bita";

/// One stream, in brief.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct StreamSummary {
    /// The stream's name.
    pub(crate) name: String,
    /// The stream's permanent identifier.
    pub(crate) stream_id: u64,
    /// The current writer generation.
    pub(crate) epoch: u64,
    /// The identity of the writer that holds the stream, if one does.
    pub(crate) writer: Option<String>,
    /// How many replicas hold each segment.
    pub(crate) replicas: usize,
    /// How many replicas must store a record before it is committed.
    pub(crate) write_quorum: usize,
    /// How many segments the stream has.
    pub(crate) segments: usize,
    /// The first sequence number that can still be read.
    pub(crate) low_water_mark: u64,
}

impl StreamSummary {
    /// Summarizes a stream record.
    fn of(stream: &StreamRecord) -> Self {
        Self {
            name: stream.name.clone(),
            stream_id: stream.stream_id.get(),
            epoch: stream.epoch.get(),
            writer: stream.writer.as_ref().map(|lease| lease.writer_identity.clone()),
            replicas: stream.replication.replica_count(),
            write_quorum: stream.replication.write_quorum(),
            segments: stream.segments.len(),
            low_water_mark: stream.low_water_mark.get(),
        }
    }

    /// The cells of this stream's row in a table of streams.
    fn table_row(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            self.stream_id.to_string(),
            self.epoch.to_string(),
            self.writer.clone().unwrap_or_else(|| "-".to_owned()),
            format!("{}/{}", self.replicas, self.write_quorum),
            self.segments.to_string(),
            self.low_water_mark.to_string(),
        ]
    }

    /// Renders the stream as a table of one row.
    pub(crate) fn render_text(&self) -> String {
        let mut table = stream_table();
        table.push_row(self.table_row());
        table.render()
    }
}

/// An empty table with the columns of a stream summary.
fn stream_table() -> Table {
    Table::new(&["NAME", "ID", "EPOCH", "WRITER", "REPLICATION", "SEGMENTS", "LOW-WATER"])
}

/// Every stream, in brief.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct StreamList {
    /// The streams, sorted by name.
    pub(crate) streams: Vec<StreamSummary>,
}

impl StreamList {
    /// Renders the streams as a table, one row each.
    pub(crate) fn render_text(&self) -> String {
        let mut table = stream_table();
        for stream in &self.streams {
            table.push_row(stream.table_row());
        }
        table.render()
    }
}

/// The lease of a stream's writer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct WriterDetails {
    /// The name the writer chose for itself.
    pub(crate) identity: String,
    /// When the lease lapses unless renewed, in UTC.
    pub(crate) lease_expires_at: String,
    /// Whether the lease had not lapsed when the command ran.
    pub(crate) is_lease_current: bool,
}

/// One materializer and its checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct MaterializerDetails {
    /// The materializer's name.
    pub(crate) name: String,
    /// The last sequence number covered by the materializer's artifacts.
    pub(crate) checkpoint: u64,
    /// How many times a runner has taken the materializer over.
    pub(crate) fencing_token: u64,
    /// Where the manifest of the checkpoint's artifacts is stored, if a checkpoint was committed.
    pub(crate) manifest: Option<String>,
}

impl MaterializerDetails {
    /// Describes a checkpoint record.
    fn of(checkpoint: &CheckpointRecord) -> Self {
        Self {
            name: checkpoint.materializer.clone(),
            checkpoint: checkpoint.checkpoint.get(),
            fencing_token: checkpoint.fencing_token,
            manifest: checkpoint.manifest.clone(),
        }
    }

    /// The cells of this materializer's row in a table of materializers.
    fn table_row(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            self.checkpoint.to_string(),
            self.fencing_token.to_string(),
            self.manifest.clone().unwrap_or_else(|| "-".to_owned()),
        ]
    }

    /// Renders the materializer as a table of one row.
    pub(crate) fn render_text(&self) -> String {
        let mut table = materializer_table();
        table.push_row(self.table_row());
        table.render()
    }
}

/// An empty table with the columns of a materializer.
fn materializer_table() -> Table {
    Table::new(&["MATERIALIZER", "CHECKPOINT", "TOKEN", "MANIFEST"])
}

/// One stream, in full.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct StreamDetails {
    /// The stream's name.
    pub(crate) name: String,
    /// The stream's permanent identifier.
    pub(crate) stream_id: u64,
    /// The current writer generation.
    pub(crate) epoch: u64,
    /// The writer that holds the stream, if one does.
    pub(crate) writer: Option<WriterDetails>,
    /// How many replicas hold each segment.
    pub(crate) replicas: usize,
    /// How many replicas must store a record before it is committed.
    pub(crate) write_quorum: usize,
    /// How long sealed segments are kept at least, in seconds, if the stream sets a minimum.
    pub(crate) keep_sealed_for_seconds: Option<u64>,
    /// The first sequence number that can still be read.
    pub(crate) low_water_mark: u64,
    /// The position below which the owner asked for records to be deleted, if it asked.
    pub(crate) truncate_before: Option<u64>,
    /// How many segments the stream has.
    pub(crate) segments: usize,
    /// The identifier the stream's next segment will get.
    pub(crate) next_segment_id: u64,
    /// The replicas the controller set aside for the next segment, if it has.
    pub(crate) standby_replicas: Option<Vec<String>>,
    /// The registered materializers and their checkpoints, sorted by name.
    pub(crate) materializers: Vec<MaterializerDetails>,
}

impl StreamDetails {
    /// Renders the stream as named values, followed by a table of its materializers if it has any.
    pub(crate) fn render_text(&self) -> String {
        let writer = match &self.writer {
            Some(writer) if writer.is_lease_current => {
                format!("{} (lease until {})", writer.identity, writer.lease_expires_at)
            }
            Some(writer) => format!("{} (lease lapsed at {})", writer.identity, writer.lease_expires_at),
            None => "-".to_owned(),
        };
        let retention = match self.keep_sealed_for_seconds {
            Some(seconds) => format!("sealed segments kept at least {seconds}s"),
            None => "deleted as soon as truncation allows".to_owned(),
        };
        let truncate_before = match self.truncate_before {
            Some(sequence) => sequence.to_string(),
            None => "-".to_owned(),
        };
        let standby = match &self.standby_replicas {
            Some(replicas) => replicas.join(" "),
            None => "-".to_owned(),
        };
        let mut rendered = key_value_lines(&[
            ("name", self.name.clone()),
            ("id", self.stream_id.to_string()),
            ("epoch", self.epoch.to_string()),
            ("writer", writer),
            (
                "replication",
                format!("{} replicas, write quorum {}", self.replicas, self.write_quorum),
            ),
            ("retention", retention),
            ("low-water mark", self.low_water_mark.to_string()),
            ("truncate before", truncate_before),
            ("segments", self.segments.to_string()),
            ("next segment id", self.next_segment_id.to_string()),
            ("standby replicas", standby),
        ]);
        if !self.materializers.is_empty() {
            let mut table = materializer_table();
            for materializer in &self.materializers {
                table.push_row(materializer.table_row());
            }
            rendered.push('\n');
            rendered.push_str(&table.render());
        }
        rendered
    }
}

/// What an append committed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct AppendedRecords {
    /// The stream that was appended to.
    pub(crate) stream: String,
    /// The epoch the tool wrote under.
    pub(crate) epoch: u64,
    /// The sequence number of the first record appended.
    pub(crate) first_sequence: u64,
    /// The sequence number of the last record appended.
    pub(crate) last_sequence: u64,
    /// How many records were appended.
    pub(crate) records: u64,
}

impl AppendedRecords {
    /// Renders the append as one line.
    pub(crate) fn render_text(&self) -> String {
        format!(
            "appended {} records to {} at sequences {} to {} under epoch {}\n",
            self.records, self.stream, self.first_sequence, self.last_sequence, self.epoch
        )
    }
}

/// One record, as it is printed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RecordLine {
    /// The record's sequence number.
    pub(crate) sequence: u64,
    /// The payload as text. Bytes that are not UTF-8 are replaced, so this is for reading, not for
    /// processing.
    pub(crate) payload_text: String,
    /// The payload in hexadecimal, exactly as stored.
    pub(crate) payload_hex: String,
}

impl RecordLine {
    /// Describes a record.
    fn of(record: &LogRecord) -> Self {
        Self {
            sequence: record.sequence.get(),
            payload_text: format_payload(&record.payload),
            payload_hex: hex_of(&record.payload),
        }
    }
}

/// Records that were read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ReadRecords {
    /// The records, in sequence order.
    pub(crate) records: Vec<RecordLine>,
}

impl ReadRecords {
    /// Renders the records as a table of sequence numbers and payload texts.
    pub(crate) fn render_text(&self) -> String {
        let mut table = Table::new(&["SEQUENCE", "PAYLOAD"]);
        for record in &self.records {
            table.push_row(vec![record.sequence.to_string(), record.payload_text.clone()]);
        }
        table.render()
    }
}

/// Creates a stream and places its first segment.
///
/// # Arguments
///
/// * `cluster` - The interfaces to the cluster.
/// * `name` - The stream's name.
/// * `replica_count` - How many replicas hold each segment.
/// * `write_quorum` - How many replicas must store a record before it is committed.
/// * `keep_sealed_for` - How long sealed segments are kept at least; `None` deletes as soon as
///   truncation allows.
///
/// # Errors
///
/// * [`CommandError::InvalidReplication`] unless `1 <= write_quorum <= replica_count`.
/// * [`CommandError::Metadata`] if the name is taken or invalid, or the metadata store is unavailable.
pub(crate) async fn create(
    cluster: &Cluster,
    name: &str,
    replica_count: u8,
    write_quorum: u8,
    keep_sealed_for: Option<Duration>,
) -> Result<StreamSummary, CommandError> {
    let replication = ReplicationSettings::new(replica_count, write_quorum)?;
    let retention = match keep_sealed_for {
        Some(duration) => RetentionPolicy::keep_for(duration),
        None => RetentionPolicy::default(),
    };
    let options = StreamOptions { replication, retention };
    let admin = cluster.client(WRITER_IDENTITY).admin();
    let created = admin.create_stream(name, options).await?;
    Ok(StreamSummary::of(&created))
}

/// Lists every stream, sorted by name.
///
/// # Errors
///
/// Returns [`CommandError::Metadata`] if the metadata store is unavailable.
pub(crate) async fn list(cluster: &Cluster) -> Result<StreamList, CommandError> {
    let admin = cluster.client(WRITER_IDENTITY).admin();
    let stream_records = admin.list_streams().await?;
    let mut streams = Vec::with_capacity(stream_records.len());
    for stream in &stream_records {
        streams.push(StreamSummary::of(stream));
    }
    Ok(StreamList { streams })
}

/// Describes one stream and its materializers.
///
/// # Errors
///
/// Returns [`CommandError::Metadata`] if no stream has this name or the metadata store is unavailable.
pub(crate) async fn describe(cluster: &Cluster, name: &str) -> Result<StreamDetails, CommandError> {
    let admin = cluster.client(WRITER_IDENTITY).admin();
    let description = admin.describe_stream(name).await?;
    let stream = description.stream;
    let now = cluster.clock.now();
    let writer = stream.writer.as_ref().map(|lease| WriterDetails {
        identity: lease.writer_identity.clone(),
        lease_expires_at: format_timestamp(lease.expires_at),
        is_lease_current: lease.expires_at > now,
    });
    let mut materializers = Vec::with_capacity(description.checkpoints.len());
    for checkpoint in &description.checkpoints {
        materializers.push(MaterializerDetails::of(checkpoint));
    }
    let standby_replicas = stream.standby_replicas.as_ref().map(|replica_ids| {
        let mut names = Vec::with_capacity(replica_ids.len());
        for replica_id in replica_ids {
            names.push(replica_id.to_string());
        }
        names
    });
    Ok(StreamDetails {
        name: stream.name.clone(),
        stream_id: stream.stream_id.get(),
        epoch: stream.epoch.get(),
        writer,
        replicas: stream.replication.replica_count(),
        write_quorum: stream.replication.write_quorum(),
        keep_sealed_for_seconds: stream.retention.keep_sealed_for().map(|duration| duration.as_secs()),
        low_water_mark: stream.low_water_mark.get(),
        truncate_before: stream.truncate_before.map(SequenceNumber::get),
        segments: stream.segments.len(),
        next_segment_id: stream.next_segment_id.get(),
        standby_replicas,
        materializers,
    })
}

/// Deletes a stream. Its segments are removed by the controller afterwards.
///
/// # Arguments
///
/// * `cluster` - The interfaces to the cluster.
/// * `name` - The stream's name.
/// * `is_confirmed` - Whether the operator passed the confirmation. Without it nothing is deleted.
///
/// # Errors
///
/// * [`CommandError::Refused`] without the confirmation.
/// * [`CommandError::Metadata`] if no stream has this name or the metadata store is unavailable.
pub(crate) async fn delete(cluster: &Cluster, name: &str, is_confirmed: bool) -> Result<Acknowledgement, CommandError> {
    if !is_confirmed {
        return Err(CommandError::Refused {
            reason: format!(
                "deleting stream {name:?} deletes all of its records and cannot be undone; \
                 pass --yes to confirm"
            ),
        });
    }
    let admin = cluster.client(WRITER_IDENTITY).admin();
    admin.delete_stream(name).await?;
    Ok(Acknowledgement {
        message: format!("stream {name} deleted; the controller removes its segments"),
    })
}

/// Allows every record below `sequence` to be deleted, as far as materializers and retention permit.
///
/// # Errors
///
/// Returns [`CommandError::Metadata`] if no stream has this name or the metadata store is unavailable.
pub(crate) async fn truncate_before(
    cluster: &Cluster,
    name: &str,
    sequence: u64,
) -> Result<Acknowledgement, CommandError> {
    let admin = cluster.client(WRITER_IDENTITY).admin();
    admin.truncate_before(name, SequenceNumber::new(sequence)).await?;
    Ok(Acknowledgement {
        message: format!(
            "stream {name} may be truncated before sequence {sequence}; \
             the controller deletes whole segments below it"
        ),
    })
}

/// Registers a materializer on a stream.
///
/// # Errors
///
/// Returns [`CommandError::Metadata`] if the stream does not exist, the materializer's name is invalid,
/// or the metadata store is unavailable.
pub(crate) async fn register_materializer(
    cluster: &Cluster,
    name: &str,
    materializer: &str,
) -> Result<MaterializerDetails, CommandError> {
    let admin = cluster.client(WRITER_IDENTITY).admin();
    let checkpoint = admin.register_materializer(name, materializer).await?;
    Ok(MaterializerDetails::of(&checkpoint))
}

/// Unregisters a materializer, so that it no longer holds records back.
///
/// # Errors
///
/// Returns [`CommandError::Metadata`] if the stream does not exist or the metadata store is unavailable.
pub(crate) async fn unregister_materializer(
    cluster: &Cluster,
    name: &str,
    materializer: &str,
) -> Result<Acknowledgement, CommandError> {
    let admin = cluster.client(WRITER_IDENTITY).admin();
    admin.unregister_materializer(name, materializer).await?;
    Ok(Acknowledgement {
        message: format!("materializer {materializer} unregistered from stream {name}"),
    })
}

/// Becomes the stream's writer, appends `payloads` in order and steps down.
///
/// This runs the takeover protocol: a writer that held the stream is fenced. See the file header.
///
/// # Arguments
///
/// * `cluster` - The interfaces to the cluster.
/// * `name` - The stream's name.
/// * `payloads` - The records to append, in order. Must not be empty.
/// * `policy` - Whether to wait for a live writer's lease or to take the stream at once.
///
/// # Returns
///
/// The range of sequence numbers the records were committed at.
///
/// # Errors
///
/// * [`CommandError::NothingToDo`] if `payloads` is empty.
/// * [`CommandError::Open`] if the writer role could not be taken.
/// * [`CommandError::Append`] if an append did not commit. Records appended before it stay committed.
pub(crate) async fn append(
    cluster: &Cluster,
    name: &str,
    payloads: Vec<Bytes>,
    policy: TakeoverPolicy,
) -> Result<AppendedRecords, CommandError> {
    if payloads.is_empty() {
        return Err(CommandError::NothingToDo {
            reason: "no records to append: give them as arguments or on standard input".to_owned(),
        });
    }
    let writer = cluster.client(WRITER_IDENTITY).open_writer(name, policy).await?;
    let epoch = writer.epoch().get();
    let appended = writer.append_batch(payloads).await;
    // Step down whatever happened, so that the next writer does not have to wait for this lease.
    let closed = writer.close().await;
    let committed_range = appended?;
    closed?;
    Ok(AppendedRecords {
        stream: name.to_owned(),
        epoch,
        first_sequence: committed_range.first().get(),
        last_sequence: committed_range.last().get(),
        records: committed_range.record_count(),
    })
}

/// Reads up to `count` committed records.
///
/// # Arguments
///
/// * `cluster` - The interfaces to the cluster.
/// * `name` - The stream's name.
/// * `from` - The first sequence number to read; `None` starts at the stream's low-water mark.
/// * `count` - How many records to read at most.
///
/// # Returns
///
/// The records found, fewer than `count` if the stream ends first.
///
/// # Errors
///
/// * [`CommandError::Read`] if the stream does not exist, `from` lies below the low-water mark, or no
///   replica and no object store could serve the records.
pub(crate) async fn read(
    cluster: &Cluster,
    name: &str,
    from: Option<u64>,
    count: u64,
) -> Result<ReadRecords, CommandError> {
    let reader = cluster.client(WRITER_IDENTITY).open_reader(name);
    let first = match from {
        Some(sequence) => SequenceNumber::new(sequence.max(1)),
        None => reader.low_water_mark().await?,
    };
    if count == 0 {
        return Ok(ReadRecords { records: Vec::new() });
    }
    let last = SequenceNumber::new(first.get().saturating_add(count - 1));
    let found = reader.read_range(SequenceRange::new(first, last)?).await?;
    let mut records = Vec::with_capacity(found.len());
    for record in &found {
        records.push(RecordLine::of(record));
    }
    Ok(ReadRecords { records })
}

/// Follows a stream and writes every committed record to `output` as it commits.
///
/// Text output is one line per record: the sequence number, two spaces, the payload as text. JSON output
/// is one JSON object per line, with the fields of [`RecordLine`].
///
/// # Arguments
///
/// * `cluster` - The interfaces to the cluster.
/// * `name` - The stream's name.
/// * `from` - The first sequence number to print; `None` starts at the stream's low-water mark.
/// * `count` - Stop after this many records; `None` follows until the stream ends for good (it was
///   deleted or the position was truncated) or the process is interrupted.
/// * `format` - Text or JSON lines.
/// * `output` - Where the lines go. It is flushed after every record, so a pipe sees them at once.
///
/// # Returns
///
/// How many records were written.
///
/// # Errors
///
/// * [`CommandError::Read`] if the stream does not exist or following cannot continue.
/// * [`CommandError::Output`] if a line cannot be written.
pub(crate) async fn tail(
    cluster: &Cluster,
    name: &str,
    from: Option<u64>,
    count: Option<u64>,
    format: OutputFormat,
    output: &mut (dyn Write + Send),
) -> Result<u64, CommandError> {
    let reader = cluster.client(WRITER_IDENTITY).open_reader(name);
    let first = match from {
        Some(sequence) => SequenceNumber::new(sequence),
        None => reader.low_water_mark().await?,
    };
    let mut followed = reader.follow_from(first).await?;
    let mut written_records = 0;
    while count.is_none_or(|limit| written_records < limit) {
        let Some(next) = followed.next().await else {
            break;
        };
        let record = RecordLine::of(&next?);
        let line = match format {
            OutputFormat::Table => format!("{}  {}", record.sequence, record.payload_text),
            OutputFormat::Json => serde_json::to_string(&record).expect("a record line holds a number and two texts"),
        };
        let written = writeln!(output, "{line}").and_then(|()| output.flush());
        if let Err(io_error) = written {
            return Err(CommandError::Output {
                reason: io_error.to_string(),
            });
        }
        written_records += 1;
    }
    Ok(written_records)
}

#[cfg(test)]
#[path = "stream_test.rs"]
mod tests;
