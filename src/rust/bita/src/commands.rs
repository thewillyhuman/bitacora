//! Running a command and showing its result.
//!
//! # Why this file exists
//!
//! The command line names a command; the code that does the work lives in one module per noun. Something
//! has to connect the two, and something has to decide how a result becomes text or JSON. Doing both here
//! keeps `main` down to parsing, connecting and printing, and keeps every command a plain function that
//! returns a value, which is what makes the commands testable against an in-process cluster.
//!
//! # What it contains
//!
//! - [`run`]: runs one parsed command against a cluster.
//! - [`CommandOutput`]: every kind of result, and how each is rendered.
//! - [`CommandError`]: why a command failed.
//! - [`Acknowledgement`]: the result of a command that only changes something.
//!
//! # How it works
//!
//! [`run`] matches the command and calls the function of the noun's module with the cluster's interfaces.
//! The function returns a result structure. [`CommandOutput::render`] turns it into JSON with `serde`, or
//! into text with the structure's own `render_text`.
//!
//! `stream tail` is the one command that is not run here: it prints records as they commit instead of
//! returning a result, so `main` calls it directly.
//!
//! # Invariants
//!
//! - A command prints nothing itself. Everything it has to say is in its result or its error.
//! - The JSON of a result is the structure's fields as they are named in the code: renaming a field
//!   changes the tool's interface.
//!
//! # See also
//!
//! - `commands/stream.rs`, `commands/replica.rs`, `commands/segment.rs`, `commands/controller.rs`,
//!   `commands/metadata.rs`, `commands/rebuild.rs`: the commands.
//! - `output.rs`: the text building blocks the results render with.

use bitacora::{AppendError, OpenError, ReadError, TakeoverPolicy};
use bitacora_artifact::ArtifactError;
use bitacora_meta::{MetaError, SnapshotError};
use bitacora_types::replica_api::ReplicaError;
use bitacora_types::{InvalidReplicationSettings, InvalidSequenceRange, ReplicaId};
use bytes::Bytes;
use serde::Serialize;
use thiserror::Error;

use crate::arguments::{
    Command, ControllerCommand, MetadataCommand, ReplicaCommand, SegmentCommand, SnapshotCommand, StreamCommand,
};
use crate::cluster::Cluster;
use crate::output::OutputFormat;

pub(crate) mod controller;
pub(crate) mod metadata;
pub(crate) mod rebuild;
pub(crate) mod replica;
pub(crate) mod segment;
pub(crate) mod stream;

/// Why a command failed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub(crate) enum CommandError {
    /// The metadata store refused the operation or could not be reached.
    #[error(transparent)]
    Metadata(#[from] MetaError),
    /// The requested replication is not possible.
    #[error(transparent)]
    InvalidReplication(#[from] InvalidReplicationSettings),
    /// The requested range of records is not a range.
    #[error(transparent)]
    InvalidRange(#[from] InvalidSequenceRange),
    /// The stream's writer role could not be taken.
    #[error("cannot become the writer: {0}")]
    Open(#[from] OpenError),
    /// An append did not commit.
    #[error("cannot append: {0}")]
    Append(#[from] AppendError),
    /// Records could not be read.
    #[error("cannot read: {0}")]
    Read(#[from] ReadError),
    /// No replica with this identifier is registered.
    #[error("replica {replica_id} is not registered")]
    UnknownReplica {
        /// The identifier that was looked up.
        replica_id: ReplicaId,
    },
    /// A replica answered with an error.
    #[error("replica {replica_id} refused: {reason}")]
    Replica {
        /// The replica that was asked.
        replica_id: ReplicaId,
        /// What it answered.
        reason: ReplicaError,
    },
    /// A replica did not answer within the request timeout.
    #[error("replica {replica_id} did not answer in time")]
    ReplicaTimedOut {
        /// The replica that was asked.
        replica_id: ReplicaId,
    },
    /// The command would do something the operator has to confirm first.
    #[error("{reason}")]
    Refused {
        /// What is refused, and which option allows it.
        reason: String,
    },
    /// The command was given nothing to work on.
    #[error("{reason}")]
    NothingToDo {
        /// What is missing.
        reason: String,
    },
    /// The result could not be written to the terminal.
    #[error("cannot write the output: {reason}")]
    Output {
        /// What the operating system reported.
        reason: String,
    },
    /// The command needs the object store and the cluster configuration names none.
    #[error(
        "this command needs the cluster's object store: name it in a configuration file and pass \
         --config"
    )]
    NoObjectStore,
    /// The object store failed a request.
    #[error("the object store failed: {reason}")]
    ObjectStore {
        /// What the object store reported.
        reason: ArtifactError,
    },
    /// The object store did not answer within the request timeout.
    #[error("the object store did not answer in time")]
    ObjectStoreTimedOut,
    /// An object that should be a metadata snapshot is not an intact one.
    #[error("snapshot {object_key:?} cannot be used: {reason}")]
    UnreadableSnapshot {
        /// The object that was read.
        object_key: String,
        /// What is wrong with it.
        reason: SnapshotError,
    },
}

/// The result of a command that changes something and has nothing else to report.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Acknowledgement {
    /// What was done, as one sentence without a final period.
    pub(crate) message: String,
}

impl Acknowledgement {
    /// Renders the acknowledgement as one line.
    fn render_text(&self) -> String {
        format!("{}\n", self.message)
    }
}

/// The result of a command, whatever the command was.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub(crate) enum CommandOutput {
    /// A change was made and there is nothing else to report.
    Acknowledgement(Acknowledgement),
    /// One stream, in brief.
    StreamSummary(stream::StreamSummary),
    /// Every stream, in brief.
    StreamList(stream::StreamList),
    /// One stream, in full.
    StreamDetails(stream::StreamDetails),
    /// One materializer and its checkpoint.
    Materializer(stream::MaterializerDetails),
    /// What an append committed.
    AppendedRecords(stream::AppendedRecords),
    /// Records that were read.
    ReadRecords(stream::ReadRecords),
    /// The registered replicas.
    ReplicaList(replica::ReplicaList),
    /// What one replica reports about itself.
    ReplicaReport(replica::ReplicaReport),
    /// How far a drain has got.
    DrainReport(replica::DrainReport),
    /// The segments of a stream.
    SegmentList(segment::SegmentList),
    /// Who leads the controllers.
    ControllerStatus(controller::ControllerStatus),
    /// The snapshots of the metadata store.
    SnapshotList(metadata::SnapshotList),
    /// What a restore of the metadata store did, or would do.
    RestoreReport(metadata::RestoreReport),
    /// What a rebuild of the metadata store found, planned and did.
    RebuildReport(rebuild::RebuildReport),
}

impl CommandOutput {
    /// Renders the result for printing.
    ///
    /// # Arguments
    ///
    /// * `format` - Text for a person, or JSON for a script.
    ///
    /// # Returns
    ///
    /// The text to print, ended by a newline.
    pub(crate) fn render(&self, format: OutputFormat) -> String {
        match format {
            OutputFormat::Json => {
                let mut rendered = serde_json::to_string_pretty(self)
                    .expect("results hold only text, numbers, lists and maps with text keys");
                rendered.push('\n');
                rendered
            }
            OutputFormat::Table => self.render_text(),
        }
    }

    /// Renders the result as text for a person.
    fn render_text(&self) -> String {
        match self {
            Self::Acknowledgement(acknowledgement) => acknowledgement.render_text(),
            Self::StreamSummary(summary) => summary.render_text(),
            Self::StreamList(list) => list.render_text(),
            Self::StreamDetails(details) => details.render_text(),
            Self::Materializer(materializer) => materializer.render_text(),
            Self::AppendedRecords(appended) => appended.render_text(),
            Self::ReadRecords(records) => records.render_text(),
            Self::ReplicaList(list) => list.render_text(),
            Self::ReplicaReport(report) => report.render_text(),
            Self::DrainReport(report) => report.render_text(),
            Self::SegmentList(list) => list.render_text(),
            Self::ControllerStatus(status) => status.render_text(),
            Self::SnapshotList(list) => list.render_text(),
            Self::RestoreReport(report) => report.render_text(),
            Self::RebuildReport(report) => report.render_text(),
        }
    }
}

/// Runs one command against a cluster.
///
/// # Arguments
///
/// * `command` - The parsed command. `stream tail` must not be passed: it streams its output and is run
///   by `main` through [`stream::tail`].
/// * `cluster` - The interfaces to the cluster.
/// * `standard_input_records` - The lines of standard input, for `stream append` without record
///   arguments. Empty for every other command.
///
/// # Returns
///
/// The command's result, ready to be rendered.
///
/// # Errors
///
/// Returns the [`CommandError`] of the command that failed.
pub(crate) async fn run(
    command: &Command,
    cluster: &Cluster,
    standard_input_records: Vec<Bytes>,
) -> Result<CommandOutput, CommandError> {
    match command {
        Command::Stream(stream_command) => run_stream_command(stream_command, cluster, standard_input_records).await,
        Command::Replica(replica_command) => run_replica_command(replica_command, cluster).await,
        Command::Segment(SegmentCommand::List { stream }) => {
            Ok(CommandOutput::SegmentList(segment::list(cluster, stream).await?))
        }
        Command::Controller(ControllerCommand::Status) => {
            Ok(CommandOutput::ControllerStatus(controller::status(cluster).await?))
        }
        Command::Metadata(metadata_command) => run_metadata_command(metadata_command, cluster).await,
    }
}

/// Runs one `bita metadata` command.
async fn run_metadata_command(command: &MetadataCommand, cluster: &Cluster) -> Result<CommandOutput, CommandError> {
    match command {
        MetadataCommand::Snapshot(SnapshotCommand::List) => {
            Ok(CommandOutput::SnapshotList(metadata::list_snapshots(cluster).await?))
        }
        MetadataCommand::Restore {
            snapshot,
            force,
            dry_run,
        } => Ok(CommandOutput::RestoreReport(
            metadata::restore(cluster, snapshot, *force, *dry_run).await?,
        )),
        MetadataCommand::Rebuild {
            names,
            replicas,
            apply,
            force,
        } => Ok(CommandOutput::RebuildReport(
            rebuild::rebuild(cluster, names, replicas, *apply, *force).await?,
        )),
    }
}

/// Runs one `bita stream` command.
///
/// One arm per command, each a call of the function in `commands/stream.rs` that does the work. It is
/// longer than most functions of this crate because the command has ten forms; nothing else happens here.
async fn run_stream_command(
    command: &StreamCommand,
    cluster: &Cluster,
    standard_input_records: Vec<Bytes>,
) -> Result<CommandOutput, CommandError> {
    match command {
        StreamCommand::Create {
            name,
            replicas,
            write_quorum,
            keep_sealed_for,
        } => {
            let created = stream::create(cluster, name, *replicas, *write_quorum, *keep_sealed_for).await?;
            Ok(CommandOutput::StreamSummary(created))
        }
        StreamCommand::List => Ok(CommandOutput::StreamList(stream::list(cluster).await?)),
        StreamCommand::Describe { name } => Ok(CommandOutput::StreamDetails(stream::describe(cluster, name).await?)),
        StreamCommand::Delete { name, yes } => Ok(CommandOutput::Acknowledgement(
            stream::delete(cluster, name, *yes).await?,
        )),
        StreamCommand::TruncateBefore { name, sequence } => Ok(CommandOutput::Acknowledgement(
            stream::truncate_before(cluster, name, *sequence).await?,
        )),
        StreamCommand::RegisterMaterializer { name, materializer } => Ok(CommandOutput::Materializer(
            stream::register_materializer(cluster, name, materializer).await?,
        )),
        StreamCommand::UnregisterMaterializer { name, materializer } => Ok(CommandOutput::Acknowledgement(
            stream::unregister_materializer(cluster, name, materializer).await?,
        )),
        StreamCommand::Append { name, records, force } => {
            let payloads = payloads_to_append(standard_input_records, records);
            let policy = takeover_policy(*force);
            Ok(CommandOutput::AppendedRecords(
                stream::append(cluster, name, payloads, policy).await?,
            ))
        }
        StreamCommand::Read { name, from, count } => Ok(CommandOutput::ReadRecords(
            stream::read(cluster, name, *from, *count).await?,
        )),
        StreamCommand::Tail { .. } => Err(CommandError::NothingToDo {
            reason: "stream tail prints as it goes and is not run as a command with a result".to_owned(),
        }),
    }
}

/// The records `stream append` appends: the lines of standard input, then the arguments, in that order.
/// The command line gives one or the other, so in practice one of the two is empty.
fn payloads_to_append(standard_input_records: Vec<Bytes>, argument_records: &[String]) -> Vec<Bytes> {
    let mut payloads = standard_input_records;
    for record in argument_records {
        payloads.push(Bytes::from(record.clone()));
    }
    payloads
}

/// How `stream append` takes the stream: at once if the operator forced it, otherwise only after the
/// lease of a live writer has expired.
const fn takeover_policy(is_forced: bool) -> TakeoverPolicy {
    if is_forced {
        TakeoverPolicy::Force
    } else {
        TakeoverPolicy::WaitForLeaseExpiry
    }
}

/// Runs one `bita replica` command.
async fn run_replica_command(command: &ReplicaCommand, cluster: &Cluster) -> Result<CommandOutput, CommandError> {
    match command {
        ReplicaCommand::List => Ok(CommandOutput::ReplicaList(replica::list(cluster).await?)),
        ReplicaCommand::Status { replica_id } => Ok(CommandOutput::ReplicaReport(
            replica::status(cluster, replica_id).await?,
        )),
        ReplicaCommand::Drain { replica_id, wait } => Ok(CommandOutput::DrainReport(
            replica::drain(cluster, replica_id, *wait).await?,
        )),
        ReplicaCommand::Remove { replica_id, force } => Ok(CommandOutput::Acknowledgement(
            replica::remove(cluster, replica_id, *force).await?,
        )),
    }
}

#[cfg(test)]
pub(crate) mod testing;

#[cfg(test)]
#[path = "commands_test.rs"]
mod tests;
