//! The command line of `bita`.
//!
//! # Why this file exists
//!
//! An operator's tool is judged by whether its commands can be guessed. This file is the whole command
//! line in one place: every command, every option, and the help text shown for each, so that the shape of
//! the tool can be read top to bottom and changed without hunting through the commands' code.
//!
//! # What it contains
//!
//! - [`Arguments`]: the options every command takes, and the command.
//! - [`ClusterArguments`]: where the cluster is.
//! - [`Command`] and one enum per noun: [`StreamCommand`], [`ReplicaCommand`], [`SegmentCommand`],
//!   [`ControllerCommand`], [`MetadataCommand`] with its [`SnapshotCommand`].
//!
//! # How it works
//!
//! Commands are `bita <noun> <verb>`: the noun is what is acted on, the verb what is done to it. The types
//! here only describe the command line; `clap` derives the parser and the help from them, and the doc
//! comment of each item is its help text. Values that have a notation of their own (durations, replica
//! identifiers) are checked while parsing, so a mistyped one is a usage error before anything is sent.
//!
//! # Invariants
//!
//! - Parsing has no side effect: nothing is connected or read here.
//! - Every option that names the cluster or shapes the output is global, so it may be written before or
//!   after the command.
//!
//! # See also
//!
//! - `cluster.rs`: turns [`ClusterArguments`] into a cluster configuration.
//! - `commands.rs`: runs a parsed [`Command`].

use std::path::PathBuf;
use std::time::Duration;

use bitacora_types::ReplicaId;
use clap::{Args, Parser, Subcommand};

use crate::duration::parse_duration;
use crate::output::OutputFormat;

/// Everything written after `bita` on a command line.
#[derive(Clone, Debug, PartialEq, Eq, Parser)]
#[command(
    name = "bita",
    version,
    about = "Operate a bitacora cluster: streams, replicas, segments and the controller.",
    propagate_version = true
)]
pub(crate) struct Arguments {
    /// Where the cluster is.
    #[command(flatten)]
    pub(crate) cluster: ClusterArguments,

    /// How results are printed: aligned text for people, or JSON with stable field names for scripts.
    #[arg(long, global = true, value_enum, default_value = "table")]
    pub(crate) output: OutputFormat,

    /// How long one request to a replica may take, for example 5s or 500ms.
    #[arg(
        long,
        global = true,
        default_value = "5s",
        value_parser = parse_duration,
        value_name = "DURATION"
    )]
    pub(crate) request_timeout: Duration,

    /// What to do.
    #[command(subcommand)]
    pub(crate) command: Command,
}

/// Where the cluster is: a configuration file, or the etcd endpoints given directly.
///
/// Options win over environment variables, and both win over the configuration file. The environment
/// variables are `BITA_CONFIG`, `BITA_ETCD_ENDPOINTS` (separated by commas) and `BITA_ETCD_KEY_PREFIX`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Args)]
pub(crate) struct ClusterArguments {
    /// The configuration file named on the command line, if one was. The help text is given apart from
    /// this comment because it names an environment variable the way a help screen does.
    #[arg(
        long,
        global = true,
        value_name = "FILE",
        help = "A TOML file with the cluster configuration: etcd endpoints, key prefix and \
                object store [env: BITA_CONFIG]"
    )]
    pub(crate) config: Option<PathBuf>,

    /// The etcd endpoints given on the command line, in the order they were given.
    #[arg(
        long = "etcd-endpoint",
        global = true,
        value_name = "URL",
        help = "The client URL of an etcd member that holds the cluster's metadata; repeat it \
                for every member [env: BITA_ETCD_ENDPOINTS, separated by commas]"
    )]
    pub(crate) etcd_endpoints: Vec<String>,

    /// The etcd key prefix given on the command line, if one was.
    #[arg(
        long,
        global = true,
        value_name = "PREFIX",
        help = "The text in front of every key of this cluster in etcd \
                [env: BITA_ETCD_KEY_PREFIX] [default: /bitacora/]"
    )]
    pub(crate) etcd_key_prefix: Option<String>,
}

/// What `bita` is asked to do, by the kind of thing it is done to.
#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
pub(crate) enum Command {
    /// Create, inspect, write to, read from and delete streams.
    #[command(subcommand)]
    Stream(StreamCommand),
    /// Inspect, drain and remove replicas.
    #[command(subcommand)]
    Replica(ReplicaCommand),
    /// Inspect the segments of a stream.
    #[command(subcommand)]
    Segment(SegmentCommand),
    /// Inspect the controller.
    #[command(subcommand)]
    Controller(ControllerCommand),
    /// Recover a lost metadata store: list its snapshots, restore one, or rebuild it from the replicas.
    #[command(subcommand)]
    Metadata(MetadataCommand),
}

/// What can be done to a stream.
#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
pub(crate) enum StreamCommand {
    /// Create a stream and place its first segment.
    Create {
        /// The stream's name: 1 to 255 ASCII letters, digits, '.', '_' or '-'.
        name: String,
        /// How many replicas hold each segment.
        #[arg(long, default_value_t = 3)]
        replicas: u8,
        /// How many replicas must store a record before it is committed.
        #[arg(long, default_value_t = 2)]
        write_quorum: u8,
        /// Keep every sealed segment at least this long, for example 7d. Without it, records are deleted
        /// as soon as truncation allows.
        #[arg(long, value_parser = parse_duration, value_name = "DURATION")]
        keep_sealed_for: Option<Duration>,
    },
    /// List every stream.
    List,
    /// Show everything the metadata store knows about one stream.
    Describe {
        /// The stream's name.
        name: String,
    },
    /// Delete a stream and, through the controller, all of its records. This cannot be undone.
    Delete {
        /// The stream's name.
        name: String,
        /// Confirms that the stream and its records are to be deleted.
        #[arg(long)]
        yes: bool,
    },
    /// Allow every record below a sequence number to be deleted, as far as materializers and retention
    /// permit.
    TruncateBefore {
        /// The stream's name.
        name: String,
        /// The first sequence number to keep.
        sequence: u64,
    },
    /// Register a materializer: from then on the log keeps every record above its checkpoint.
    RegisterMaterializer {
        /// The stream's name.
        name: String,
        /// The materializer's name, unique within the stream.
        materializer: String,
    },
    /// Unregister a materializer, so that it no longer holds records back.
    UnregisterMaterializer {
        /// The stream's name.
        name: String,
        /// The materializer's name.
        materializer: String,
    },
    /// Become the stream's writer, append records and step down. For smoke tests: a stream has one
    /// writer, and this takes its place.
    Append {
        /// The stream's name.
        name: String,
        /// The records to append, one per argument. Without any, every line of standard input is one
        /// record.
        records: Vec<String>,
        /// Take the stream from a live writer at once, instead of waiting for its lease to expire.
        #[arg(long)]
        force: bool,
    },
    /// Read committed records and stop.
    Read {
        /// The stream's name.
        name: String,
        /// The first sequence number to read. Without it, reading starts at the oldest record still
        /// kept.
        #[arg(long, value_name = "SEQUENCE")]
        from: Option<u64>,
        /// How many records to read at most.
        #[arg(long, default_value_t = 100)]
        count: u64,
    },
    /// Follow a stream: print every committed record as it commits, until interrupted.
    Tail {
        /// The stream's name.
        name: String,
        /// The first sequence number to print. Without it, following starts at the oldest record still
        /// kept.
        #[arg(long, value_name = "SEQUENCE")]
        from: Option<u64>,
        /// Stop after this many records, instead of following until interrupted.
        #[arg(long)]
        count: Option<u64>,
    },
}

/// What can be done to a replica.
#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
pub(crate) enum ReplicaCommand {
    /// List the registered replicas: liveness, disk usage, draining and failure domains.
    List,
    /// Ask one replica what it reports about itself.
    Status {
        /// The replica's identifier, as `bita replica list` shows it.
        #[arg(value_parser = parse_replica_id)]
        replica_id: ReplicaId,
    },
    /// Drain a replica before maintenance: it refuses new appends, so writers roll away from it.
    Drain {
        /// The replica's identifier.
        #[arg(value_parser = parse_replica_id)]
        replica_id: ReplicaId,
        /// Wait up to this long for every open segment to have left the replica, for example 2m.
        #[arg(long, value_parser = parse_duration, value_name = "DURATION")]
        wait: Option<Duration>,
    },
    /// Remove a failed replica from the registry, so that repair re-creates its copies elsewhere.
    Remove {
        /// The replica's identifier.
        #[arg(value_parser = parse_replica_id)]
        replica_id: ReplicaId,
        /// Remove the replica even though it still renews its registration.
        #[arg(long)]
        force: bool,
    },
}

/// What can be done to segments.
#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
pub(crate) enum SegmentCommand {
    /// List the segments of a stream: range, state, copies and object.
    List {
        /// The stream's name.
        stream: String,
    },
}

/// What can be done to the controller.
#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
pub(crate) enum ControllerCommand {
    /// Show which controller instance leads, and since when.
    Status,
}

/// What can be done to recover the metadata store.
#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
pub(crate) enum MetadataCommand {
    /// Inspect the snapshots of the metadata store that the controller keeps in the object store.
    #[command(subcommand)]
    Snapshot(SnapshotCommand),
    /// Write a snapshot back into the metadata store. Run it with writers and controllers stopped,
    /// and run `bita metadata rebuild` afterwards: a snapshot is some minutes older than the cluster.
    Restore {
        /// The snapshot to restore: its object key as `bita metadata snapshot list` shows it, or
        /// `latest`.
        snapshot: String,
        /// Restore into a metadata store that is not empty: keys of the snapshot are overwritten and
        /// every other key is deleted.
        #[arg(long)]
        force: bool,
        /// Only say what would be written and deleted.
        #[arg(long)]
        dry_run: bool,
    },
    /// Last resort: put the stream records back together from what the replicas and the object store
    /// hold. Prints its plan and writes nothing unless --apply is given.
    Rebuild {
        /// The name of a stream that the metadata store does not know, as `<stream id>=<name>`. Names
        /// are not stored on replicas, so every such stream needs one; repeat the option.
        #[arg(long = "name", value_parser = parse_stream_name, value_name = "ID=NAME")]
        names: Vec<(u64, String)>,
        /// A replica to ask that is not in the registry, as `<replica id>=<address>`; repeat the
        /// option. Registered replicas are always asked.
        #[arg(long = "replica", value_parser = parse_replica_address, value_name = "ID=ADDRESS")]
        replicas: Vec<(ReplicaId, String)>,
        /// Write the rebuilt stream records. Without it, the plan is only printed.
        #[arg(long)]
        apply: bool,
        /// Write what could be rebuilt although the plan has problems.
        #[arg(long)]
        force: bool,
        /// Say that the cluster has no object store. Finding none configured is then not a problem
        /// of the plan; without this option it is, because a configuration that was forgotten looks
        /// the same and would leave the segments in the object store out of the rebuilt records.
        #[arg(long)]
        no_object_store: bool,
    },
}

/// What can be done to the snapshots of the metadata store.
#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
pub(crate) enum SnapshotCommand {
    /// List the snapshots in the object store, oldest first.
    List,
}

/// Reads a replica identifier, for `clap`.
///
/// # Errors
///
/// Returns why the text is not a replica identifier, as the usage error shows it.
fn parse_replica_id(text: &str) -> Result<ReplicaId, String> {
    match ReplicaId::new(text) {
        Ok(replica_id) => Ok(replica_id),
        Err(invalid) => Err(invalid.to_string()),
    }
}

/// Reads `<stream id>=<name>`, for `clap`.
///
/// # Errors
///
/// Returns what is wrong with the text, as the usage error shows it.
fn parse_stream_name(text: &str) -> Result<(u64, String), String> {
    let Some((stream_id, name)) = text.split_once('=') else {
        return Err("expected <stream id>=<name>, as in 7=orders".to_owned());
    };
    let Ok(stream_id) = stream_id.parse::<u64>() else {
        return Err(format!("{stream_id:?} is not a stream id: expected a number"));
    };
    if name.is_empty() {
        return Err("the name after `=` is empty".to_owned());
    }
    Ok((stream_id, name.to_owned()))
}

/// Reads `<replica id>=<address>`, for `clap`.
///
/// # Errors
///
/// Returns what is wrong with the text, as the usage error shows it.
fn parse_replica_address(text: &str) -> Result<(ReplicaId, String), String> {
    let Some((replica_id, address)) = text.split_once('=') else {
        return Err("expected <replica id>=<address>, as in replica-a=10.0.0.1:7000".to_owned());
    };
    if address.is_empty() {
        return Err("the address after `=` is empty".to_owned());
    }
    Ok((parse_replica_id(replica_id)?, address.to_owned()))
}

#[cfg(test)]
#[path = "arguments_test.rs"]
mod tests;
