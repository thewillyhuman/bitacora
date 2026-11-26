//! The entry point of the client library.
//!
//! # Why this file exists
//!
//! An application does three things with a bitacora cluster: it writes a stream, it reads streams, and it creates and
//! removes them. Each of those has its own handle, and all of them need the same injected interfaces and settings.
//! [`BitacoraClient`] is the one value an application holds: it is built once from those interfaces, and every writer,
//! reader and administration handle is opened from it.
//!
//! # What it contains
//!
//! - [`BitacoraClient`]: the handle to a cluster.
//! - [`ClientSettings`]: the settings of the writers and readers it opens.
//!
//! # How it works
//!
//! The client owns a shared `ClientContext` with the metadata store, the way to reach replicas, the clock and,
//! optionally, the object store. Opening a reader or the administration copies a reference to it and contacts nothing.
//! Opening a writer runs the takeover protocol, which is the only thing here that takes time.
//!
//! Production code passes the etcd store, the gRPC connector and the system clock (the `bitacora` crate does that from
//! a configuration); tests and the simulator pass in-memory ones. The client cannot tell the difference, which is the
//! point.
//!
//! # Invariants
//!
//! - A client holds no connection and no task of its own. Dropping it stops nothing; writers and readers opened from it
//!   keep working.
//! - Cloning a client is cheap and gives a handle to the same cluster with the same settings.
//!
//! # See also
//!
//! - `context.rs`: what the client shares with what it opens.
//! - `writer.rs`, `reader.rs`, `admin.rs`: the three handles.
//! - The `bitacora` crate: `connect`, which builds a client for a real cluster.

use std::sync::Arc;

use bitacora_artifact::ArtifactStore;
use bitacora_meta::MetadataStore;
use bitacora_types::Clock;
use bitacora_types::replica_api::ReplicaConnector;

use crate::admin::StreamAdmin;
use crate::context::ClientContext;
use crate::error::OpenError;
use crate::reader::StreamReader;
use crate::settings::{ReaderSettings, TakeoverPolicy, WriterSettings};
use crate::writer::StreamWriter;

/// The settings of everything a client opens.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_client::{ClientSettings, ReaderSettings, WriterSettings};
///
/// let settings = ClientSettings {
///     writer: WriterSettings {
///         writer_identity: "compute-7".to_owned(),
///         ..WriterSettings::default()
///     },
///     reader: ReaderSettings {
///         tail_wait: Duration::from_millis(250),
///         ..ReaderSettings::default()
///     },
/// };
///
/// assert_eq!(settings.writer.writer_identity, "compute-7");
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientSettings {
    /// How writers opened by this client behave.
    pub writer: WriterSettings,
    /// How readers opened by this client behave.
    pub reader: ReaderSettings,
}

/// A handle to a bitacora cluster.
///
/// It is built from the injected interfaces of the design: the metadata store, the way to reach replicas, and the
/// clock. Writers, readers and the administration handle are opened from it. See the file header for how it works.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions, TakeoverPolicy};
/// use bitacora_types::SequenceNumber;
///
/// // An in-process cluster of three replicas stands in for a real one.
/// let cluster = bitacora_testkit::Cluster::start(3).await;
/// let client = BitacoraClient::new(
///     cluster.metadata().clone(),
///     cluster.connector(),
///     cluster.clock(),
///     ClientSettings::default(),
/// );
///
/// client.admin().create_stream("orders", StreamOptions::default()).await?;
/// let writer = client.open_writer("orders", TakeoverPolicy::Force).await?;
/// let commit_sequence = writer.append("first order".into()).await?;
///
/// assert_eq!(commit_sequence, SequenceNumber::FIRST);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct BitacoraClient {
    /// The injected interfaces, shared with every handle opened from this client.
    context: Arc<ClientContext>,
    /// The settings handed to the writers and readers this client opens.
    settings: ClientSettings,
}

impl BitacoraClient {
    /// Creates a client over the given interfaces.
    ///
    /// Nothing is contacted. The client has no object store; give it one with [`BitacoraClient::with_artifact_store`]
    /// if the cluster tiers its segments.
    ///
    /// # Arguments
    ///
    /// * `metadata` - The cluster's metadata store.
    /// * `connector` - How replicas are reached at the addresses they registered.
    /// * `clock` - The wall clock, used for leases and seal times.
    /// * `settings` - How the writers and readers opened from this client behave.
    ///
    /// # Returns
    ///
    /// A client that every writer, reader and administration handle of the cluster is opened from.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_client::{BitacoraClient, ClientSettings};
    ///
    /// let cluster = bitacora_testkit::Cluster::start(3).await;
    ///
    /// let client = BitacoraClient::new(
    ///     cluster.metadata().clone(),
    ///     cluster.connector(),
    ///     cluster.clock(),
    ///     ClientSettings::default(),
    /// );
    ///
    /// assert!(client.admin().list_streams().await.is_ok());
    /// # }
    /// ```
    #[must_use]
    pub fn new(
        metadata: MetadataStore,
        connector: Arc<dyn ReplicaConnector>,
        clock: Arc<dyn Clock>,
        settings: ClientSettings,
    ) -> Self {
        Self {
            context: Arc::new(ClientContext {
                metadata,
                connector,
                clock,
                artifact_store: None,
            }),
            settings,
        }
    }

    /// Gives the client the object store that sealed segments are tiered to (design, section 10, "Tiering"), so that
    /// its readers can read segments whose copies the replicas have dropped.
    ///
    /// Without one, a read of such a segment fails as [`ReadError::Unavailable`](crate::ReadError::Unavailable). Call
    /// it right after [`BitacoraClient::new`]: readers opened before keep reading from replicas only.
    ///
    /// # Arguments
    ///
    /// * `artifact_store` - The object store the cluster's replicas upload to. It must be the same store, or the
    ///   objects the metadata store names will not be found.
    ///
    /// # Returns
    ///
    /// A client with the same interfaces and settings, and the object store.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use bitacora_client::{BitacoraClient, ClientSettings};
    ///
    /// let cluster = bitacora_testkit::Cluster::start(3).await;
    ///
    /// let client = BitacoraClient::new(
    ///     cluster.metadata().clone(),
    ///     cluster.connector(),
    ///     cluster.clock(),
    ///     ClientSettings::default(),
    /// )
    /// .with_artifact_store(Arc::new(cluster.artifact_store().clone()));
    /// # drop(client);
    /// # }
    /// ```
    #[must_use]
    pub fn with_artifact_store(self, artifact_store: Arc<dyn ArtifactStore>) -> Self {
        Self {
            context: Arc::new(ClientContext {
                metadata: self.context.metadata.clone(),
                connector: Arc::clone(&self.context.connector),
                clock: Arc::clone(&self.context.clock),
                artifact_store: Some(artifact_store),
            }),
            settings: self.settings,
        }
    }

    /// Becomes the writer of `stream_name`.
    ///
    /// This runs the takeover protocol (design, section 9): it claims the next epoch, fences and seals whatever segment
    /// the previous writer left open, and opens a new one. bitacora does not choose which compute node should be the
    /// writer; it guarantees that whoever calls this last wins cleanly. The call makes several requests to the metadata
    /// store and to the replicas of the previous writer's segment, and returns once the new segment is open.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The stream to write.
    /// * `policy` - Whether to wait for the previous writer's lease to lapse.
    ///
    /// # Returns
    ///
    /// The single writer of the stream, until another caller opens one.
    ///
    /// # Errors
    ///
    /// * [`OpenError::Metadata`] if the stream does not exist or the metadata store cannot be reached.
    /// * [`OpenError::TakenByAnotherWriter`] if another candidate claimed the stream first.
    /// * [`OpenError::LeaseHeld`] if a live writer holds the lease, `policy` says to wait, and the lease outlasts the
    ///   writer's `open_timeout`.
    /// * [`OpenError::RecoveryQuorumUnavailable`] if too few replicas of the previous writer's segment answered in
    ///   time.
    /// * [`OpenError::NoStandbySegment`] if the controller did not place the next segment in time.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions};
    /// use bitacora_client::TakeoverPolicy;
    /// use bitacora_types::Epoch;
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("orders", StreamOptions::default()).await?;
    ///
    /// let writer = client.open_writer("orders", TakeoverPolicy::WaitForLeaseExpiry).await?;
    ///
    /// assert_eq!(writer.epoch(), Epoch::new(1));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open_writer(&self, stream_name: &str, policy: TakeoverPolicy) -> Result<StreamWriter, OpenError> {
        StreamWriter::open(
            Arc::clone(&self.context),
            self.settings.writer.clone(),
            stream_name,
            policy,
        )
        .await
    }

    /// Returns the administrative interface: creating, describing, truncating and deleting streams, and registering
    /// materializers.
    ///
    /// # Returns
    ///
    /// A handle that shares this client's metadata store. Nothing is contacted until it is used.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings};
    /// use bitacora_client::StreamOptions;
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    ///
    /// let created = client.admin().create_stream("orders", StreamOptions::default()).await?;
    ///
    /// assert_eq!(created.name, "orders");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn admin(&self) -> StreamAdmin {
        StreamAdmin::new(Arc::clone(&self.context))
    }

    /// Returns a reader of `stream_name`. Any number of readers can follow the same stream.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The stream to read.
    ///
    /// # Returns
    ///
    /// The reader, with this client's reader settings. Nothing is contacted until it is used, so a reader of a stream
    /// that does not exist fails at its first read.
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
    ///
    /// let reader = client.open_reader("orders");
    ///
    /// assert_eq!(reader.low_water_mark().await?, SequenceNumber::FIRST);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn open_reader(&self, stream_name: &str) -> StreamReader {
        StreamReader::new(Arc::clone(&self.context), self.settings.reader.clone(), stream_name)
    }
}
