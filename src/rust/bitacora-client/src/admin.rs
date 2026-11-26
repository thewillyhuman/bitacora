//! Stream administration: creating, describing, truncating and deleting streams.
//!
//! # Why this file exists
//!
//! Before anything can be appended a stream has to exist, and over its life an owner wants to look at it, give back the
//! space of records nobody needs, register the materializers whose progress decides what may be deleted, and in the end
//! remove it. None of that touches a replica: it is all a few records in the metadata store. This file is the part of
//! the client's API surface (design, section 13) that does it.
//!
//! # What it contains
//!
//! - [`StreamAdmin`]: the handle, obtained from [`crate::BitacoraClient::admin`].
//! - [`StreamOptions`]: how a new stream is replicated and how long it keeps its records.
//! - [`StreamDescription`]: a stream's record together with the checkpoints of its materializers.
//!
//! # How it works
//!
//! Every operation is one call, or a read followed by one conditional write, to the metadata store. The handle decides
//! nothing about replicas, with one exception: creating a stream also chooses the replica set of its first segment, so
//! that a writer can open the stream at once without waiting for the controller. Placement is the same pure function
//! the controller uses. If too few replicas are registered at that moment, the stream is created without a first
//! segment and the controller places it as soon as it can.
//!
//! Truncation here is a request, not a deletion. `truncate_before` records the position the owner no longer needs; the
//! controller moves the low-water mark and deletes whole segments, and only as far as every registered materializer's
//! checkpoint and the retention policy allow.
//!
//! # Invariants
//!
//! - The requested truncation position only ever moves forward.
//! - Nothing here deletes a record or a segment. That is the controller's job, done in passes.
//!
//! # See also
//!
//! - `bitacora-meta`: the operations these calls go through.
//! - `bitacora-controller`: what acts on a truncation request, a deletion and a missing first segment.

use std::collections::BTreeSet;
use std::sync::Arc;

use bitacora_meta::{CheckpointRecord, MetaError, PlacementPolicy, RetentionPolicy, StreamRecord, choose_replica_set};
use bitacora_types::{ReplicaId, ReplicationSettings, SequenceNumber};

use crate::context::ClientContext;

/// How a new stream is replicated and how long it keeps its records.
///
/// # Examples
///
/// ```
/// use bitacora_client::StreamOptions;
/// use bitacora_types::ReplicationSettings;
///
/// let options = StreamOptions::default();
///
/// assert_eq!(options.replication, ReplicationSettings::DEFAULT);
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamOptions {
    /// How many replicas hold each segment and how many commit a record.
    pub replication: ReplicationSettings,
    /// How long records nothing needs are kept.
    pub retention: RetentionPolicy,
}

/// Everything the metadata store knows about one stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamDescription {
    /// The stream's record: epoch, writer, segments, low-water mark.
    pub stream: StreamRecord,
    /// The checkpoints of the materializers registered on the stream.
    pub checkpoints: Vec<CheckpointRecord>,
}

/// Creates, describes, truncates and deletes streams, and registers the materializers whose checkpoints decide what may
/// be truncated.
///
/// Obtained from [`crate::BitacoraClient::admin`]. Cloning it is cheap.
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
/// let admin = client.admin();
/// admin.create_stream("orders", StreamOptions::default()).await?;
/// admin.register_materializer("orders", "pages").await?;
///
/// let description = admin.describe_stream("orders").await?;
///
/// assert_eq!(description.checkpoints.len(), 1);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct StreamAdmin {
    /// The injected interfaces. Only the metadata store and the clock are used.
    context: Arc<ClientContext>,
}

impl StreamAdmin {
    /// Wraps the client's context.
    ///
    /// # Arguments
    ///
    /// * `context` - The injected interfaces of the client that opens this handle.
    ///
    /// # Returns
    ///
    /// The administration handle. Nothing is contacted.
    pub(crate) const fn new(context: Arc<ClientContext>) -> Self {
        Self { context }
    }

    /// Creates a stream and places its first segment, so that a writer can open it at once.
    ///
    /// Reads the replica registry, chooses a replica set for the first segment and writes the stream's record. If too
    /// few replicas are registered right now, the stream is created anyway and the controller places the segment as
    /// soon as it can.
    ///
    /// # Arguments
    ///
    /// * `name` - The stream's name: a non-empty path segment, unique in the cluster.
    /// * `options` - Replication and retention of the new stream.
    ///
    /// # Returns
    ///
    /// The record of the new stream. Its `standby_replicas` are `None` if no first segment could be placed.
    ///
    /// # Errors
    ///
    /// * [`MetaError::StreamAlreadyExists`] if a stream has this name.
    /// * [`MetaError::InvalidName`] if the name cannot be used.
    /// * [`MetaError::Unavailable`] if the metadata store cannot be reached.
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
    /// // Three replicas are registered, so the first segment was placed.
    /// assert!(created.standby_replicas.is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_stream(&self, name: &str, options: StreamOptions) -> Result<StreamRecord, MetaError> {
        let first_replica_set = self.place_first_segment(name, options.replication).await?;
        self.context
            .metadata
            .create_stream(name, options.replication, options.retention, first_replica_set)
            .await
    }

    /// Chooses the replicas of a new stream's first segment, or `None`, with a warning, if too few are registered.
    async fn place_first_segment(
        &self,
        name: &str,
        replication: ReplicationSettings,
    ) -> Result<Option<Vec<ReplicaId>>, MetaError> {
        let registered_replicas = self.context.metadata.list_replicas().await?;
        let placed = choose_replica_set(
            &registered_replicas,
            replication.replica_count(),
            self.context.clock.now(),
            PlacementPolicy::default(),
            &BTreeSet::new(),
        );
        match placed {
            Ok(first_replica_set) => Ok(Some(first_replica_set)),
            Err(placement_error) => {
                tracing::warn!(
                    stream = name,
                    %placement_error,
                    "created without a first segment; the controller will place it"
                );
                Ok(None)
            }
        }
    }

    /// Describes a stream and its materializers.
    ///
    /// # Arguments
    ///
    /// * `name` - The stream's name.
    ///
    /// # Returns
    ///
    /// The stream's record and the checkpoint of every materializer registered on it.
    ///
    /// # Errors
    ///
    /// * [`MetaError::StreamNotFound`] if no stream has this name.
    /// * [`MetaError::Unavailable`] if the metadata store cannot be reached.
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
    /// let description = client.admin().describe_stream("orders").await?;
    ///
    /// assert_eq!(description.stream.low_water_mark, SequenceNumber::FIRST);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn describe_stream(&self, name: &str) -> Result<StreamDescription, MetaError> {
        let stream = self.context.metadata.get_stream(name).await?;
        let checkpoints = self.context.metadata.list_checkpoints(stream.stream_id).await?;
        Ok(StreamDescription { stream, checkpoints })
    }

    /// Lists every stream, sorted by name.
    ///
    /// # Returns
    ///
    /// The record of every stream of the cluster. Empty if there is none.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::Unavailable`] if the metadata store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions};
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// let admin = client.admin();
    /// admin.create_stream("payments", StreamOptions::default()).await?;
    /// admin.create_stream("orders", StreamOptions::default()).await?;
    ///
    /// let streams = admin.list_streams().await?;
    ///
    /// assert_eq!(streams[0].name, "orders");
    /// assert_eq!(streams[1].name, "payments");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_streams(&self) -> Result<Vec<StreamRecord>, MetaError> {
        self.context.metadata.list_streams().await
    }

    /// Allows every record below `sequence` to be deleted, as far as the registered materializers' checkpoints and the
    /// retention policy permit.
    ///
    /// This records a request; the controller acts on it, deleting whole segments only. The position only ever moves
    /// forward: asking for an earlier one changes nothing.
    ///
    /// # Arguments
    ///
    /// * `name` - The stream's name.
    /// * `sequence` - The first sequence number the owner still needs.
    ///
    /// # Errors
    ///
    /// * [`MetaError::StreamNotFound`] if no stream has this name.
    /// * [`MetaError::Unavailable`] or [`MetaError::TooMuchContention`] if the store could not do it now.
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
    /// let admin = client.admin();
    ///
    /// admin.truncate_before("orders", SequenceNumber::new(500)).await?;
    /// admin.truncate_before("orders", SequenceNumber::new(200)).await?;
    ///
    /// let description = admin.describe_stream("orders").await?;
    /// assert_eq!(description.stream.truncate_before, Some(SequenceNumber::new(500)));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn truncate_before(&self, name: &str, sequence: SequenceNumber) -> Result<(), MetaError> {
        self.context
            .metadata
            .update_stream(name, |stream| {
                stream.truncate_before = Some(later_truncation_position(stream.truncate_before, sequence));
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Deletes a stream. Its segments are removed from the replicas and the object store by the controller.
    ///
    /// # Arguments
    ///
    /// * `name` - The stream's name. It can be used for a new stream at once; the new stream gets a new identifier.
    ///
    /// # Errors
    ///
    /// * [`MetaError::StreamNotFound`] if no stream has this name.
    /// * [`MetaError::Unavailable`] if the metadata store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions};
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("orders", StreamOptions::default()).await?;
    /// let admin = client.admin();
    ///
    /// admin.delete_stream("orders").await?;
    ///
    /// assert!(admin.describe_stream("orders").await.is_err());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_stream(&self, name: &str) -> Result<(), MetaError> {
        self.context.metadata.delete_stream(name).await?;
        Ok(())
    }

    /// Registers a materializer on a stream.
    ///
    /// From then on the log keeps every record above the materializer's checkpoint: a registered materializer that
    /// stops checkpointing pins the log until it is unregistered. Registering one that is registered already changes
    /// nothing.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The stream the materializer reads.
    /// * `materializer` - The materializer's name, unique on the stream.
    ///
    /// # Returns
    ///
    /// The materializer's checkpoint record: a new one, which starts right below the stream's low-water mark because
    /// nothing earlier can be read any more, or the existing one if it was registered before.
    ///
    /// # Errors
    ///
    /// * [`MetaError::StreamNotFound`] if no stream has this name.
    /// * [`MetaError::InvalidName`] if the materializer's name cannot be used.
    /// * [`MetaError::Unavailable`] or [`MetaError::TooMuchContention`] if the store could not do it now.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions};
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("orders", StreamOptions::default()).await?;
    /// let checkpoint = client.admin().register_materializer("orders", "pages").await?;
    ///
    /// assert_eq!(checkpoint.materializer, "pages");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn register_materializer(
        &self,
        stream_name: &str,
        materializer: &str,
    ) -> Result<CheckpointRecord, MetaError> {
        self.context
            .metadata
            .register_materializer(stream_name, materializer)
            .await
    }

    /// Unregisters a materializer, so that it no longer pins the log.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The stream the materializer was registered on.
    /// * `materializer` - The materializer's name. Unregistering one that is not registered changes nothing.
    ///
    /// # Errors
    ///
    /// * [`MetaError::StreamNotFound`] if no stream has this name.
    /// * [`MetaError::Unavailable`] or [`MetaError::TooMuchContention`] if the store could not do it now.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions};
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("orders", StreamOptions::default()).await?;
    /// let admin = client.admin();
    /// admin.register_materializer("orders", "pages").await?;
    ///
    /// admin.unregister_materializer("orders", "pages").await?;
    ///
    /// assert!(admin.describe_stream("orders").await?.checkpoints.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn unregister_materializer(&self, stream_name: &str, materializer: &str) -> Result<(), MetaError> {
        self.context
            .metadata
            .unregister_materializer(stream_name, materializer)
            .await
    }
}

/// The truncation position to record when the owner asks for `requested` and `recorded` is what the stream already
/// holds: the later of the two, so that a request never moves the position back.
fn later_truncation_position(recorded: Option<SequenceNumber>, requested: SequenceNumber) -> SequenceNumber {
    match recorded {
        Some(recorded_position) => recorded_position.max(requested),
        None => requested,
    }
}

#[cfg(test)]
#[path = "admin_test.rs"]
mod tests;
