//! The client side of the transport: a replica's registration, turned into
//! a `dyn ReplicaApi` that talks gRPC.
//!
//! # Why this file exists
//!
//! Writers, readers, the controller and repairing replicas reach a replica
//! through a `ReplicaConnector`: they give it the identity and the address
//! found in the metadata store and get back something to call. In one
//! process the connector hands out the replica itself. Across machines it
//! has to hand out something that sends each call over the network, and
//! that reports every way the network can fail as the one error callers
//! already handle: `ReplicaError::Unavailable`. That connector is here.
//!
//! # What it contains
//!
//! - [`GrpcReplicaConnector`]: the connector. One per process is enough.
//! - [`GrpcConnectorSettings`]: its settings.
//! - `GrpcReplica`: the handle it returns, an implementation of
//!   `ReplicaApi` with one small method per call.
//! - `ChannelCache`: the connections, one per address.
//!
//! # How it works
//!
//! `connect` does no I/O. It checks that the address can be dialled and
//! returns a handle; an address that cannot be dialled gets a handle that
//! fails every request, as the `ReplicaConnector` contract asks.
//!
//! The first request through a handle asks the `ChannelCache` for the
//! channel to its address. A channel is one HTTP/2 connection that is
//! opened on first use, carries any number of concurrent calls, and is
//! opened again after it breaks. The cache keeps one per address, so every
//! handle to the same replica shares a connection, however many segments
//! and streams are involved.
//!
//! Each request then goes through three steps:
//!
//! 1. The domain request is converted to its message, and the identity of
//!    the replica the handle is for is attached in the
//!    `bitacora-replica-id` header.
//! 2. The call is made. Any gRPC status other than OK becomes
//!    `ReplicaError::Unavailable`: connection refused, connection lost,
//!    a timeout, a cancellation, or the server refusing the identity.
//! 3. The response message is read back into the replica's own `Result`.
//!
//! # Invariants
//!
//! - A handle only ever reports an answer from the replica it was created
//!   for. The server checks the header on every request, so an address
//!   that came to be served by another replica yields `Unavailable`, never
//!   that other replica's answer.
//! - A request never fails with anything but the replica's own error or
//!   `Unavailable`.
//! - The cache's lock is never held across an `.await`, and a handle takes
//!   it once in its life.
//!
//! # See also
//!
//! - `server.rs`: the other end, which checks the header this file sends.
//! - `request_conversions.rs` and `response_conversions.rs`: the
//!   conversions used here.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Write;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bitacora_types::replica_api as domain;
use bitacora_types::replica_api::{ReplicaApi, ReplicaConnector, ReplicaError, UnreachableReplica};
use bitacora_types::{ReplicaId, SegmentAddress};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use crate::response_conversions::{
    append_response_from_wire, delete_segment_response_from_wire, drain_response_from_wire,
    fence_response_from_wire, list_segments_response_from_wire, read_response_from_wire,
    repair_segment_response_from_wire, seal_response_from_wire, status_response_from_wire,
    upload_segment_response_from_wire,
};
use crate::v1 as wire;
use crate::v1::replica_admin_client::ReplicaAdminClient;
use crate::v1::replica_data_client::ReplicaDataClient;
use crate::v1::replica_transfer_client::ReplicaTransferClient;
use crate::{MAX_MESSAGE_BYTES, REPLICA_ID_HEADER};

/// How a [`GrpcReplicaConnector`] behaves.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_proto::GrpcConnectorSettings;
///
/// let settings = GrpcConnectorSettings {
///     connect_timeout: Duration::from_millis(500),
/// };
/// assert!(settings.connect_timeout < GrpcConnectorSettings::default().connect_timeout);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrpcConnectorSettings {
    /// How long opening a connection to a replica may take before the
    /// request that needed it fails as unavailable. It bounds how long a
    /// request to a machine that is switched off, and so answers nothing
    /// at all, can hang. The default is two seconds.
    pub connect_timeout: Duration,
}

impl Default for GrpcConnectorSettings {
    /// The settings for a cluster on a local network: a connection that
    /// takes more than two seconds to open is not coming.
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(2),
        }
    }
}

/// Connects to replicas over gRPC.
///
/// It is the `ReplicaConnector` of a real deployment: give it to a
/// `BitacoraClient`, a controller or a replica, and each will reach
/// replicas on other machines through it. Create one per process and share
/// it, so that everything talking to the same replica shares a connection.
///
/// Connecting does no I/O and never fails. The connection to an address is
/// opened by the first request that needs it and opened again after it
/// breaks. Connections are kept for the life of the connector, one per
/// address ever connected to.
///
/// Every request through a handle carries the identity of the replica the
/// handle is for, and fails as `ReplicaError::Unavailable` unless the
/// replica with that identity answers it.
///
/// # Examples
///
/// ```no_run
/// use bitacora_proto::{GrpcConnectorSettings, GrpcReplicaConnector};
/// use bitacora_types::ReplicaId;
/// use bitacora_types::replica_api::ReplicaConnector;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let connector = GrpcReplicaConnector::new(GrpcConnectorSettings::default());
///
/// // The identity and the address come from the replica's registration.
/// let replica_id = ReplicaId::new("replica-a")?;
/// let replica = connector.connect(&replica_id, "replica-a.example:7000");
///
/// match replica.status().await {
///     Ok(status) => println!("{} free bytes", status.free_bytes),
///     Err(error) => println!("no answer: {error}"),
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct GrpcReplicaConnector {
    /// The connections, shared with every handle this connector returned.
    channels: Arc<ChannelCache>,
}

impl GrpcReplicaConnector {
    /// Creates a connector that has no connection yet.
    ///
    /// # Arguments
    ///
    /// * `settings` - How connections are opened.
    #[must_use]
    pub fn new(settings: GrpcConnectorSettings) -> Self {
        Self {
            channels: Arc::new(ChannelCache {
                settings,
                channels_by_address: Mutex::new(BTreeMap::new()),
            }),
        }
    }
}

impl ReplicaConnector for GrpcReplicaConnector {
    /// Returns a handle that sends every call to `address` over gRPC, in
    /// the name of `replica_id`.
    ///
    /// Does no I/O. An address that is not a `host:port` yields a handle
    /// whose every request fails as `ReplicaError::Unavailable`, like a
    /// replica that is down.
    ///
    /// # Arguments
    ///
    /// * `replica_id` - The identity of the replica to reach. Requests
    ///   that anything else answers fail as unavailable.
    /// * `address` - Where the replica listens, as `host:port`.
    fn connect(&self, replica_id: &ReplicaId, address: &str) -> Arc<dyn ReplicaApi> {
        let endpoint = match endpoint_of(address, &self.channels.settings) {
            Ok(endpoint) => endpoint,
            Err(invalid_address) => {
                return Arc::new(UnreachableReplica::new(format!(
                    "replica {replica_id} is registered at {address:?}, \
                     which is not a host and a port: {invalid_address}"
                )));
            }
        };
        let identity_header = MetadataValue::try_from(replica_id.as_str())
            .expect("a replica identifier holds only ASCII letters, digits, '.', '_' and '-'");
        Arc::new(GrpcReplica {
            replica_id: replica_id.clone(),
            address: address.to_owned(),
            identity_header,
            endpoint,
            channels: Arc::clone(&self.channels),
            channel: OnceLock::new(),
        })
    }
}

/// Describes how to dial `address`: plain HTTP/2, with the connector's
/// connect timeout.
///
/// # Arguments
///
/// * `address` - Where a replica listens, as `host:port`.
/// * `settings` - The connector's settings.
///
/// # Errors
///
/// Returns the transport's error if `address` is not a `host:port`. No
/// name is resolved and nothing is dialled here.
fn endpoint_of(
    address: &str,
    settings: &GrpcConnectorSettings,
) -> Result<Endpoint, tonic::transport::Error> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?;
    Ok(endpoint.connect_timeout(settings.connect_timeout))
}

/// The connections of a connector: one channel per address.
///
/// A channel multiplexes every call to its address over one HTTP/2
/// connection, and connects again by itself after a failure, so one per
/// address is all a process needs. Channels are never removed: there are
/// as many as addresses the process has ever connected to, which is
/// bounded by the replicas that were ever registered.
#[derive(Debug)]
struct ChannelCache {
    /// How connections are opened.
    settings: GrpcConnectorSettings,
    /// The channel to each address connected to so far, by that address.
    /// A `BTreeMap`, as everywhere in the workspace, for a stable order.
    channels_by_address: Mutex<BTreeMap<String, Channel>>,
}

impl ChannelCache {
    /// Returns the channel to `address`, creating it the first time.
    ///
    /// Creating a channel starts a background task but dials nothing: the
    /// connection is opened by the first call. Must be called from within
    /// a Tokio runtime, which is why handles call it from their first
    /// request and `connect` does not.
    ///
    /// # Arguments
    ///
    /// * `address` - The address the channel is cached under.
    /// * `endpoint` - How to dial that address, used if no channel exists.
    fn channel_to(&self, address: &str, endpoint: &Endpoint) -> Channel {
        // A panic while the lock is held cannot leave the map half
        // changed: an entry is either inserted or not. So a poisoned lock
        // is still good to use.
        let mut channels_by_address = self
            .channels_by_address
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(channel) = channels_by_address.get(address) {
            return channel.clone();
        }
        let channel = endpoint.connect_lazy();
        channels_by_address.insert(address.to_owned(), channel.clone());
        channel
    }
}

/// A replica reached over gRPC: what [`GrpcReplicaConnector`] returns.
struct GrpcReplica {
    /// The identity of the replica this handle is for.
    replica_id: ReplicaId,
    /// Where that replica is registered, as `host:port`. Used as the key
    /// of the channel cache and in error messages.
    address: String,
    /// `replica_id` as the value of the identity header, built once so
    /// that a request only has to clone it.
    identity_header: MetadataValue<Ascii>,
    /// How to dial `address`, should this handle be the first to need it.
    endpoint: Endpoint,
    /// The connector's connections.
    channels: Arc<ChannelCache>,
    /// The channel to `address`, taken from `channels` by the first
    /// request. Kept here so that later requests do not take the cache's
    /// lock.
    channel: OnceLock<Channel>,
}

impl GrpcReplica {
    /// Returns the channel to this replica's address. A channel is a
    /// handle to a shared connection, so cloning one per call is cheap and
    /// is what lets calls run concurrently.
    fn channel(&self) -> Channel {
        self.channel
            .get_or_init(|| self.channels.channel_to(&self.address, &self.endpoint))
            .clone()
    }

    /// Returns a client of the `ReplicaData` service on this replica.
    fn data_client(&self) -> ReplicaDataClient<Channel> {
        ReplicaDataClient::new(self.channel())
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
    }

    /// Returns a client of the `ReplicaTransfer` service on this replica.
    fn transfer_client(&self) -> ReplicaTransferClient<Channel> {
        ReplicaTransferClient::new(self.channel())
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
    }

    /// Returns a client of the `ReplicaAdmin` service on this replica.
    fn admin_client(&self) -> ReplicaAdminClient<Channel> {
        ReplicaAdminClient::new(self.channel())
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
    }

    /// Wraps `message` in a request that names this handle's replica, so
    /// that only that replica answers it.
    fn request_for_this_replica<Message>(&self, message: Message) -> Request<Message> {
        let mut request = Request::new(message);
        request
            .metadata_mut()
            .insert(REPLICA_ID_HEADER, self.identity_header.clone());
        request
    }

    /// The error for a call that ended with a gRPC status instead of an
    /// answer: whatever the status, the replica is unavailable to the
    /// caller, and the request may or may not have been applied.
    ///
    /// # Arguments
    ///
    /// * `status` - The status the call ended with.
    fn unavailable(&self, status: &Status) -> ReplicaError {
        ReplicaError::Unavailable {
            reason: format!(
                "replica {} at {} gave no answer: {}",
                self.replica_id,
                self.address,
                describe_status(status)
            ),
        }
    }
}

/// Describes a gRPC status in one line: its code, its message, and every
/// underlying cause. The causes are where the transport says what really
/// happened, such as "connection refused".
fn describe_status(status: &Status) -> String {
    let mut description = format!("{:?}: {}", status.code(), status.message());
    let mut cause = status.source();
    while let Some(error) = cause {
        // Writing to a `String` cannot fail.
        let _ = write!(description, ": {error}");
        cause = error.source();
    }
    description
}

#[async_trait]
impl ReplicaApi for GrpcReplica {
    /// Sends the append to `ReplicaData.Append`.
    async fn append(
        &self,
        request: domain::AppendRequest,
    ) -> Result<domain::AppendResponse, ReplicaError> {
        let wire_request = self.request_for_this_replica(wire::AppendRequest::from(request));
        let response = self
            .data_client()
            .append(wire_request)
            .await
            .map_err(|status| self.unavailable(&status))?;
        append_response_from_wire(response.into_inner())
    }

    /// Sends the read to `ReplicaData.Read`.
    async fn read(
        &self,
        request: domain::ReadRequest,
    ) -> Result<domain::ReadResponse, ReplicaError> {
        let wire_request = self.request_for_this_replica(wire::ReadRequest::from(request));
        let response = self
            .data_client()
            .read(wire_request)
            .await
            .map_err(|status| self.unavailable(&status))?;
        read_response_from_wire(response.into_inner())
    }

    /// Sends the fence to `ReplicaData.Fence`.
    async fn fence(
        &self,
        request: domain::FenceRequest,
    ) -> Result<domain::FenceResponse, ReplicaError> {
        let wire_request = self.request_for_this_replica(wire::FenceRequest::from(request));
        let response = self
            .data_client()
            .fence(wire_request)
            .await
            .map_err(|status| self.unavailable(&status))?;
        fence_response_from_wire(response.into_inner())
    }

    /// Sends the seal to `ReplicaData.Seal`.
    async fn seal(
        &self,
        request: domain::SealRequest,
    ) -> Result<domain::SealResponse, ReplicaError> {
        let wire_request = self.request_for_this_replica(wire::SealRequest::from(request));
        let response = self
            .data_client()
            .seal(wire_request)
            .await
            .map_err(|status| self.unavailable(&status))?;
        seal_response_from_wire(response.into_inner())
    }

    /// Sends the repair to `ReplicaTransfer.RepairSegment`.
    async fn repair_segment(
        &self,
        request: domain::RepairRequest,
    ) -> Result<domain::RepairResponse, ReplicaError> {
        let wire_request = self.request_for_this_replica(wire::RepairSegmentRequest::from(request));
        let response = self
            .transfer_client()
            .repair_segment(wire_request)
            .await
            .map_err(|status| self.unavailable(&status))?;
        repair_segment_response_from_wire(response.into_inner())
    }

    /// Sends the upload to `ReplicaTransfer.UploadSegment`.
    async fn upload_segment(
        &self,
        request: domain::UploadRequest,
    ) -> Result<domain::UploadResponse, ReplicaError> {
        let wire_request = self.request_for_this_replica(wire::UploadSegmentRequest::from(request));
        let response = self
            .transfer_client()
            .upload_segment(wire_request)
            .await
            .map_err(|status| self.unavailable(&status))?;
        upload_segment_response_from_wire(response.into_inner())
    }

    /// Sends the deletion to `ReplicaAdmin.DeleteSegment`.
    async fn delete_segment(&self, segment: SegmentAddress) -> Result<(), ReplicaError> {
        let wire_request = self.request_for_this_replica(wire::DeleteSegmentRequest::from(segment));
        let response = self
            .admin_client()
            .delete_segment(wire_request)
            .await
            .map_err(|status| self.unavailable(&status))?;
        delete_segment_response_from_wire(response.into_inner())
    }

    /// Sends the question to `ReplicaAdmin.ListSegments`.
    async fn list_segments(&self) -> Result<Vec<domain::SegmentStatus>, ReplicaError> {
        let wire_request = self.request_for_this_replica(wire::ListSegmentsRequest {});
        let response = self
            .admin_client()
            .list_segments(wire_request)
            .await
            .map_err(|status| self.unavailable(&status))?;
        list_segments_response_from_wire(response.into_inner())
    }

    /// Sends the question to `ReplicaAdmin.Status`.
    async fn status(&self) -> Result<domain::ReplicaStatus, ReplicaError> {
        let wire_request = self.request_for_this_replica(wire::StatusRequest {});
        let response = self
            .admin_client()
            .status(wire_request)
            .await
            .map_err(|status| self.unavailable(&status))?;
        status_response_from_wire(response.into_inner())
    }

    /// Sends the order to `ReplicaAdmin.Drain`.
    async fn drain(&self) -> Result<(), ReplicaError> {
        let wire_request = self.request_for_this_replica(wire::DrainRequest {});
        let response = self
            .admin_client()
            .drain(wire_request)
            .await
            .map_err(|status| self.unavailable(&status))?;
        drain_response_from_wire(response.into_inner())
    }
}

#[cfg(test)]
#[path = "client_test.rs"]
mod tests;
