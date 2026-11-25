//! The server side of the transport: any `dyn ReplicaApi`, served over gRPC.
//!
//! # Why this file exists
//!
//! A replica implements `ReplicaApi` and knows nothing about the network.
//! To be reached from another machine it needs a gRPC server in front of
//! it that changes nothing about what it answers. That server is here, and
//! it is deliberately thin: everything a replica decides stays in the
//! replica, so the in-process replica that the simulator tests and the one
//! behind a socket behave the same.
//!
//! # What it contains
//!
//! - [`serve_replica`]: serves a replica on a listening socket until told
//!   to stop.
//! - [`ServeError`]: why the server stopped on its own.
//! - `ReplicaService`: the implementation of the three generated service
//!   traits, one small method per call.
//! - `IdentityCheck` and `check_identity`: the refusal of requests meant
//!   for another replica.
//!
//! # How it works
//!
//! Every request passes two steps before it reaches the replica:
//!
//! 1. `IdentityCheck` runs first, for every call of every service. It
//!    compares the `bitacora-replica-id` header with the identity this
//!    server was started with. A request without the header, or for
//!    another replica, is refused with the gRPC status
//!    `FAILED_PRECONDITION` and never reaches the replica.
//! 2. The request message is converted to the domain request. If it is
//!    malformed the replica is not called, and the answer is
//!    `ReplicaError::InvalidRequest`.
//!
//! The replica's `Result` is then written into the response message. A
//! refusal by the replica is an answer, so the gRPC status of a call that
//! reached this file's methods is always OK.
//!
//! A read is the one request the server adjusts: it lowers the size cap to
//! `LARGEST_READ_CAP_BYTES`, so that the answer always fits in a message.
//!
//! # Invariants
//!
//! - A request that does not name this server's replica never reaches the
//!   replica. A node whose disk was replaced serves under a new identity,
//!   and must not answer for the copies the old one held.
//! - A handler does nothing but convert and await the replica. Dropping it
//!   when a client goes away is safe, because the replica applies every
//!   change in a task of its own (docs/spec.md, section 17).
//! - No handler holds a lock or any state of its own.
//!
//! # See also
//!
//! - `client.rs`: the other end, which sends the header this file checks.
//! - `request_conversions.rs` and `response_conversions.rs`: the
//!   conversions used here.

use std::sync::Arc;

use bitacora_types::replica_api as domain;
use bitacora_types::replica_api::{ReplicaApi, ReplicaError};
use bitacora_types::{ReplicaId, SegmentAddress};
use thiserror::Error;
use tokio::net::TcpListener;
use tonic::metadata::MetadataMap;
use tonic::service::{Interceptor, InterceptorLayer};
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};

use crate::conversions::MalformedMessage;
use crate::response_conversions::{
    append_response_to_wire, delete_segment_response_to_wire, drain_response_to_wire, fence_response_to_wire,
    list_segments_response_to_wire, read_response_to_wire, repair_segment_response_to_wire, seal_response_to_wire,
    status_response_to_wire, upload_segment_response_to_wire,
};
use crate::v1 as wire;
use crate::v1::replica_admin_server::{ReplicaAdmin, ReplicaAdminServer};
use crate::v1::replica_data_server::{ReplicaData, ReplicaDataServer};
use crate::v1::replica_transfer_server::{ReplicaTransfer, ReplicaTransferServer};
use crate::{MAX_MESSAGE_BYTES, REPLICA_ID_HEADER};

/// The largest soft cap on the size of a read's answer, in bytes. A read
/// that asks for more gets this much.
///
/// A replica stops adding batches once the cap is reached, so an answer
/// exceeds its cap by less than one batch frame, and a frame is at most
/// 8 MiB plus its header. A quarter of the message limit, 4 MiB, plus one
/// frame stays well under [`MAX_MESSAGE_BYTES`]. A test holds this constant
/// to that sum.
const LARGEST_READ_CAP_BYTES: usize = MAX_MESSAGE_BYTES / 4;

/// Why a replica's gRPC server stopped before it was asked to.
///
/// The cause is the transport's own error, available through
/// [`std::error::Error::source`]: typically the listening socket failed.
#[derive(Debug, Error)]
#[error("the replica's gRPC server failed")]
pub struct ServeError {
    /// What the gRPC transport reported.
    #[source]
    source: tonic::transport::Error,
}

/// Serves a replica over gRPC until `shutdown` completes.
///
/// Registers the three services of the protocol (`ReplicaData`,
/// `ReplicaTransfer`, `ReplicaAdmin`) on `listener` and answers every call
/// by calling `replica`. Requests that do not carry `replica_id` in the
/// [`REPLICA_ID_HEADER`](crate::REPLICA_ID_HEADER) header are refused
/// before they reach `replica`. Messages of up to
/// [`MAX_MESSAGE_BYTES`](crate::MAX_MESSAGE_BYTES) are accepted and sent.
/// Accepted connections have `TCP_NODELAY` set, so a small append is not
/// held back waiting for more data to send.
///
/// When `shutdown` completes the server stops accepting connections, lets
/// the calls in flight finish, and returns.
///
/// # Arguments
///
/// * `replica` - What answers the calls: a replica, or anything else that
///   implements `ReplicaApi`.
/// * `replica_id` - The identity of `replica`. It must be the identity of
///   the data directory behind it, the one it registers under: it is what
///   keeps a rebuilt node from answering for its predecessor.
/// * `listener` - A bound socket to accept connections on. The caller
///   binds it, so the caller chooses the address and learns the port.
/// * `shutdown` - Completes when the server should stop.
///
/// # Errors
///
/// Returns [`ServeError`] if the transport fails while serving, for
/// example because the listening socket breaks.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use bitacora_proto::serve_replica;
/// use bitacora_types::ReplicaId;
/// use bitacora_types::replica_api::{ReplicaApi, UnreachableReplica};
/// use tokio::net::TcpListener;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // A real server passes its `bitacora_replica::Replica` here.
/// let replica: Arc<dyn ReplicaApi> = Arc::new(UnreachableReplica::new("an example"));
/// let replica_id = ReplicaId::new("replica-a")?;
/// let listener = TcpListener::bind("127.0.0.1:0").await?;
/// println!("serving on {}", listener.local_addr()?);
///
/// // A real server waits for a termination signal here.
/// let shutdown = async {};
/// serve_replica(replica, replica_id, listener, shutdown).await?;
/// # Ok(())
/// # }
/// ```
pub async fn serve_replica(
    replica: Arc<dyn ReplicaApi>,
    replica_id: ReplicaId,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), ServeError> {
    let service = ReplicaService { replica };
    let data_server = ReplicaDataServer::new(service.clone())
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
    let transfer_server = ReplicaTransferServer::new(service.clone())
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
    let admin_server = ReplicaAdminServer::new(service)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
    let identity_check = IdentityCheck {
        own_replica_id: replica_id,
    };
    // Without TCP_NODELAY the kernel may hold a small answer back for tens
    // of milliseconds, hoping to send it together with more data.
    let incoming_connections = TcpIncoming::from(listener).with_nodelay(Some(true));

    Server::builder()
        .layer(InterceptorLayer::new(identity_check))
        .add_service(data_server)
        .add_service(transfer_server)
        .add_service(admin_server)
        .serve_with_incoming_shutdown(incoming_connections, shutdown)
        .await
        .map_err(|source| ServeError { source })
}

/// Refuses every request that is not meant for this server's replica. It
/// runs before any service, for every call.
#[derive(Clone)]
struct IdentityCheck {
    /// The identity of the replica this server answers for.
    own_replica_id: ReplicaId,
}

impl Interceptor for IdentityCheck {
    /// Lets `request` through if it names this server's replica.
    ///
    /// # Errors
    ///
    /// Returns the status of [`check_identity`] otherwise.
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        check_identity(&self.own_replica_id, request.metadata())?;
        Ok(request)
    }
}

/// Checks that a request is meant for the replica `own_replica_id`.
///
/// The request must carry the `bitacora-replica-id` header, and every value
/// of it must be `own_replica_id`.
///
/// # Arguments
///
/// * `own_replica_id` - The identity of the replica behind this server.
/// * `request_metadata` - The metadata headers of the request.
///
/// # Errors
///
/// Returns a `FAILED_PRECONDITION` status if the header is missing or
/// names another replica. The client reads any status as unavailability,
/// which is what the caller must see: the replica it asked for is not
/// here.
fn check_identity(own_replica_id: &ReplicaId, request_metadata: &MetadataMap) -> Result<(), Status> {
    let mut names_this_replica = false;
    for requested_replica_id in request_metadata.get_all(REPLICA_ID_HEADER) {
        if requested_replica_id.as_bytes() != own_replica_id.as_str().as_bytes() {
            return Err(Status::failed_precondition(format!(
                "this is replica {own_replica_id}, not {requested_replica_id:?}"
            )));
        }
        names_this_replica = true;
    }
    if !names_this_replica {
        return Err(Status::failed_precondition(format!(
            "the request names no replica in the `{REPLICA_ID_HEADER}` header; \
             this is replica {own_replica_id}"
        )));
    }
    Ok(())
}

/// The error a replica is not bothered with: the answer to a request that
/// cannot be converted to a domain request.
fn invalid_request(malformed_message: &MalformedMessage) -> ReplicaError {
    ReplicaError::InvalidRequest {
        reason: malformed_message.to_string(),
    }
}

/// Implements the three generated service traits by calling a replica.
#[derive(Clone)]
struct ReplicaService {
    /// What answers the calls. Shared by the three services and by every
    /// call in flight.
    replica: Arc<dyn ReplicaApi>,
}

#[tonic::async_trait]
impl ReplicaData for ReplicaService {
    /// Answers `ReplicaData.Append` with `ReplicaApi::append`.
    async fn append(&self, request: Request<wire::AppendRequest>) -> Result<Response<wire::AppendResponse>, Status> {
        let result = match domain::AppendRequest::try_from(request.into_inner()) {
            Ok(append_request) => self.replica.append(append_request).await,
            Err(malformed_message) => Err(invalid_request(&malformed_message)),
        };
        Ok(Response::new(append_response_to_wire(result)))
    }

    /// Answers `ReplicaData.Read` with `ReplicaApi::read`, after lowering
    /// the size cap to [`LARGEST_READ_CAP_BYTES`].
    async fn read(&self, request: Request<wire::ReadRequest>) -> Result<Response<wire::ReadResponse>, Status> {
        let result = match domain::ReadRequest::try_from(request.into_inner()) {
            Ok(read_request) => {
                let capped_request = domain::ReadRequest {
                    max_bytes: read_request.max_bytes.min(LARGEST_READ_CAP_BYTES),
                    ..read_request
                };
                self.replica.read(capped_request).await
            }
            Err(malformed_message) => Err(invalid_request(&malformed_message)),
        };
        Ok(Response::new(read_response_to_wire(result)))
    }

    /// Answers `ReplicaData.Fence` with `ReplicaApi::fence`.
    async fn fence(&self, request: Request<wire::FenceRequest>) -> Result<Response<wire::FenceResponse>, Status> {
        let result = match domain::FenceRequest::try_from(request.into_inner()) {
            Ok(fence_request) => self.replica.fence(fence_request).await,
            Err(malformed_message) => Err(invalid_request(&malformed_message)),
        };
        Ok(Response::new(fence_response_to_wire(result)))
    }

    /// Answers `ReplicaData.Seal` with `ReplicaApi::seal`.
    async fn seal(&self, request: Request<wire::SealRequest>) -> Result<Response<wire::SealResponse>, Status> {
        let result = match domain::SealRequest::try_from(request.into_inner()) {
            Ok(seal_request) => self.replica.seal(seal_request).await,
            Err(malformed_message) => Err(invalid_request(&malformed_message)),
        };
        Ok(Response::new(seal_response_to_wire(result)))
    }
}

#[tonic::async_trait]
impl ReplicaTransfer for ReplicaService {
    /// Answers `ReplicaTransfer.RepairSegment` with
    /// `ReplicaApi::repair_segment`.
    async fn repair_segment(
        &self,
        request: Request<wire::RepairSegmentRequest>,
    ) -> Result<Response<wire::RepairSegmentResponse>, Status> {
        let result = match domain::RepairRequest::try_from(request.into_inner()) {
            Ok(repair_request) => self.replica.repair_segment(repair_request).await,
            Err(malformed_message) => Err(invalid_request(&malformed_message)),
        };
        Ok(Response::new(repair_segment_response_to_wire(result)))
    }

    /// Answers `ReplicaTransfer.UploadSegment` with
    /// `ReplicaApi::upload_segment`.
    async fn upload_segment(
        &self,
        request: Request<wire::UploadSegmentRequest>,
    ) -> Result<Response<wire::UploadSegmentResponse>, Status> {
        let result = match domain::UploadRequest::try_from(request.into_inner()) {
            Ok(upload_request) => self.replica.upload_segment(upload_request).await,
            Err(malformed_message) => Err(invalid_request(&malformed_message)),
        };
        Ok(Response::new(upload_segment_response_to_wire(result)))
    }
}

#[tonic::async_trait]
impl ReplicaAdmin for ReplicaService {
    /// Answers `ReplicaAdmin.Status` with `ReplicaApi::status`.
    async fn status(&self, _request: Request<wire::StatusRequest>) -> Result<Response<wire::StatusResponse>, Status> {
        let result = self.replica.status().await;
        Ok(Response::new(status_response_to_wire(result)))
    }

    /// Answers `ReplicaAdmin.Drain` with `ReplicaApi::drain`.
    async fn drain(&self, _request: Request<wire::DrainRequest>) -> Result<Response<wire::DrainResponse>, Status> {
        let result = self.replica.drain().await;
        Ok(Response::new(drain_response_to_wire(result)))
    }

    /// Answers `ReplicaAdmin.ListSegments` with `ReplicaApi::list_segments`.
    async fn list_segments(
        &self,
        _request: Request<wire::ListSegmentsRequest>,
    ) -> Result<Response<wire::ListSegmentsResponse>, Status> {
        let result = self.replica.list_segments().await;
        Ok(Response::new(list_segments_response_to_wire(result)))
    }

    /// Answers `ReplicaAdmin.DeleteSegment` with
    /// `ReplicaApi::delete_segment`.
    async fn delete_segment(
        &self,
        request: Request<wire::DeleteSegmentRequest>,
    ) -> Result<Response<wire::DeleteSegmentResponse>, Status> {
        let result = match SegmentAddress::try_from(request.into_inner()) {
            Ok(segment) => self.replica.delete_segment(segment).await,
            Err(malformed_message) => Err(invalid_request(&malformed_message)),
        };
        Ok(Response::new(delete_segment_response_to_wire(result)))
    }
}

#[cfg(test)]
#[path = "server_test.rs"]
mod tests;
