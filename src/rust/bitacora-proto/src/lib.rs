//! The bitacora wire protocol and its gRPC transport.
//!
//! # Why this crate exists
//!
//! Writers, readers, the controller and the replicas themselves only ever
//! hold a `dyn ReplicaApi` (`bitacora-types`). In one process that is the
//! replica itself. Across machines something has to carry each call over
//! the network and bring the answer back without changing its meaning. This
//! crate is that something, and the only crate of the workspace that opens
//! a socket or knows about gRPC: the domain crates stay free of both.
//!
//! # What it contains
//!
//! - [`v1`]: the messages and gRPC stubs generated from
//!   `src/proto/bitacora/v1/replica.proto`, the language-neutral source of
//!   truth of the protocol.
//! - [`GrpcReplicaConnector`]: the client side. It turns a replica's
//!   registration into a `dyn ReplicaApi` that talks gRPC.
//! - [`serve_replica`]: the server side. It serves any `dyn ReplicaApi` on a
//!   listening socket.
//! - [`MalformedMessage`]: why a message cannot be converted to a domain
//!   value. The conversions themselves are `From` and `TryFrom`
//!   implementations between the [`v1`] messages and the domain types.
//! - [`REPLICA_ID_HEADER`] and [`MAX_MESSAGE_BYTES`]: the two constants of
//!   the protocol that are not in the protocol file.
//!
//! # How it works
//!
//! `build.rs` compiles the protocol file into the [`v1`] module. The
//! conversion modules translate between those generated messages and the
//! domain types, in both directions. The client and the server are thin
//! adapters on top: convert, call, convert back.
//!
//! Three rules shape every call (docs/decisions/0002-grpc-transport.md):
//!
//! - **Identity.** Every request names the replica it is meant for in the
//!   [`REPLICA_ID_HEADER`] metadata header, and a server refuses a request
//!   that is not for it. A node whose disk was replaced returns at the same
//!   address under a new identity, and it must never answer in the name of
//!   the old one.
//! - **Refusals are answers.** A `ReplicaError` travels inside the response
//!   message, field by field, so every variant reaches the caller exactly
//!   as the replica returned it.
//! - **Anything else is unavailability.** A gRPC status other than OK, or
//!   an answer that cannot be read, becomes `ReplicaError::Unavailable`:
//!   the request may or may not have been applied.
//!
//! # Invariants
//!
//! - A conversion never panics on input from the network. A malformed
//!   request is answered with `ReplicaError::InvalidRequest`; a malformed
//!   answer is read as `ReplicaError::Unavailable`.
//! - Converting a domain value to its message and back yields the same
//!   value.
//! - No domain crate depends on this one, on `tonic` or on `prost`.
//!
//! # See also
//!
//! - `conversions.rs`: values shared by several messages, and what makes a
//!   message malformed.
//! - `error_conversions.rs`: `ReplicaError` to and from its message.
//! - `request_conversions.rs` and `response_conversions.rs`: one conversion
//!   per call and direction.
//! - `client.rs`: [`GrpcReplicaConnector`].
//! - `server.rs`: [`serve_replica`].

#![warn(clippy::missing_docs_in_private_items)]

mod client;
mod conversions;
mod error_conversions;
mod request_conversions;
mod response_conversions;
mod server;

/// The messages and gRPC stubs of protocol version 1, generated at build
/// time from `src/proto/bitacora/v1/replica.proto`. The protocol file
/// documents every message and field; that documentation is repeated on the
/// generated items.
// Generated code is not written to this workspace's lints, and it is not
// ours to fix.
#[allow(
    missing_docs,
    clippy::all,
    clippy::pedantic,
    clippy::missing_docs_in_private_items
)]
pub mod v1 {
    tonic::include_proto!("bitacora.v1");
}

pub use client::{GrpcConnectorSettings, GrpcReplicaConnector};
pub use conversions::MalformedMessage;
pub use server::{ServeError, serve_replica};

/// The name of the metadata header that carries the identity of the replica
/// a request is meant for. Every request must carry it, and a server refuses
/// any request whose value is not its own replica identifier.
pub const REPLICA_ID_HEADER: &str = "bitacora-replica-id";

/// The largest message either side sends or accepts, in bytes.
///
/// gRPC accepts 4 MiB by default, which is exactly the largest record
/// payload and so too small for the batch frame around it. A batch frame is
/// at most 8 MiB plus its header; 16 MiB leaves room for one, with the other
/// fields of its message, in both directions.
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
