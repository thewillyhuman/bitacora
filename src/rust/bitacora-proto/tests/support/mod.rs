//! What the integration tests of the transport share: a gRPC server on the
//! loopback interface.
//!
//! # Why this file exists
//!
//! Every integration test needs a replica that is really served over gRPC
//! and an address to reach it at. Ports must not be fixed, or two tests
//! running at once would collide, so the operating system picks one.
//!
//! # How it works
//!
//! [`serve_on_loopback`] binds `127.0.0.1:0`, reads back the port it was
//! given, and starts `serve_replica` in a task of the test's runtime. The
//! task is never asked to stop: it ends when the test's runtime does, at
//! the end of the test. A test that needs to stop its server calls
//! `serve_replica` itself.

use std::future;
use std::sync::Arc;

use bitacora_proto::serve_replica;
use bitacora_types::ReplicaId;
use bitacora_types::replica_api::ReplicaApi;
use tokio::net::TcpListener;

/// Serves `replica` over gRPC on a free loopback port, for the rest of the
/// test.
///
/// # Arguments
///
/// * `replica` - What answers the calls.
/// * `replica_id` - The identity the server answers for: requests that
///   name another replica are refused.
///
/// # Returns
///
/// The address the server listens on, as `127.0.0.1:port`. By the time
/// this returns the socket is bound, so a client can connect at once.
pub async fn serve_on_loopback(replica: Arc<dyn ReplicaApi>, replica_id: ReplicaId) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the loopback interface has a free port");
    let address = listener
        .local_addr()
        .expect("a bound socket has an address")
        .to_string();
    tokio::spawn(serve_replica(replica, replica_id, listener, future::pending()));
    address
}
